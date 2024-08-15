/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/serializers/PrestoSerializer.h"
#include <folly/Random.h>
#include <gtest/gtest.h>
#include <vector>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/time/Timer.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

struct SerializeStats {
  int64_t actualSize{0};
  int64_t estimatedSize{0};
};

class PrestoSerializerTest
    : public ::testing::TestWithParam<common::CompressionKind>,
      public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  }

  void sanityCheckEstimateSerializedSize(const RowVectorPtr& rowVector) {
    Scratch scratch;
    const auto numRows = rowVector->size();

    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }

    std::vector<vector_size_t> rowSizes(numRows, 0);
    std::vector<vector_size_t*> rawRowSizes(numRows);
    for (auto i = 0; i < numRows; i++) {
      rawRowSizes[i] = &rowSizes[i];
    }
    serde_->estimateSerializedSize(
        rowVector.get(),
        folly::Range(rows.data(), numRows),
        rawRowSizes.data(),
        scratch);
  }

  serializer::presto::PrestoVectorSerde::PrestoOptions getParamSerdeOptions(
      const serializer::presto::PrestoVectorSerde::PrestoOptions*
          serdeOptions) {
    const bool useLosslessTimestamp =
        serdeOptions == nullptr ? false : serdeOptions->useLosslessTimestamp;
    common::CompressionKind kind = GetParam();
    const bool nullsFirst =
        serdeOptions == nullptr ? false : serdeOptions->nullsFirst;
    serializer::presto::PrestoVectorSerde::PrestoOptions paramOptions{
        useLosslessTimestamp, kind, nullsFirst};

    return paramOptions;
  }

  SerializeStats serialize(
      const RowVectorPtr& rowVector,
      std::ostream* output,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions,
      std::optional<folly::Range<const IndexRange*>> indexRanges = std::nullopt,
      std::optional<folly::Range<const vector_size_t*>> rows = std::nullopt,
      std::unique_ptr<IterativeVectorSerializer>* reuseSerializer = nullptr,
      std::unique_ptr<StreamArena>* reuseArena = nullptr) {
    auto streamInitialSize = output->tellp();
    sanityCheckEstimateSerializedSize(rowVector);

    std::unique_ptr<StreamArena> arena;
    auto rowType = asRowType(rowVector->type());
    auto numRows = rowVector->size();
    auto paramOptions = getParamSerdeOptions(serdeOptions);
    std::unique_ptr<IterativeVectorSerializer> serializer;
    if (reuseSerializer && *reuseSerializer) {
      arena = std::move(*reuseArena);
      serializer = std::move(*reuseSerializer);
      serializer->clear();
    } else {
      arena = std::make_unique<StreamArena>(pool_.get());
      serializer = serde_->createIterativeSerializer(
          rowType, numRows, arena.get(), &paramOptions);
    }
    vector_size_t sizeEstimate = 0;

    Scratch scratch;
    if (indexRanges.has_value()) {
      raw_vector<vector_size_t*> sizes(indexRanges.value().size());
      std::fill(sizes.begin(), sizes.end(), &sizeEstimate);
      serde_->estimateSerializedSize(
          rowVector.get(), indexRanges.value(), sizes.data(), scratch);
      serializer->append(rowVector, indexRanges.value(), scratch);
    } else if (rows.has_value()) {
      raw_vector<vector_size_t*> sizes(rows.value().size());
      std::fill(sizes.begin(), sizes.end(), &sizeEstimate);
      serde_->estimateSerializedSize(
          rowVector.get(), rows.value(), sizes.data(), scratch);
      serializer->append(rowVector, rows.value(), scratch);
    } else {
      vector_size_t* sizes = &sizeEstimate;
      IndexRange range{0, rowVector->size()};
      serde_->estimateSerializedSize(
          rowVector.get(),
          folly::Range<const IndexRange*>(&range, 1),
          &sizes,
          scratch);
      serializer->append(rowVector);
    }
    auto size = serializer->maxSerializedSize();
    auto estimatePct = (100.0 * sizeEstimate) / static_cast<float>(size + 1);
    LOG(INFO) << "Size=" << size << " estimate=" << sizeEstimate << " "
              << estimatePct << "%";
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    OStreamOutputStream out(output, &listener);
    serializer->flush(&out);
    if (paramOptions.compressionKind == common::CompressionKind_NONE) {
      EXPECT_EQ(size, out.tellp() - streamInitialSize);
    } else {
      EXPECT_GE(size, out.tellp() - streamInitialSize);
    }
    if (reuseSerializer) {
      *reuseArena = std::move(arena);
      *reuseSerializer = std::move(serializer);
    }
    return {static_cast<int64_t>(size), sizeEstimate};
  }

  std::unique_ptr<ByteInputStream> toByteStream(const std::string& input) {
    ByteRange byteRange{
        reinterpret_cast<uint8_t*>(const_cast<char*>(input.data())),
        (int32_t)input.length(),
        0};
    return std::make_unique<BufferInputStream>(
        std::vector<ByteRange>{{byteRange}});
  }

  void validateLexer(
      const std::string& input,
      const serializer::presto::PrestoVectorSerde::PrestoOptions&
          paramOptions) {
    if (paramOptions.useLosslessTimestamp ||
        paramOptions.compressionKind !=
            common::CompressionKind::CompressionKind_NONE ||
        paramOptions.nullsFirst) {
      // Unsupported options
      return;
    }
    std::vector<serializer::presto::PrestoVectorSerde::Token> tokens;
    const auto status = serializer::presto::PrestoVectorSerde::lex(
        input, tokens, &paramOptions);
    EXPECT_TRUE(status.ok()) << status.message();
    size_t tokenLengthSum = 0;
    for (auto const& token : tokens) {
      tokenLengthSum += token.length;
    }
    for (auto const& token : tokens) {
      // The lexer should not produce empty tokens
      EXPECT_NE(token.length, 0);
    }
    for (size_t i = 1; i < tokens.size(); ++i) {
      // The lexer should merge consecutive tokens of the same type
      EXPECT_NE(tokens[i].tokenType, tokens[i - 1].tokenType);
    }
    EXPECT_EQ(tokenLengthSum, input.size());
  }

  RowVectorPtr deserialize(
      const RowTypePtr& rowType,
      const std::string& input,
      const serializer::presto::PrestoVectorSerde::PrestoOptions*
          serdeOptions) {
    auto byteStream = toByteStream(input);
    auto paramOptions = getParamSerdeOptions(serdeOptions);
    validateLexer(input, paramOptions);
    RowVectorPtr result;
    serde_->deserialize(
        byteStream.get(), pool_.get(), rowType, &result, 0, &paramOptions);
    return result;
  }

  RowVectorPtr makeTestVector(vector_size_t size) {
    auto a =
        makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; });
    auto b = makeFlatVector<double>(
        size, [](vector_size_t row) { return row * 0.1; });
    auto c = makeFlatVector<std::string>(size, [](vector_size_t row) {
      return row % 2 == 0 ? "LaaaaaaaaargeString" : "inlineStr";
    });

    std::vector<VectorPtr> childVectors = {a, b, c};

    return makeRowVector(childVectors);
  }

  RowVectorPtr wrapChildren(const RowVectorPtr& row) {
    auto children = row->children();
    std::vector<VectorPtr> newChildren = children;
    auto indices = makeIndices(row->size(), [](auto row) { return row; });
    for (auto& child : newChildren) {
      child = BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indices, row->size(), child);
    }
    return makeRowVector(newChildren);
  }

  void testRoundTrip(
      VectorPtr vector,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions =
          nullptr) {
    std::unique_ptr<IterativeVectorSerializer> reuseSerializer;
    std::unique_ptr<StreamArena> reuseArena;
    auto rowVector = makeRowVector({vector});
    std::ostringstream out;
    serialize(
        rowVector,
        &out,
        serdeOptions,
        std::nullopt,
        std::nullopt,
        &reuseSerializer,
        &reuseArena);

    auto rowType = asRowType(rowVector->type());
    auto deserialized = deserialize(rowType, out.str(), serdeOptions);
    assertEqualVectors(deserialized, rowVector);

    if (rowVector->size() < 3) {
      return;
    }

    // Split input into 3 batches. Serialize each separately. Then, deserialize
    // all into one vector.
    auto splits = split(rowVector, 3);
    std::vector<std::string> serialized;
    for (const auto& split : splits) {
      std::ostringstream out;
      serialize(
          split,
          &out,
          serdeOptions,
          std::nullopt,
          std::nullopt,
          &reuseSerializer,
          &reuseArena);
      serialized.push_back(out.str());
    }

    auto paramOptions = getParamSerdeOptions(serdeOptions);
    RowVectorPtr result;
    vector_size_t offset = 0;
    for (auto i = 0; i < serialized.size(); ++i) {
      auto byteStream = toByteStream(serialized[i]);
      serde_->deserialize(
          byteStream.get(),
          pool_.get(),
          rowType,
          &result,
          offset,
          &paramOptions);
      offset = result->size();
    }

    assertEqualVectors(result, rowVector);

    // Serialize the vector with even and odd rows in different partitions.
    auto even =
        makeIndices(rowVector->size() / 2, [&](auto row) { return row * 2; });
    auto odd = makeIndices(
        (rowVector->size() - 1) / 2, [&](auto row) { return (row * 2) + 1; });
    testSerializeRows(
        rowVector, even, serdeOptions, &reuseSerializer, &reuseArena);
    auto oddStats = testSerializeRows(
        rowVector, odd, serdeOptions, &reuseSerializer, &reuseArena);
    auto wrappedRowVector = wrapChildren(rowVector);
    auto wrappedStats = testSerializeRows(
        wrappedRowVector, odd, serdeOptions, &reuseSerializer, &reuseArena);
    EXPECT_EQ(oddStats.estimatedSize, wrappedStats.estimatedSize);
    // The second serialization may come out smaller if encoding is better.
    EXPECT_GE(oddStats.actualSize, wrappedStats.actualSize);
  }

  void testLexer(
      VectorPtr vector,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions =
          nullptr) {
    auto rowVector = makeRowVector({vector});
    std::ostringstream out;
    serialize(rowVector, &out, serdeOptions);
    validateLexer(out.str(), getParamSerdeOptions(serdeOptions));
  }

  SerializeStats testSerializeRows(
      const RowVectorPtr& rowVector,
      BufferPtr indices,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions,
      std::unique_ptr<IterativeVectorSerializer>* reuseSerializer,
      std::unique_ptr<StreamArena>* reuseArena) {
    std::ostringstream out;
    auto rows = folly::Range<const vector_size_t*>(
        indices->as<vector_size_t>(), indices->size() / sizeof(vector_size_t));
    auto stats = serialize(
        rowVector,
        &out,
        serdeOptions,
        std::nullopt,
        rows,
        reuseSerializer,
        reuseArena);

    auto rowType = asRowType(rowVector->type());
    auto deserialized = deserialize(rowType, out.str(), serdeOptions);
    assertEqualVectors(
        deserialized,
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr),
            indices,
            indices->size() / sizeof(vector_size_t),
            rowVector));
    return stats;
  }

  void assertEqualEncoding(
      const RowVectorPtr& expected,
      const RowVectorPtr& actual,
      // If true, we allow the encodings of actual and expected to differ if a
      // dictionary was flattened.
      bool allowFlatteningDictionaries = false) {
    for (auto i = 0; i < expected->childrenSize(); ++i) {
      auto expectedEncoding = expected->childAt(i)->encoding();
      auto actualEncoding = actual->childAt(i)->encoding();

      if (allowFlatteningDictionaries &&
          actualEncoding == VectorEncoding::Simple::FLAT &&
          expectedEncoding == VectorEncoding::Simple::DICTIONARY) {
        continue;
      }

      VELOX_CHECK_EQ(actualEncoding, expectedEncoding);

      if (expectedEncoding == VectorEncoding::Simple::ROW) {
        assertEqualEncoding(
            std::dynamic_pointer_cast<RowVector>(expected->childAt(i)),
            std::dynamic_pointer_cast<RowVector>(actual->childAt(i)),
            allowFlatteningDictionaries);
      }
    }
  }

  void verifySerializedEncodedData(
      const RowVectorPtr& original,
      const std::string& serialized,
      bool allowFlatteningDictionaries,
      const serializer::presto::PrestoVectorSerde::PrestoOptions*
          serdeOptions) {
    auto rowType = asRowType(original->type());
    auto deserialized = deserialize(rowType, serialized, serdeOptions);

    assertEqualVectors(original, deserialized);
    // Dictionaries may get flattened depending on the nature of the data.
    assertEqualEncoding(original, deserialized, allowFlatteningDictionaries);

    // Deserialize 3 times while appending to a single vector.
    auto paramOptions = getParamSerdeOptions(serdeOptions);
    RowVectorPtr result;
    vector_size_t offset = 0;
    for (auto i = 0; i < 3; ++i) {
      auto byteStream = toByteStream(serialized);
      serde_->deserialize(
          byteStream.get(),
          pool_.get(),
          rowType,
          &result,
          offset,
          &paramOptions);
      offset = result->size();
    }

    auto expected =
        BaseVector::create(original->type(), original->size() * 3, pool());
    for (auto i = 0; i < 3; ++i) {
      expected->copy(original.get(), original->size() * i, 0, original->size());
    }

    assertEqualVectors(expected, result);
  }

  void makePermutations(
      const std::vector<VectorPtr>& vectors,
      int32_t size,
      std::vector<VectorPtr>& items,
      std::vector<std::vector<VectorPtr>>& result) {
    if (size == items.size()) {
      result.push_back(items);
      return;
    }
    for (const auto& vector : vectors) {
      items.push_back(vector);
      makePermutations(vectors, size, items, result);
      items.pop_back();
    }
  }

  // tests combining encodings in serialization and deserialization. Serializes
  // each of 'vectors' with encoding, then reads them back into a single vector.
  void testEncodedConcatenation(
      std::vector<VectorPtr>& vectors,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions =
          nullptr) {
    std::vector<std::string> pieces;
    std::vector<std::string> reusedPieces;
    auto rowType = ROW({{"f", vectors[0]->type()}});
    auto concatenation = BaseVector::create(rowType, 0, pool_.get());
    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto paramOptions = getParamSerdeOptions(serdeOptions);
    auto serializer = serde_->createIterativeSerializer(
        rowType, 10, arena.get(), &paramOptions);
    auto reusedSerializer = serde_->createIterativeSerializer(
        rowType, 10, arena.get(), &paramOptions);

    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    std::ostringstream allOut;
    OStreamOutputStream allOutStream(&allOut, &listener);
    serializer->flush(&allOutStream);

    auto allDeserialized = deserialize(rowType, allOut.str(), &paramOptions);
    assertEqualVectors(allDeserialized, concatenation);
    RowVectorPtr deserialized =
        BaseVector::create<RowVector>(rowType, 0, pool_.get());
    for (auto pieceIdx = 0; pieceIdx < pieces.size(); ++pieceIdx) {
      auto piece = pieces[pieceIdx];
      auto byteStream = toByteStream(piece);
      serde_->deserialize(
          byteStream.get(),
          pool_.get(),
          rowType,
          &deserialized,
          deserialized->size(),
          &paramOptions);

      RowVectorPtr single =
          BaseVector::create<RowVector>(rowType, 0, pool_.get());
      byteStream = toByteStream(piece);
      serde_->deserialize(
          byteStream.get(), pool_.get(), rowType, &single, 0, &paramOptions);
      assertEqualVectors(single->childAt(0), vectors[pieceIdx]);

      RowVectorPtr single2 =
          BaseVector::create<RowVector>(rowType, 0, pool_.get());
      byteStream = toByteStream(reusedPieces[pieceIdx]);
      serde_->deserialize(
          byteStream.get(), pool_.get(), rowType, &single2, 0, &paramOptions);
      assertEqualVectors(single2->childAt(0), vectors[pieceIdx]);
    }
    assertEqualVectors(concatenation, deserialized);
  }
  int64_t randInt(int64_t min, int64_t max) {
    return min + folly::Random::rand64(rng_) % (max - min);
  }

  void randomOptions(VectorFuzzer::Options& options, bool isFirst) {
    options.nullRatio = randInt(1, 10) < 3 ? 0.0 : randInt(1, 10) / 10.0;
    options.stringLength = randInt(1, 100);
    options.stringVariableLength = true;
    options.containerLength = randInt(1, 50);
    options.containerVariableLength = true;
    options.complexElementsMaxSize = 20000;
    options.maxConstantContainerSize = 2;
    options.normalizeMapKeys = randInt(0, 20) < 16;
    if (isFirst) {
      options.timestampPrecision =
          static_cast<VectorFuzzer::Options::TimestampPrecision>(randInt(0, 3));
    }
    options.allowLazyVector = false;
  }

  void serializeBatch(
      const RowVectorPtr& rowVector,
      std::ostream* output,
      const serializer::presto::PrestoVectorSerde::PrestoOptions*
          serdeOptions) {
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    OStreamOutputStream out(output, &listener);
    auto paramOptions = getParamSerdeOptions(serdeOptions);

    auto serializer = serde_->createBatchSerializer(pool_.get(), &paramOptions);
    serializer->serialize(rowVector, &out);
  }

  void testBatchVectorSerializerRoundTrip(
      const RowVectorPtr& data,
      bool allowFlatteningDictionaries = false,
      const serializer::presto::PrestoVectorSerde::PrestoOptions* serdeOptions =
          nullptr) {
    std::ostringstream out;
    serializeBatch(data, &out, serdeOptions);
    const auto serialized = out.str();

    verifySerializedEncodedData(
        data, serialized, allowFlatteningDictionaries, serdeOptions);
  }

  RowVectorPtr encodingsTestVector() {
    // String is variable length, so this ensures the data isn't flattened.
    auto baseNoNulls = makeFlatVector<std::string>({"a", "b", "c", "d"});
    auto baseWithNulls =
        makeNullableFlatVector<std::string>({"a", std::nullopt, "b", "c"});
    auto baseArray =
        makeArrayVector<int32_t>({{1, 2, 3}, {}, {4, 5}, {6, 7, 8, 9, 10}});
    auto indices = makeIndices(8, [](auto row) { return row / 2; });

    return makeRowVector({
        BaseVector::wrapInDictionary(nullptr, indices, 8, baseNoNulls),
        BaseVector::wrapInDictionary(nullptr, indices, 8, baseWithNulls),
        BaseVector::wrapInDictionary(nullptr, indices, 8, baseArray),
        BaseVector::createConstant(INTEGER(), 123, 8, pool_.get()),
        BaseVector::createNullConstant(VARCHAR(), 8, pool_.get()),
        BaseVector::wrapInConstant(8, 1, baseArray),
        BaseVector::wrapInConstant(8, 2, baseArray),
        makeRowVector({
            BaseVector::wrapInDictionary(nullptr, indices, 8, baseNoNulls),
            BaseVector::wrapInDictionary(nullptr, indices, 8, baseWithNulls),
            BaseVector::wrapInDictionary(nullptr, indices, 8, baseArray),
            BaseVector::createConstant(INTEGER(), 123, 8, pool_.get()),
            BaseVector::createNullConstant(VARCHAR(), 8, pool_.get()),
            BaseVector::wrapInConstant(8, 1, baseArray),
            BaseVector::wrapInConstant(8, 2, baseArray),
            makeRowVector({
                BaseVector::wrapInDictionary(
                    nullptr, indices, 8, baseWithNulls),
                BaseVector::createConstant(INTEGER(), 123, 8, pool_.get()),
                BaseVector::wrapInConstant(8, 2, baseArray),
            }),
        }),
    });
  }

  RowVectorPtr encodingsArrayElementsTestVector() {
    auto baseNoNulls = makeFlatVector<int64_t>({1, 2, 3, 4});
    auto baseWithNulls =
        makeNullableFlatVector<int32_t>({1, std::nullopt, 2, 3});
    auto baseArray =
        makeArrayVector<int32_t>({{1, 2, 3}, {}, {4, 5}, {6, 7, 8, 9, 10}});
    auto elementIndices = makeIndices(16, [](auto row) { return row / 4; });
    std::vector<vector_size_t> offsets{0, 2, 4, 6, 8, 10, 12, 14, 16};

    return makeRowVector({
        makeArrayVector(
            offsets,
            BaseVector::wrapInDictionary(
                nullptr, elementIndices, 16, baseNoNulls)),
        makeArrayVector(
            offsets,
            BaseVector::wrapInDictionary(
                nullptr, elementIndices, 16, baseWithNulls)),
        makeArrayVector(
            offsets,
            BaseVector::wrapInDictionary(
                nullptr, elementIndices, 16, baseArray)),
        makeArrayVector(
            offsets,
            BaseVector::createConstant(INTEGER(), 123, 16, pool_.get())),
        makeArrayVector(
            offsets,
            BaseVector::createNullConstant(VARCHAR(), 16, pool_.get())),
        makeArrayVector(offsets, BaseVector::wrapInConstant(16, 1, baseArray)),
        makeRowVector({
            makeArrayVector(
                offsets,
                BaseVector::wrapInDictionary(
                    nullptr, elementIndices, 16, baseNoNulls)),
            makeArrayVector(
                offsets,
                BaseVector::wrapInDictionary(
                    nullptr, elementIndices, 16, baseWithNulls)),
            makeArrayVector(
                offsets,
                BaseVector::wrapInDictionary(
                    nullptr, elementIndices, 16, baseArray)),
            makeArrayVector(
                offsets,
                BaseVector::createConstant(INTEGER(), 123, 16, pool_.get())),
            makeArrayVector(
                offsets,
                BaseVector::createNullConstant(VARCHAR(), 16, pool_.get())),
            makeArrayVector(
                offsets, BaseVector::wrapInConstant(16, 1, baseArray)),
        }),
    });
  }

  RowVectorPtr encodingsMapValuesTestVector() {
    auto baseNoNulls = makeFlatVector<int64_t>({1, 2, 3, 4});
    auto baseWithNulls =
        makeNullableFlatVector<int32_t>({1, std::nullopt, 2, 3});
    auto baseArray =
        makeArrayVector<int32_t>({{1, 2, 3}, {}, {4, 5}, {6, 7, 8, 9, 10}});
    auto valueIndices = makeIndices(16, [](auto row) { return row / 4; });
    std::vector<vector_size_t> offsets{0, 2, 4, 6, 8, 10, 12, 14, 16};
    auto mapKeys = makeFlatVector<int32_t>(16, [](auto row) { return row; });

    return makeRowVector({
        makeMapVector(
            offsets,
            mapKeys,
            BaseVector::wrapInDictionary(
                nullptr, valueIndices, 16, baseNoNulls)),
        makeMapVector(
            offsets,
            mapKeys,
            BaseVector::wrapInDictionary(
                nullptr, valueIndices, 16, baseWithNulls)),
        makeMapVector(
            offsets,
            mapKeys,
            BaseVector::wrapInDictionary(nullptr, valueIndices, 16, baseArray)),
        makeMapVector(
            offsets,
            mapKeys,
            BaseVector::createConstant(INTEGER(), 123, 16, pool_.get())),
        makeMapVector(
            offsets,
            mapKeys,
            BaseVector::createNullConstant(VARCHAR(), 16, pool_.get())),
        makeMapVector(
            offsets, mapKeys, BaseVector::wrapInConstant(16, 1, baseArray)),
        makeRowVector({
            makeMapVector(
                offsets,
                mapKeys,
                BaseVector::wrapInDictionary(
                    nullptr, valueIndices, 16, baseNoNulls)),
            makeMapVector(
                offsets,
                mapKeys,
                BaseVector::wrapInDictionary(
                    nullptr, valueIndices, 16, baseWithNulls)),
            makeMapVector(
                offsets,
                mapKeys,
                BaseVector::wrapInDictionary(
                    nullptr, valueIndices, 16, baseArray)),
            makeMapVector(
                offsets,
                mapKeys,
                BaseVector::createConstant(INTEGER(), 123, 16, pool_.get())),
            makeMapVector(
                offsets,
                mapKeys,
                BaseVector::createNullConstant(VARCHAR(), 16, pool_.get())),
            makeMapVector(
                offsets, mapKeys, BaseVector::wrapInConstant(16, 1, baseArray)),
        }),
    });
  }

  void testMinimalDictionaryEncoding(
      vector_size_t numRows,
      vector_size_t alphabetSize) {
    // This factor is used to ensure we have some repetition in the dictionary
    // in each of the cases to ensure the serializer doesn't flatten the data.
    auto factor = alphabetSize <= numRows ? 2 : alphabetSize / numRows * 2;
    // String is variable length, so this ensures the data isn't flattened.
    auto base = makeFlatVector<std::string>(
        alphabetSize, [](vector_size_t row) { return fmt::format("{}", row); });
    auto evenIndices = makeIndices(numRows, [alphabetSize, factor](auto row) {
      return (row * factor) % alphabetSize;
    });
    auto oddIndices = makeIndices(numRows, [alphabetSize, factor](auto row) {
      return (row * factor + 1) % alphabetSize;
    });
    auto prefixIndices = makeIndices(numRows, [alphabetSize, factor](auto row) {
      return row % (alphabetSize / factor);
    });
    auto suffixIndices = makeIndices(numRows, [alphabetSize, factor](auto row) {
      return row % (alphabetSize / factor) +
          (factor - 1) * (alphabetSize / factor);
    });

    auto rows = makeRowVector({
        BaseVector::wrapInDictionary(nullptr, evenIndices, numRows, base),
        BaseVector::wrapInDictionary(nullptr, oddIndices, numRows, base),
        BaseVector::wrapInDictionary(nullptr, prefixIndices, numRows, base),
        BaseVector::wrapInDictionary(nullptr, suffixIndices, numRows, base),
    });

    std::ostringstream out;
    serializeBatch(rows, &out, /*serdeOptions=*/nullptr);
    const auto serialized = out.str();

    auto rowType = asRowType(rows->type());
    auto deserialized =
        deserialize(rowType, serialized, /*serdeOptions=*/nullptr);

    assertEqualVectors(rows, deserialized);
    assertEqualEncoding(rows, deserialized);

    ASSERT_EQ(
        deserialized->childAt(0)
            ->as<DictionaryVector<StringView>>()
            ->valueVector()
            ->size(),
        alphabetSize / factor);
    ASSERT_EQ(
        deserialized->childAt(1)
            ->as<DictionaryVector<StringView>>()
            ->valueVector()
            ->size(),
        alphabetSize / factor);
    ASSERT_EQ(
        deserialized->childAt(2)
            ->as<DictionaryVector<StringView>>()
            ->valueVector()
            ->size(),
        alphabetSize / factor);
    ASSERT_EQ(
        deserialized->childAt(3)
            ->as<DictionaryVector<StringView>>()
            ->valueVector()
            ->size(),
        alphabetSize / factor);
  }

  RowVectorPtr makeEmptyTestVector() {
    return makeRowVector(
        {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
        {makeFlatVector<bool>({}),
         makeFlatVector<int8_t>({}),
         makeFlatVector<int16_t>({}),
         makeFlatVector<int32_t>({}),
         makeFlatVector<int64_t>({}),
         makeFlatVector<float>({}),
         makeFlatVector<double>({}),
         makeFlatVector<StringView>({}),
         makeFlatVector<Timestamp>({}),
         makeRowVector(
             {"1", "2"},
             {makeFlatVector<StringView>({}), makeFlatVector<int32_t>({})}),
         makeArrayVector<int32_t>({}),
         makeMapVector<StringView, int32_t>({})});
  }

  std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_;
  folly::Random::DefaultGenerator rng_;
};

TEST_P(PrestoSerializerTest, basic) {
  vector_size_t numRows = 1'000;
  auto rowVector = makeTestVector(numRows);
  testRoundTrip(rowVector);
}

/// Test serialization of a dictionary vector that adds nulls to the base
/// vector.
TEST_P(PrestoSerializerTest, dictionaryWithExtraNulls) {
  vector_size_t size = 1'000;

  auto base = makeFlatVector<int64_t>(10, [](auto row) { return row; });

  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, i % 5 == 0);
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    if (i % 5 != 0) {
      rawIndices[i] = i % 10;
    }
  }

  auto dictionary = BaseVector::wrapInDictionary(nulls, indices, size, base);
  testRoundTrip(dictionary);
}

TEST_P(PrestoSerializerTest, emptyPage) {
  auto rowVector = makeEmptyTestVector();

  std::ostringstream out;
  serialize(rowVector, &out, nullptr);

  auto rowType = asRowType(rowVector->type());
  auto deserialized = deserialize(rowType, out.str(), nullptr);
  assertEqualVectors(deserialized, rowVector);
}

TEST_P(PrestoSerializerTest, serializeNoRowsSelected) {
  std::ostringstream out;
  facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
  OStreamOutputStream output(&out, &listener);
  auto paramOptions = getParamSerdeOptions(nullptr);
  auto arena = std::make_unique<StreamArena>(pool_.get());
  auto rowVector = makeRowVector(
      {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
      {makeFlatVector<bool>({true, false, true, false}),
       makeFlatVector<int8_t>({1, 2, 3, 4}),
       makeFlatVector<int16_t>({5, 6, 7, 8}),
       makeFlatVector<int32_t>({9, 10, 11, 12}),
       makeFlatVector<int64_t>({13, 14, 15, 16}),
       makeFlatVector<float>({17.0, 18.0, 19.0, 20.0}),
       makeFlatVector<double>({21.0, 22.0, 23.0, 24.0}),
       makeFlatVector<StringView>({"25", "26", "27", "28"}),
       makeFlatVector<Timestamp>({{29, 30}, {32, 32}, {33, 34}, {35, 36}}),
       makeRowVector(
           {"1", "2"},
           {makeFlatVector<StringView>({"37", "38", "39", "40"}),
            makeFlatVector<int32_t>({41, 42, 43, 44})}),
       makeArrayVector<int32_t>({{45, 46}, {47, 48}, {49, 50}, {51, 52}}),
       makeMapVector<StringView, int32_t>(
           {{{"53", 54}, {"55", 56}},
            {{"57", 58}, {"59", 60}},
            {{"61", 62}, {"63", 64}},
            {{"65", 66}, {"67", 68}}})});
  const IndexRange noRows{0, 0};

  auto serializer = serde_->createIterativeSerializer(
      asRowType(rowVector->type()),
      rowVector->size(),
      arena.get(),
      &paramOptions);

  serializer->append(rowVector, folly::Range(&noRows, 1));
  serializer->flush(&output);

  auto expected = makeEmptyTestVector();
  auto rowType = asRowType(expected->type());
  auto deserialized = deserialize(rowType, out.str(), nullptr);
  assertEqualVectors(deserialized, expected);
}

TEST_P(PrestoSerializerTest, emptyArray) {
  auto arrayVector = makeArrayVector<int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; });

  testRoundTrip(arrayVector);
}

TEST_P(PrestoSerializerTest, emptyMap) {
  auto mapVector = makeMapVector<int32_t, int32_t>(
      1'000,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row * 2; });

  testRoundTrip(mapVector);
}

TEST_P(PrestoSerializerTest, timestampWithTimeZone) {
  auto timestamp = makeFlatVector<int64_t>(
      100,
      [](auto row) { return pack(10'000 + row, row % 37); },
      /* isNullAt */ nullptr,
      TIMESTAMP_WITH_TIME_ZONE());

  testRoundTrip(timestamp);

  // Add some nulls.
  for (auto i = 0; i < 100; i += 7) {
    timestamp->setNull(i, true);
  }
  testRoundTrip(timestamp);
}

TEST_P(PrestoSerializerTest, intervalDayTime) {
  auto vector = makeFlatVector<int64_t>(
      100,
      [](auto row) { return row + folly::Random::rand32(); },
      nullptr, // nullAt
      INTERVAL_DAY_TIME());

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 100; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

TEST_P(PrestoSerializerTest, unknown) {
  // Verify vectors of UNKNOWN type. Also verifies a special case where a
  // vector, not of UNKNOWN type and with all nulls is serialized as an UNKNOWN
  // type having BYTE_ARRAY encoding.
  auto testAllNullSerializedAsUnknown = [&](VectorPtr vector,
                                            TypePtr outputType) {
    auto rowVector = makeRowVector({vector});
    auto expected = makeRowVector(
        {BaseVector::createNullConstant(outputType, vector->size(), pool())});
    std::ostringstream out;
    serialize(rowVector, &out, nullptr);

    auto rowType = asRowType(expected->type());
    auto deserialized = deserialize(rowType, out.str(), nullptr);
    assertEqualVectors(expected, deserialized);

    if (rowVector->size() < 3) {
      return;
    }

    // Split input into 3 batches. Serialize each separately. Then, deserialize
    // all into one vector.
    auto splits = split(rowVector, 3);
    std::vector<std::string> serialized;
    for (const auto& split : splits) {
      std::ostringstream oss;
      serialize(split, &oss, nullptr);
      serialized.push_back(oss.str());
    }

    auto paramOptions = getParamSerdeOptions(nullptr);
    RowVectorPtr result;
    vector_size_t offset = 0;
    for (auto i = 0; i < serialized.size(); ++i) {
      auto byteStream = toByteStream(serialized[i]);
      serde_->deserialize(
          byteStream.get(),
          pool_.get(),
          rowType,
          &result,
          offset,
          &paramOptions);
      offset = result->size();
    }

    assertEqualVectors(expected, result);
  };

  const vector_size_t size = 123;
  auto constantVector =
      BaseVector::createNullConstant(UNKNOWN(), size, pool_.get());
  testRoundTrip(constantVector);
  testAllNullSerializedAsUnknown(constantVector, BIGINT());

  auto flatVector = BaseVector::create(UNKNOWN(), size, pool_.get());
  for (auto i = 0; i < size; i++) {
    flatVector->setNull(i, true);
  }
  testRoundTrip(flatVector);
  testAllNullSerializedAsUnknown(flatVector, BIGINT());
}

TEST_P(PrestoSerializerTest, multiPage) {
  std::ostringstream out;
  std::vector<RowVectorPtr> testVectors;
  // Note: Page of size 1250 is a slight increase in size that initiates string
  // buffer re-use.
  for (int size : {1234, 1250, 538, 2408}) {
    auto vec = makeTestVector(size);
    serialize(vec, &out, nullptr);
    testVectors.push_back(std::move(vec));
  }

  auto bytes = out.str();

  auto rowType = asRowType(testVectors[0]->type());
  auto byteStream = toByteStream(bytes);

  RowVectorPtr deserialized;
  auto paramOptions = getParamSerdeOptions(nullptr);

  for (int i = 0; i < testVectors.size(); i++) {
    RowVectorPtr& vec = testVectors[i];
    serde_->deserialize(
        byteStream.get(),
        pool_.get(),
        rowType,
        &deserialized,
        0,
        &paramOptions);
    if (i < testVectors.size() - 1) {
      ASSERT_FALSE(byteStream->atEnd());
    } else {
      ASSERT_TRUE(byteStream->atEnd());
    }
    assertEqualVectors(deserialized, vec);
    deserialized->validate({});
  }
}

TEST_P(PrestoSerializerTest, timestampWithNanosecondPrecision) {
  // Verify that nanosecond precision is preserved when the right options are
  // passed to the serde.
  const serializer::presto::PrestoVectorSerde::PrestoOptions
      kUseLosslessTimestampOptions(
          true, common::CompressionKind::CompressionKind_NONE);
  auto timestamp = makeFlatVector<Timestamp>(
      {Timestamp{0, 0},
       Timestamp{12, 0},
       Timestamp{0, 17'123'456},
       Timestamp{1, 17'123'456},
       Timestamp{-1, 17'123'456}});
  testRoundTrip(timestamp, &kUseLosslessTimestampOptions);

  // Verify that precision is lost when no option is passed to the serde.
  auto timestampMillis = makeFlatVector<Timestamp>(
      {Timestamp{0, 0},
       Timestamp{12, 0},
       Timestamp{0, 17'000'000},
       Timestamp{1, 17'000'000},
       Timestamp{-1, 17'000'000}});
  auto inputRowVector = makeRowVector({timestamp});
  auto expectedOutputWithLostPrecision = makeRowVector({timestampMillis});
  std::ostringstream out;
  serialize(inputRowVector, &out, {});
  auto rowType = asRowType(inputRowVector->type());
  auto deserialized = deserialize(rowType, out.str(), {});
  assertEqualVectors(deserialized, expectedOutputWithLostPrecision);
}

TEST_P(PrestoSerializerTest, longDecimal) {
  std::vector<int128_t> decimalValues(102);
  decimalValues[0] = DecimalUtil::kLongDecimalMin;
  for (int row = 1; row < 101; row++) {
    decimalValues[row] = row - 50;
  }
  decimalValues[101] = DecimalUtil::kLongDecimalMax;
  auto vector = makeFlatVector<int128_t>(decimalValues, DECIMAL(20, 5));

  testRoundTrip(vector);

  // Add some nulls.
  for (auto i = 0; i < 102; i += 7) {
    vector->setNull(i, true);
  }
  testRoundTrip(vector);
}

// Test that hierarchically encoded columns (rows) have their encodings
// preserved by the PrestoBatchVectorSerializer.
TEST_P(PrestoSerializerTest, encodingsBatchVectorSerializer) {
  testBatchVectorSerializerRoundTrip(encodingsTestVector());
}

// Test that array elements have their encodings preserved by the
// PrestoBatchVectorSerializer.
TEST_P(PrestoSerializerTest, encodingsArrayElementsBatchVectorSerializer) {
  testBatchVectorSerializerRoundTrip(encodingsArrayElementsTestVector());
}

// Test that map values have their encodings preserved by the
// PrestoBatchVectorSerializer.
TEST_P(PrestoSerializerTest, encodingsMapValuesBatchVectorSerializer) {
  testBatchVectorSerializerRoundTrip(encodingsMapValuesTestVector());
}

// Test that dictionary encoding is preserved and that only the minimum number
// of entries from the alphabet are serialized.
TEST_P(PrestoSerializerTest, minimalDictionaryEncodings) {
  // Dictionary same size as rows.
  testMinimalDictionaryEncoding(32, 32);
  // Dictionary smaller than rows.
  testMinimalDictionaryEncoding(32, 16);
  // Dictionary larger than rows.
  testMinimalDictionaryEncoding(32, 64);
}

// Test that dictionary encoded inputs are flattened in cases where it doesn't
// help.
TEST_P(PrestoSerializerTest, dictionaryEncodingTurnedOff) {
  auto smallIntBase =
      makeFlatVector<int16_t>(32, [](vector_size_t row) { return row; });
  auto intBase =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto bigintBase =
      makeFlatVector<int64_t>(32, [](vector_size_t row) { return row; });
  auto stringBase = makeFlatVector<std::string>(
      32, [](vector_size_t row) { return fmt::format("{}", row); });
  auto oneIndex = makeIndices(32, [](auto) { return 0; });
  auto quarterIndices = makeIndices(32, [](auto row) { return row % 8; });
  auto allButOneIndices = makeIndices(32, [](auto row) { return row % 31; });
  auto allIndices = makeIndices(32, [](auto row) { return row; });

  auto rows = makeRowVector({
      // Even though these have very effective dictionary encoding, they should
      // be flattened because their types are too small.
      BaseVector::wrapInDictionary(nullptr, oneIndex, 32, smallIntBase),
      BaseVector::wrapInDictionary(nullptr, oneIndex, 32, intBase),
      // These should keep dictionary encoding.
      BaseVector::wrapInDictionary(nullptr, oneIndex, 32, bigintBase),
      BaseVector::wrapInDictionary(nullptr, quarterIndices, 32, bigintBase),
      // These should be flattened because dictionary encoding isn't giving
      // enough benefit to outweigh the cost.
      BaseVector::wrapInDictionary(nullptr, allButOneIndices, 32, bigintBase),
      BaseVector::wrapInDictionary(nullptr, allIndices, 32, bigintBase),
      // These should keep dictionary encoding because strings are variable
      // length.
      BaseVector::wrapInDictionary(nullptr, oneIndex, 32, stringBase),
      BaseVector::wrapInDictionary(nullptr, quarterIndices, 32, stringBase),
      BaseVector::wrapInDictionary(nullptr, allButOneIndices, 32, stringBase),
      // This should be flattened because the alphabet is the same as the
      // flattened vector.
      BaseVector::wrapInDictionary(nullptr, allIndices, 32, stringBase),
  });

  std::ostringstream out;
  serializeBatch(rows, &out, /*serdeOptions=*/nullptr);
  const auto serialized = out.str();

  auto rowType = asRowType(rows->type());
  auto deserialized =
      deserialize(rowType, serialized, /*serdeOptions=*/nullptr);

  assertEqualVectors(rows, deserialized);

  // smallInt + one index
  ASSERT_EQ(deserialized->childAt(0)->encoding(), VectorEncoding::Simple::FLAT);
  // int + one index
  ASSERT_EQ(deserialized->childAt(1)->encoding(), VectorEncoding::Simple::FLAT);
  // bigint + one index
  ASSERT_EQ(
      deserialized->childAt(2)->encoding(), VectorEncoding::Simple::DICTIONARY);
  // bigint + quarter indices
  ASSERT_EQ(
      deserialized->childAt(3)->encoding(), VectorEncoding::Simple::DICTIONARY);
  // bigint + all but one indices
  ASSERT_EQ(deserialized->childAt(4)->encoding(), VectorEncoding::Simple::FLAT);
  // bigint + all indices
  ASSERT_EQ(deserialized->childAt(5)->encoding(), VectorEncoding::Simple::FLAT);
  // string + one index
  ASSERT_EQ(
      deserialized->childAt(6)->encoding(), VectorEncoding::Simple::DICTIONARY);
  // string + quarter indices
  ASSERT_EQ(
      deserialized->childAt(7)->encoding(), VectorEncoding::Simple::DICTIONARY);
  // string + all but one indices
  ASSERT_EQ(
      deserialized->childAt(8)->encoding(), VectorEncoding::Simple::DICTIONARY);
  // string + all indices
  ASSERT_EQ(deserialized->childAt(9)->encoding(), VectorEncoding::Simple::FLAT);
}

TEST_P(PrestoSerializerTest, emptyVectorBatchVectorSerializer) {
  // Serialize an empty RowVector.
  auto rowVector = makeEmptyTestVector();

  std::ostringstream out;
  serializeBatch(rowVector, &out, nullptr);

  auto rowType = asRowType(rowVector->type());
  auto deserialized = deserialize(rowType, out.str(), nullptr);
  assertEqualVectors(deserialized, rowVector);
}

TEST_P(PrestoSerializerTest, serializeNoRowsSelectedBatchVectorSerializer) {
  std::ostringstream out;
  facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
  OStreamOutputStream output(&out, &listener);
  auto paramOptions = getParamSerdeOptions(nullptr);
  auto serializer = serde_->createBatchSerializer(pool_.get(), &paramOptions);
  auto rowVector = makeRowVector(
      {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
      {makeFlatVector<bool>({true, false, true, false}),
       makeFlatVector<int8_t>({1, 2, 3, 4}),
       makeFlatVector<int16_t>({5, 6, 7, 8}),
       makeFlatVector<int32_t>({9, 10, 11, 12}),
       makeFlatVector<int64_t>({13, 14, 15, 16}),
       makeFlatVector<float>({17.0, 18.0, 19.0, 20.0}),
       makeFlatVector<double>({21.0, 22.0, 23.0, 24.0}),
       makeFlatVector<StringView>({"25", "26", "27", "28"}),
       makeFlatVector<Timestamp>({{29, 30}, {32, 32}, {33, 34}, {35, 36}}),
       makeRowVector(
           {"1", "2"},
           {makeFlatVector<StringView>({"37", "38", "39", "40"}),
            makeFlatVector<int32_t>({41, 42, 43, 44})}),
       makeArrayVector<int32_t>({{45, 46}, {47, 48}, {49, 50}, {51, 52}}),
       makeMapVector<StringView, int32_t>(
           {{{"53", 54}, {"55", 56}},
            {{"57", 58}, {"59", 60}},
            {{"61", 62}, {"63", 64}},
            {{"65", 66}, {"67", 68}}})});
  const IndexRange noRows{0, 0};

  serializer->serialize(rowVector, folly::Range(&noRows, 1), &output);

  auto expected = makeEmptyTestVector();
  auto rowType = asRowType(expected->type());
  auto deserialized = deserialize(rowType, out.str(), nullptr);
  assertEqualVectors(deserialized, expected);
}

TEST_P(PrestoSerializerTest, lazy) {
  constexpr int kSize = 1000;
  auto rowVector = makeTestVector(kSize);
  auto lazyVector = std::make_shared<LazyVector>(
      pool_.get(),
      rowVector->type(),
      kSize,
      std::make_unique<SimpleVectorLoader>([&](auto) { return rowVector; }));
  testRoundTrip(lazyVector);
}

TEST_P(PrestoSerializerTest, ioBufRoundTrip) {
  VectorFuzzer::Options opts;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  opts.nullRatio = 0.1;
  VectorFuzzer fuzzer(opts, pool_.get());

  const size_t numRounds = 200;

  for (size_t i = 0; i < numRounds; ++i) {
    auto rowType = fuzzer.randRowType();
    auto inputRowVector = fuzzer.fuzzInputRow(rowType);
    auto outputRowVector = IOBufToRowVector(
        rowVectorToIOBuf(inputRowVector, *pool_), rowType, *pool_);

    assertEqualVectors(inputRowVector, outputRowVector);
  }
}

TEST_P(PrestoSerializerTest, roundTrip) {
  VectorFuzzer::Options opts;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  opts.nullRatio = 0.1;
  VectorFuzzer fuzzer(opts, pool_.get());
  VectorFuzzer::Options nonNullOpts;
  nonNullOpts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  nonNullOpts.nullRatio = 0;
  VectorFuzzer nonNullFuzzer(nonNullOpts, pool_.get());

  const size_t numRounds = 100;

  for (size_t i = 0; i < numRounds; ++i) {
    auto rowType = fuzzer.randRowType();
    auto inputRowVector = (i % 2 == 0) ? fuzzer.fuzzInputRow(rowType)
                                       : nonNullFuzzer.fuzzInputRow(rowType);
    serializer::presto::PrestoVectorSerde::PrestoOptions prestoOpts;
    // Test every 2/4 with struct nulls first.
    prestoOpts.nullsFirst = i % 4 < 2;
    testRoundTrip(inputRowVector, &prestoOpts);
  }
}

TEST_P(PrestoSerializerTest, encodedRoundtrip) {
  VectorFuzzer::Options opts;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  opts.nullRatio = 0.1;
  opts.dictionaryHasNulls = false;
  VectorFuzzer fuzzer(opts, pool_.get());

  const size_t numRounds = 200;

  for (size_t i = 0; i < numRounds; ++i) {
    auto rowType = fuzzer.randRowType();
    auto inputRowVector = fuzzer.fuzzInputRow(rowType);
    serializer::presto::PrestoVectorSerde::PrestoOptions serdeOpts;
    serdeOpts.nullsFirst = i % 2 == 0;
    testBatchVectorSerializerRoundTrip(inputRowVector, true, &serdeOpts);
  }
}

TEST_P(PrestoSerializerTest, encodedConcatenation) {
  // Slow test, run only for no compression.
  if (GetParam() != common::CompressionKind::CompressionKind_NONE) {
    return;
  }

  std::vector<TypePtr> types = {ROW({{"s0", VARCHAR()}}), VARCHAR()};
  VectorFuzzer::Options nonNullOpts{
      .nullRatio = 0, .dictionaryHasNulls = false};
  VectorFuzzer nonNullFuzzer(nonNullOpts, pool_.get());
  VectorFuzzer::Options nullOpts{.nullRatio = 1, .dictionaryHasNulls = false};
  VectorFuzzer nullFuzzer(nullOpts, pool_.get());
  VectorFuzzer::Options mixedOpts{
      .nullRatio = 0.5, .dictionaryHasNulls = false};
  VectorFuzzer mixedFuzzer(mixedOpts, pool_.get());

  for (const auto& type : types) {
    auto flatNull = nullFuzzer.fuzzFlat(type, 12);
    auto flatNonNull = nonNullFuzzer.fuzzFlat(type, 13);

    std::vector<VectorPtr> vectors = {
        nullFuzzer.fuzzConstant(type, 10),
        nonNullFuzzer.fuzzConstant(type, 11),
        flatNonNull,
        flatNull,
        nonNullFuzzer.fuzzDictionary(flatNonNull),
        nonNullFuzzer.fuzzDictionary(flatNull),
        nullFuzzer.fuzzDictionary(flatNull),
    };

    if (type->isRow()) {
      auto& rowType = type->as<TypeKind::ROW>();
      auto row = makeRowVector(
          {rowType.nameOf(0)},
          {nonNullFuzzer.fuzzConstant(rowType.childAt(0), 14)});
      row->setNull(2, true);
      row->setNull(10, true);
      vectors.push_back(row);
    }
    std::vector<std::vector<VectorPtr>> permutations;
    std::vector<VectorPtr> temp;
    makePermutations(vectors, 4, temp, permutations);
    for (auto i = 0; i < permutations.size(); ++i) {
      serializer::presto::PrestoVectorSerde::PrestoOptions opts;
      opts.nullsFirst = i % 2 == 0;

      testEncodedConcatenation(permutations[i], &opts);
    }
  }
}

TEST_P(PrestoSerializerTest, encodedConcatenation2) {
  // Slow test, run only for no compression.
  if (GetParam() != common::CompressionKind::CompressionKind_NONE) {
    return;
  }
  VectorFuzzer::Options options;
  VectorFuzzer fuzzer(options, pool_.get());
  for (auto nthType = 0; nthType < 20; ++nthType) {
    auto type = (fuzzer.randRowType());

    std::vector<VectorPtr> vectors;
    for (auto nth = 0; nth < 3; ++nth) {
      randomOptions(options, 0 == nth);
      fuzzer.setOptions(options);
      vectors.push_back(fuzzer.fuzzInputRow(type));
    }
    std::vector<std::vector<VectorPtr>> permutations;
    std::vector<VectorPtr> temp;
    makePermutations(vectors, 3, temp, permutations);
    for (auto i = 0; i < permutations.size(); ++i) {
      serializer::presto::PrestoVectorSerde::PrestoOptions opts;
      opts.useLosslessTimestamp = true;
      opts.nullsFirst = i % 2 == 0;

      testEncodedConcatenation(permutations[i], &opts);
    }
  }
}

TEST_P(PrestoSerializerTest, emptyArrayOfRowVector) {
  // The value of nullCount_ + nonNullCount_ of the inner RowVector is 0.
  auto arrayOfRow = makeArrayOfRowVector(ROW({UNKNOWN()}), {{}});
  testRoundTrip(arrayOfRow);
}

TEST_P(PrestoSerializerTest, typeMismatch) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"}),
  });

  std::ostringstream out;
  serialize(data, &out, nullptr);
  auto serialized = out.str();

  // Too many columns to deserialize.
  VELOX_ASSERT_THROW(
      deserialize(ROW({BIGINT(), VARCHAR(), BOOLEAN()}), serialized, nullptr),
      "Number of columns in serialized data doesn't match "
      "number of columns requested for deserialization");

  // TMore columns in serialization than in type.
  if (GetParam() == common::CompressionKind_NONE) {
    // No throw.
    deserialize(ROW({BIGINT()}), serialized, nullptr);
  } else {
    VELOX_ASSERT_THROW(
        deserialize(ROW({BIGINT()}), serialized, nullptr),
        "Number of columns in serialized data doesn't match "
        "number of columns requested for deserialization");
  }

  // Wrong types of columns.
  VELOX_ASSERT_THROW(
      deserialize(ROW({BIGINT(), DOUBLE()}), serialized, nullptr),
      "Serialized encoding is not compatible with requested type: DOUBLE. "
      "Expected LONG_ARRAY. Got VARIABLE_WIDTH.");
}

TEST_P(PrestoSerializerTest, lexer) {
  VectorFuzzer::Options opts;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  opts.nullRatio = 0.1;
  VectorFuzzer fuzzer(opts, pool_.get());
  VectorFuzzer::Options nonNullOpts;
  nonNullOpts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  nonNullOpts.nullRatio = 0;
  VectorFuzzer nonNullFuzzer(nonNullOpts, pool_.get());

  const size_t numRounds = 20;

  for (size_t i = 0; i < numRounds; ++i) {
    auto rowType = fuzzer.randRowType();

    auto inputRowVector = (i % 2 == 0) ? fuzzer.fuzzInputRow(rowType)
                                       : nonNullFuzzer.fuzzInputRow(rowType);
    testLexer(inputRowVector);
  }
}

TEST_P(PrestoSerializerTest, checksum) {
  std::ostringstream output;
  // The payload (doesn't matter what as long as it's not 0).
  vector_size_t data = 4;

  // Write out the number of rows.
  vector_size_t numRows = std::numeric_limits<int32_t>::max();
  output.write(reinterpret_cast<char*>(&numRows), sizeof(numRows));
  // Set the bit indicating there's a checksum.
  char marker = 4;
  output.write(&marker, sizeof(marker));
  vector_size_t size = sizeof(data);
  // Write out the uncompressed size and size (we're not compressing the data so
  // they're the same).
  output.write(reinterpret_cast<char*>(&size), sizeof(size));
  output.write(reinterpret_cast<char*>(&size), sizeof(size));
  // Write out the checksum, it shouldn't match the checksum of data.
  int64_t checksum = 0;
  output.write(reinterpret_cast<char*>(&checksum), sizeof(checksum));
  output.write(reinterpret_cast<char*>(&data), sizeof(data));

  auto paramOptions = getParamSerdeOptions(nullptr);
  RowVectorPtr result;
  auto serialized = output.str();
  auto byteStream = toByteStream(serialized);
  // Make a small memory pool, if we try to allocate a Vector with numRows we'll
  // OOM.
  auto pool = memory::memoryManager()->addRootPool("checksum", 1UL << 10);
  // This should fail because the checksums don't match.
  VELOX_ASSERT_THROW(
      serde_->deserialize(
          byteStream.get(),
          pool->addLeafChild("child").get(),
          ROW({BIGINT()}),
          &result,
          0,
          &paramOptions),
      "Received corrupted serialized page.");
}

INSTANTIATE_TEST_SUITE_P(
    PrestoSerializerTest,
    PrestoSerializerTest,
    ::testing::Values(
        common::CompressionKind::CompressionKind_NONE,
        common::CompressionKind::CompressionKind_ZLIB,
        common::CompressionKind::CompressionKind_SNAPPY,
        common::CompressionKind::CompressionKind_ZSTD,
        common::CompressionKind::CompressionKind_LZ4,
        common::CompressionKind::CompressionKind_GZIP));

TEST_F(PrestoSerializerTest, deserializeSingleColumn) {
  // Verify that deserializeSingleColumn API can handle all supported types.
  static const size_t kPrestoPageHeaderBytes = 21;
  static const size_t kNumOfColumnsSerializedBytes = sizeof(int32_t);
  static const size_t kBytesToTrim =
      kPrestoPageHeaderBytes + kNumOfColumnsSerializedBytes;

  auto testRoundTripSingleColumn = [&](const VectorPtr& vector) {
    auto rowVector = makeRowVector({vector});
    // Serialize to PrestoPage format.
    std::ostringstream output;
    auto arena = std::make_unique<StreamArena>(pool_.get());
    auto rowType = asRowType(rowVector->type());
    auto numRows = rowVector->size();
    auto serializer = serde_->createIterativeSerializer(
        rowType, numRows, arena.get(), nullptr);
    serializer->append(rowVector);
    facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
    OStreamOutputStream out(&output, &listener);
    serializer->flush(&out);

    validateLexer(output.str(), {});

    // Remove the PrestoPage header and Number of columns section from the
    // serialized data.
    std::string input = output.str().substr(kBytesToTrim);

    auto byteStream = toByteStream(input);
    VectorPtr deserialized;
    serde_->deserializeSingleColumn(
        byteStream.get(), pool(), vector->type(), &deserialized, nullptr);
    assertEqualVectors(vector, deserialized);
  };

  std::vector<TypePtr> typesToTest = {
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      TIMESTAMP(),
      ROW({VARCHAR(), INTEGER()}),
      ARRAY(INTEGER()),
      ARRAY(INTEGER()),
      MAP(VARCHAR(), INTEGER()),
      MAP(VARCHAR(), ARRAY(INTEGER())),
  };

  VectorFuzzer::Options opts;
  opts.vectorSize = 5;
  opts.nullRatio = 0.1;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  opts.containerVariableLength = false;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  opts.containerLength = 10;

  auto seed = 0;

  LOG(ERROR) << "Seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  for (const auto& type : typesToTest) {
    SCOPED_TRACE(fmt::format("Type: {}", type->toString()));
    auto data = fuzzer.fuzz(type);
    testRoundTripSingleColumn(data);
  }
}

class PrestoSerializerBatchEstimateSizeTest : public testing::Test,
                                              public VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }

    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
    serializer_ = serde_->createBatchSerializer(pool_.get(), &paramOptions_);
  }

  void testEstimateSerializedSize(
      VectorPtr input,
      const std::vector<IndexRange>& ranges,
      const std::vector<vector_size_t>& expectedSizes) {
    ASSERT_EQ(ranges.size(), expectedSizes.size());

    // Wrap the input a RowVector to better emulate production.
    auto row = makeRowVector({input});

    std::vector<vector_size_t> sizes(ranges.size(), 0);
    std::vector<vector_size_t*> sizesPtrs(ranges.size());
    for (int i = 0; i < ranges.size(); i++) {
      sizesPtrs[i] = &sizes[i];
    }

    Scratch scratch;
    serializer_->estimateSerializedSize(row, ranges, sizesPtrs.data(), scratch);

    for (int i = 0; i < expectedSizes.size(); i++) {
      // Add 4 bytes for each row in the wrapper. This is needed because we wrap
      // the input in a RowVector.
      ASSERT_EQ(sizes[i], expectedSizes[i] + 4 * ranges[i].size)
          << "Mismatched estimated size for range" << i << " "
          << ranges[i].begin << ":" << ranges[i].size;
    }
  }

  void testEstimateSerializedSize(
      const VectorPtr& vector,
      vector_size_t totalExpectedSize) {
    // The whole Vector is a single range.
    testEstimateSerializedSize(
        vector, {{0, vector->size()}}, {totalExpectedSize});
    // Split the Vector into two equal ranges.
    testEstimateSerializedSize(
        vector,
        {{0, vector->size() / 2}, {vector->size() / 2, vector->size() / 2}},
        {totalExpectedSize / 2, totalExpectedSize / 2});
    // Split the Vector into three ranges of 1/4, 1/2, 1/4.
    testEstimateSerializedSize(
        vector,
        {{0, vector->size() / 4},
         {vector->size() / 4, vector->size() / 2},
         {vector->size() * 3 / 4, vector->size() / 4}},
        {totalExpectedSize / 4, totalExpectedSize / 2, totalExpectedSize / 4});
  }

  std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_;
  std::unique_ptr<BatchVectorSerializer> serializer_;
  serializer::presto::PrestoVectorSerde::PrestoOptions paramOptions_;
};

TEST_F(PrestoSerializerBatchEstimateSizeTest, flat) {
  auto flatBoolVector =
      makeFlatVector<bool>(32, [](vector_size_t row) { return row % 2 == 0; });

  // Bools are 1 byte each.
  // 32 * 1 = 32
  testEstimateSerializedSize(flatBoolVector, 32);

  auto flatIntVector =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });

  // Ints are 4 bytes each.
  // 4 * 32 = 128
  testEstimateSerializedSize(flatIntVector, 128);

  auto flatDoubleVector =
      makeFlatVector<double>(32, [](vector_size_t row) { return row; });

  // Doubles are 8 bytes each.
  // 8 * 32 = 256
  testEstimateSerializedSize(flatDoubleVector, 256);

  auto flatStringVector = makeFlatVector<std::string>(
      32, [](vector_size_t row) { return fmt::format("{}", row); });

  // Strings are variable length, the first 10 are 1 byte each, the rest are 2
  // bytes.  Plus 4 bytes for the length of each string.
  // 10 * 1 + 22 * 2 + 4 * 32 = 182
  testEstimateSerializedSize(flatStringVector, {{0, 32}}, {182});
  testEstimateSerializedSize(flatStringVector, {{0, 16}, {16, 16}}, {86, 96});
  testEstimateSerializedSize(
      flatStringVector, {{0, 8}, {8, 16}, {24, 8}}, {40, 94, 48});

  auto flatVectorWithNulls = makeFlatVector<double>(
      32,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 4 == 0; });

  // Doubles are 8 bytes each, and only non-null doubles are counted. In
  // addition there's a null bit per row.
  // 8 * 24 + (32 / 8) = 196
  testEstimateSerializedSize(flatVectorWithNulls, {{0, 32}}, {196});
  testEstimateSerializedSize(
      flatVectorWithNulls, {{0, 16}, {16, 16}}, {98, 98});
  testEstimateSerializedSize(
      flatVectorWithNulls, {{0, 8}, {8, 16}, {24, 8}}, {49, 98, 49});
}

TEST_F(PrestoSerializerBatchEstimateSizeTest, array) {
  std::vector<vector_size_t> offsets{
      0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
  auto elements =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto arrayVector = makeArrayVector(offsets, elements);

  // The ints in the array are 4 bytes each, and the array length is another 4
  // bytes per row.
  // 4 * 32 + 4 * 16 = 192
  testEstimateSerializedSize(arrayVector, 192);

  std::vector<vector_size_t> offsetsWithEmptyOrNulls{
      0, 4, 4, 8, 8, 12, 12, 16, 16, 20, 20, 24, 24, 28, 28, 32};
  auto arrayVectorWithEmptyArrays =
      makeArrayVector(offsetsWithEmptyOrNulls, elements);

  // The ints in the array are 4 bytes each, and the array length is another 4
  // bytes per row.
  // 4 * 32 + 4 * 16 = 192
  testEstimateSerializedSize(arrayVectorWithEmptyArrays, 192);

  std::vector<vector_size_t> nullOffsets{1, 3, 5, 7, 9, 11, 13, 15};
  auto arrayVectorWithNulls =
      makeArrayVector(offsetsWithEmptyOrNulls, elements, nullOffsets);

  // The ints in the array are 4 bytes each, and the array length is another 4
  // bytes per non-null row, and 1 null bit per row.
  // 4 * 32 + 4 * 8 + 16 / 8 = 162
  testEstimateSerializedSize(arrayVectorWithNulls, {{0, 16}}, {162});
  testEstimateSerializedSize(arrayVectorWithNulls, {{0, 8}, {8, 8}}, {81, 81});
  testEstimateSerializedSize(
      arrayVectorWithNulls, {{0, 4}, {4, 8}, {8, 4}}, {41, 81, 41});
}

TEST_F(PrestoSerializerBatchEstimateSizeTest, map) {
  std::vector<vector_size_t> offsets{
      0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
  auto keys =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto values =
      makeFlatVector<double>(32, [](vector_size_t row) { return row; });
  auto mapVector = makeMapVector(offsets, keys, values);

  // The ints in the map are 4 bytes each, the doubles are 8 bytes, and the map
  // length is another 4 bytes per row.
  // 4 * 32 + 8 * 32 + 4 * 16 = 448
  testEstimateSerializedSize(mapVector, 448);

  std::vector<vector_size_t> offsetsWithEmptyOrNulls{
      0, 4, 4, 8, 8, 12, 12, 16, 16, 20, 20, 24, 24, 28, 28, 32};
  auto mapVectorWithEmptyMaps =
      makeMapVector(offsetsWithEmptyOrNulls, keys, values);

  // The ints in the map are 4 bytes each, the doubles are 8 bytes, and the map
  // length is another 4 bytes per row.
  // 4 * 32 + 8 * 32 + 4 * 16 = 448
  testEstimateSerializedSize(mapVectorWithEmptyMaps, 448);

  std::vector<vector_size_t> nullOffsets{1, 3, 5, 7, 9, 11, 13, 15};
  auto mapVectorWithNulls =
      makeMapVector(offsetsWithEmptyOrNulls, keys, values, nullOffsets);

  // The ints in the map are 4 bytes each, the doubles are 8 bytes, and the map
  // length is another 4 bytes per non-null row, and 1 null bit per row.
  // 4 * 32 + 8 * 32 + 4 * 8 + 16 / 8 = 216
  testEstimateSerializedSize(mapVectorWithNulls, {{0, 16}}, {418});
  testEstimateSerializedSize(mapVectorWithNulls, {{0, 8}, {8, 8}}, {209, 209});
  testEstimateSerializedSize(
      mapVectorWithNulls, {{0, 4}, {4, 8}, {12, 4}}, {105, 209, 105});
}

TEST_F(PrestoSerializerBatchEstimateSizeTest, row) {
  auto field1 =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto field2 =
      makeFlatVector<double>(32, [](vector_size_t row) { return row; });
  auto rowVector = makeRowVector({field1, field2});

  // The ints in the row are 4 bytes each, the doubles are 8 bytes, and the
  // offsets are 4 bytes per row.
  // 4 * 32 + 8 * 32 + 4 * 32 = 512
  testEstimateSerializedSize(rowVector, 512);

  auto rowVectorWithNulls =
      makeRowVector({field1, field2}, [](auto row) { return row % 4 == 0; });

  // The ints in the row are 4 bytes each, the doubles are 8 bytes, and the
  // offsets are 4 bytes per row, and 1 null bit per row.
  // 4 * 24 + 8 * 24 + 4 * 32 + 32 / 8 = 420
  testEstimateSerializedSize(rowVectorWithNulls, 420);
}

TEST_F(PrestoSerializerBatchEstimateSizeTest, constant) {
  auto flatVector =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto constantInt = BaseVector::wrapInConstant(32, 0, flatVector);

  // The single constant int is 4 bytes.
  testEstimateSerializedSize(constantInt, {{0, 32}}, {4});
  testEstimateSerializedSize(constantInt, {{0, 16}, {16, 16}}, {4, 4});
  testEstimateSerializedSize(
      constantInt, {{0, 8}, {8, 16}, {24, 8}}, {4, 4, 4});

  auto nullConstant = BaseVector::createNullConstant(BIGINT(), 32, pool_.get());

  // The single constant null is 1 byte (for the bit mask).
  testEstimateSerializedSize(nullConstant, {{0, 32}}, {1});
  testEstimateSerializedSize(nullConstant, {{0, 16}, {16, 16}}, {1, 1});
  testEstimateSerializedSize(
      nullConstant, {{0, 8}, {8, 16}, {24, 8}}, {1, 1, 1});

  std::vector<vector_size_t> offsets{
      0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
  auto arrayVector = makeArrayVector(offsets, flatVector);
  auto constantArray = BaseVector::wrapInConstant(32, 0, arrayVector);

  // The single constant array is 4 bytes for the length, and 4 bytes for each
  // of the 2 integer elements.
  // 4 + 2 * 4 = 12
  testEstimateSerializedSize(constantArray, {{0, 32}}, {12});
  testEstimateSerializedSize(constantArray, {{0, 16}, {16, 16}}, {12, 12});
  testEstimateSerializedSize(
      constantArray, {{0, 8}, {8, 16}, {24, 8}}, {12, 12, 12});

  auto arrayVectorWithConstantElements = makeArrayVector(offsets, constantInt);
  auto constantArrayWithConstantElements =
      BaseVector::wrapInConstant(32, 0, arrayVectorWithConstantElements);

  // The single constant array is 4 bytes for the length, and 4 bytes for each
  // of the 2 integer elements (encodings for children of encoded complex types
  // are not currently preserved).
  // 4 + 2 * 4 = 12
  testEstimateSerializedSize(
      constantArrayWithConstantElements, {{0, 32}}, {12});
  testEstimateSerializedSize(
      constantArrayWithConstantElements, {{0, 16}, {16, 16}}, {12, 12});
  testEstimateSerializedSize(
      constantArrayWithConstantElements,
      {{0, 8}, {8, 16}, {24, 8}},
      {12, 12, 12});
}

TEST_F(PrestoSerializerBatchEstimateSizeTest, dictionary) {
  auto indices = makeIndices(32, [](auto row) { return (row * 2) % 32; });
  auto flatVector =
      makeFlatVector<int32_t>(32, [](vector_size_t row) { return row; });
  auto dictionaryInts =
      BaseVector::wrapInDictionary(nullptr, indices, 32, flatVector);

  // The indices are 4 bytes, and the dictionary entries are 4 bytes each.
  // 4 * 32 + 4 * 16 = 192
  testEstimateSerializedSize(dictionaryInts, {{0, 32}}, {192});
  testEstimateSerializedSize(dictionaryInts, {{0, 16}, {16, 16}}, {128, 128});
  testEstimateSerializedSize(
      dictionaryInts, {{0, 8}, {8, 16}, {24, 8}}, {64, 128, 64});

  auto flatVectorWithNulls = makeFlatVector<double>(
      32,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 4 == 0; });
  auto dictionaryNullElements =
      BaseVector::wrapInDictionary(nullptr, indices, 32, flatVectorWithNulls);

  // The indices are 4 bytes, half the dictionary entries are 8 byte doubles.
  // Note that the bytes for the null bits in the entries are not accounted for,
  // this is a limitation of having non-contiguous ranges selected from the
  // dictionary values.
  // 4 * 32 + 8 * 8 = 192
  testEstimateSerializedSize(dictionaryNullElements, {{0, 32}}, {192});
  testEstimateSerializedSize(
      dictionaryNullElements, {{0, 16}, {16, 16}}, {128, 128});
  testEstimateSerializedSize(
      dictionaryNullElements, {{0, 8}, {8, 16}, {24, 8}}, {64, 128, 64});

  auto arrayIndices = makeIndices(16, [](auto row) { return (row * 2) % 16; });
  std::vector<vector_size_t> offsets{
      0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30};
  auto arrayVector = makeArrayVector(offsets, flatVector);
  auto dictionaryArray =
      BaseVector::wrapInDictionary(nullptr, arrayIndices, 16, arrayVector);

  // The indices are 4 bytes, and the dictionary entries are 4 bytes length + 4
  // bytes for each of the 2 array elements.
  // 4 * 16 + 4 * 8 + 2 * 8 * 4 = 160
  testEstimateSerializedSize(dictionaryArray, {{0, 16}}, {160});
  testEstimateSerializedSize(dictionaryArray, {{0, 8}, {8, 8}}, {128, 128});
  testEstimateSerializedSize(
      dictionaryArray, {{0, 4}, {4, 8}, {12, 4}}, {64, 128, 64});

  auto constantInt = BaseVector::wrapInConstant(32, 0, flatVector);
  auto arrayVectorWithConstantElements = makeArrayVector(offsets, constantInt);
  auto dictionaryArrayWithConstantElements = BaseVector::wrapInDictionary(
      nullptr, arrayIndices, 16, arrayVectorWithConstantElements);

  // The indices are 4 bytes, and the dictionary entries are 4 bytes length + 4
  // bytes for each of the 2 array elements (encodings for children of encoded
  // complex types are not currently preserved).
  // 4 * 16 + 4 * 8 + 2 * 8 * 4 = 160
  testEstimateSerializedSize(
      dictionaryArrayWithConstantElements, {{0, 16}}, {160});
  testEstimateSerializedSize(
      dictionaryArrayWithConstantElements, {{0, 8}, {8, 8}}, {128, 128});
  testEstimateSerializedSize(
      dictionaryArrayWithConstantElements,
      {{0, 4}, {4, 8}, {12, 4}},
      {64, 128, 64});

  auto dictionaryWithNulls = BaseVector::wrapInDictionary(
      makeNulls(32, [](auto row) { return row % 2 == 0; }),
      indices,
      32,
      flatVector);

  // When nulls are present in the dictionary, currently we flatten the data. So
  // there are 4 bytes per row.  Null bits are only accounted for the null
  // elements because the non-null elements in the wrapped vector or
  // non-contiguous.
  // 4 * 16 + 16 / 8 = 66
  testEstimateSerializedSize(dictionaryWithNulls, {{0, 32}}, {66});
  testEstimateSerializedSize(
      dictionaryWithNulls, {{0, 16}, {16, 16}}, {33, 33});
  testEstimateSerializedSize(
      dictionaryWithNulls, {{0, 8}, {8, 16}, {24, 8}}, {17, 33, 17});
}
