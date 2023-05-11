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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <functional>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/BiasVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/VectorTypeUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using facebook::velox::ComplexType;

// LazyVector loader for testing. Minimal implementation that documents the API
// contract.
class TestingLoader : public VectorLoader {
 public:
  explicit TestingLoader(VectorPtr data) : data_(data), rowCounter_(0) {}

  void loadInternal(RowSet rows, ValueHook* hook, VectorPtr* result) override {
    if (hook) {
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          applyHook, data_->typeKind(), rows, hook);
      return;
    }
    *result = data_;
    rowCounter_ += rows.size();
  }

  int32_t rowCount() const {
    return rowCounter_;
  }

 private:
  template <TypeKind Kind>
  void applyHook(RowSet rows, ValueHook* hook) {
    using T = typename TypeTraits<Kind>::NativeType;
    auto values = data_->as<SimpleVector<T>>();
    bool acceptsNulls = hook->acceptsNulls();
    for (auto i = 0; i < rows.size(); ++i) {
      auto row = rows[i];
      if (values->isNullAt(row)) {
        if (acceptsNulls) {
          hook->addNull(i);
        }
      } else {
        T value = values->valueAt(row);
        hook->addValue(i, &value);
      }
    }
  }
  VectorPtr data_;
  int32_t rowCounter_;
};

struct NonPOD {
  static int alive;

  int x;
  NonPOD(int x = 123) : x(x) {
    ++alive;
  }
  ~NonPOD() {
    --alive;
  }
  bool operator==(const NonPOD& other) const {
    return x == other.x;
  }
};

int NonPOD::alive = 0;

class VectorTest : public testing::Test, public test::VectorTestBase {
 protected:
  void SetUp() override {
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
  }

  template <typename T>
  T testValue(int32_t i, BufferPtr& space) {
    return i;
  }

  template <TypeKind KIND>
  VectorPtr createScalar(TypePtr type, vector_size_t size, bool withNulls) {
    using T = typename TypeTraits<KIND>::NativeType;
    BufferPtr buffer;
    VectorPtr base = BaseVector::create(type, size, pool_.get());
    auto flat = std::dynamic_pointer_cast<FlatVector<T>>(base);
    for (int32_t i = 0; i < flat->size(); ++i) {
      if (withNulls && i % 3 == 0) {
        flat->setNull(i, true);
      } else {
        flat->set(i, testValue<T>(i, buffer));
      }
    }
    return base;
  }

  template <TypeKind Kind, TypeKind BiasKind>
  VectorPtr createBias(vector_size_t size, bool withNulls) {
    using T = typename TypeTraits<Kind>::NativeType;
    using TBias = typename TypeTraits<BiasKind>::NativeType;
    BufferPtr buffer;
    BufferPtr values = AlignedBuffer::allocate<TBias>(size, pool_.get());
    values->setSize(size * sizeof(TBias));
    BufferPtr nulls;
    uint64_t* rawNulls = nullptr;
    if (withNulls) {
      int32_t bytes = BaseVector::byteSize<bool>(size);
      nulls = AlignedBuffer::allocate<char>(bytes, pool_.get());
      rawNulls = nulls->asMutable<uint64_t>();
      memset(rawNulls, bits::kNotNullByte, bytes);
      nulls->setSize(bytes);
    }
    auto rawValues = values->asMutable<TBias>();
    int32_t numNulls = 0;
    constexpr int32_t kBias = 100;
    for (int32_t i = 0; i < size; ++i) {
      if (withNulls && i % 3 == 0) {
        ++numNulls;
        bits::setNull(rawNulls, i);
      } else {
        rawValues[i] = testValue<TBias>(i, buffer) - kBias;
      }
    }
    return std::make_shared<BiasVector<T>>(
        pool_.get(),
        nulls,
        size,
        BiasKind,
        std::move(values),
        kBias,
        SimpleVectorStats<T>{},
        std::nullopt,
        numNulls,
        false,
        size * sizeof(T));
  }

  VectorPtr createRow(int32_t numRows, bool withNulls) {
    auto childType =
        ROW({"child_bigint", "child_string"}, {BIGINT(), VARCHAR()});
    auto parentType =
        ROW({"parent_bigint", "parent_row"}, {BIGINT(), childType});
    auto baseRow = BaseVector::create(parentType, numRows, pool_.get());
    auto row = baseRow->as<RowVector>();
    EXPECT_EQ(row->size(), numRows);
    EXPECT_EQ(row->nulls(), nullptr);

    std::vector<VectorPtr> nested = {
        createScalar<TypeKind::BIGINT>(BIGINT(), numRows, withNulls),
        createScalar<TypeKind::VARCHAR>(VARCHAR(), numRows, withNulls)};
    auto childRow = std::make_shared<RowVector>(
        pool_.get(),
        childType,
        BufferPtr(nullptr),
        numRows,
        std::move(nested),
        0 /*nullCount*/);
    BufferPtr nulls;
    if (withNulls) {
      nulls = AlignedBuffer::allocate<bool>(numRows, pool_.get());
      int32_t childCounter = 0;
      auto rawNulls = nulls->asMutable<uint64_t>();
      for (int32_t i = 0; i < numRows; ++i) {
        bits::setNull(rawNulls, i, i % 8 == 0);
      }
    }
    std::vector<VectorPtr> parentFields = {
        createScalar<TypeKind::BIGINT>(BIGINT(), numRows, withNulls), childRow};
    return std::make_shared<RowVector>(
        pool_.get(),
        parentType,
        nulls,
        numRows,
        std::move(parentFields),
        BaseVector::countNulls(nulls, numRows));
  }

  int32_t createRepeated(
      int32_t numRows,
      bool withNulls,
      BufferPtr* nulls,
      BufferPtr* offsets,
      BufferPtr* sizes,
      int forceWidth) {
    *offsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto* rawOffsets = (*offsets)->asMutable<vector_size_t>();

    *sizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto* rawSizes = (*sizes)->asMutable<vector_size_t>();

    uint64_t* rawNulls = nullptr;
    if (withNulls) {
      *nulls =
          AlignedBuffer::allocate<bool>(numRows, pool_.get(), bits::kNotNull);
      rawNulls = (*nulls)->asMutable<uint64_t>();
    }
    int32_t offset = 0;
    for (int32_t i = 0; i < numRows; ++i) {
      int32_t size = (forceWidth > 0) ? forceWidth : i % 7;
      if (withNulls && i % 5 == 0) {
        bits::setNull(rawNulls, i, true);
        if (forceWidth == 0) {
          // in forceWidth case, the size of a null element is still
          // the same as a non-null element.
          rawOffsets[i] = 0;
          rawSizes[i] = 0;
          continue;
        }
      }
      rawOffsets[i] = offset;
      rawSizes[i] = size;
      offset += size;
    }
    return offset;
  }

  VectorPtr createArray(int32_t numRows, bool withNulls) {
    BufferPtr nulls;
    BufferPtr offsets;
    BufferPtr sizes;
    int32_t numElements =
        createRepeated(numRows, withNulls, &nulls, &offsets, &sizes, 0);
    VectorPtr elements = createRow(numElements, withNulls);
    return std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(elements->type()),
        nulls,
        numRows,
        offsets,
        sizes,
        elements,
        BaseVector::countNulls(nulls, numRows));
  }

  VectorPtr createMap(int32_t numRows, bool withNulls);

  template <TypeKind KIND>
  void testFlat(TypePtr type, vector_size_t size) {
    testFlat<KIND>(type, size, false);
    testFlat<KIND>(type, size, true);
  }

  template <TypeKind KIND>
  void testFlat(TypePtr type, vector_size_t size, bool withNulls) {
    using T = typename TypeTraits<KIND>::NativeType;
    VectorPtr base = BaseVector::create(type, size, pool_.get());
    auto flat = std::dynamic_pointer_cast<FlatVector<T>>(base);
    ASSERT_NE(flat.get(), nullptr);
    EXPECT_EQ(flat->size(), size);
    EXPECT_GE(flat->values()->size(), BaseVector::byteSize<T>(size));
    EXPECT_EQ(flat->nulls(), nullptr);

    flat->resize(size * 2);
    EXPECT_EQ(flat->size(), size * 2);
    EXPECT_GE(flat->values()->capacity(), BaseVector::byteSize<T>(size * 2));
    EXPECT_EQ(flat->nulls(), nullptr);

    if (withNulls) {
      flat->setNull(size * 2 - 1, true);
      EXPECT_EQ(flat->rawNulls()[0], bits::kNotNull64);
      EXPECT_GE(
          flat->nulls()->capacity(), BaseVector::byteSize<bool>(flat->size()));
      EXPECT_TRUE(flat->isNullAt(size * 2 - 1));
    }

    // Test that the null is cleared.
    BufferPtr buffer;
    flat->set(size * 2 - 1, testValue<T>(size, buffer));
    EXPECT_FALSE(flat->isNullAt(size * 2 - 1));

    if (withNulls) {
      // Check that new elements are initialized as not null after downsize
      // and upsize.
      flat->setNull(size * 2 - 1, true);
      flat->resize(size * 2 - 1);
      flat->resize(size * 2);
      EXPECT_FALSE(flat->isNullAt(size * 2 - 1));
    }

    // Check that new StringView elements are initialized as empty after
    // downsize and upsize which does not involve a capacity change.
    if constexpr (std::is_same_v<T, StringView>) {
      flat->mutableRawValues()[size * 2 - 1] = StringView("a");
      flat->resize(size * 2 - 1);
      flat->resize(size * 2);
      EXPECT_EQ(flat->valueAt(size * 2 - 1).size(), 0);
    }

    // Fill, the values at size * 2 - 1 gets assigned a second time.
    for (int32_t i = 0; i < flat->size(); ++i) {
      if (withNulls && i % 3 == 0) {
        flat->setNull(i, true);
      } else {
        flat->set(i, testValue<T>(i, buffer));
      }
    }

    for (int32_t i = 0; i < flat->size(); ++i) {
      if (withNulls && i % 3 == 0) {
        EXPECT_TRUE(flat->isNullAt(i));
      } else {
        EXPECT_FALSE(flat->isNullAt(i));
        if constexpr (KIND == TypeKind::OPAQUE) {
          EXPECT_TRUE(flat->valueAt(i));
          EXPECT_EQ(
              *std::static_pointer_cast<NonPOD>(flat->valueAt(i)),
              *std::static_pointer_cast<NonPOD>(testValue<T>(i, buffer)));
        } else {
          EXPECT_EQ(flat->valueAt(i), testValue<T>(i, buffer));
        }
      }
    }

    testCopy(flat, numIterations_);
    testSlices(flat);

    // Check that type kind is preserved in cases where T is the same with.
    EXPECT_EQ(
        flat->typeKind(), BaseVector::wrapInConstant(100, 0, flat)->typeKind());
  }

  static SelectivityVector selectEven(vector_size_t size) {
    SelectivityVector even(size);
    for (auto i = 0; i < size; ++i) {
      even.setValid(i, i % 2 == 0);
    }
    even.updateBounds();
    return even;
  }

  static SelectivityVector selectOdd(vector_size_t size) {
    SelectivityVector odd(size);
    for (auto i = 0; i < size; ++i) {
      odd.setValid(i, i % 2 != 0);
    }
    odd.updateBounds();
    return odd;
  }

  static std::string printEncodings(const VectorPtr& vector) {
    std::stringstream out;
    out << vector->encoding();

    VectorPtr inner = vector;
    do {
      switch (inner->encoding()) {
        case VectorEncoding::Simple::DICTIONARY:
        case VectorEncoding::Simple::SEQUENCE:
        case VectorEncoding::Simple::CONSTANT:
          inner = inner->valueVector();
          if (inner == nullptr) {
            return out.str();
          }
          break;
        default:
          return out.str();
      }

      out << ", " << inner->encoding();
    } while (true);
    VELOX_UNREACHABLE();
  }

  void testCopyEncoded(VectorPtr source) {
    bool maybeConstant = false;
    bool isSequence = false;
    auto sourcePtr = source.get();
    for (;;) {
      auto encoding = sourcePtr->encoding();
      maybeConstant = encoding == VectorEncoding::Simple::CONSTANT ||
          encoding == VectorEncoding::Simple::LAZY;
      isSequence = encoding == VectorEncoding::Simple::SEQUENCE;
      if (maybeConstant || isSequence) {
        break;
      }
      if (encoding != VectorEncoding::Simple::DICTIONARY &&
          encoding != VectorEncoding::Simple::SEQUENCE) {
        break;
      }
      sourcePtr = sourcePtr->valueVector().get();
    }

    auto kind = source->typeKind();
    auto isSmall = kind == TypeKind::BOOLEAN || kind == TypeKind::TINYINT;
    auto sourceSize = source->size();
    auto target = BaseVector::create(source->type(), sourceSize, pool_.get());
    // Writes target out of sequence by copying the first half of
    // source to odd positions and the second half to even positions.
    auto even = selectEven(sourceSize);
    auto odd = selectOdd(sourceSize);
    std::vector<vector_size_t> evenSource(sourceSize, INT32_MAX);
    std::vector<vector_size_t> oddSource(sourceSize, INT32_MAX);

    for (auto i = 0; i < sourceSize; ++i) {
      if (i % 2 == 0) {
        evenSource[i] = i / 2;
      } else {
        oddSource[i] = (sourceSize / 2) + (i / 2);
      }
    }
    target->copy(source.get(), even, &evenSource[0]);
    target->copy(source.get(), odd, &oddSource[0]);
    // We check that a wrapped source tests the same via equalValueAt
    // on the wrapping and via the translation of DecodedVector.
    SelectivityVector allRows(sourceSize);
    DecodedVector decoded(*source, allRows);
    auto base = decoded.base();
    auto nulls = decoded.nulls();
    auto indices = decoded.indices();
    for (int32_t i = 0; i < sourceSize; ++i) {
      if (i % 2 == 0) {
        auto sourceIdx = evenSource[i];
        EXPECT_TRUE(target->equalValueAt(source.get(), i, sourceIdx))
            << "at " << i << ": " << target->toString(i) << " vs. "
            << source->toString(sourceIdx);

        // We check the same with 'decoded'.
        if (source->isNullAt(sourceIdx)) {
          EXPECT_TRUE(decoded.isNullAt(sourceIdx));
          EXPECT_TRUE(nulls && bits::isBitNull(nulls, sourceIdx));
          EXPECT_TRUE(target->isNullAt(i));
        } else {
          EXPECT_FALSE(nulls && bits::isBitNull(nulls, sourceIdx));
          EXPECT_FALSE(decoded.isNullAt(sourceIdx));
          EXPECT_FALSE(target->isNullAt(i));
          EXPECT_TRUE(target->equalValueAt(
              base, i, indices ? indices[sourceIdx] : sourceIdx));
        }
      } else {
        EXPECT_TRUE(target->equalValueAt(source.get(), i, oddSource[i]));
        // We check the same with 'decoded'.
        auto sourceIdx = oddSource[i];
        if (source->isNullAt(sourceIdx)) {
          EXPECT_TRUE(nulls && bits::isBitNull(nulls, sourceIdx));
          EXPECT_TRUE(target->isNullAt(i));
        } else {
          EXPECT_FALSE(nulls && bits::isBitNull(nulls, sourceIdx));
          EXPECT_FALSE(target->isNullAt(i));
          EXPECT_TRUE(target->equalValueAt(
              base, i, indices ? indices[sourceIdx] : sourceIdx));
        }
      }
      if (i > 1 && i < sourceSize - 1 && !target->isNullAt(i) && !isSmall &&
          !maybeConstant && !isSequence && source->isScalar()) {
        EXPECT_FALSE(target->equalValueAt(source.get(), i, i));
      }
    }

    target->resize(target->size() * 2);
    // Nulls must be clear after resize.
    for (int32_t i = target->size() / 2; i < target->size(); ++i) {
      EXPECT_FALSE(target->isNullAt(i));
    }
    // Copy all of source to second half of target.
    target->copy(source.get(), sourceSize, 0, sourceSize);
    for (int32_t i = 0; i < sourceSize; ++i) {
      EXPECT_TRUE(target->equalValueAt(source.get(), sourceSize + i, i));
      EXPECT_TRUE(source->equalValueAt(target.get(), i, sourceSize + i));
    }

    std::vector<BaseVector::CopyRange> ranges = {
        {0, 0, sourceSize},
        {0, sourceSize, sourceSize},
    };
    target->copyRanges(source.get(), ranges);
    for (int32_t i = 0; i < sourceSize; ++i) {
      EXPECT_TRUE(source->equalValueAt(target.get(), i, i));
      EXPECT_TRUE(source->equalValueAt(target.get(), i, sourceSize + i));
    }

    // Check that uninitialized is copyable.
    target->resize(target->size() + 100);
    target->copy(target.get(), target->size() - 50, target->size() - 100, 50);
  }

  /// Add up to 'level' wrappings to 'source' and test copy and serialize
  /// operations.
  void testCopy(VectorPtr source, int level) {
    SCOPED_TRACE(printEncodings(source));
    testCopyEncoded(source);

    if (source->type()->isOpaque()) {
      // No support for serialization of opaque types yet, make sure something
      // throws
      EXPECT_THROW(testSerialization(source), std::exception);
    } else {
      testSerialization(source);
    }

    if (level == 0) {
      return;
    }

    // Add dictionary wrapping to put vectors elements in reverse order.
    // [1, 2, 3, 4] becomes [4, 3, 2, 1].
    // If 'source' has nulls, add more nulls every 11-th row starting with row #
    // 'level'.
    auto sourceSize = source->size();
    BufferPtr dictionaryNulls;
    uint64_t* rawNulls = nullptr;
    if (source->mayHaveNulls()) {
      dictionaryNulls = AlignedBuffer::allocate<bool>(
          sourceSize, pool_.get(), bits::kNotNull);
      rawNulls = dictionaryNulls->asMutable<uint64_t>();
    }
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(sourceSize, pool_.get());
    for (int32_t i = 0; i < sourceSize; ++i) {
      indices->asMutable<vector_size_t>()[i] = sourceSize - i - 1;
      if (rawNulls && (i + level) % 11 == 0) {
        bits::setNull(rawNulls, i);
      }
    }
    auto inDictionary = BaseVector::wrapInDictionary(
        dictionaryNulls, indices, sourceSize, source);
    testCopy(inDictionary, level - 1);

    // Add sequence wrapping repeating each 'source' row once.
    BufferPtr lengths =
        AlignedBuffer::allocate<vector_size_t>(sourceSize, pool_.get());
    for (int32_t i = 0; i < sourceSize; ++i) {
      lengths->asMutable<vector_size_t>()[i] = 1;
    }
    auto inSequence = BaseVector::wrapInSequence(lengths, sourceSize, source);
    testCopy(inSequence, level - 1);

    // Add constant wrapping.
    auto constant = BaseVector::wrapInConstant(20, 10 + level, source);
    testCopy(constant, level - 1);

    // Add constant wrapping using null row.
    if (source->mayHaveNulls() && !source->isLazy()) {
      int32_t firstNull = 0;
      for (; firstNull < sourceSize; ++firstNull) {
        if (source->isNullAt(firstNull)) {
          break;
        }
      }
      constant = BaseVector::wrapInConstant(firstNull + 123, firstNull, source);
      testCopy(constant, level - 1);
    }

    // Add lazy wrapping.
    auto lazy = std::make_shared<LazyVector>(
        source->pool(),
        source->type(),
        sourceSize,
        std::make_unique<TestingLoader>(source));
    testCopy(lazy, level - 1);
  }

  static void testSlice(
      const VectorPtr& vec,
      int level,
      vector_size_t offset,
      vector_size_t length) {
    SCOPED_TRACE(fmt::format(
        "testSlice encoding={} offset={} length={}",
        vec->encoding(),
        offset,
        length));
    ASSERT_GE(vec->size(), offset + length);
    auto slice = vec->loadedVector()->slice(offset, length);
    ASSERT_EQ(slice->size(), length);
    // Compare values and nulls directly.
    for (int i = 0; i < length; ++i) {
      EXPECT_TRUE(slice->equalValueAt(vec.get(), i, offset + i));
    }
    { // Check DecodedVector works on slice.
      SelectivityVector allRows(length);
      DecodedVector decoded(*slice, allRows);
      auto base = decoded.base();
      auto indices = decoded.indices();
      for (int i = 0; i < length; ++i) {
        auto j = offset + i;
        if (vec->isNullAt(j)) {
          EXPECT_TRUE(decoded.isNullAt(i));
        } else {
          ASSERT_FALSE(decoded.isNullAt(i));
          auto ii = indices ? indices[i] : i;
          EXPECT_TRUE(base->equalValueAt(vec.get(), ii, j));
        }
      }
    }
    if (level == 0) {
      return;
    }
    // Add constant wrapping.
    if (auto i = 3 + level; i < slice->size()) {
      testSlices(BaseVector::wrapInConstant(123, i, slice), level - 1);
    }
    // Add constant wrapping using null row.
    if (slice->mayHaveNulls() && !slice->isLazy()) {
      for (int i = 0; i < slice->size(); ++i) {
        if (slice->isNullAt(i)) {
          testSlices(BaseVector::wrapInConstant(i + 123, i, slice), level - 1);
          break;
        }
      }
    }
    {
      // Add dictionary wrapping to put vectors elements in reverse order.  If
      // 'source' has nulls, add more nulls every 11-th row starting with row #
      // 'level'.
      BufferPtr nulls;
      uint64_t* rawNulls = nullptr;
      if (slice->mayHaveNulls()) {
        nulls = AlignedBuffer::allocate<bool>(
            slice->size(), slice->pool(), bits::kNotNull);
        rawNulls = nulls->asMutable<uint64_t>();
      }
      BufferPtr indices =
          AlignedBuffer::allocate<vector_size_t>(slice->size(), slice->pool());
      auto rawIndices = indices->asMutable<vector_size_t>();
      for (vector_size_t i = 0; i < slice->size(); ++i) {
        rawIndices[i] = slice->size() - i - 1;
        if (rawNulls && (i + level) % 11 == 0) {
          bits::setNull(rawNulls, i);
        }
      }
      auto wrapped =
          BaseVector::wrapInDictionary(nulls, indices, slice->size(), slice);
      testSlices(wrapped, level - 1);
    }
    { // Add lazy wrapping.
      auto wrapped = std::make_shared<LazyVector>(
          slice->pool(),
          slice->type(),
          slice->size(),
          std::make_unique<TestingLoader>(slice));
      testSlices(wrapped, level - 1);
    }
    {
      // Test that resize works even when the underlying buffers are
      // immutable. This is true for a slice since it creates buffer views over
      // the buffers of the original vector that it sliced.
      auto newSize = slice->size() * 2;
      slice->resize(newSize);
      EXPECT_EQ(slice->size(), newSize);
    }
  }

  static void testSlices(const VectorPtr& slice, int level = 2) {
    for (vector_size_t offset : {0, 16, 17}) {
      for (vector_size_t length : {0, 1, 83}) {
        if (offset + length <= slice->size()) {
          testSlice(slice, level, offset, length);
        }
      }
    }
  }

  void prepareInput(ByteStream* input, std::string& string) {
    // Put 'string' in 'input' in many pieces.
    std::vector<ByteRange> ranges;
    int32_t size = string.size();
    for (int32_t i = 0; i < 10; ++i) {
      int32_t start = i * (size / 10);
      int32_t end = (i == 9) ? size : (i + 1) * (size / 10);
      ranges.emplace_back();
      ranges.back().buffer = reinterpret_cast<uint8_t*>(&string[start]);
      ranges.back().size = end - start;
      ranges.back().position = 0;
    }
    input->resetInput(std::move(ranges));
  }

  void checkSizes(
      BaseVector* source,
      std::vector<vector_size_t>& sizes,
      std::string& string) {
    int32_t total = 0;
    for (auto size : sizes) {
      total += size;
    }
    // Approx. 10 bytes for encoding, 4 for length, 1 for null presence. This
    // can vary.
    total -= 15;
    float tolerance = source->mayHaveNulls() ? 0.3 : 0.2;
    switch (source->typeKind()) {
      case TypeKind::BOOLEAN:
        tolerance = 10;
        break;
      case TypeKind::TINYINT:
        tolerance = 2;
        break;
      case TypeKind::SMALLINT:
        tolerance = 1.5;
        break;
      case TypeKind::TIMESTAMP:
        // The size is reported as 16 bytes but Presto wire format is 8 bytes.
        tolerance = 2.2;
        break;
      default:
        break;
    }
    if (total > 1024) {
      int32_t size = string.size();
      EXPECT_GE(total, size * (1 - tolerance));
      EXPECT_LE(total, size * (1 + tolerance));
    }
  }

  /// Serialize odd and even rows into separate streams. Read serialized data
  /// back and compare with the original data.
  void testSerialization(VectorPtr source) {
    // Serialization functions expect loaded vectors.
    source = BaseVector::loadedVectorShared(source);
    auto sourceRow = makeRowVector({"c"}, {source});
    auto sourceRowType = asRowType(sourceRow->type());

    VectorStreamGroup even(pool_.get());
    even.createStreamTree(sourceRowType, source->size() / 4);

    VectorStreamGroup odd(pool_.get());
    odd.createStreamTree(sourceRowType, source->size() / 3);

    std::vector<IndexRange> evenIndices;
    std::vector<IndexRange> oddIndices;
    for (vector_size_t i = 0; i < source->size(); ++i) {
      if (i % 2 == 0) {
        evenIndices.push_back(IndexRange{i, 1});
      } else {
        oddIndices.push_back(IndexRange{i, 1});
      }
    }

    std::vector<vector_size_t> evenSizes(evenIndices.size());
    std::vector<vector_size_t*> evenSizePointers(evenSizes.size());
    for (int32_t i = 0; i < evenSizes.size(); ++i) {
      evenSizePointers[i] = &evenSizes[i];
    }

    std::vector<vector_size_t> oddSizes(oddIndices.size());
    std::vector<vector_size_t*> oddSizePointers(oddSizes.size());
    for (int32_t i = 0; i < oddSizes.size(); ++i) {
      oddSizePointers[i] = &oddSizes[i];
    }

    VectorStreamGroup::estimateSerializedSize(
        source, evenIndices, evenSizePointers.data());
    VectorStreamGroup::estimateSerializedSize(
        source, oddIndices, oddSizePointers.data());
    even.append(
        sourceRow, folly::Range(evenIndices.data(), evenIndices.size() / 2));
    even.append(
        sourceRow,
        folly::Range(
            &evenIndices[evenIndices.size() / 2],
            evenIndices.size() - evenIndices.size() / 2));
    odd.append(
        sourceRow, folly::Range(oddIndices.data(), oddIndices.size() / 2));
    odd.append(
        sourceRow,
        folly::Range(
            &oddIndices[oddIndices.size() / 2],
            oddIndices.size() - oddIndices.size() / 2));

    std::stringstream evenStream;
    std::stringstream oddStream;
    OStreamOutputStream evenOutputStream(&evenStream);
    OStreamOutputStream oddOutputStream(&oddStream);
    even.flush(&evenOutputStream);
    odd.flush(&oddOutputStream);

    auto evenString = evenStream.str();
    checkSizes(source.get(), evenSizes, evenString);

    ByteStream input;
    prepareInput(&input, evenString);

    RowVectorPtr resultRow;
    VectorStreamGroup::read(&input, pool_.get(), sourceRowType, &resultRow);
    VectorPtr result = resultRow->childAt(0);
    switch (source->encoding()) {
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::ROW:
      case VectorEncoding::Simple::ARRAY:
      case VectorEncoding::Simple::MAP: {
        // Test that retained sizes are in reasonable bounds if vectors
        // are not wrapped.
        auto originalRetained = source->retainedSize();
        auto deserializedRetained = resultRow->retainedSize();
        EXPECT_LE(deserializedRetained, originalRetained)
            << "Deserializing half is larger than original";
        EXPECT_GT(deserializedRetained, 0);
        break;
      }
      default:
        // skip non-flat encodings
        break;
    }

    for (int32_t i = 0; i < evenIndices.size(); ++i) {
      EXPECT_TRUE(result->equalValueAt(source.get(), i, evenIndices[i].begin))
          << "at " << i << ", " << source->encoding();
    }

    auto oddString = oddStream.str();
    prepareInput(&input, oddString);

    VectorStreamGroup::read(&input, pool_.get(), sourceRowType, &resultRow);
    result = resultRow->childAt(0);
    for (int32_t i = 0; i < oddIndices.size(); ++i) {
      EXPECT_TRUE(result->equalValueAt(source.get(), i, oddIndices[i].begin))
          << "at " << i << ", " << source->encoding();
    }
  }

  template <typename T>
  static void testArrayOrMapSliceMutability(const std::shared_ptr<T>& vec) {
    ASSERT_GE(vec->size(), 3);
    auto slice = std::dynamic_pointer_cast<T>(vec->slice(0, 3));
    auto offsets = slice->rawOffsets();
    slice->mutableOffsets(slice->size());
    EXPECT_NE(slice->rawOffsets(), offsets);
    auto sizes = slice->rawSizes();
    slice->mutableSizes(slice->size());
    EXPECT_NE(slice->rawSizes(), sizes);
  }

  size_t vectorSize_{100};
  size_t numIterations_{3};
};

template <>
int128_t VectorTest::testValue<int128_t>(int32_t i, BufferPtr& /*space*/) {
  return HugeInt::build(i % 2 ? (i * -1) : i, 0xAAAAAAAAAAAAAAAA);
}

template <>
StringView VectorTest::testValue(int32_t n, BufferPtr& buffer) {
  if (!buffer || buffer->capacity() < 1000) {
    buffer = AlignedBuffer::allocate<char>(1000, pool_.get());
  }
  std::stringstream out;
  out << n;
  for (int32_t i = 0; i < n % 20; ++i) {
    out << " " << i * i;
  }
  std::string str = out.str();
  EXPECT_LE(str.size(), buffer->capacity());
  memcpy(buffer->asMutable<char>(), str.data(), str.size());
  return StringView(buffer->as<char>(), str.size());
}

template <>
bool VectorTest::testValue(int32_t i, BufferPtr& /*space*/) {
  return (i % 2) == 1;
}

template <>
Timestamp VectorTest::testValue(int32_t i, BufferPtr& /*space*/) {
  // Return even milliseconds.
  return Timestamp(i * 1000, (i % 1000) * 1000000);
}

template <>
Date VectorTest::testValue(int32_t i, BufferPtr& /*space*/) {
  return Date(i);
}

template <>
std::shared_ptr<void> VectorTest::testValue(int32_t i, BufferPtr& /*space*/) {
  return std::make_shared<NonPOD>(i);
}

VectorPtr VectorTest::createMap(int32_t numRows, bool withNulls) {
  BufferPtr nulls;
  BufferPtr offsets;
  BufferPtr sizes;
  int32_t numElements =
      createRepeated(numRows, withNulls, &nulls, &offsets, &sizes, 0);
  VectorPtr elements = createRow(numElements, withNulls);
  auto keysBase = BaseVector::create(VARCHAR(), 7, pool_.get());
  auto flatKeys = keysBase->as<FlatVector<StringView>>();
  for (int32_t i = 0; i < keysBase->size(); ++i) {
    BufferPtr buffer;
    flatKeys->set(i, testValue<StringView>(1000 + i, buffer));
  }

  auto indices =
      AlignedBuffer::allocate<vector_size_t>(elements->size(), pool_.get());

  auto rawSizes = sizes->as<vector_size_t>();
  int32_t offset = 0;
  for (int32_t i = 0; i < numRows; ++i) {
    int32_t size = rawSizes[i];
    for (int32_t index = 0; index < size; ++index) {
      indices->asMutable<vector_size_t>()[offset++] = index;
    }
  }
  VELOX_CHECK_EQ(offset, elements->size());
  auto keys = BaseVector::wrapInDictionary(
      BufferPtr(nullptr),
      std::move(indices),
      elements->size(),
      std::move(keysBase));
  return std::make_shared<MapVector>(
      pool_.get(),
      MAP(VARCHAR(), elements->type()),
      nulls,
      numRows,
      offsets,
      sizes,
      keys,
      elements,
      BaseVector::countNulls(nulls, numRows));
}

TEST_F(VectorTest, createInt) {
  testFlat<TypeKind::BIGINT>(BIGINT(), vectorSize_);
  testFlat<TypeKind::INTEGER>(INTEGER(), vectorSize_);
  testFlat<TypeKind::SMALLINT>(SMALLINT(), vectorSize_);
  testFlat<TypeKind::TINYINT>(TINYINT(), vectorSize_);
}

TEST_F(VectorTest, createDouble) {
  testFlat<TypeKind::REAL>(REAL(), vectorSize_);
  testFlat<TypeKind::DOUBLE>(DOUBLE(), vectorSize_);
}

TEST_F(VectorTest, createStr) {
  testFlat<TypeKind::VARCHAR>(VARCHAR(), vectorSize_);
  testFlat<TypeKind::VARBINARY>(VARBINARY(), vectorSize_);
}

TEST_F(VectorTest, createOther) {
  testFlat<TypeKind::BOOLEAN>(BOOLEAN(), vectorSize_);
  testFlat<TypeKind::TIMESTAMP>(TIMESTAMP(), vectorSize_);
  testFlat<TypeKind::DATE>(DATE(), vectorSize_);
  testFlat<TypeKind::BIGINT>(INTERVAL_DAY_TIME(), vectorSize_);
}

TEST_F(VectorTest, createDecimal) {
  testFlat<TypeKind::BIGINT>(DECIMAL(10, 5), vectorSize_);
  testFlat<TypeKind::HUGEINT>(DECIMAL(30, 5), vectorSize_);
}

TEST_F(VectorTest, createOpaque) {
  NonPOD::alive = 0;
  testFlat<TypeKind::OPAQUE>(OPAQUE<int>(), vectorSize_);
  EXPECT_EQ(NonPOD::alive, 0);
}

TEST_F(VectorTest, getOrCreateEmpty) {
  auto empty = BaseVector::getOrCreateEmpty(nullptr, VARCHAR(), pool_.get());
  EXPECT_NE(empty, nullptr);
  EXPECT_EQ(empty->size(), 0);
  EXPECT_EQ(empty->type(), VARCHAR());
}

TEST_F(VectorTest, bias) {
  auto base =
      createBias<TypeKind::BIGINT, TypeKind::INTEGER>(vectorSize_, false);
  testCopy(base, 4);
  base = createBias<TypeKind::INTEGER, TypeKind::SMALLINT>(vectorSize_, true);
  testCopy(base, 4);
}

TEST_F(VectorTest, row) {
  auto baseRow = createRow(vectorSize_, false);
  testCopy(baseRow, numIterations_);
  testSlices(baseRow);
  baseRow = createRow(vectorSize_, true);
  testCopy(baseRow, numIterations_);
  testSlices(baseRow);
  auto allNull =
      BaseVector::createNullConstant(baseRow->type(), 50, pool_.get());
  testCopy(allNull, numIterations_);
  testSlices(allNull);
}

TEST_F(VectorTest, array) {
  auto baseArray = createArray(vectorSize_, false);
  testCopy(baseArray, numIterations_);
  testSlices(baseArray);
  baseArray = createArray(vectorSize_, true);
  testCopy(baseArray, numIterations_);
  testSlices(baseArray);
  auto allNull =
      BaseVector::createNullConstant(baseArray->type(), 50, pool_.get());
  testCopy(allNull, numIterations_);
  testSlices(allNull);
}

TEST_F(VectorTest, map) {
  auto baseMap = createMap(vectorSize_, false);
  testCopy(baseMap, numIterations_);
  testSlices(baseMap);
  baseMap = createRow(vectorSize_, true);
  testCopy(baseMap, numIterations_);
  testSlices(baseMap);
  auto allNull =
      BaseVector::createNullConstant(baseMap->type(), 50, pool_.get());
  testCopy(allNull, numIterations_);
  testSlices(allNull);
}

TEST_F(VectorTest, unknown) {
  // Creates a const UNKNOWN vector.
  auto constUnknownVector = BaseVector::createConstant(
      UNKNOWN(), variant(TypeKind::UNKNOWN), 123, pool_.get());
  ASSERT_FALSE(constUnknownVector->isScalar());
  ASSERT_EQ(TypeKind::UNKNOWN, constUnknownVector->typeKind());
  ASSERT_EQ(123, constUnknownVector->size());
  for (auto i = 0; i < constUnknownVector->size(); i++) {
    ASSERT_TRUE(constUnknownVector->isNullAt(i));
  }

  // Create an int vector and copy UNKNOWN const vector into it.
  auto intVector = BaseVector::create(BIGINT(), 10, pool_.get());
  ASSERT_FALSE(intVector->isNullAt(0));
  ASSERT_FALSE(intVector->mayHaveNulls());
  intVector->copy(constUnknownVector.get(), 0, 0, 1);
  ASSERT_TRUE(intVector->isNullAt(0));

  SelectivityVector rows(2);
  intVector->copy(constUnknownVector.get(), rows, nullptr);
  for (int i = 0; i < intVector->size(); ++i) {
    if (i < 2) {
      ASSERT_TRUE(intVector->isNullAt(i));
    } else {
      ASSERT_FALSE(intVector->isNullAt(i));
    }
  }

  // Can't copy to constant.
  EXPECT_ANY_THROW(constUnknownVector->copy(intVector.get(), rows, nullptr));

  // Create a flat UNKNOWN vector.
  auto unknownVector = BaseVector::create(UNKNOWN(), 10, pool_.get());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, unknownVector->encoding());
  ASSERT_FALSE(unknownVector->isScalar());
  for (int i = 0; i < unknownVector->size(); ++i) {
    ASSERT_TRUE(unknownVector->isNullAt(i)) << i;
  }
  // Can't copy to UNKNOWN vector.
  EXPECT_ANY_THROW(unknownVector->copy(intVector.get(), rows, nullptr));

  // Copying flat UNKNOWN into integer vector nulls out all the copied element.
  ASSERT_FALSE(intVector->isNullAt(3));
  intVector->copy(unknownVector.get(), 3, 3, 1);
  ASSERT_TRUE(intVector->isNullAt(3));

  rows.resize(intVector->size());
  intVector->copy(unknownVector.get(), rows, nullptr);
  for (int i = 0; i < intVector->size(); ++i) {
    ASSERT_TRUE(intVector->isNullAt(i));
  }

  // It is okay to copy to a non-constant UNKNOWN vector.
  unknownVector->copy(constUnknownVector.get(), rows, nullptr);
  for (int i = 0; i < unknownVector->size(); ++i) {
    ASSERT_TRUE(unknownVector->isNullAt(i)) << i;
  }

  // Can't copy to constant.
  EXPECT_ANY_THROW(
      constUnknownVector->copy(unknownVector.get(), rows, nullptr));

  for (auto& vec : {constUnknownVector, unknownVector}) {
    auto slice = vec->slice(1, 3);
    ASSERT_EQ(TypeKind::UNKNOWN, slice->typeKind());
    ASSERT_EQ(3, slice->size());
    for (auto i = 0; i < slice->size(); i++) {
      ASSERT_TRUE(slice->isNullAt(i));
    }
  }
}

TEST_F(VectorTest, copyBoolAllNullFlatVector) {
  const vector_size_t size = 1'000;
  auto allNulls = makeAllNullFlatVector<bool>(size);

  auto copy = makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; });

  // Copy first 10 rows from allNulls.
  SelectivityVector rows(10);
  copy->copy(allNulls.get(), rows, nullptr);

  for (auto i = 0; i < size; i++) {
    ASSERT_EQ(copy->isNullAt(i), i < 10) << "at " << i;
    if (i >= 10) {
      ASSERT_EQ(copy->valueAt(i), i % 2 == 0) << "at " << i;
    }
  }
}

/// Test copying non-null values into an all-nulls flat vector.
TEST_F(VectorTest, copyToAllNullsFlatVector) {
  // Create all-nulls flat vector with values buffer unset.
  const vector_size_t size = 1'000;
  auto allNulls = makeAllNullFlatVector<int32_t>(size);

  auto source = makeFlatVector<int32_t>({0, 1, 2, 3, 4});

  allNulls->copy(source.get(), 0, 0, 3);

  for (auto i = 0; i < 3; ++i) {
    ASSERT_EQ(i, allNulls->valueAt(i));
  }
  for (auto i = 3; i < size; ++i) {
    ASSERT_TRUE(allNulls->isNullAt(i));
  }

  // Reset allNulls vector back to all nulls.
  allNulls = makeAllNullFlatVector<int32_t>(size);

  SelectivityVector rows(4);
  allNulls->copy(source.get(), rows, nullptr);

  for (auto i = 0; i < 4; ++i) {
    ASSERT_EQ(i, allNulls->valueAt(i));
  }
  for (auto i = 4; i < size; ++i) {
    ASSERT_TRUE(allNulls->isNullAt(i));
  }
}

TEST_F(VectorTest, wrapInConstant) {
  // wrap flat vector
  const vector_size_t size = 1'000;
  auto flatVector = makeFlatVector<int32_t>(
      size, [](auto row) { return row; }, nullEvery(7));

  auto constVector = std::dynamic_pointer_cast<ConstantVector<int32_t>>(
      BaseVector::wrapInConstant(size, 5, flatVector));
  EXPECT_EQ(constVector->valueVector(), nullptr);
  for (auto i = 0; i < size; i++) {
    ASSERT_FALSE(constVector->isNullAt(i));
    ASSERT_EQ(5, constVector->valueAt(i));
  }

  constVector = std::dynamic_pointer_cast<ConstantVector<int32_t>>(
      BaseVector::wrapInConstant(size, 7, flatVector));
  EXPECT_EQ(constVector->valueVector(), nullptr);
  for (auto i = 0; i < size; i++) {
    ASSERT_TRUE(constVector->isNullAt(i));
  }

  // wrap dictionary vector
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    rawIndices[i] = 2 * i % size;
  }

  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_.get());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, i % 11 == 0);
  }

  auto dictVector =
      BaseVector::wrapInDictionary(nulls, indices, size, flatVector);

  constVector = std::dynamic_pointer_cast<ConstantVector<int32_t>>(
      BaseVector::wrapInConstant(size, 5, dictVector));
  EXPECT_EQ(constVector->valueVector(), nullptr);
  for (auto i = 0; i < size; i++) {
    ASSERT_FALSE(constVector->isNullAt(i));
    ASSERT_EQ(10, constVector->valueAt(i));
  }

  constVector = std::dynamic_pointer_cast<ConstantVector<int32_t>>(
      BaseVector::wrapInConstant(size, 11, dictVector));
  EXPECT_EQ(constVector->valueVector(), nullptr);
  for (auto i = 0; i < size; i++) {
    ASSERT_TRUE(constVector->isNullAt(i));
  }
}

TEST_F(VectorTest, wrapConstantInDictionary) {
  // Wrap Constant in Dictionary with no extra nulls. Expect Constant.
  auto indices = makeIndices(10, [](auto row) { return row % 2; });
  auto vector = BaseVector::wrapInDictionary(
      nullptr,
      indices,
      10,
      BaseVector::createConstant(INTEGER(), 7, 100, pool_.get()));
  ASSERT_EQ(vector->encoding(), VectorEncoding::Simple::CONSTANT);
  auto constantVector =
      std::dynamic_pointer_cast<ConstantVector<int32_t>>(vector);
  for (auto i = 0; i < 10; ++i) {
    ASSERT_FALSE(constantVector->isNullAt(i));
    ASSERT_EQ(7, constantVector->valueAt(i));
  }

  // Wrap Constant in Dictionary with extra nulls. Expect Dictionary.
  auto nulls = makeNulls(10, [](auto row) { return row % 3 == 0; });
  vector = BaseVector::wrapInDictionary(
      nulls,
      indices,
      10,
      BaseVector::createConstant(INTEGER(), 11, 100, pool_.get()));
  ASSERT_EQ(vector->encoding(), VectorEncoding::Simple::DICTIONARY);
  auto dictVector = std::dynamic_pointer_cast<SimpleVector<int32_t>>(vector);
  for (auto i = 0; i < 10; ++i) {
    if (i % 3 == 0) {
      ASSERT_TRUE(dictVector->isNullAt(i));
    } else {
      ASSERT_FALSE(dictVector->isNullAt(i));
      ASSERT_EQ(11, dictVector->valueAt(i));
    }
  }
}

TEST_F(VectorTest, setFlatVectorStringView) {
  auto vector = BaseVector::create(VARCHAR(), 1, pool_.get());
  auto flat = vector->asFlatVector<StringView>();
  EXPECT_EQ(0, flat->stringBuffers().size());

  const std::string originalString = "This string is too long to be inlined";

  flat->set(0, StringView(originalString));
  EXPECT_EQ(flat->valueAt(0).getString(), originalString);
  EXPECT_EQ(1, flat->stringBuffers().size());
  EXPECT_EQ(originalString.size(), flat->stringBuffers()[0]->size());

  // Make a copy of the vector. Verify that string buffer is shared.
  auto copy = BaseVector::create(VARCHAR(), 1, pool_.get());
  copy->copy(flat, 0, 0, 1);

  auto flatCopy = copy->asFlatVector<StringView>();
  EXPECT_EQ(1, flatCopy->stringBuffers().size());
  EXPECT_EQ(flat->stringBuffers()[0].get(), flatCopy->stringBuffers()[0].get());

  // Modify the string in the copy. Make sure it is written into a new string
  // buffer.
  const std::string newString = "A different string";
  flatCopy->set(0, StringView(newString));
  EXPECT_EQ(2, flatCopy->stringBuffers().size());
  EXPECT_EQ(flat->stringBuffers()[0].get(), flatCopy->stringBuffers()[0].get());
  EXPECT_EQ(originalString.size(), flatCopy->stringBuffers()[0]->size());
  EXPECT_EQ(newString.size(), flatCopy->stringBuffers()[1]->size());
}

TEST_F(VectorTest, resizeAtConstruction) {
  const size_t realSize = 10;

  vector_size_t oldByteSize = BaseVector::byteSize<int64_t>(realSize);
  BufferPtr values = AlignedBuffer::allocate<char>(oldByteSize, pool_.get());

  EXPECT_EQ(oldByteSize, values->size());
  EXPECT_GE(values->capacity(), values->size());

  // Resize the vector.
  const size_t newByteSize = oldByteSize / 2;
  const size_t curCapacity = values->capacity();
  values->setSize(newByteSize);

  // Check it has been resized.
  EXPECT_EQ(newByteSize, values->size());
  EXPECT_EQ(curCapacity, values->capacity());

  // Now create a FlatVector with the resized buffer.
  auto flat = std::make_shared<FlatVector<int64_t>>(
      pool_.get(),
      BIGINT(),
      BufferPtr(nullptr),
      realSize,
      std::move(values),
      std::vector<BufferPtr>());

  // Check that the vector was resized back to the old size.
  EXPECT_EQ(oldByteSize, flat->values()->size());
}

TEST_F(VectorTest, resizeStringAsciiness) {
  std::vector<std::string> stringInput = {"hellow", "how", "are"};
  auto flatVector = makeFlatVector(stringInput);
  auto stringVector = flatVector->as<SimpleVector<StringView>>();
  SelectivityVector rows(stringInput.size());
  stringVector->computeAndSetIsAscii(rows);
  ASSERT_TRUE(stringVector->isAscii(rows).value());
  stringVector->resize(2);
  ASSERT_FALSE(stringVector->isAscii(rows));
}

TEST_F(VectorTest, copyNoRows) {
  {
    auto source = makeFlatVector<int32_t>({1, 2, 3});
    auto target = BaseVector::create(INTEGER(), 10, pool_.get());
    SelectivityVector rows(3, false);
    target->copy(source.get(), rows, nullptr);
  }

  {
    auto source = makeFlatVector<StringView>({"a", "b", "c"});
    auto target = BaseVector::create(VARCHAR(), 10, pool_.get());
    SelectivityVector rows(3, false);
    target->copy(source.get(), rows, nullptr);
  }
}

TEST_F(VectorTest, copyAscii) {
  std::vector<std::string> stringData = {"a", "b", "c"};
  auto source = makeFlatVector(stringData);
  SelectivityVector all(stringData.size());
  source->setAllIsAscii(true);

  auto other = makeFlatVector(stringData);
  other->copy(source.get(), all, nullptr);
  auto ascii = other->isAscii(all);
  ASSERT_TRUE(ascii.has_value());
  ASSERT_TRUE(ascii.value());

  // Copy over asciness from a vector without asciiness set.
  source->invalidateIsAscii();
  other->copy(source.get(), all, nullptr);
  // Ensure isAscii returns nullopt
  ascii = other->isAscii(all);
  ASSERT_FALSE(ascii.has_value());

  // Now copy over some rows from vector without ascii compute on it.
  other->setAllIsAscii(false);
  SelectivityVector some(all.size(), false);
  some.setValid(1, true);
  some.updateBounds();
  other->copy(source.get(), some, nullptr);
  // We will have invalidated all.
  ascii = other->isAscii(all);
  ASSERT_FALSE(ascii.has_value());

  auto largerSource = makeFlatVector<std::string>({"a", "b", "c", "d"});
  vector_size_t sourceMappings[] = {3, 2, 1, 0};
  some.setAll();
  some.setValid(0, false);
  some.updateBounds();

  other->setAllIsAscii(false);
  largerSource->setAllIsAscii(true);
  other->copy(largerSource.get(), some, sourceMappings);
  ascii = other->isAscii(all);
  ASSERT_TRUE(ascii.has_value());
  ASSERT_FALSE(ascii.value());
}

TEST_F(VectorTest, compareNan) {
  // Double input.
  {
    std::vector<double> doubleInput = {1.1, std::nan("nan"), 0.9};

    auto flatVector1 = makeFlatVector(doubleInput);
    auto flatVector2 = makeFlatVector(doubleInput);

    for (size_t i = 0; i < doubleInput.size(); ++i) {
      EXPECT_TRUE(flatVector1->equalValueAt(flatVector2.get(), i, i));
    }
  }

  // Float input.
  {
    std::vector<float> floatInput = {1.1, std::nanf("nan"), 0.9};

    auto flatVector1 = makeFlatVector(floatInput);
    auto flatVector2 = makeFlatVector(floatInput);

    for (size_t i = 0; i < floatInput.size(); ++i) {
      EXPECT_TRUE(flatVector1->equalValueAt(flatVector2.get(), i, i));
    }
  }
}

class VectorCreateConstantTest : public VectorTest {
 public:
  template <TypeKind KIND>
  void testPrimitiveConstant(
      typename TypeTraits<KIND>::NativeType val,
      const TypePtr& type = Type::create<KIND>()) {
    using TCpp = typename TypeTraits<KIND>::NativeType;
    variant var = variant::create<KIND>(val);

    auto baseVector = BaseVector::createConstant(type, var, size_, pool_.get());
    auto simpleVector = baseVector->template as<SimpleVector<TCpp>>();
    ASSERT_TRUE(simpleVector != nullptr);

    ASSERT_EQ(KIND, simpleVector->typeKind());
    ASSERT_EQ(size_, simpleVector->size());
    ASSERT_TRUE(simpleVector->isScalar());

    for (auto i = 0; i < simpleVector->size(); i++) {
      if constexpr (std::is_same_v<TCpp, StringView>) {
        ASSERT_EQ(
            var.template value<KIND>(), std::string(simpleVector->valueAt(i)));
      } else {
        ASSERT_EQ(var.template value<KIND>(), simpleVector->valueAt(i));
      }
    }

    verifyConstantToString(type, baseVector);
  }

  template <TypeKind KIND>
  void testComplexConstant(const TypePtr& type, const VectorPtr& vector) {
    ASSERT_EQ(KIND, type->kind());
    auto baseVector = BaseVector::wrapInConstant(size_, 0, vector);
    auto simpleVector = baseVector->template as<
        SimpleVector<typename KindToFlatVector<KIND>::WrapperType>>();
    ASSERT_TRUE(simpleVector != nullptr);

    ASSERT_EQ(KIND, simpleVector->typeKind());
    ASSERT_EQ(size_, simpleVector->size());
    ASSERT_FALSE(simpleVector->isScalar());

    for (auto i = 0; i < simpleVector->size(); i++) {
      ASSERT_TRUE(vector->equalValueAt(baseVector.get(), 0, i));
    }

    verifyConstantToString(type, baseVector);
  }

  void verifyConstantToString(const TypePtr& type, const VectorPtr& constant) {
    auto expectedStr = fmt::format(
        "[CONSTANT {}: {} elements, {}]",
        type->toString(),
        size_,
        constant->toString(0));
    EXPECT_EQ(expectedStr, constant->toString());
    for (auto i = 1; i < constant->size(); ++i) {
      EXPECT_EQ(constant->toString(0), constant->toString(i));
    }
  }

  template <TypeKind KIND>
  void testNullConstant(const TypePtr& type) {
    ASSERT_EQ(KIND, type->kind());
    auto baseVector = BaseVector::createNullConstant(type, size_, pool_.get());
    auto simpleVector = baseVector->template as<
        SimpleVector<typename KindToFlatVector<KIND>::WrapperType>>();
    ASSERT_TRUE(simpleVector != nullptr);

    ASSERT_EQ(KIND, simpleVector->typeKind());
    ASSERT_TRUE(type->equivalent(*simpleVector->type()));
    ASSERT_EQ(size_, simpleVector->size());
    ASSERT_EQ(simpleVector->isScalar(), TypeTraits<KIND>::isPrimitiveType);

    for (auto i = 0; i < simpleVector->size(); i++) {
      ASSERT_TRUE(simpleVector->isNullAt(i));
    }

    auto expectedStr = fmt::format(
        "[CONSTANT {}: {} elements, null]", type->toString(), size_);
    EXPECT_EQ(expectedStr, baseVector->toString());
    for (auto i = 1; i < baseVector->size(); ++i) {
      EXPECT_EQ(baseVector->toString(0), baseVector->toString(i));
    }
  }

 protected:
  // Theoretical "size" of the constant vector created.
  const size_t size_{23};
};

TEST_F(VectorCreateConstantTest, scalar) {
  testPrimitiveConstant<TypeKind::BIGINT>(-123456789);
  testPrimitiveConstant<TypeKind::INTEGER>(98765);
  testPrimitiveConstant<TypeKind::SMALLINT>(1234);
  testPrimitiveConstant<TypeKind::TINYINT>(123);

  testPrimitiveConstant<TypeKind::BOOLEAN>(true);
  testPrimitiveConstant<TypeKind::BOOLEAN>(false);

  testPrimitiveConstant<TypeKind::REAL>(99.98);
  testPrimitiveConstant<TypeKind::DOUBLE>(12.345);

  testPrimitiveConstant<TypeKind::VARCHAR>(StringView("hello world"));
  testPrimitiveConstant<TypeKind::VARBINARY>(StringView("my binary buffer"));
}

TEST_F(VectorCreateConstantTest, complex) {
  testComplexConstant<TypeKind::ARRAY>(
      ARRAY(INTEGER()),
      makeArrayVector<int32_t>(
          1, [](auto) { return 10; }, [](auto i) { return i; }));

  testComplexConstant<TypeKind::MAP>(
      MAP(INTEGER(), REAL()),
      makeMapVector<int32_t, float>(
          1,
          [](auto) { return 10; },
          [](auto i) { return i; },
          [](auto i) { return i; }));

  testComplexConstant<TypeKind::ROW>(
      ROW({{"c0", INTEGER()}}),
      makeRowVector({makeFlatVector<int32_t>(1, [](auto i) { return i; })}));
}

TEST_F(VectorCreateConstantTest, null) {
  testNullConstant<TypeKind::BIGINT>(BIGINT());
  testNullConstant<TypeKind::INTEGER>(INTEGER());
  testNullConstant<TypeKind::SMALLINT>(SMALLINT());
  testNullConstant<TypeKind::TINYINT>(TINYINT());

  testNullConstant<TypeKind::BOOLEAN>(BOOLEAN());

  testNullConstant<TypeKind::REAL>(REAL());
  testNullConstant<TypeKind::DOUBLE>(DOUBLE());
  testNullConstant<TypeKind::BIGINT>(DECIMAL(10, 5));
  testNullConstant<TypeKind::HUGEINT>(DECIMAL(20, 5));

  testNullConstant<TypeKind::TIMESTAMP>(TIMESTAMP());
  testNullConstant<TypeKind::DATE>(DATE());
  testNullConstant<TypeKind::BIGINT>(INTERVAL_DAY_TIME());

  testNullConstant<TypeKind::VARCHAR>(VARCHAR());
  testNullConstant<TypeKind::VARBINARY>(VARBINARY());

  testNullConstant<TypeKind::ROW>(ROW({BIGINT(), REAL()}));
  testNullConstant<TypeKind::ARRAY>(ARRAY(DOUBLE()));
  testNullConstant<TypeKind::MAP>(MAP(INTEGER(), DOUBLE()));
}

class TestingHook : public ValueHook {
 public:
  TestingHook(RowSet rows, SimpleVector<int64_t>* values)
      : rows_(rows), values_(values) {}

  bool acceptsNulls() const override {
    return false;
  }

  void addValue(vector_size_t row, const void* value) override {
    if (values_->isNullAt(rows_[row])) {
      ++errors_;
    } else {
      auto original = values_->valueAt(rows_[row]);
      if (original != *reinterpret_cast<const int64_t*>(value)) {
        ++errors_;
      }
    }
  }
  int32_t errors() const {
    return errors_;
  }

 private:
  RowSet rows_;
  SimpleVector<int64_t>* values_;
  int32_t errors_ = 0;
};

TEST_F(VectorTest, valueHook) {
  VectorPtr values = createScalar<TypeKind::BIGINT>(BIGINT(), 1000, true);
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      BIGINT(),
      values->size(),
      std::make_unique<TestingLoader>(values));
  std::vector<vector_size_t> rows;
  for (int i = 0; i < values->size(); i += 2) {
    rows.push_back(i);
  }
  TestingHook hook(rows, values->as<SimpleVector<int64_t>>());
  // The hook is called with the row number being an index 'i' into
  // 'rows' and the value being the value at rows[i]. The hook is not
  // called if the value at the row is null because the hook does not
  // accept nulls.  In this way, for the use case of aggregation, the
  // row number is directly usable as an index into an array of groups
  // to update. The caller will compose the RowSet based on what rows
  // have been filtered out beteen creating the LazyVector and the
  // point of loading. The hook itself is concerned with the loaded
  // subset of rows and the position of each row within this set.
  lazy->load(rows, &hook);
  EXPECT_EQ(hook.errors(), 0);
}

TEST_F(VectorTest, byteSize) {
  constexpr vector_size_t count = std::numeric_limits<vector_size_t>::max();
  constexpr uint64_t expected = count * sizeof(int64_t);
  EXPECT_EQ(BaseVector::byteSize<int64_t>(count), expected);
}

TEST_F(VectorTest, clearNulls) {
  auto vectorSize = 100;
  auto vector = BaseVector::create(INTEGER(), vectorSize, pool_.get());
  ASSERT_FALSE(vector->mayHaveNulls());

  // No op if doesn't have nulls
  SelectivityVector selection{vectorSize};
  vector->clearNulls(selection);
  ASSERT_FALSE(vector->mayHaveNulls());

  // De-allocate nulls if all selected
  auto rawNulls = vector->mutableRawNulls();
  ASSERT_EQ(bits::countNulls(rawNulls, 0, vectorSize), 0);
  bits::setNull(rawNulls, 50);
  ASSERT_TRUE(vector->isNullAt(50));
  ASSERT_EQ(bits::countNulls(rawNulls, 0, vectorSize), 1);
  vector->clearNulls(selection);
  ASSERT_FALSE(vector->mayHaveNulls());

  // Clear within vectorSize
  rawNulls = vector->mutableRawNulls();
  bits::setNull(rawNulls, 50);
  bits::setNull(rawNulls, 70);
  selection.clearAll();
  selection.setValidRange(40, 60, true);
  selection.updateBounds();
  vector->clearNulls(selection);
  ASSERT_TRUE(!vector->isNullAt(50));
  ASSERT_TRUE(vector->isNullAt(70));

  // Clear with end > vector size
  selection.resize(120);
  selection.clearAll();
  selection.setValidRange(60, 120, true);
  selection.updateBounds();
  vector->clearNulls(selection);
  ASSERT_TRUE(!vector->isNullAt(70));
  ASSERT_TRUE(vector->mayHaveNulls());

  // Clear with begin > vector size
  rawNulls = vector->mutableRawNulls();
  bits::setNull(rawNulls, 70);
  selection.clearAll();
  selection.setValidRange(100, 120, true);
  selection.updateBounds();
  vector->clearNulls(selection);
  ASSERT_TRUE(vector->isNullAt(70));
}

TEST_F(VectorTest, setStringToNull) {
  constexpr int32_t kSize = 100;
  auto target = makeFlatVector<StringView>(
      kSize, [](auto /*row*/) { return StringView("Non-inlined string"); });
  target->setNull(kSize - 1, true);
  auto unknownNull = std::make_shared<ConstantVector<UnknownValue>>(
      pool_.get(), kSize, true, UNKNOWN(), UnknownValue());

  auto stringNull = BaseVector::wrapInConstant(kSize, kSize - 1, target);
  SelectivityVector rows(kSize, false);
  rows.setValid(2, true);
  rows.updateBounds();
  target->copy(unknownNull.get(), rows, nullptr);
  EXPECT_TRUE(target->isNullAt(2));

  rows.setValid(4, true);
  rows.updateBounds();
  target->copy(stringNull.get(), rows, nullptr);
  EXPECT_TRUE(target->isNullAt(4));
  auto nulls = AlignedBuffer::allocate<uint64_t>(
      bits::nwords(kSize), pool_.get(), bits::kNull64);
  auto flatNulls = std::make_shared<FlatVector<UnknownValue>>(
      pool_.get(),
      UNKNOWN(),
      nulls,
      kSize,
      BufferPtr(nullptr),
      std::vector<BufferPtr>());
  rows.setValid(6, true);
  rows.updateBounds();
  target->copy(flatNulls.get(), rows, nullptr);
  EXPECT_TRUE(target->isNullAt(6));
  EXPECT_EQ(4, bits::countNulls(target->rawNulls(), 0, kSize));
}

TEST_F(VectorTest, clearAllNulls) {
  auto vectorSize = 100;
  auto vector = BaseVector::create(INTEGER(), vectorSize, pool_.get());
  ASSERT_FALSE(vector->mayHaveNulls());

  auto rawNulls = vector->mutableRawNulls();
  ASSERT_EQ(bits::countNulls(rawNulls, 0, vectorSize), 0);
  bits::setNull(rawNulls, 50);
  ASSERT_TRUE(vector->isNullAt(50));
  ASSERT_EQ(bits::countNulls(rawNulls, 0, vectorSize), 1);
  vector->clearAllNulls();
  ASSERT_FALSE(vector->mayHaveNulls());
  ASSERT_FALSE(vector->isNullAt(50));
}

TEST_F(VectorTest, constantSetNull) {
  auto vector = makeConstant<int64_t>(123, 10);

  VELOX_ASSERT_THROW(
      vector->setNull(0, true), "setNull not supported on ConstantVector");
}

/// Test lazy vector wrapped in multiple layers of dictionaries.
TEST_F(VectorTest, multipleDictionariesOverLazy) {
  vector_size_t size = 10;
  auto indices = makeIndices(size, [&](auto row) { return size - row - 1; });
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      size,
      std::make_unique<TestingLoader>(
          makeFlatVector<int32_t>(size, [](auto row) { return row; })));

  auto dict = BaseVector::wrapInDictionary(
      nullptr,
      indices,
      size,
      BaseVector::wrapInDictionary(nullptr, indices, size, lazy));
  dict->loadedVector();
  for (auto i = 0; i < size; i++) {
    ASSERT_EQ(i, dict->as<SimpleVector<int32_t>>()->valueAt(i));
  }
}

/// Test lazy loading of nested dictionary vector
TEST_F(VectorTest, selectiveLoadingOfLazyDictionaryNested) {
  vector_size_t size = 10;
  auto indices =
      makeIndices(size, [&](auto row) { return (row % 2 == 0) ? row : 0; });
  auto data = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto loader = std::make_unique<TestingLoader>(data);
  auto loaderPtr = loader.get();
  auto lazyVector = std::make_shared<LazyVector>(
      pool_.get(), INTEGER(), size, std::move(loader));

  auto indicesInner =
      makeIndices(size, [&](auto row) { return (row % 2 == 0) ? row : 0; });
  auto indicesOuter =
      makeIndices(size, [&](auto row) { return (row % 4 == 0) ? row : 0; });

  auto dict = BaseVector::wrapInDictionary(
      nullptr,
      indicesOuter,
      size,
      BaseVector::wrapInDictionary(nullptr, indicesInner, size, lazyVector));

  dict->loadedVector();
  ASSERT_EQ(loaderPtr->rowCount(), 1 + size / 4);

  dict->loadedVector();
  ASSERT_EQ(loaderPtr->rowCount(), 1 + size / 4);
}

TEST_F(VectorTest, nestedLazy) {
  // Verify that explicit checks are triggered that ensure lazy vectors cannot
  // be nested within two different top level vectors.
  vector_size_t size = 10;
  auto indexAt = [](vector_size_t) { return 0; };
  auto makeLazy = [&]() {
    return std::make_shared<LazyVector>(
        pool_.get(),
        INTEGER(),
        size,
        std::make_unique<TestingLoader>(
            makeFlatVector<int64_t>(size, [](auto row) { return row; })));
  };
  auto lazy = makeLazy();
  auto dict = BaseVector::wrapInDictionary(
      nullptr, makeIndices(size, indexAt), size, lazy);

  VELOX_ASSERT_THROW(
      BaseVector::wrapInDictionary(
          nullptr, makeIndices(size, indexAt), size, lazy),
      "An unloaded lazy vector cannot be wrapped by two different top level"
      " vectors.");

  // Verify that the unloaded dictionary can be nested as long as it has one top
  // level vector.
  EXPECT_NO_THROW(BaseVector::wrapInDictionary(
      nullptr, makeIndices(size, indexAt), size, dict));

  // Limitation: Current checks cannot prevent existing references of the lazy
  // vector to load rows. For example, the following would succeed even though
  // it was wrapped under 2 layer of dictionary above:
  EXPECT_FALSE(lazy->isLoaded());
  EXPECT_NO_THROW(lazy->loadedVector());
  EXPECT_TRUE(lazy->isLoaded());
}

TEST_F(VectorTest, dictionaryResize) {
  vector_size_t size = 10;
  std::vector<int64_t> elements{0, 1, 2, 3, 4};
  auto makeIndicesFn = [&]() {
    return makeIndices(size, [&](auto row) { return row % elements.size(); });
  };

  auto indices = makeIndicesFn();
  auto flatVector = makeFlatVector<int64_t>(elements);

  // Create a simple dictionary.
  auto dict = wrapInDictionary(std::move(indices), size, flatVector);

  auto expectedValues = std::vector<int64_t>{0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
  auto expectedVector = makeFlatVector<int64_t>(expectedValues);
  test::assertEqualVectors(expectedVector, dict);

  // Double size.
  dict->resize(size * 2);

  // Check all the newly resized indices point to 0th value.
  expectedVector = makeFlatVector<int64_t>(
      {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
  test::assertEqualVectors(expectedVector, dict);

  // Resize  a nested dictionary.
  auto innerDict = wrapInDictionary(makeIndicesFn(), size, flatVector);
  auto outerDict = wrapInDictionary(makeIndicesFn(), size, innerDict);

  expectedVector = makeFlatVector<int64_t>(expectedValues);
  test::assertEqualVectors(expectedVector, outerDict);
  innerDict->resize(size * 2);
  // Check that the outer dictionary remains unaffected.
  test::assertEqualVectors(expectedVector, outerDict);

  // Resize a shared nested dictionary with shared indices.
  indices = makeIndicesFn();
  dict = wrapInDictionary(
      indices, size, wrapInDictionary(indices, size, flatVector));

  ASSERT_TRUE(!indices->unique());
  dict->resize(size * 2);
  expectedVector = makeFlatVector<int64_t>(
      {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
  test::assertEqualVectors(expectedVector, dict);
  // Check to ensure that indices has not changed.
  ASSERT_EQ(indices->size(), size * sizeof(vector_size_t));
}

TEST_F(VectorTest, acquireSharedStringBuffers) {
  const int numBuffers = 10;
  std::vector<BufferPtr> buffers;
  const int bufferSize = 100;
  for (int i = 0; i < numBuffers; ++i) {
    buffers.push_back(AlignedBuffer::allocate<char>(bufferSize, pool_.get()));
  }
  auto vector = BaseVector::create(VARCHAR(), 100, pool_.get());
  auto flatVector = vector->as<FlatVector<StringView>>();
  EXPECT_EQ(0, flatVector->stringBuffers().size());

  flatVector->setStringBuffers(buffers);
  EXPECT_EQ(numBuffers, flatVector->stringBuffers().size());
  for (int i = 0; i < numBuffers; ++i) {
    EXPECT_EQ(buffers[i], flatVector->stringBuffers()[i]);
  }

  flatVector->setStringBuffers({});
  EXPECT_EQ(0, flatVector->stringBuffers().size());

  int numSourceVectors = 2;
  std::vector<VectorPtr> sourceVectors;
  for (int i = 0; i < numSourceVectors; ++i) {
    sourceVectors.push_back(BaseVector::create(VARCHAR(), 100, pool_.get()));
    sourceVectors.back()->asFlatVector<StringView>()->setStringBuffers(
        {buffers[i]});
  }
  flatVector->setStringBuffers({buffers[0]});
  EXPECT_EQ(1, flatVector->stringBuffers().size());
  flatVector->acquireSharedStringBuffers(sourceVectors[0].get());
  EXPECT_EQ(1, flatVector->stringBuffers().size());
  flatVector->acquireSharedStringBuffers(sourceVectors[1].get());
  EXPECT_EQ(2, flatVector->stringBuffers().size());

  flatVector->acquireSharedStringBuffers(sourceVectors[0].get());
  flatVector->acquireSharedStringBuffers(sourceVectors[1].get());
  EXPECT_EQ(2, flatVector->stringBuffers().size());
  for (int i = 0; i < numSourceVectors; ++i) {
    EXPECT_EQ(buffers[i], flatVector->stringBuffers()[i]);
  }

  // insert with duplicate buffers and expect an exception.
  flatVector->setStringBuffers({buffers[0], buffers[0]});
  EXPECT_EQ(2, flatVector->stringBuffers().size());
  flatVector->acquireSharedStringBuffers(sourceVectors[0].get());
  EXPECT_EQ(2, flatVector->stringBuffers().size());

  // Function does not throw if the input is not varchar or varbinary.
  flatVector->setStringBuffers({buffers[0]});
  auto arrayVector = makeArrayVector<int32_t>({});
  ASSERT_NO_THROW(flatVector->acquireSharedStringBuffers(arrayVector.get()));

  auto unkownVector = BaseVector::create(UNKNOWN(), 100, pool_.get());
  ASSERT_NO_THROW(flatVector->acquireSharedStringBuffers(unkownVector.get()));
}

TEST_F(VectorTest, acquireSharedStringBuffersRecursive) {
  auto vector = BaseVector::create(VARCHAR(), 100, pool_.get());
  auto flatVector = vector->as<FlatVector<StringView>>();

  auto testWithEncodings = [&](const VectorPtr& source,
                               const std::function<void()>& check) {
    flatVector->setStringBuffers({});
    flatVector->acquireSharedStringBuffersRecursive(source.get());
    check();

    // Constant vector.
    auto constantVector = BaseVector::wrapInConstant(10, 0, source);
    flatVector->setStringBuffers({});
    flatVector->acquireSharedStringBuffersRecursive(constantVector.get());
    check();

    // Dictionary Vector.
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(source->size(), pool_.get());
    for (int32_t i = 0; i < source->size(); ++i) {
      indices->asMutable<vector_size_t>()[i] = source->size() - i - 1;
    }
    auto dictionaryVector =
        BaseVector::wrapInDictionary(nullptr, indices, source->size(), source);
    flatVector->setStringBuffers({});
    flatVector->acquireSharedStringBuffersRecursive(dictionaryVector.get());
    check();
  };

  // Acquiring buffer from array

  { // Array<int>
    auto arrayVector = makeArrayVector<int32_t>({{1, 2, 3}});
    testWithEncodings(arrayVector, [&]() {
      ASSERT_EQ(flatVector->stringBuffers().size(), 0);
    });
  }

  { // Array<Varchar>
    auto arrayVector = makeArrayVector<StringView>(
        {{"This is long enough not to be inlined !!!", "b"}});

    testWithEncodings(arrayVector, [&]() {
      EXPECT_EQ(1, flatVector->stringBuffers().size());
      ASSERT_EQ(
          flatVector->stringBuffers()[0],
          arrayVector->as<ArrayVector>()
              ->elements()
              ->asFlatVector<StringView>()
              ->stringBuffers()[0]);
    });
  }

  // Array<Array<Varchar>>
  auto arrayVector = makeNullableNestedArrayVector<StringView>(
      {{{{{"This is long enough not to be inlined !!!"_sv}}}}});

  testWithEncodings(arrayVector, [&]() {
    EXPECT_EQ(1, flatVector->stringBuffers().size());
    ASSERT_EQ(
        flatVector->stringBuffers()[0],
        arrayVector->as<ArrayVector>()
            ->elements()
            ->as<ArrayVector>()
            ->elements()
            ->asFlatVector<StringView>()
            ->stringBuffers()[0]);
  });

  // Map<Varchar,Varchar>
  auto mapVector = makeMapVector<StringView, StringView>(
      {{{"This is long enough not to be inlined !!!"_sv,
         "This is long enough not to be inlined !!!"_sv}}});

  testWithEncodings(mapVector, [&]() {
    EXPECT_EQ(2, flatVector->stringBuffers().size());
    ASSERT_EQ(
        flatVector->stringBuffers()[0],
        mapVector->as<MapVector>()
            ->mapKeys()
            ->asFlatVector<StringView>()
            ->stringBuffers()[0]);
    ASSERT_EQ(
        flatVector->stringBuffers()[1],
        mapVector->as<MapVector>()
            ->mapValues()
            ->asFlatVector<StringView>()
            ->stringBuffers()[0]);
  });

  // Row
  flatVector->setStringBuffers({});
  auto rowVector =
      makeRowVector({mapVector, arrayVector, makeFlatVector<int32_t>({1, 2})});
  testWithEncodings(
      rowVector, [&]() { EXPECT_EQ(3, flatVector->stringBuffers().size()); });
}

/// Test MapVector::canonicalize for a MapVector with 'values' vector shorter
/// than 'keys' vector.
TEST_F(VectorTest, mapCanonicalizeValuesShorterThenKeys) {
  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), BIGINT()),
      nullptr,
      2,
      makeIndices({0, 2}),
      makeIndices({2, 2}),
      makeFlatVector<int64_t>({7, 6, 5, 4, 3, 2, 1}),
      makeFlatVector<int64_t>({6, 5, 4, 3, 2, 1}));
  EXPECT_FALSE(mapVector->hasSortedKeys());

  MapVector::canonicalize(mapVector);
  EXPECT_TRUE(mapVector->hasSortedKeys());

  // Verify that keys and values referenced from the map are sorted. Keys and
  // values not referenced from the map are not sorted.
  test::assertEqualVectors(
      makeFlatVector<int64_t>({6, 7, 4, 5, 3, 2}), mapVector->mapKeys());
  test::assertEqualVectors(
      makeFlatVector<int64_t>({5, 6, 3, 4, 2, 1}), mapVector->mapValues());
}

TEST_F(VectorTest, mapCanonicalize) {
  auto mapVector = makeMapVector<int64_t, int64_t>({
      {{4, 40}, {3, 30}, {2, 20}, {5, 50}, {1, 10}},
      {{4, 41}},
      {},
  });

  EXPECT_FALSE(mapVector->hasSortedKeys());

  test::assertEqualVectors(
      mapVector->mapKeys(), makeFlatVector<int64_t>({4, 3, 2, 5, 1, 4}));
  test::assertEqualVectors(
      mapVector->mapValues(),
      makeFlatVector<int64_t>({40, 30, 20, 50, 10, 41}));

  // Sort keys.
  MapVector::canonicalize(mapVector);
  EXPECT_TRUE(mapVector->hasSortedKeys());

  test::assertEqualVectors(
      mapVector->mapKeys(), makeFlatVector<int64_t>({1, 2, 3, 4, 5, 4}));
  test::assertEqualVectors(
      mapVector->mapValues(),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 41}));

  EXPECT_EQ(mapVector->sizeAt(0), 5);
  EXPECT_EQ(mapVector->offsetAt(0), 0);

  EXPECT_EQ(mapVector->sizeAt(1), 1);
  EXPECT_EQ(mapVector->offsetAt(1), 5);

  EXPECT_EQ(mapVector->sizeAt(2), 0);
  EXPECT_EQ(mapVector->offsetAt(2), 6);
}

TEST_F(VectorTest, mapCreateSorted) {
  BufferPtr offsets = makeIndices({0, 5, 6});
  BufferPtr sizes = makeIndices({5, 1, 0});
  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), BIGINT()),
      nullptr,
      3,
      offsets,
      sizes,
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 4}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 41}),
      std::nullopt,
      true);

  EXPECT_TRUE(mapVector->hasSortedKeys());
}

TEST_F(VectorTest, flatSliceMutability) {
  auto vec = makeFlatVector<int64_t>(
      10,
      [](vector_size_t i) { return i; },
      [](vector_size_t i) { return i % 3 == 0; });
  auto slice = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec->slice(0, 3));
  ASSERT_TRUE(slice);
  slice->setNull(0, false);
  EXPECT_FALSE(slice->isNullAt(0));
  EXPECT_TRUE(vec->isNullAt(0));
  slice = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec->slice(0, 3));
  slice->mutableRawValues()[2] = -1;
  EXPECT_EQ(slice->valueAt(2), -1);
  EXPECT_EQ(vec->valueAt(2), 2);
}

TEST_F(VectorTest, arraySliceMutability) {
  testArrayOrMapSliceMutability(
      std::dynamic_pointer_cast<ArrayVector>(createArray(vectorSize_, false)));
}

TEST_F(VectorTest, mapSliceMutability) {
  testArrayOrMapSliceMutability(
      std::dynamic_pointer_cast<MapVector>(createMap(vectorSize_, false)));
}

// Demonstrates incorrect usage of the memory pool. The pool is destroyed
// while the vector allocated from it is still alive.
TEST_F(VectorTest, lifetime) {
  ASSERT_DEATH(
      {
        auto childPool = memory::addDefaultLeafMemoryPool();
        auto v = BaseVector::create(INTEGER(), 10, childPool.get());

        // BUG: Memory pool needs to stay alive until all memory allocated from
        // it is freed.
        childPool.reset();
        v.reset();
      },
      "");
}

TEST_F(VectorTest, ensureNullsCapacity) {
  // Has to be more than 1 byte's worth of bits.
  size_t size = 100;
  auto vec = makeFlatVector<int64_t>(size, [](vector_size_t i) { return i; });
  auto nulls = vec->mutableNulls(2);
  ASSERT_GE(nulls->size(), bits::nbytes(size));
}

TEST_F(VectorTest, createVectorWithNullType) {
  std::string kErrorMessage = "Vector creation requires a non-null type.";

  VELOX_ASSERT_THROW(BaseVector::create(nullptr, 100, pool()), kErrorMessage);
  VELOX_ASSERT_THROW(
      BaseVector::createNullConstant(nullptr, 100, pool()), kErrorMessage);
  VELOX_ASSERT_THROW(
      BaseVector::getOrCreateEmpty(nullptr, nullptr, pool()), kErrorMessage);

  VELOX_ASSERT_THROW(
      std::make_shared<ConstantVector<int64_t>>(pool(), 100, false, nullptr, 0),
      kErrorMessage);

  std::vector<BufferPtr> stringBuffers;
  VELOX_ASSERT_THROW(
      std::make_shared<FlatVector<int64_t>>(
          pool(), nullptr, nullptr, 100, nullptr, std::move(stringBuffers)),
      kErrorMessage);

  std::vector<VectorPtr> children;
  VELOX_ASSERT_THROW(
      std::make_shared<RowVector>(pool(), nullptr, nullptr, 100, children),
      kErrorMessage);

  VELOX_ASSERT_THROW(RowVector::createEmpty(nullptr, pool()), kErrorMessage);

  VELOX_ASSERT_THROW(
      std::make_shared<ArrayVector>(
          pool(), nullptr, nullptr, 100, nullptr, nullptr, nullptr),
      kErrorMessage);

  VELOX_ASSERT_THROW(
      std::make_shared<MapVector>(
          pool(), nullptr, nullptr, 100, nullptr, nullptr, nullptr, nullptr),
      kErrorMessage);
}

TEST_F(VectorTest, testCopyWithZeroCount) {
  auto runTest = [&](const VectorPtr& vector) {
    // We pass invalid targetIndex and sourceIndex and expect the
    // function not to throw since count is 0.
    ASSERT_NO_THROW(
        vector->copy(vector.get(), vector->size() + 1, vector->size() + 1, 0));

    BaseVector::CopyRange range{vector->size() + 1, vector->size() + 1, 0};
    ASSERT_NO_THROW(vector->copyRanges(vector.get(), folly::Range(&range, 1)));

    ASSERT_NO_THROW(vector->copyRanges(
        vector.get(), std::vector<BaseVector::CopyRange>{range, range, range}));
  };

  // Flat.
  runTest(makeFlatVector<bool>({1, 0, 1, 1}));
  runTest(makeFlatVector<StringView>({"s"_sv}));
  runTest(makeFlatVector<int32_t>({1, 2}));

  // Complex types.
  runTest(makeArrayVector<int32_t>(
      1, [](auto) { return 10; }, [](auto i) { return i; }));

  runTest(makeMapVector<int32_t, float>(
      1,
      [](auto) { return 10; },
      [](auto i) { return i; },
      [](auto i) { return i; }));

  runTest(
      makeRowVector({makeFlatVector<int32_t>(1, [](auto i) { return i; })}));
}

TEST_F(VectorTest, flattenVector) {
  auto test = [&](VectorPtr& vector, bool stayTheSame) {
    auto original = vector;
    BaseVector::flattenVector(vector);
    if (stayTheSame) {
      EXPECT_EQ(original.get(), vector.get());
    } else {
      EXPECT_NE(original.get(), vector.get());
    }
    test::assertEqualVectors(original, vector);
  };

  // Flat input.
  VectorPtr flat = makeFlatVector<int32_t>({1, 2, 3});
  test(flat, true);

  VectorPtr array = makeArrayVector<int32_t>({{1, 2, 3}, {1}, {1}});
  test(array, true);

  VectorPtr map =
      makeMapVector<int32_t, int32_t>({{{4, 40}, {3, 30}}, {{4, 41}}, {}});
  test(map, true);

  VectorPtr row = makeRowVector({flat, array, map});
  test(row, true);

  // Constant
  VectorPtr constant = BaseVector::wrapInConstant(100, 1, flat);
  test(constant, false);
  EXPECT_TRUE(constant->isFlatEncoding());

  // Dictionary
  VectorPtr dictionary = BaseVector::wrapInDictionary(
      nullptr, makeIndices(100, [](auto row) { return row % 2; }), 100, flat);
  test(dictionary, false);
  EXPECT_TRUE(dictionary->isFlatEncoding());

  // Array with constant elements.
  auto* arrayVector = array->as<ArrayVector>();
  arrayVector->elements() = BaseVector::wrapInConstant(100, 1, flat);
  auto originalElements = arrayVector->elements();
  auto original = array;

  BaseVector::flattenVector(array);
  test::assertEqualVectors(original, array);

  EXPECT_EQ(array->encoding(), VectorEncoding::Simple::ARRAY);
  EXPECT_TRUE(array->as<ArrayVector>()->elements()->isFlatEncoding());

  EXPECT_EQ(original.get(), array.get());
  EXPECT_NE(originalElements.get(), array->as<ArrayVector>()->elements().get());

  // Map with constant keys and values.
  auto* mapVector = map->as<MapVector>();
  auto originalValues = mapVector->mapValues() =
      BaseVector::wrapInConstant(100, 1, flat);
  auto originalKeys = mapVector->mapKeys() =
      BaseVector::wrapInConstant(100, 1, flat);

  original = map;
  BaseVector::flattenVector(map);
  test::assertEqualVectors(original, map);

  EXPECT_EQ(map->encoding(), VectorEncoding::Simple::MAP);
  EXPECT_TRUE(map->as<MapVector>()->mapValues()->isFlatEncoding());
  EXPECT_TRUE(map->as<MapVector>()->mapKeys()->isFlatEncoding());

  EXPECT_EQ(original.get(), map.get());
  EXPECT_NE(originalValues.get(), map->as<MapVector>()->mapValues().get());
  EXPECT_NE(originalKeys.get(), map->as<MapVector>()->mapKeys().get());

  // Row with constant field.
  row = makeRowVector({flat, BaseVector::wrapInConstant(3, 1, flat)});
  auto* rowVector = row->as<RowVector>();
  auto originalRow0 = rowVector->children()[0];
  auto originalRow1 = rowVector->children()[1];
  original = row;
  BaseVector::flattenVector(row);
  EXPECT_EQ(row->encoding(), VectorEncoding::Simple::ROW);
  EXPECT_TRUE(row->as<RowVector>()->children()[0]->isFlatEncoding());
  EXPECT_TRUE(row->as<RowVector>()->children()[1]->isFlatEncoding());
  test::assertEqualVectors(original, row);

  EXPECT_EQ(original.get(), row.get());
  EXPECT_EQ(originalRow0.get(), row->as<RowVector>()->children()[0].get());
  EXPECT_NE(originalRow1.get(), row->as<RowVector>()->children()[1].get());

  // Row with constant field.
  // Null input
  VectorPtr nullVector = nullptr;
  BaseVector::flattenVector(nullVector);
  EXPECT_EQ(nullVector, nullptr);
}

TEST_F(VectorTest, findDuplicateValue) {
  const CompareFlags flags;
  auto data = makeFlatVector<int64_t>({1, 3, 2, 4, 3, 5, 4, 6});

  // No duplicates in the first 4 values.
  auto dup = data->findDuplicateValue(0, 4, flags);
  ASSERT_FALSE(dup.has_value());

  // No duplicates in the last 4 values.
  dup = data->findDuplicateValue(4, 4, flags);
  ASSERT_FALSE(dup.has_value());

  // No duplicates in rows 2 to 6.
  dup = data->findDuplicateValue(2, 4, flags);
  ASSERT_FALSE(dup.has_value());

  // Values in rows 1 and 4 are the same.
  dup = data->findDuplicateValue(0, 5, flags);
  ASSERT_TRUE(dup.has_value());
  ASSERT_EQ(4, dup.value());

  // Values in rows 3 and 7 are the same.
  dup = data->findDuplicateValue(2, 6, flags);
  ASSERT_TRUE(dup.has_value());
  ASSERT_EQ(6, dup.value());

  // Verify that empty range doesn't throw.
  dup = data->findDuplicateValue(2, 0, flags);
  ASSERT_FALSE(dup.has_value());
}
