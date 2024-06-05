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

#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

using dwio::common::MemorySink;

class E2EFilterTest : public E2EFilterTestBase, public test::VectorTestBase {
 protected:
  void SetUp() override {
    E2EFilterTestBase::SetUp();
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations) {
    testScenario(columns, customize, wrapInStruct, filterable, numCombinations);

    // Always test no null case.
    auto newCustomize = [&]() {
      if (customize) {
        customize();
      }
      makeNotNull(0);
    };
    testScenario(
        columns, newCustomize, wrapInStruct, filterable, numCombinations);
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip = false) override {
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = leafPool_.get()});
    auto* sinkPtr = sink.get();
    options_.memoryPool = E2EFilterTestBase::rootPool_.get();
    int32_t flushCounter = 0;
    options_.flushPolicyFactory = [&]() {
      return std::make_unique<LambdaFlushPolicy>(
          rowsInRowGroup_, bytesInRowGroup_, [&]() {
            return forRowGroupSkip
                ? false
                : (++flushCounter % flushEveryNBatches_ == 0);
          });
    };

    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), options_, asRowType(type));
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
    writer_->close();
    sinkData_ = std::string_view(sinkPtr->data(), sinkPtr->size());
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) override {
    return std::make_unique<ParquetReader>(std::move(input), opts);
  }

  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  facebook::velox::parquet::WriterOptions options_;
  uint64_t rowsInRowGroup_ = 10'000;
  int64_t bytesInRowGroup_ = 128 * 1'024 * 1'024;
};

TEST_F(E2EFilterTest, writerMagic) {
  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  batches.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 20000, *leafPool_, nullptr, 0)));
  writeToMemory(rowType_, batches, false);
  auto data = sinkData_.data();
  auto size = sinkData_.size();
  EXPECT_EQ("PAR1", std::string(data, 4));
  EXPECT_EQ("PAR1", std::string(data + size - 4, 4));
}

TEST_F(E2EFilterTest, boolean) {
  testWithTypes(
      "boolean_val:boolean,"
      "boolean_null:boolean",
      [&]() { makeAllNulls("boolean_null"); },
      true,
      {"boolean_val"},
      20);
}

TEST_F(E2EFilterTest, integerDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, integerDeltaBinaryPack) {
  options_.enableDictionary = false;
  options_.encoding =
      facebook::velox::parquet::arrow::Encoding::DELTA_BINARY_PACKED;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, compression) {
  for (const auto compression :
       {common::CompressionKind_SNAPPY,
        common::CompressionKind_ZSTD,
        common::CompressionKind_GZIP,
        common::CompressionKind_NONE,
        common::CompressionKind_LZ4}) {
    if (!facebook::velox::parquet::Writer::isCodecAvailable(compression)) {
      continue;
    }

    options_.dataPageSize = 4 * 1024;
    options_.compression = compression;

    testWithTypes(
        "tinyint_val:tinyint,"
        "short_val:smallint,"
        "int_val:int,"
        "long_val:bigint",
        [&]() {
          makeIntDistribution<int64_t>(
              "long_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -9999, // rareMin
              10000000000, // rareMax
              true); // keepNulls

          makeIntDistribution<int32_t>(
              "int_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -9999, // rareMin
              100000000, // rareMax
              false); // keepNulls

          makeIntDistribution<int16_t>(
              "short_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true); // keepNulls

          makeIntDistribution<int8_t>(
              "tinyint_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -99, // rareMin
              3000, // rareMax
              true); // keepNulls
        },
        true,
        {"tinyint_val", "short_val", "int_val", "long_val"},
        3);
  }
}

TEST_F(E2EFilterTest, integerDictionary) {
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int64_t>(
            "long_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls

        makeIntDistribution<int32_t>(
            "int_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls

        makeIntDistribution<int16_t>(
            "short_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      true,
      {"short_val", "int_val", "long_val"},
      20);
}

TEST_F(E2EFilterTest, floatAndDoubleDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "float_val2:float,"
      "double_val2:double,"
      "long_val:bigint,"
      "float_null:float",
      [&]() {
        makeAllNulls("float_null");
        makeQuantizedFloat<float>("float_val2", 200, true);
        makeQuantizedFloat<double>("double_val2", 522, true);
      },
      true,
      {"float_val", "double_val", "float_val2", "double_val2", "float_null"},
      20);
}

TEST_F(E2EFilterTest, floatAndDouble) {
  // float_val and double_val may be direct since the
  // values are random.float_val2 and double_val2 are expected to be
  // dictionaries since the values are quantized.
  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "float_val2:float,"
      "double_val2:double,"

      "long_val:bigint,"
      "float_null:float",
      [&]() {
        makeAllNulls("float_null");
        makeQuantizedFloat<float>("float_val2", 200, true);
        makeQuantizedFloat<double>("double_val2", 522, true);
        // Make sure there are RLE's.
        makeReapeatingValues<float>("float_val2", 0, 100, 200, 10.1);
        makeReapeatingValues<double>("double_val2", 0, 100, 200, 100.8);
      },
      true,
      {"float_val", "double_val", "float_val2", "double_val2", "float_null"},
      20);
}

TEST_F(E2EFilterTest, shortDecimalDictionary) {
  // decimal(10, 5) maps to 5 bytes FLBA in Parquet.
  // decimal(17, 5) maps to 8 bytes FLBA in Parquet.
  for (const auto& type : {
           "shortdecimal_val:decimal(10, 5)",
           "shortdecimal_val:decimal(17, 5)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int64_t>(
              "shortdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }
}

TEST_F(E2EFilterTest, shortDecimalDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  // decimal(10, 5) maps to 5 bytes FLBA in Parquet.
  // decimal(17, 5) maps to 8 bytes FLBA in Parquet.
  for (const auto& type : {
           "shortdecimal_val:decimal(10, 5)",
           "shortdecimal_val:decimal(17, 5)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int64_t>(
              "shortdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }

  testWithTypes(
      "shortdecimal_val:decimal(10, 5)",
      [&]() {
        useSuppliedValues<int64_t>("shortdecimal_val", 0, {-479, 40000000});
      },
      false,
      {"shortdecimal_val"},
      20);
}

TEST_F(E2EFilterTest, longDecimalDictionary) {
  // decimal(30, 10) maps to 13 bytes FLBA in Parquet.
  // decimal(37, 15) maps to 16 bytes FLBA in Parquet.
  for (const auto& type : {
           "longdecimal_val:decimal(30, 10)",
           "longdecimal_val:decimal(37, 15)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int128_t>(
              "longdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        true,
        {},
        20);
  }
}

TEST_F(E2EFilterTest, longDecimalDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  // decimal(30, 10) maps to 13 bytes FLBA in Parquet.
  // decimal(37, 15) maps to 16 bytes FLBA in Parquet.
  for (const auto& type : {
           "longdecimal_val:decimal(30, 10)",
           "longdecimal_val:decimal(37, 15)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<int128_t>(
              "longdecimal_val",
              10, // min
              100, // max
              22, // repeats
              19, // rareFrequency
              -999, // rareMin
              30000, // rareMax
              true);
        },
        true,
        {},
        20);
  }

  testWithTypes(
      "longdecimal_val:decimal(30, 10)",
      [&]() {
        useSuppliedValues<int128_t>(
            "longdecimal_val",
            0,
            {-479, HugeInt::build(1546093991, 4054979645)});
      },
      false,
      {},
      20);
}

TEST_F(E2EFilterTest, stringDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique("string_val");
        makeStringUnique("string_val_2");
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, stringDictionary) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string,"
      "string_const: string",
      [&]() {
        makeStringDistribution("string_val", 100, true, false);
        makeStringDistribution("string_val_2", 170, false, true);
        makeStringDistribution("string_const", 1, true, false);
      },
      true,
      {"string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, dedictionarize) {
  rowsInRowGroup_ = 10'000;
  options_.dictionaryPageSizeLimit = 20'000;

  testWithTypes(
      "long_val: bigint,"
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringDistribution("string_val", 10000000, true, false);
        makeStringDistribution("string_val_2", 1700000, false, true);
      },
      true,
      {"long_val", "string_val", "string_val_2"},
      20);
}

TEST_F(E2EFilterTest, filterStruct) {
  // The data has a struct member with one second level struct
  // column. Both structs have a column that gets filtered 'nestedxxx'
  // and one that does not 'dataxxx'.
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "  data1: string, "
      "  inner_struct: struct<nested2: bigint, data2: array<smallint>>>",
      [&]() {},
      false,
      {"long_val",
       "outer_struct.inner_struct",
       "outer_struct.nested1",
       "outer_struct.inner_struct.nested2"},
      40);
}

TEST_F(E2EFilterTest, list) {
  // Break up the leaf data in small pages to cover coalescing repdefs.
  options_.dataPageSize = 4 * 1024;

  batchCount_ = 2;
  batchSize_ = 12000;
  testWithTypes(
      "long_val:bigint, array_val:array<int>,"
      "struct_array: struct<a: array<struct<k:int, v:int, va: array<smallint>>>>",
      nullptr,
      false,
      {"long_val", "array_val"},
      10);
}

TEST_F(E2EFilterTest, metadataFilter) {
  // Follow the batch size in `E2EFiltersTestBase`,
  // so that each batch can produce a row group.
  rowsInRowGroup_ = 10;
  testMetadataFilter();
}

TEST_F(E2EFilterTest, subfieldsPruning) {
  testSubfieldsPruning();
}

TEST_F(E2EFilterTest, mutationCornerCases) {
  testMutationCornerCases();
}

TEST_F(E2EFilterTest, map) {
  // Break up the leaf data in small pages to cover coalescing repdefs.
  options_.dataPageSize = 4 * 1024;

  batchCount_ = 2;
  batchSize_ = 12000;
  testWithTypes(
      "long_val:bigint,"
      "map_val:map<int, int>,"
      "nested_map:map<int, map<int, bigint>>,"
      "struct_map: struct<m: map<int, struct<k:int, v:int, vm: map<bigint, smallint>>>>",
      nullptr,
      false,
      {"long_val", "map_val"},
      10);
}

TEST_F(E2EFilterTest, varbinaryDirect) {
  options_.enableDictionary = false;
  options_.dataPageSize = 4 * 1024;

  testWithTypes(
      "varbinary_val:varbinary,"
      "varbinary_val_2:varbinary",
      [&]() {
        makeStringUnique("varbinary_val");
        makeStringUnique("varbinary_val_2");
      },
      true,
      {"varbinary_val", "varbinary_val_2"},
      20);
}

TEST_F(E2EFilterTest, varbinaryDictionary) {
  testWithTypes(
      "varbinary_val:varbinary,"
      "varbinary_val_2:varbinary,"
      "varbinary_const:varbinary",
      [&]() {
        makeStringDistribution("varbinary_val", 100, true, false);
        makeStringDistribution("varbinary_val_2", 170, false, true);
        makeStringDistribution("varbinary_const", 1, true, false);
      },
      true,
      {"varbinary_val", "varbinary_val_2"},
      20);
}

TEST_F(E2EFilterTest, largeMetadata) {
  rowsInRowGroup_ = 1;

  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  batches.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 1000, *leafPool_, nullptr, 0)));
  writeToMemory(rowType_, batches, false);
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  readerOpts.setFooterEstimatedSize(1024);
  readerOpts.setFilePreloadThreshold(1024 * 8);
  dwio::common::RowReaderOptions rowReaderOpts;
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_), readerOpts.memoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  EXPECT_EQ(1000, reader->numberOfRows());
}

TEST_F(E2EFilterTest, date) {
  testWithTypes(
      "date_val:date",
      [&]() {
        makeIntDistribution<int32_t>(
            "date_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      false,
      {"date_val"},
      20);
}

TEST_F(E2EFilterTest, combineRowGroup) {
  rowsInRowGroup_ = 5;
  rowType_ = ROW({"c0"}, {INTEGER()});
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 5; i++) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        test::BatchMaker::createBatch(rowType_, 1, *leafPool_, nullptr, 0)));
  }
  writeToMemory(rowType_, batches, false);
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_), readerOpts.memoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());
  EXPECT_EQ(parquetReader.fileMetaData().numRowGroups(), 1);
  EXPECT_EQ(parquetReader.numberOfRows(), 5);
}

TEST_F(E2EFilterTest, writeDecimalAsInteger) {
  auto rowVector = makeRowVector(
      {makeFlatVector<int64_t>({1, 2}, DECIMAL(8, 2)),
       makeFlatVector<int64_t>({1, 2}, DECIMAL(10, 2)),
       makeFlatVector<int64_t>({1, 2}, DECIMAL(19, 2))});
  writeToMemory(rowVector->type(), {rowVector}, false);
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_), readerOpts.memoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());

  auto types = parquetReader.typeWithId()->getChildren();
  auto c0 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[0]);
  EXPECT_EQ(c0->parquetType_.value(), thrift::Type::type::INT32);
  auto c1 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[1]);
  EXPECT_EQ(c1->parquetType_.value(), thrift::Type::type::INT64);
  auto c2 = std::dynamic_pointer_cast<const ParquetTypeWithId>(types[2]);
  EXPECT_EQ(c2->parquetType_.value(), thrift::Type::type::FIXED_LEN_BYTE_ARRAY);
}

TEST_F(E2EFilterTest, configurableWriteSchema) {
  auto test = [&](auto& type, auto& newType) {
    std::vector<RowVectorPtr> batches;
    for (auto i = 0; i < 5; i++) {
      auto vector = BaseVector::create(type, 100, pool());
      auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
      batches.push_back(rowVector);
    }

    writeToMemory(newType, batches, false);
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<InMemoryReadFile>(sinkData_), readerOpts.memoryPool());
    auto reader = makeReader(readerOpts, std::move(input));
    auto parquetReader = dynamic_cast<ParquetReader&>(*reader.get());

    EXPECT_EQ(parquetReader.rowType()->toString(), newType->toString());
  };

  // ROW(ROW(ROW))
  auto type =
      ROW({"a", "b"}, {INTEGER(), ROW({"c"}, {ROW({"d"}, {INTEGER()})})});
  auto newType =
      ROW({"aa", "bb"}, {INTEGER(), ROW({"cc"}, {ROW({"dd"}, {INTEGER()})})});
  test(type, newType);

  // ARRAY(ROW)
  type =
      ROW({"a", "b"}, {ARRAY(ROW({"c", "d"}, {BIGINT(), BIGINT()})), BIGINT()});
  newType = ROW(
      {"aa", "bb"}, {ARRAY(ROW({"cc", "dd"}, {BIGINT(), BIGINT()})), BIGINT()});
  test(type, newType);

  // // MAP(ROW)
  type =
      ROW({"a", "b"},
          {MAP(ROW({"c", "d"}, {BIGINT(), BIGINT()}),
               ROW({"e", "f"}, {BIGINT(), BIGINT()})),
           BIGINT()});
  newType =
      ROW({"aa", "bb"},
          {MAP(ROW({"cc", "dd"}, {BIGINT(), BIGINT()}),
               ROW({"ee", "ff"}, {BIGINT(), BIGINT()})),
           BIGINT()});
  test(type, newType);
}

// Define main so that gflags get processed.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
