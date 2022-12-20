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

#include "velox/dwio/common/tests/E2EFilterTestBase.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"

#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

using dwio::common::MemorySink;

class E2EFilterTest : public E2EFilterTestBase {
 protected:
  void SetUp() override {
    E2EFilterTestBase::SetUp();
    writerProperties_ = ::parquet::WriterProperties::Builder().build();
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations) {
    testSenario(columns, customize, wrapInStruct, filterable, numCombinations);

    // Always test no null case.
    auto newCustomize = [&]() {
      if (customize) {
        customize();
      }
      makeNotNull(0);
    };
    testSenario(
        columns, newCustomize, wrapInStruct, filterable, numCombinations);
  }

  void writeToMemory(
      const TypePtr&,
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) override {
    auto sink = std::make_unique<MemorySink>(*pool_, 200 * 1024 * 1024);
    sinkPtr_ = sink.get();

    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), *pool_, rowGroupSize_, writerProperties_);
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->close();
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) override {
    return std::make_unique<ParquetReader>(std::move(input), opts);
  }

  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  std::shared_ptr<::parquet::WriterProperties> writerProperties_;
  int32_t rowGroupSize_{10000};
};

TEST_F(E2EFilterTest, writerMagic) {
  rowType_ = ROW({INTEGER()});
  std::vector<RowVectorPtr> batches;
  batches.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 20000, *pool_, nullptr, 0)));
  writeToMemory(rowType_, batches, false);
  auto data = sinkPtr_->getData();
  auto size = sinkPtr_->size();
  EXPECT_EQ("PAR1", std::string(data, 4));
  EXPECT_EQ("PAR1", std::string(data + size - 4, 4));
}

TEST_F(E2EFilterTest, integerDirect) {
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(4 * 1024)
                          ->build();
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
       {::parquet::Compression::SNAPPY,
        ::parquet::Compression::ZSTD,
        ::parquet::Compression::GZIP,
        ::parquet::Compression::UNCOMPRESSED}) {
    if (!arrow::util::Codec::IsAvailable(compression)) {
      continue;
    }

    writerProperties_ = ::parquet::WriterProperties::Builder()
                            .data_pagesize(4 * 1024)
                            ->compression(compression)
                            ->build();

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
        3);
  }
}

TEST_F(E2EFilterTest, integerDictionary) {
  writerProperties_ =
      ::parquet::WriterProperties::Builder().data_pagesize(4 * 1024)->build();

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
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(4 * 1024)
                          ->build();

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
          makeIntDistribution<UnscaledShortDecimal>(
              "shortdecimal_val",
              UnscaledShortDecimal(10), // min
              UnscaledShortDecimal(100), // max
              22, // repeats
              19, // rareFrequency
              UnscaledShortDecimal(-999), // rareMin
              UnscaledShortDecimal(30000), // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }
}

TEST_F(E2EFilterTest, shortDecimalDirect) {
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(4 * 1024)
                          ->build();
  // decimal(10, 5) maps to 5 bytes FLBA in Parquet.
  // decimal(17, 5) maps to 8 bytes FLBA in Parquet.
  for (const auto& type : {
           "shortdecimal_val:decimal(10, 5)",
           "shortdecimal_val:decimal(17, 5)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<UnscaledShortDecimal>(
              "shortdecimal_val",
              UnscaledShortDecimal(10), // min
              UnscaledShortDecimal(100), // max
              22, // repeats
              19, // rareFrequency
              UnscaledShortDecimal(-999), // rareMin
              UnscaledShortDecimal(30000), // rareMax
              true);
        },
        false,
        {"shortdecimal_val"},
        20);
  }

  testWithTypes(
      "shortdecimal_val:decimal(10, 5)",
      [&]() {
        useSuppliedValues<UnscaledShortDecimal>(
            "shortdecimal_val",
            0,
            {UnscaledShortDecimal(-479), UnscaledShortDecimal(40000000)});
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
          makeIntDistribution<UnscaledLongDecimal>(
              "longdecimal_val",
              UnscaledLongDecimal(10), // min
              UnscaledLongDecimal(100), // max
              22, // repeats
              19, // rareFrequency
              UnscaledLongDecimal(-999), // rareMin
              UnscaledLongDecimal(30000), // rareMax
              true);
        },
        true,
        {},
        20);
  }
}

TEST_F(E2EFilterTest, longDecimalDirect) {
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(4 * 1024)
                          ->build();
  // decimal(30, 10) maps to 13 bytes FLBA in Parquet.
  // decimal(37, 15) maps to 16 bytes FLBA in Parquet.
  for (const auto& type : {
           "longdecimal_val:decimal(30, 10)",
           "longdecimal_val:decimal(37, 15)",
       }) {
    testWithTypes(
        type,
        [&]() {
          makeIntDistribution<UnscaledLongDecimal>(
              "longdecimal_val",
              UnscaledLongDecimal(10), // min
              UnscaledLongDecimal(100), // max
              22, // repeats
              19, // rareFrequency
              UnscaledLongDecimal(-999), // rareMin
              UnscaledLongDecimal(30000), // rareMax
              true);
        },
        true,
        {},
        20);
  }

  testWithTypes(
      "longdecimal_val:decimal(30, 10)",
      [&]() {
        useSuppliedValues<UnscaledLongDecimal>(
            "longdecimal_val",
            0,
            {UnscaledLongDecimal(-479),
             UnscaledLongDecimal(buildInt128(1546093991, 4054979645))});
      },
      false,
      {},
      20);
}

TEST_F(E2EFilterTest, stringDirect) {
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .disable_dictionary()
                          ->data_pagesize(4 * 1024)
                          ->build();

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
  writerProperties_ = ::parquet::WriterProperties::Builder()
                          .max_row_group_length(10000000)
                          ->dictionary_pagesize_limit(20000)
                          ->build();

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
  writerProperties_ =
      ::parquet::WriterProperties::Builder().data_pagesize(4 * 1024)->build();
  batchCount_ = 2;
  batchSize_ = 12000;
  testWithTypes(
      "long_val:bigint, array_val:array<int>,"
      "struct_array: struct<a: array<struct<k:int, v:int, va: array<smallint>>>>",
      nullptr,
      false,
      {"long_val"},
      10);
}

TEST_F(E2EFilterTest, metadataFilter) {
  testMetadataFilter();
}

// Define main so that gflags get processed.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
