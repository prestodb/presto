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

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

using dwio::common::MemoryInputStream;
using dwio::common::MemorySink;

class E2EFilterTest : public E2EFilterTestBase {
 protected:
  void SetUp() override {
    E2EFilterTestBase::SetUp();
    writerProperties_ = ::parquet::WriterProperties::Builder().build();
  }

  void writeToMemory(
      const TypePtr&,
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) override {
    auto sink = std::make_unique<MemorySink>(*pool_, 200 * 1024 * 1024);
    sinkPtr_ = sink.get();

    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), *pool_, 10000, writerProperties_);
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->close();
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::InputStream> input) override {
    return std::make_unique<ParquetReader>(std::move(input), opts);
  }

  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  std::shared_ptr<::parquet::WriterProperties> writerProperties_;
};

TEST_F(E2EFilterTest, writerMagic) {
  rowType_ = ROW({INTEGER()});
  batches_.push_back(std::static_pointer_cast<RowVector>(
      test::BatchMaker::createBatch(rowType_, 20000, *pool_, nullptr, 0)));
  writeToMemory(rowType_, batches_, false);
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
      false,
      {"short_val", "int_val", "long_val"},
      20,
      true);
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
            Subfield("long_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls

        makeIntDistribution<int32_t>(
            Subfield("int_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls

        makeIntDistribution<int16_t>(
            Subfield("short_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      false,
      {"short_val", "int_val", "long_val"},
      20,
      true);
}
