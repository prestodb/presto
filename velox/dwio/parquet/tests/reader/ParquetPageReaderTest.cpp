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

#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/tests/ParquetReaderTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::parquet;
using namespace facebook::velox::parquet;

class ParquetPageReaderTest : public ParquetReaderTestBase {};

namespace {
auto defaultPool = memory::getDefaultMemoryPool();
}

TEST_F(ParquetPageReaderTest, smallPage) {
  auto readFile =
      std::make_shared<LocalReadFile>(getExampleFilePath("smallPageHeader"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *defaultPool, LogType::TEST);
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *defaultPool,
      thrift::CompressionCodec::type::GZIP,
      headerSize);
  auto header = pageReader->readPageHeader();
  EXPECT_EQ(header.type, thrift::PageType::type::DATA_PAGE);
  EXPECT_EQ(header.uncompressed_page_size, 16950);
  EXPECT_EQ(header.compressed_page_size, 10759);
  EXPECT_EQ(header.data_page_header.num_values, 21738);

  // expectedMinValue: "aaaa...aaaa"
  std::string expectedMinValue(39, 'a');
  // expectedMaxValue: "zzzz...zzzz"
  std::string expectedMaxValue(49, 'z');
  auto minValue = header.data_page_header.statistics.min_value;
  auto maxValue = header.data_page_header.statistics.max_value;
  EXPECT_EQ(minValue, expectedMinValue);
  EXPECT_EQ(maxValue, expectedMaxValue);
}

TEST_F(ParquetPageReaderTest, largePage) {
  auto readFile =
      std::make_shared<LocalReadFile>(getExampleFilePath("largePageHeader"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *defaultPool, LogType::TEST);
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *defaultPool,
      thrift::CompressionCodec::type::GZIP,
      headerSize);
  auto header = pageReader->readPageHeader();

  EXPECT_EQ(header.type, thrift::PageType::type::DATA_PAGE);
  EXPECT_EQ(header.uncompressed_page_size, 1050822);
  EXPECT_EQ(header.compressed_page_size, 66759);
  EXPECT_EQ(header.data_page_header.num_values, 970);

  // expectedMinValue: "aaaa...aaaa"
  std::string expectedMinValue(1295, 'a');
  // expectedMinValue: "zzzz...zzzz"
  std::string expectedMaxValue(2255, 'z');
  auto minValue = header.data_page_header.statistics.min_value;
  auto maxValue = header.data_page_header.statistics.max_value;
  EXPECT_EQ(minValue, expectedMinValue);
  EXPECT_EQ(maxValue, expectedMaxValue);
}

TEST_F(ParquetPageReaderTest, corruptedPageHeader) {
  auto readFile = std::make_shared<LocalReadFile>(
      getExampleFilePath("corruptedPageHeader"));
  auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));
  auto headerSize = file->getLength();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::move(file), 0, headerSize, *defaultPool, LogType::TEST);

  // In the corruptedPageHeader, the min_value length is set incorrectly on
  // purpose. This is to simulate the situation where the Parquet Page Header is
  // corrupted. And an error is expected to be thrown.
  auto pageReader = std::make_unique<PageReader>(
      std::move(inputStream),
      *defaultPool,
      thrift::CompressionCodec::type::GZIP,
      headerSize);

  EXPECT_THROW(pageReader->readPageHeader(), VeloxException);
}
