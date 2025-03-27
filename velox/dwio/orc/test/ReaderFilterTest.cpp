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

#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;

class OrcReaderFilterBase : public VectorTestBase {
 protected:
  std::string getExamplesFilePath(const std::string& fileName) {
    return test::getDataFilePath("velox/dwio/orc/test", "examples/" + fileName);
  }
};

struct OrcReaderFilterParam {
  std::string columnName;
  std::shared_ptr<Filter> filter;
  int resultsExpected;
};

class OrcReaderFilterTestP
    : public ::testing::TestWithParam<OrcReaderFilterParam>,
      public OrcReaderFilterBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

INSTANTIATE_TEST_SUITE_P(
    OrcReaderFilterTestsP,
    OrcReaderFilterTestP,
    testing::Values(
        // "a": 111, int
        OrcReaderFilterParam{
            "a",
            std::make_shared<common::BigintRange>(100, 200, false),
            1},
        OrcReaderFilterParam{
            "a",
            std::make_shared<common::BigintRange>(200, 300, false),
            0},
        // "b": 1111, bigint
        OrcReaderFilterParam{
            "b",
            std::make_shared<common::BigintRange>(1000, 2000, false),
            1},
        OrcReaderFilterParam{
            "b",
            std::make_shared<common::BigintRange>(2000, 3000, false),
            0},
        // "c": 127, tinyint
        OrcReaderFilterParam{
            "c",
            std::make_shared<common::BigintRange>(100, 200, false),
            1},
        OrcReaderFilterParam{
            "c",
            std::make_shared<common::BigintRange>(200, 300, false),
            0},
        // "d": 11, smallint
        OrcReaderFilterParam{
            "d",
            std::make_shared<common::BigintRange>(10, 20, false),
            1},
        OrcReaderFilterParam{
            "d",
            std::make_shared<common::BigintRange>(20, 30, false),
            0},
        // "e": 1.1, float
        OrcReaderFilterParam{
            "e",
            std::make_shared<common::FloatingPointRange<
                float>>(1.0, false, false, 2.0, false, false, false),
            1},
        OrcReaderFilterParam{
            "e",
            std::make_shared<common::FloatingPointRange<
                float>>(2.0, false, false, 3.0, false, false, false),
            0},
        // "f": 1.12, double
        OrcReaderFilterParam{
            "f",
            std::make_shared<common::FloatingPointRange<
                double>>(1.0, false, false, 2.0, false, false, false),
            1},
        OrcReaderFilterParam{
            "f",
            std::make_shared<common::FloatingPointRange<
                double>>(2.0, false, false, 3.0, false, false, false),
            0},
        // "g": "velox", varchar
        OrcReaderFilterParam{
            "g",
            std::make_shared<common::BytesRange>(
                "velox",
                false,
                false,
                "velox",
                false,
                false,
                false),
            1},
        OrcReaderFilterParam{
            "g",
            std::make_shared<common::BytesRange>(
                "velox_bad",
                false,
                false,
                "velox_bad",
                false,
                false,
                false),
            0},
        // "h": false, boolean
        OrcReaderFilterParam{
            "h",
            std::make_shared<common::BoolValue>(false, false),
            1},
        OrcReaderFilterParam{
            "h",
            std::make_shared<common::BoolValue>(true, false),
            0},
        // "i": 1242141234.123456, decimal(38, 6)
        // "j": 321423.21, decimal(9, 2)
        // "k": "2023-08-18", days as date
        OrcReaderFilterParam{
            "k",
            std::make_shared<common::BigintRange>(19587, 19587, false),
            1},
        OrcReaderFilterParam{
            "k",
            std::make_shared<common::BigintRange>(19588, 19588, false),
            0},
        // "l": "2023-08-18 01:12:23.0", timestamp
        OrcReaderFilterParam{
            "l",
            std::make_shared<common::TimestampRange>(
                Timestamp(1692342000, 0),
                Timestamp(1692428400, 0),
                false),
            1},
        OrcReaderFilterParam{
            "l",
            std::make_shared<common::TimestampRange>(
                Timestamp(1692428400, 0),
                Timestamp(1692514800, 0),
                false),
            0} // "m": ["aaaa", "BBBB", "velox"], array<string>
               // "n": [{"key": "foo", "value": 1}, {"key": "bar", "value": 2}],
               // map<string, "o": {"x": 1, "y": 2} struct<int, double>
        ));
// DATA
// {
// "a": 111,
// "b": 1111,
// "c": 127,
// "d": 11,
// "e": 1.1,
// "f": 1.12,
// "g": "velox",
// // "h": false,
// "i": 1242141234.123456,
// "j": 321423.21,
// "k": "2023-08-18",
// "l": "2023-08-18 01:12:23.0",
// "m": ["aaaa", "BBBB", "velox"],
// "n": [{"key": "foo", "value": 1}, {"key": "bar", "value": 2}],
// "o": {"x": 1, "y": 2}
// }
TEST_P(OrcReaderFilterTestP, tests) {
  // Define Schema
  auto schema = ROW({
      {"a", INTEGER()},
      {"b", BIGINT()},
      {"c", TINYINT()},
      {"d", SMALLINT()},
      {"e", REAL()},
      {"f", DOUBLE()},
      {"g", VARCHAR()},
      {"h", BOOLEAN()},
      {"i", DECIMAL(38, 6)},
      {"j", DECIMAL(9, 2)},
      {"k", INTEGER()},
      {"l", TIMESTAMP()},
      {"m", ARRAY(VARCHAR())},
      {"n", MAP(VARCHAR(), BIGINT())},
      {"o", ROW({{"x", BIGINT()}, {"y", DOUBLE()}})},
  });

  // auto rowType = DataSetBuilder::makeRowType(schema, true);
  // auto filterGenerator = std::make_unique<FilterGenerator>(rowType);
  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*schema);

  std::string fileName = "orc_all_type.orc";

  dwio::common::ReaderOptions readerOpts{pool()};

  // To make DwrfReader reads ORC file, setFileFormat to FileFormat::ORC
  readerOpts.setFileFormat(dwio::common::FileFormat::ORC);
  readerOpts.setScanSpec(scanSpec);

  // Read orc file
  auto orcFilePath = getExamplesFilePath(fileName);
  auto reader = DwrfReader::create(
      createFileBufferedInput(orcFilePath, readerOpts.memoryPool()),
      readerOpts);

  // Apply Filter
  scanSpec->childByName(GetParam().columnName)
      ->setFilter(GetParam().filter->clone());

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto batch = BaseVector::create(schema, 0, &readerOpts.memoryPool());

  rowReader->next(10, batch);
  auto rowVector = batch->as<RowVector>();

  EXPECT_EQ(GetParam().resultsExpected, rowVector->size());
}
