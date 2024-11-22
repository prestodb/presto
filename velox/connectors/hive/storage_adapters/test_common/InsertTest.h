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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class InsertTest : public velox::test::VectorTestBase {
 public:
  void runInsertTest(
      std::string_view outputDirectory,
      int numRows,
      memory::MemoryPool* pool) {
    auto rowType = ROW(
        {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});

    auto input = makeRowVector(
        {makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<int32_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<int16_t>(numRows, [](auto row) { return row; }),
         makeFlatVector<double>(numRows, [](auto row) { return row; })});

    // Insert with one writer.
    auto plan =
        exec::test::PlanBuilder()
            .values({input})
            .tableWrite(
                outputDirectory.data(), dwio::common::FileFormat::PARQUET)
            .planNode();

    // Execute the write plan.
    auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool);

    // First column has number of rows written in the first row and nulls in
    // other rows.
    auto rowCount = results->childAt(exec::TableWriteTraits::kRowCountChannel)
                        ->as<FlatVector<int64_t>>();
    ASSERT_FALSE(rowCount->isNullAt(0));
    ASSERT_EQ(numRows, rowCount->valueAt(0));
    ASSERT_TRUE(rowCount->isNullAt(1));

    // Second column contains details about written files.
    auto details = results->childAt(exec::TableWriteTraits::kFragmentChannel)
                       ->as<FlatVector<StringView>>();
    ASSERT_TRUE(details->isNullAt(0));
    ASSERT_FALSE(details->isNullAt(1));
    folly::dynamic obj = folly::parseJson(details->valueAt(1));

    ASSERT_EQ(numRows, obj["rowCount"].asInt());
    auto fileWriteInfos = obj["fileWriteInfos"];
    ASSERT_EQ(1, fileWriteInfos.size());

    auto writeFileName = fileWriteInfos[0]["writeFileName"].asString();

    // Read from 'writeFileName' and verify the data matches the original.
    plan = exec::test::PlanBuilder().tableScan(rowType).planNode();

    auto filePath = fmt::format("{}{}", outputDirectory, writeFileName);
    const int64_t fileSize = fileWriteInfos[0]["fileSize"].asInt();
    auto split = exec::test::HiveConnectorSplitBuilder(filePath)
                     .fileFormat(dwio::common::FileFormat::PARQUET)
                     .length(fileSize)
                     .build();
    auto copy =
        exec::test::AssertQueryBuilder(plan).split(split).copyResults(pool);
    exec::test::assertEqualResults({input}, {copy});
  }
};
} // namespace facebook::velox::test
