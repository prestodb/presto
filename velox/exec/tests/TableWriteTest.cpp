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
#include "velox/dwio/common/DataSink.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "velox/common/base/tests/Fs.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector::hive;

class TableWriteTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  VectorPtr createConstant(variant value, vector_size_t size) const {
    return BaseVector::createConstant(value, size, pool_.get());
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
};

// Runs a pipeline with read + filter + project (with substr) + write
TEST_F(TableWriteTest, scanFilterProjectWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  auto outputFile = TempFilePath::create();

  createDuckDbTable(vectors);

  auto planBuilder = PlanBuilder();
  auto project = planBuilder.tableScan(rowType_)
                     .filter("c0 <> 0")
                     .project({"c0", "c1", "c1 + c2", "substr(c5, 1, 1)"})
                     .planNode();

  std::vector<std::string> columnNames = {
      "c0", "c1", "c1_plus_c2", "substr_c5"};
  auto plan =
      planBuilder
          .tableWrite(
              columnNames,
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<HiveInsertTableHandle>(outputFile->path)),
              "rows")
          .project({"rows"})
          .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp WHERE c0 <> 0");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query

  auto types = project->outputType()->children();
  auto rowType = ROW(std::move(columnNames), std::move(types));
  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      {outputFile},
      "SELECT c0, c1, c1 + c2, substr(c5, 1, 1) FROM tmp WHERE c0 <> 0");
}

TEST_F(TableWriteTest, renameAndReorderColumns) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BIGINT(), INTEGER(), DOUBLE(), VARCHAR()});
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType, filePaths.size(), 1'000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  auto outputFile = TempFilePath::create();
  createDuckDbTable(vectors);

  auto tableRowType = ROW({"d", "c", "b"}, {VARCHAR(), DOUBLE(), INTEGER()});

  auto plan =
      PlanBuilder()
          .tableScan(rowType)
          .tableWrite(
              tableRowType,
              {"x", "y", "z"},
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<HiveInsertTableHandle>(outputFile->path)),
              "rows")
          .project({"rows"})
          .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  assertQuery(
      PlanBuilder()
          .tableScan(ROW({"x", "y", "z"}, {VARCHAR(), DOUBLE(), INTEGER()}))
          .planNode(),
      {outputFile},
      "SELECT d, c, b FROM tmp");
}

// Runs a pipeline with read + write
TEST_F(TableWriteTest, directReadWrite) {
  auto filePaths = makeFilePaths(10);
  auto vectors = makeVectors(rowType_, filePaths.size(), 1000);
  for (int i = 0; i < filePaths.size(); i++) {
    writeToFile(filePaths[i]->path, vectors[i]);
  }

  auto outputFile = TempFilePath::create();
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder()
          .tableScan(rowType_)
          .tableWrite(
              rowType_->names(),
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<HiveInsertTableHandle>(outputFile->path)),
              "rows")
          .project({"rows"})
          .planNode();

  assertQuery(plan, filePaths, "SELECT count(*) FROM tmp");

  // To test the correctness of the generated output,
  // We create a new plan that only read that file and then
  // compare that against a duckDB query that runs the whole query

  assertQuery(
      PlanBuilder().tableScan(rowType_).planNode(),
      {outputFile},
      "SELECT * FROM tmp");
}

// Tests writing constant vectors
TEST_F(TableWriteTest, constantVectors) {
  vector_size_t size = 1'000;

  // Make constant vectors of various types with null and non-null values.
  std::string somewhatLongString = "Somewhat long string";
  auto vector = makeRowVector({
      createConstant((int64_t)123'456, size),
      createConstant(variant(TypeKind::BIGINT), size),
      createConstant((int32_t)12'345, size),
      createConstant(variant(TypeKind::INTEGER), size),
      createConstant((int16_t)1'234, size),
      createConstant(variant(TypeKind::SMALLINT), size),
      createConstant((int8_t)123, size),
      createConstant(variant(TypeKind::TINYINT), size),
      createConstant(true, size),
      createConstant(false, size),
      createConstant(variant(TypeKind::BOOLEAN), size),
      createConstant(somewhatLongString.c_str(), size),
      createConstant(variant(TypeKind::VARCHAR), size),
  });
  auto rowType = std::dynamic_pointer_cast<const RowType>(vector->type());

  createDuckDbTable({vector});

  auto outputFile = TempFilePath::create();
  auto op =
      PlanBuilder()
          .values({vector})
          .tableWrite(
              rowType->names(),
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<HiveInsertTableHandle>(outputFile->path)),
              "rows")
          .project({"rows"})
          .planNode();

  assertQuery(op, fmt::format("SELECT {}", size));

  assertQuery(
      PlanBuilder().tableScan(rowType).planNode(),
      {outputFile},
      "SELECT * FROM tmp");
}

// Test TableWriter create empty ORC or not based on the config
TEST_F(TableWriteTest, writeEmptyFile) {
  auto outputFile = TempFilePath::create();
  fs::remove(outputFile->path);

  auto plan =
      PlanBuilder()
          .tableScan(rowType_)
          .filter("false")
          .tableWrite(
              rowType_->names(),
              std::make_shared<core::InsertTableHandle>(
                  kHiveConnectorId,
                  std::make_shared<HiveInsertTableHandle>(outputFile->path)),
              "rows")
          .planNode();

  auto execute = [](const std::shared_ptr<const core::PlanNode>& plan,
                    std::shared_ptr<core::QueryCtx> queryCtx =
                        core::QueryCtx::createForTest()) {
    CursorParameters params;
    params.planNode = plan;
    params.queryCtx = queryCtx;
    readCursor(params, [&](Task* task) { task->noMoreSplits("0"); });
  };

  execute(plan);
  ASSERT_FALSE(fs::exists(outputFile->path));

  auto queryCtx = core::QueryCtx::createForTest();
  queryCtx->setConfigOverridesUnsafe(
      {{core::QueryConfig::kCreateEmptyFiles, "true"}});
  execute(plan, queryCtx);
  ASSERT_TRUE(fs::exists(outputFile->path));
}
