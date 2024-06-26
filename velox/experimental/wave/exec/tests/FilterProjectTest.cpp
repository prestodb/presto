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
#include <cuda_runtime.h> // @manual
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class FilterProjectTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    wave::registerWave();
  }

  void assertFilter(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& filter = "c1 % 10  > 0") {
    auto plan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(plan, "SELECT * FROM tmp WHERE " + filter);
  }

  void makeNotNull(
      RowVectorPtr row,
      int64_t mod = std::numeric_limits<int64_t>::max()) {
    for (auto i = 0; i < row->type()->size(); ++i) {
      auto child = row->childAt(i);
      if (auto ints = child->as<FlatVector<int64_t>>()) {
        for (auto i = 0; i < child->size(); ++i) {
          ints->set(i, ints->valueAt(i) % mod);
        }
      }
      child->clearNulls(0, row->size());
    }
  }

  void assertProject(const std::vector<RowVectorPtr>& vectors) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .project({"c0", "c1", "c0 + c1"})
                    .planNode();

    auto task = assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp");
  }

  std::shared_ptr<Task> assertFilterProject(
      const std::string& filter,
      const std::vector<std::string>& projections,
      const std::vector<RowVectorPtr>& vectors) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .filter(filter)
                    .project(projections)
                    .planNode();
    std::stringstream sql;
    sql << "SELECT ";
    for (auto i = 0; i < projections.size(); ++i) {
      sql << " " << projections[i] << (i == projections.size() - 1 ? "" : ",");
    }
    sql << " FROM tmp WHERE " << filter;

    auto task = assertQuery(plan, sql.str());
    return task;
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), BIGINT(), SMALLINT(), DOUBLE()})};
};

TEST_F(FilterProjectTest, roundTrip) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    makeNotNull(vector);
    vectors.push_back(vector);
  }
  auto plan = PlanBuilder().values(vectors).planNode();
  AssertQueryBuilder(plan).assertResults(vectors);
}

TEST_F(FilterProjectTest, project) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    makeNotNull(vector, 1000000000);
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertProject(vectors);
}

TEST_F(FilterProjectTest, filterProject) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 1; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    makeNotNull(vector, 1000000000);
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertFilterProject(
      "c0 < 400000000",
      std::vector<std::string>{"c0", "c1", "c1 + c0 as s", "c2", "c3"},
      vectors);
}
