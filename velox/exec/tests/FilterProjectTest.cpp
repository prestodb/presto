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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class FilterProjectTest : public OperatorTestBase {
 protected:
  void assertFilter(
      std::vector<RowVectorPtr>&& vectors,
      const std::string& filter = "c1 % 10  > 0") {
    auto plan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(plan, "SELECT * FROM tmp WHERE " + filter);
  }

  void assertProject(std::vector<RowVectorPtr>&& vectors) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .project({"c0", "c1", "c0 + c1"})
                    .planNode();

    auto task = assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp");

    // A quick sanity check for memory usage reporting. Check that peak total
    // memory usage for the project node is > 0.
    auto planStats = toPlanStats(task->taskStats());
    auto projectNodeId = plan->id();
    auto it = planStats.find(projectNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_TRUE(it->second.peakMemoryBytes > 0);
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()})};
};

TEST_F(FilterProjectTest, filter) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertFilter(std::move(vectors));
}

TEST_F(FilterProjectTest, filterOverDictionary) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));

    auto indices =
        AlignedBuffer::allocate<int32_t>(2 * vector->size(), pool_.get());
    auto indicesPtr = indices->asMutable<int32_t>();
    for (int32_t j = 0; j < vector->size() / 2; j++) {
      indicesPtr[2 * j] = j;
      indicesPtr[2 * j + 1] = j;
    }
    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vector->size(), vector->childAt(1));
    vectors.push_back(std::make_shared<RowVector>(
        pool_.get(),
        rowType_,
        BufferPtr(nullptr),
        vector->size(),
        newChildren,
        0 /*nullCount*/));
  }
  createDuckDbTable(vectors);

  assertFilter(std::move(vectors));
}

TEST_F(FilterProjectTest, filterOverConstant) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));

    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] =
        BaseVector::wrapInConstant(vector->size(), 7, vector->childAt(1));
    vectors.push_back(std::make_shared<RowVector>(
        pool_.get(),
        rowType_,
        BufferPtr(nullptr),
        vector->size(),
        newChildren,
        0 /*nullCount*/));
  }
  createDuckDbTable(vectors);

  assertFilter(std::move(vectors));
}

TEST_F(FilterProjectTest, project) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  assertProject(std::move(vectors));
}

TEST_F(FilterProjectTest, projectOverDictionary) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));

    auto indices =
        AlignedBuffer::allocate<int32_t>(2 * vector->size(), pool_.get());
    auto indicesPtr = indices->asMutable<int32_t>();
    for (int32_t j = 0; j < vector->size() / 2; j++) {
      indicesPtr[2 * j] = j;
      indicesPtr[2 * j + 1] = j;
    }
    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vector->size(), vector->childAt(1));
    vectors.push_back(std::make_shared<RowVector>(
        pool_.get(),
        rowType_,
        BufferPtr(nullptr),
        vector->size(),
        newChildren,
        0 /*nullCount*/));
  }
  createDuckDbTable(vectors);

  assertProject(std::move(vectors));
}

TEST_F(FilterProjectTest, projectOverConstant) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));

    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] =
        BaseVector::wrapInConstant(vector->size(), 7, vector->childAt(1));
    vectors.push_back(std::make_shared<RowVector>(
        pool_.get(),
        rowType_,
        BufferPtr(nullptr),
        vector->size(),
        newChildren,
        0 /*nullCount*/));
  }
  createDuckDbTable(vectors);

  assertProject(std::move(vectors));
}

TEST_F(FilterProjectTest, projectOverLazy) {
  vector_size_t size = 1'000;
  auto valueAtC0 = [](auto row) -> int32_t {
    return row % 2 == 0 ? row : -row;
  };
  auto valueAtC1 = [](auto row) -> double {
    return row % 3 == 0 ? row * 0.1 : -row * 0.1;
  };
  auto lazyVectors = makeRowVector({
      vectorMaker_.lazyFlatVector<int32_t>(size, valueAtC0),
      vectorMaker_.lazyFlatVector<double>(size, valueAtC1),
  });

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAtC0),
      makeFlatVector<double>(size, valueAtC1),
  });

  createDuckDbTable({vectors});

  auto plan = PlanBuilder()
                  .values({lazyVectors})
                  .project({"c0 > 0 AND c1 > 0.0", "c1 + 5.2"})
                  .planNode();
  assertQuery(plan, "SELECT c0 > 0 AND c1 > 0, c1 + 5.2 FROM tmp");
}

TEST_F(FilterProjectTest, filterProject) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c1 % 10  > 0")
                  .project({"c0", "c1", "c0 + c1"})
                  .planNode();

  assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");
}

TEST_F(FilterProjectTest, dereference) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .project({"row_constructor(c1, c2) AS c1_c2"})
                  .project({"c1_c2.c1", "c1_c2.c2"})
                  .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp");

  plan = PlanBuilder()
             .values(vectors)
             .project({"row_constructor(c1, c2) AS c1_c2"})
             .filter("c1_c2.c1 % 10 = 5")
             .project({"c1_c2.c1", "c1_c2.c2"})
             .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp WHERE c1 % 10 = 5");
}

TEST_F(FilterProjectTest, allFailedOrPassed) {
  auto rowType = ROW({"c0", "c1"}, {INTEGER(), INTEGER()});
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    // We alternate between a batch where all pass and a batch where
    // no row passes. c0 is flat vector. c1 is constant vector.
    int32_t value = i % 2 == 0 ? 0 : 1;

    auto c0 = std::dynamic_pointer_cast<FlatVector<int32_t>>(
        BaseVector::create(INTEGER(), 100, pool_.get()));
    for (auto row = 0; row < c0->size(); ++row) {
      c0->set(row, value);
    }

    auto c1 = BaseVector::createConstant(value, 100, pool_.get());

    vectors.push_back(std::make_shared<RowVector>(
        pool_.get(),
        rowType,
        BufferPtr(nullptr),
        2,
        std::vector<VectorPtr>{c0, c1}));
  }
  createDuckDbTable(vectors);

  // filter over flat vector
  assertFilter(std::move(vectors), "c0 = 0");

  // filter over constant vector
  assertFilter(std::move(vectors), "c1 = 0");
}

// Tests fusing of consecutive filters and projects.
TEST_F(FilterProjectTest, filterProjectFused) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 % 10 < 9")
                  .project({"c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
                  .filter("c0 % 10 < 8")
                  .project({"c0", "c1", "e1", "c0 % 100 AS e2"})
                  .filter("c0 % 10 < 5")
                  .project({"c0", "c1", "e1", "e2"})
                  .planNode();

  assertQuery(
      plan,
      "SELECT c0, c1, c0 %100 + c1 % 50, c0 % 100 FROM tmp WHERE c0 % 10 < 5");
}
