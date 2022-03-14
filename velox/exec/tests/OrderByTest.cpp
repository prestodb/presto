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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class OrderByTest : public OperatorTestBase {
 protected:
  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key) {
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan = PlanBuilder()
                    .values(input)
                    .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false)
                    .planNode();

    assertQueryOrdered(
        plan,
        fmt::format("SELECT * FROM tmp ORDER BY {} NULLS LAST", key),
        {keyIndex});

    plan = PlanBuilder()
               .values(input)
               .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false)
               .planNode();

    assertQueryOrdered(
        plan,
        fmt::format("SELECT * FROM tmp ORDER BY {} DESC NULLS FIRST", key),
        {keyIndex});
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      const std::string& filter) {
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    auto plan = PlanBuilder()
                    .values(input)
                    .filter(filter)
                    .orderBy({fmt::format("{} ASC NULLS LAST", key)}, false)
                    .planNode();

    assertQueryOrdered(
        plan,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} NULLS LAST", filter, key),
        {keyIndex});

    plan = PlanBuilder()
               .values(input)
               .filter(filter)
               .orderBy({fmt::format("{} DESC NULLS FIRST", key)}, false)
               .planNode();

    assertQueryOrdered(
        plan,
        fmt::format(
            "SELECT * FROM tmp WHERE {} ORDER BY {} DESC NULLS FIRST",
            filter,
            key),
        {keyIndex});
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& input,
      const std::string& key1,
      const std::string& key2) {
    auto rowType = input[0]->type()->asRow();
    auto keyIndices = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        core::kAscNullsLast, core::kDescNullsFirst};
    std::vector<std::string> sortOrderSqls = {"NULLS LAST", "DESC NULLS FIRST"};

    for (int i = 0; i < sortOrders.size(); i++) {
      for (int j = 0; j < sortOrders.size(); j++) {
        auto plan = PlanBuilder()
                        .values(input)
                        .orderBy(
                            {fmt::format("{} {}", key1, sortOrderSqls[i]),
                             fmt::format("{} {}", key2, sortOrderSqls[j])},
                            false)
                        .planNode();

        assertQueryOrdered(
            plan,
            fmt::format(
                "SELECT * FROM tmp ORDER BY {} {}, {} {}",
                key1,
                sortOrderSqls[i],
                key2,
                sortOrderSqls[j]),
            keyIndices);
      }
    }
  }
};

TEST_F(OrderByTest, selectiveFilter) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  // c0 values are unique across batches
  testSingleKey(vectors, "c0", "c0 % 333 = 0");

  // c1 values are unique only within a batch
  testSingleKey(vectors, "c1", "c1 % 333 = 0");
}

TEST_F(OrderByTest, singleKey) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return row; }, nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");

  // parser doesn't support "is not null" expression, hence, using c0 % 2 >= 0
  testSingleKey(vectors, "c0", "c0 % 2 >= 0");

  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 DESC NULLS LAST"}, false)
                  .planNode();

  assertQueryOrdered(
      plan, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST", {0});

  plan = PlanBuilder()
             .values(vectors)
             .orderBy({"c0 ASC NULLS FIRST"}, false)
             .planNode();

  assertQueryOrdered(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST", {0});
}

TEST_F(OrderByTest, multipleKeys) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    // c0: half of rows are null, a quarter is 0 and remaining quarter is 1
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 4; }, nullEvery(2, 1));
    auto c1 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row; }, nullEvery(7));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testTwoKeys(vectors, "c0", "c1");

  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 ASC NULLS FIRST", "c1 ASC NULLS LAST"}, false)
                  .planNode();

  assertQueryOrdered(
      plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST", {0, 1});

  plan = PlanBuilder()
             .values(vectors)
             .orderBy({"c0 DESC NULLS LAST", "c1 DESC NULLS FIRST"}, false)
             .planNode();

  assertQueryOrdered(
      plan,
      "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST, c1 DESC NULLS FIRST",
      {0, 1});
}

TEST_F(OrderByTest, multiBatchResult) {
  vector_size_t batchSize = 5000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c1, c1, c1, c1}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");
}

TEST_F(OrderByTest, varfields) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(5));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    auto c2 = makeFlatVector<StringView>(
        batchSize,
        [](vector_size_t row) { return StringView(std::to_string(row)); },
        nullEvery(17));
    // TODO: Add support for array/map in createDuckDbTable and verify
    // that we can sort by array/map as well.
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c2");
}

TEST_F(OrderByTest, unknown) {
  vector_size_t size = 1'000;
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
       BaseVector::createConstant(
           variant(TypeKind::UNKNOWN), size, pool_.get())});

  // Exclude "UNKNOWN" column as DuckDB doesn't understand UNKNOWN type
  createDuckDbTable({makeRowVector({vector->childAt(0)})});

  auto plan = PlanBuilder()
                  .values({vector})
                  .orderBy({"c0 DESC NULLS LAST"}, false)
                  .planNode();

  assertQueryOrdered(
      plan, "SELECT *, null FROM tmp ORDER BY c0 DESC NULLS LAST", {0});
}
