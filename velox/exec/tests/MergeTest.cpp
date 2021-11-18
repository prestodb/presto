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

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

static const core::SortOrder kAscNullsFirst(true, true);
static const core::SortOrder kAscNullsLast(true, false);
static const core::SortOrder kDescNullsFirst(false, true);
static const core::SortOrder kDescNullsLast(false, false);

class MergeTest : public OperatorTestBase {
 protected:
  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key) {
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);
    std::vector<core::SortOrder> sortOrders = {
        kAscNullsLast, kAscNullsFirst, kDescNullsFirst, kDescNullsLast};
    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};

    for (auto i = 0; i < sortOrders.size(); ++i) {
      auto plan = PlanBuilder()
                      .values(input)
                      .orderBy({keyIndex}, {sortOrders[i]}, true)
                      .localMerge({keyIndex}, {sortOrders[i]})
                      .planNode();

      assertQueryOrdered(
          plan,
          fmt::format(
              "SELECT * FROM tmp ORDER BY {} {}", key, sortOrderSqls[i]),
          {keyIndex});
    }
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& input,
      const std::string& key1,
      const std::string& key2) {
    auto rowType = input[0]->type()->asRow();
    auto sortingKeys = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    std::vector<core::SortOrder> sortOrders = {
        kAscNullsLast, kAscNullsFirst, kDescNullsFirst, kDescNullsLast};
    std::vector<std::string> sortOrderSqls = {
        "NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};

    for (auto i = 0; i < sortOrders.size(); ++i) {
      for (auto j = 0; j < sortOrders.size(); ++j) {
        auto plan =
            PlanBuilder()
                .values(input)
                .orderBy(sortingKeys, {sortOrders[i], sortOrders[j]}, true)
                .localMerge(sortingKeys, {sortOrders[i], sortOrders[j]})
                .planNode();

        assertQueryOrdered(
            plan,
            fmt::format(
                "SELECT * FROM tmp ORDER BY {} {}, {} {}",
                key1,
                sortOrderSqls[i],
                key2,
                sortOrderSqls[j]),
            sortingKeys);
      }
    }
  }
};

TEST_F(MergeTest, localMerge) {
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
    auto c3 = makeFlatVector<StringView>(batchSize, [](vector_size_t row) {
      return StringView(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2, c3}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0");
  testSingleKey(vectors, "c3");

  testTwoKeys(vectors, "c0", "c3");
  testTwoKeys(vectors, "c3", "c0");
}
