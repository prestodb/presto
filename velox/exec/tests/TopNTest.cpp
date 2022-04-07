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

class TopNTest : public OperatorTestBase {
 protected:
  static std::vector<std::string> getSortOrderSqls() {
    return {"NULLS LAST", "NULLS FIRST", "DESC NULLS FIRST", "DESC NULLS LAST"};
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      int32_t limit) {
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);

    auto sortOrderSqls = getSortOrderSqls();

    for (const auto& sortOrderSql : sortOrderSqls) {
      auto sql = fmt::format("{} {}", key, sortOrderSql);

      auto plan =
          PlanBuilder().values(input).topN({sql}, limit, false).planNode();

      assertQueryOrdered(
          plan,
          fmt::format("SELECT * FROM tmp ORDER BY {} LIMIT {}", sql, limit),
          {keyIndex});
    }
  }

  void testSingleKey(
      const std::vector<RowVectorPtr>& input,
      const std::string& key,
      const std::string& filter) {
    auto keyIndex = input[0]->type()->asRow().getChildIdx(key);

    auto sortOrderSqls = getSortOrderSqls();

    for (const auto& sortOrderSql : sortOrderSqls) {
      auto sql = fmt::format("{} {}", key, sortOrderSql);

      auto plan = PlanBuilder()
                      .values(input)
                      .filter(filter)
                      .topN({sql}, 10, false)
                      .planNode();

      assertQueryOrdered(
          plan,
          fmt::format(
              "SELECT * FROM tmp WHERE {} ORDER BY {} LIMIT 10", filter, sql),
          {keyIndex});
    }
  }

  void testTwoKeys(
      const std::vector<RowVectorPtr>& input,
      const std::string& key1,
      const std::string& key2,
      int32_t limit) {
    auto rowType = input[0]->type()->asRow();
    auto keyIndices = {rowType.getChildIdx(key1), rowType.getChildIdx(key2)};

    auto sortOrderSqls = getSortOrderSqls();

    for (const auto& sortOrderSql1 : sortOrderSqls) {
      for (const auto& sortOrderSql2 : sortOrderSqls) {
        auto sql1 = fmt::format("{} {}", key1, sortOrderSql1);
        auto sql2 = fmt::format("{} {}", key2, sortOrderSql2);

        auto plan = PlanBuilder()
                        .values(input)
                        .topN({sql1, sql2}, limit, false)
                        .planNode();

        assertQueryOrdered(
            plan,
            fmt::format(
                "SELECT * FROM tmp ORDER BY {}, {} LIMIT {}",
                sql1,
                sql2,
                limit),
            keyIndices);
      }
    }
  }
};

TEST_F(TopNTest, selectiveFilter) {
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

TEST_F(TopNTest, singleKey) {
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

  // DESC NULLS LAST and ASC NULLS FIRST will return all rows where c0 is null;
  // There are 400 rows where c0 is null. Use limit greater than 400 to make the
  // query deterministic.
  testSingleKey(vectors, "c0", 410);

  // parser doesn't support "is not null" expression, hence, using c0 % 2 >= 0
  testSingleKey(vectors, "c0", "c0 % 2 >= 0");
}

TEST_F(TopNTest, multipleKeys) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 2; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [](vector_size_t row) { return row % 4; }, nullEvery(31, 1));
    auto c1 = makeFlatVector<int32_t>(
        batchSize, [](vector_size_t row) { return row; }, nullEvery(17));
    auto c2 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testTwoKeys(vectors, "c0", "c1", 200);
}

TEST_F(TopNTest, compaction) {
  vector_size_t batchSize = 1000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize,
        [&](vector_size_t row) { return batchSize * i + row; },
        nullEvery(31));
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c1, c1, c1, c1}));
  }
  createDuckDbTable(vectors);

  // Make sure LIMIT is greater than number of rows with nulls to avoid
  // non-deterministic results.
  testSingleKey(vectors, "c0", 500);

  testSingleKey(vectors, "c0", 900);
}

TEST_F(TopNTest, varchar) {
  vector_size_t batchSize = 1'000;
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
        nullEvery(31));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  // Make sure LIMIT is greater than number of rows with nulls to avoid
  // non-deterministic results.
  testSingleKey(vectors, "c2", 200);
}

TEST_F(TopNTest, multiBatch) {
  vector_size_t batchSize = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return batchSize * i + row; });
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; });
    auto c2 = makeFlatVector<StringView>(batchSize, [](vector_size_t row) {
      return StringView(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0", 1'500);
  testSingleKey(vectors, "c2", 2'500);
}

TEST_F(TopNTest, empty) {
  vector_size_t batchSize = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 5; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](vector_size_t row) { return batchSize * i + row; });
    auto c1 = makeFlatVector<double>(
        batchSize, [](vector_size_t row) { return row * 0.1; });
    auto c2 = makeFlatVector<StringView>(batchSize, [](vector_size_t row) {
      return StringView(std::to_string(row));
    });
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  testSingleKey(vectors, "c0", "c0 < 0");
}
