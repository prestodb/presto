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
#include <fmt/format.h>

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::test {

namespace {

class MinMaxByAggregationTest : public AggregationTestBase {
 protected:
  void testGlobalAggregation(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& aggName,
      const std::string& colName) {
    std::string funcName = aggName == kMaxBy ? "max" : "min";

    for (int i = 0; i < 6; ++i) {
      auto valColName = fmt::format("c{}", i);

      auto op =
          PlanBuilder()
              .values(vectors)
              .partialAggregation(
                  {}, {fmt::format("{}({}, {})", aggName, valColName, colName)})
              .finalAggregation()
              .planNode();

      assertQuery(
          op,
          "SELECT " + valColName + " FROM tmp WHERE " + colName +
              " = ( SELECT " + funcName + "(" + colName +
              ") FROM tmp) LIMIT 1");
    }
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           BIGINT()})};
};

TEST_F(MinMaxByAggregationTest, maxByPartialConst) {
  // Const values.
  auto vectors = {
      makeRowVector({
          makeConstant(5, 10),
          makeConstant(10, 10),
      }),
  };

  // Const nulls.
  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"max_by(c0, c1)"})
                .planNode();
  assertQuery(op, "SELECT struct_pack(x => 5, y => 10)");
}

TEST_F(MinMaxByAggregationTest, maxByPartialConstNull) {
  auto vectors = {
      makeRowVector({
          makeConstant(5, 10),
          makeNullConstant(TypeKind::BIGINT, 10),
      }),
  };

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"max_by(c0, c1)"})
                .planNode();
  assertQuery(op, "SELECT null");
}

TEST_F(MinMaxByAggregationTest, maxByPartialNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({1, std::nullopt}),
          makeNullableFlatVector<int64_t>({5, 10}),
      }),
  };

  auto partialAgg = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({}, {"max_by(c0, c1)"})
                        .planNode();
  assertQuery(partialAgg, "SELECT struct_pack(x => NULL, y => 10)");
}

TEST_F(MinMaxByAggregationTest, maxByNoNullsCase) {
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>({1, 3}),
          makeFlatVector<int64_t>({5, 10}),
      }),
  };

  auto partialAgg = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({}, {"max_by(c0, c1)"})
                        .planNode();
  assertQuery(partialAgg, "SELECT struct_pack(x => 3, y => 10)");
}

TEST_F(MinMaxByAggregationTest, maxByPartialGroupByNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>(
              {std::nullopt, 5, std::nullopt, 5, std::nullopt, 5}),
          makeNullableFlatVector<int64_t>({20, 10, 20, 10, 20, 10}),
          makeNullableFlatVector<int32_t>({1, 1, 2, 2, 3, 3}),
      }),
  };

  auto partialAgg = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({"c2"}, {"max_by(c0, c1)"})
                        .planNode();
  assertQuery(
      partialAgg,
      "SELECT * FROM( VALUES (1, struct_pack(x => NULL, y => 20)), (2, struct_pack(x => NULL, y => 20)), (3, struct_pack(x => NULL, y => 20))) AS t");
}

TEST_F(MinMaxByAggregationTest, maxByFinalNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({1, std::nullopt}),
          makeNullableFlatVector<int64_t>({5, 10}),
      }),
  };

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"max_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT NULL");
}

TEST_F(MinMaxByAggregationTest, maxByFinalGroupByNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>(
              {std::nullopt, 5, std::nullopt, 5, std::nullopt, 5}),
          makeNullableFlatVector<int64_t>({20, 10, 15, 30, 7, 5}),
          makeNullableFlatVector<int32_t>({1, 1, 2, 2, 3, 3}),
      }),
  };

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({"c2"}, {"max_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT * FROM( VALUES (1, NULL), (2, 5), (3, NULL)) AS t");
}

TEST_F(MinMaxByAggregationTest, maxByGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  for (auto& columnName : rowType_->names()) {
    testGlobalAggregation(vectors, kMaxBy, columnName);
  }
}

TEST_F(MinMaxByAggregationTest, maxByGroupBy) {
  const size_t size = 1'000;
  auto vectors = {makeRowVector(
      {makeFlatVector<double>(
           size, [](vector_size_t row) { return row * 0.1; }),
       makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<int32_t>(
           size, [](vector_size_t row) { return row % 10; })})};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({"c2"}, {"max_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(
      op, "SELECT c2, max(CAST(c1 as DOUBLE)) * 0.1 FROM tmp GROUP BY 1");
}

TEST_F(MinMaxByAggregationTest, minByPartialNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({std::nullopt, 1}),
          makeNullableFlatVector<int64_t>({5, 10}),
      }),
  };

  auto partialAgg = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({}, {"min_by(c0, c1)"})
                        .planNode();
  assertQuery(partialAgg, "SELECT struct_pack(x => NULL, y => 5)");
}

TEST_F(MinMaxByAggregationTest, minByFinalNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({std::nullopt, 1}),
          makeNullableFlatVector<int64_t>({5, 10}),
      }),
  };

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({}, {"min_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT NULL");
}

TEST_F(MinMaxByAggregationTest, minByFinalGroupByNullCase) {
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>(
              {std::nullopt, 5, std::nullopt, 5, std::nullopt, 5}),
          makeNullableFlatVector<int64_t>({20, 10, 15, 30, 7, 5}),
          makeNullableFlatVector<int32_t>({1, 1, 2, 2, 3, 3}),
      }),
  };

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({"c2"}, {"min_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(op, "SELECT * FROM( VALUES (1, 5), (2, NULL), (3, 5)) AS t");
}

TEST_F(MinMaxByAggregationTest, minByGlobal) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  for (auto& columnName : rowType_->names()) {
    testGlobalAggregation(vectors, kMinBy, columnName);
  }
}

TEST_F(MinMaxByAggregationTest, minByGroupBy) {
  const size_t size = 1'000;
  auto vectors = {makeRowVector(
      {makeFlatVector<double>(
           size, [](vector_size_t row) { return row * 0.1; }),
       makeFlatVector<int64_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<int32_t>(
           size, [](vector_size_t row) { return row % 10; })})};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .partialAggregation({"c2"}, {"min_by(c0, c1)"})
                .finalAggregation()
                .planNode();
  assertQuery(
      op, "SELECT c2, min(CAST(c1 as DOUBLE)) * 0.1 FROM tmp GROUP BY 1");
}

} // namespace
} // namespace facebook::velox::aggregate::test
