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
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class RowNumberTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    velox::window::registerWindowFunctions();
  }

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    VectorFuzzer::Options options;
    options.vectorSize = size;
    VectorFuzzer fuzzer(options, pool_.get(), 0);
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector =
          std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzRow(rowType));
      vectors.push_back(vector);
    }
    return vectors;
  }

  void basicTests(const std::vector<RowVectorPtr>& vectors) {
    auto testWindowSql = [&](const std::vector<RowVectorPtr>& input,
                             const std::string& overClause) {
      auto windowSql = fmt::format("row_number() over ({})", overClause);

      SCOPED_TRACE(windowSql);
      VELOX_CHECK_GE(input[0]->size(), 2);

      auto op = PlanBuilder().values(input).window({windowSql}).planNode();
      assertQuery(op, "SELECT c0, c1, " + windowSql + " FROM tmp");
    };

    std::vector<std::string> overClauses = {
        "partition by c0 order by c1",
        "partition by c1 order by c0",
        "partition by c0 order by c1 desc",
        "partition by c1 order by c0 desc",
        // No partition by clause.
        "order by c0, c1",
        "order by c1, c0",
        "order by c0 asc, c1 desc",
        "order by c1 asc, c0 desc",
        // No order by clause.
        "partition by c0, c1",
    };

    for (const auto& overClause : overClauses) {
      testWindowSql(vectors, overClause);
    }
  }
};

TEST_F(RowNumberTest, basic) {
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(
          size, [](auto row) -> int32_t { return row % 5; }),
      makeFlatVector<int32_t>(
          size, [](auto row) -> int32_t { return row % 7; }),
  });

  createDuckDbTable({vectors});
  basicTests({vectors});
}

TEST_F(RowNumberTest, singlePartition) {
  // Test all input rows in a single partition.
  vector_size_t size = 1'000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
  });

  // Invoking with 2 vectors so that the underlying WindowFunction::apply() is
  // called twice for the same partition.
  std::vector<RowVectorPtr> input = {vectors, vectors};
  createDuckDbTable(input);
  basicTests(input);
}

TEST_F(RowNumberTest, randomInput) {
  auto vectors = makeVectors(
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT()}),
      10,
      2);
  createDuckDbTable(vectors);

  auto testWindowSql = [&](std::vector<RowVectorPtr>& input,
                           const std::string& overClause) {
    auto windowSql = fmt::format("row_number() over ({})", overClause);

    SCOPED_TRACE(windowSql);
    auto op = PlanBuilder().values(input).window({windowSql}).planNode();
    assertQuery(
        op, fmt::format("SELECT c0, c1, c2, c3, {} FROM tmp", windowSql));
  };

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1, c2, c3",
      "partition by c1 order by c0, c2, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c1 order by c0 desc, c2, c3",
      "order by c0, c1, c2, c3",
      "partition by c0, c1, c2, c3",
  };

  for (const auto& overClause : overClauses) {
    testWindowSql(vectors, overClause);
  }
}

}; // namespace
}; // namespace facebook::velox::window::test
