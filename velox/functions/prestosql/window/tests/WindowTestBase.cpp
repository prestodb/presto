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
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

std::vector<RowVectorPtr> WindowTestBase::makeVectors(
    const RowTypePtr& rowType,
    vector_size_t size,
    int numVectors,
    float nullRatio) {
  std::vector<RowVectorPtr> vectors;
  VectorFuzzer::Options options;
  options.vectorSize = size;
  options.nullRatio = nullRatio;
  VectorFuzzer fuzzer(options, pool_.get(), 0);
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzRow(rowType));
    vectors.push_back(vector);
  }
  return vectors;
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause) {
  auto functionSql = fmt::format("{} over ({})", function, overClause);

  SCOPED_TRACE(functionSql);
  auto op = PlanBuilder().values(input).window({functionSql}).planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());

  assertQuery(
      op, fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql));
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::vector<std::string>& overClauses) {
  for (const auto& overClause : overClauses) {
    testWindowFunction(input, function, overClause);
  }
}

void WindowTestBase::testTwoColumnInput(
    const std::vector<RowVectorPtr>& input,
    const std::string& windowFunction) {
  VELOX_CHECK_EQ(input[0]->childrenSize(), 2);

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1",
      "partition by c1 order by c0",
      "partition by c0 order by c1 desc",
      "partition by c1 order by c0 desc",
      "partition by c0 order by c1 nulls first",
      "partition by c1 order by c0 nulls first",
      "partition by c0 order by c1 desc nulls first",
      "partition by c1 order by c0 desc nulls first",
      // No partition by clause.
      "order by c0, c1",
      "order by c1, c0",
      "order by c0 asc, c1 desc",
      "order by c1 asc, c0 desc",
      "order by c0 asc nulls first, c1 desc nulls first",
      "order by c1 asc nulls first, c0 desc nulls first",
      "order by c0 desc nulls first, c1 asc nulls first",
      "order by c1 desc nulls first, c0 asc nulls first",
      // No order by clause.
      "partition by c0, c1",
  };

  createDuckDbTable(input);
  testWindowFunction(input, windowFunction, overClauses);

  // Invoking with same vector set twice so that the underlying WindowFunction
  // receives the same data set multiple times and does a full processing
  // (partition, sort) + apply of it.
  std::vector<RowVectorPtr> doubleInput;
  doubleInput.insert(doubleInput.end(), input.begin(), input.end());
  doubleInput.insert(doubleInput.end(), input.begin(), input.end());
  createDuckDbTable(doubleInput);
  testWindowFunction(doubleInput, windowFunction, overClauses);
}

}; // namespace facebook::velox::window::test
