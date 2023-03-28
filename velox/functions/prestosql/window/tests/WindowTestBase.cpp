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

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {
struct QueryInfo {
  const core::PlanNodePtr planNode;
  const std::string functionSql;
  const std::string querySql;
};

QueryInfo buildWindowQuery(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause) {
  std::string functionSql =
      fmt::format("{} over ({} {})", function, overClause, frameClause);
  auto op = PlanBuilder().values(input).window({functionSql}).planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());
  std::string querySql =
      fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql);

  return {op, functionSql, querySql};
}

}; // namespace

RowVectorPtr WindowTestBase::makeSimpleVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row % 7; }, nullEvery(15)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSinglePartitionVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row; }, nullEvery(7)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSingleRowPartitionsVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 11 + 1; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 13 + 1; }),
  });
}

VectorPtr WindowTestBase::makeRandomInputVector(
    const TypePtr& type,
    vector_size_t size,
    float nullRatio) {
  VectorFuzzer::Options options;
  options.vectorSize = size;
  options.nullRatio = nullRatio;
  options.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  VectorFuzzer fuzzer(options, pool_.get(), 0);
  return fuzzer.fuzzFlat(type);
}

RowVectorPtr WindowTestBase::makeRandomInputVector(vector_size_t size) {
  boost::random::mt19937 gen;
  // Frame index values require integer values > 0.
  auto genRandomFrameValue = [&](vector_size_t /*row*/) {
    return boost::random::uniform_int_distribution<int>(1)(gen);
  };
  return makeRowVector(
      {makeRandomInputVector(BIGINT(), size, 0.2),
       makeRandomInputVector(VARCHAR(), size, 0.3),
       makeFlatVector<int64_t>(size, genRandomFrameValue),
       makeFlatVector<int64_t>(size, genRandomFrameValue)});
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::vector<std::string>& overClauses,
    const std::vector<std::string>& frameClauses) {
  createDuckDbTable(input);
  for (const auto& overClause : overClauses) {
    for (auto& frameClause : frameClauses) {
      auto queryInfo =
          buildWindowQuery(input, function, overClause, frameClause);
      SCOPED_TRACE(queryInfo.functionSql);
      assertQuery(queryInfo.planNode, queryInfo.querySql);
    }
  }
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& errorMessage) {
  assertWindowFunctionError(input, function, overClause, "", errorMessage);
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause,
    const std::string& errorMessage) {
  auto queryInfo = buildWindowQuery(input, function, overClause, frameClause);
  SCOPED_TRACE(queryInfo.functionSql);

  VELOX_ASSERT_THROW(
      assertQuery(queryInfo.planNode, queryInfo.querySql), errorMessage);
}

}; // namespace facebook::velox::window::test
