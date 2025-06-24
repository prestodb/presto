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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class SparkQueryRunnerTest : public ::testing::Test,
                             public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    parquet::registerParquetWriterFactory();
  }

  void SetUp() override {
    velox::functions::sparksql::registerFunctions("");
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::functions::aggregate::sparksql::registerAggregateFunctions("");
    velox::parse::registerTypeResolver();
  }
};

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_basic) {
  auto aggregatePool = rootPool_->addAggregateChild("basic");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "basic");

  auto input = makeRowVector({
      makeConstant<int64_t>(1, 25),
  });
  auto plan = exec::test::PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"count(c0)"})
                  .planNode();
  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  auto expected = makeRowVector({
      makeConstant<int64_t>(25, 1),
  });
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {expected});

  input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2,
                               3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
  });
  plan = exec::test::PlanBuilder()
             .values({input})
             .singleAggregation({"c0"}, {"count(c1)"})
             .planNode();
  sparkResults = queryRunner->executeAndReturnVector(plan);
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({5, 5, 5, 5, 5}),
  });
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {expected});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_decimal) {
  auto aggregatePool = rootPool_->addAggregateChild("decimal");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "decimal");
  auto input = makeRowVector({
      makeConstant<int128_t>(123456789, 25, DECIMAL(34, 2)),
  });
  auto plan =
      exec::test::PlanBuilder().values({input}).project({"abs(c0)"}).planNode();
  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {input});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_fuzzer) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeNullableFlatVector<int64_t>({std::nullopt, 1, 2, std::nullopt, 4, 5}),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"sum(c0)", "collect_list(c1)"})
                  .project({"a0", "array_sort(a1)"})
                  .planNode();

  auto aggregatePool = rootPool_->addAggregateChild("fuzzer");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "fuzzer");
  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());

  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  auto veloxResults = exec::test::AssertQueryBuilder(plan).copyResults(pool());
  exec::test::assertEqualResults(sparkResults.first.value(), {veloxResults});
}

TEST_F(SparkQueryRunnerTest, toSql) {
  auto aggregatePool = rootPool_->addAggregateChild("toSql");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "unused", "unused", "unused");

  auto dataType = ROW({"c0", "c1", "c2"}, {DOUBLE(), DOUBLE(), BOOLEAN()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan("tmp", dataType)
                  .singleAggregation({"c1"}, {"avg(c0)"})
                  .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT c1, avg(c0) as a0 FROM (tmp) GROUP BY c1");

  plan = exec::test::PlanBuilder()
             .tableScan("tmp", dataType)
             .singleAggregation({"c1"}, {"sum(c0)"})
             .project({"a0 / c1"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT (a0 / c1) as p0 FROM (SELECT c1, sum(c0) as a0 FROM (tmp) GROUP BY c1)");

  plan = exec::test::PlanBuilder()
             .tableScan("tmp", dataType)
             .singleAggregation({}, {"avg(c0)", "avg(c1)"}, {"c2"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT avg(c0) filter (where c2) as a0, avg(c1) as a1 FROM (tmp)");

  auto data =
      makeRowVector({makeFlatVector<int64_t>({}), makeFlatVector<int64_t>({})});
  plan = exec::test::PlanBuilder()
             .values({data})
             .singleAggregation({}, {"sum(distinct c0)"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan), "SELECT sum(distinct c0) as a0 FROM (t_0)");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
