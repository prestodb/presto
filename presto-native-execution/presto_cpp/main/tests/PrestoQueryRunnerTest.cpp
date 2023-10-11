/*
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

#include "presto_cpp/main/tests/PrestoQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::presto::test {

class PrestoQueryRunnerTest : public ::testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::parse::registerTypeResolver();
  }
};

TEST_F(PrestoQueryRunnerTest, DISABLED_basic) {
  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://127.0.0.1:8080", "mbasmanova");

  auto results = queryRunner->execute("SELECT count(*) FROM nation");
  auto expected = makeRowVector({
      makeConstant<int64_t>(25, 1),
  });
  velox::exec::test::assertEqualResults({expected}, {results});

  results =
      queryRunner->execute("SELECT regionkey, count(*) FROM nation GROUP BY 1");

  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({5, 5, 5, 5, 5}),
  });
  velox::exec::test::assertEqualResults({expected}, {results});
}

TEST_F(PrestoQueryRunnerTest, DISABLED_fuzzer) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeArrayVectorFromJson<int64_t>({
          "[]",
          "[1, 2, 3]",
          "null",
          "[1, 2, 3, 4]",
      }),
  });

  auto plan = velox::exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"min(c0)", "max(c0)", "set_agg(c1)"})
                  .project({"a0", "a1", "array_sort(a2)"})
                  .planNode();

  auto queryRunner = std::make_unique<PrestoQueryRunner>(
      "http://127.0.0.1:8080", "mbasmanova");
  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());

  auto prestoResults = queryRunner->execute(
      sql.value(),
      {data},
      ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), ARRAY(BIGINT())}));

  auto veloxResults =
      velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());
  velox::exec::test::assertEqualResults(prestoResults, {veloxResults});
}

} // namespace facebook::presto::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
