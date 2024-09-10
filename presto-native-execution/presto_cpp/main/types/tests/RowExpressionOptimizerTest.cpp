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
#include "presto_cpp/main/types/RowExpressionOptimizer.h"
#include <folly/Uri.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;

// RowExpressionOptimizerTest only tests for basic expression optimization.
// End-to-end tests for different types of expressions can be found in
// TestDelegatingExpressionOptimizer.java in presto-native-sidecar-plugin.
class RowExpressionOptimizerTest
    : public ::testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    parse::registerTypeResolver();
    functions::prestosql::registerAllScalarFunctions("presto.default.");
    exec::registerFunctionCallToSpecialForms();
    rowExpressionOptimizer_ =
        std::make_unique<expression::RowExpressionOptimizer>(pool());
  }

  void testFile(const std::string& testName) {
    auto input = slurp(facebook::presto::test::utils::getDataPath(
        fmt::format("{}Input.json", testName)));
    json::array_t inputExpressions = json::parse(input);
    proxygen::HTTPMessage httpMessage;
    httpMessage.getHeaders().set(
        "X-Presto-Time-Zone", "America/Bahia_Banderas");
    httpMessage.getHeaders().set(
        "X-Presto-Expression-Optimizer-Level", "OPTIMIZED");
    auto result = rowExpressionOptimizer_->optimize(
        httpMessage.getHeaders(), inputExpressions);

    EXPECT_EQ(result.second, true);
    json resultExpressions = result.first;
    EXPECT_EQ(resultExpressions.is_array(), true);
    auto expected = slurp(facebook::presto::test::utils::getDataPath(
        fmt::format("{}Expected.json", testName)));
    json::array_t expectedExpressions = json::parse(expected);
    auto numExpressions = resultExpressions.size();
    EXPECT_EQ(numExpressions, expectedExpressions.size());
    for (auto i = 0; i < numExpressions; i++) {
      EXPECT_EQ(resultExpressions.at(i), expectedExpressions.at(i));
    }
  }

  std::unique_ptr<expression::RowExpressionOptimizer> rowExpressionOptimizer_;
};

TEST_F(RowExpressionOptimizerTest, simple) {
  // Files SimpleExpressions{Input|Expected}.json contain the input and expected
  // JSON representing the RowExpressions resulting from the following queries:
  // 1. select 1 + 2;
  // 2. select abs(-11) + ceil(cast(3.4 as double)) + floor(cast(5.6 as
  // double));
  // 3. select 2 between 1 and 3;
  // Simple expression evaluation with constant folding is verified here.
  testFile("SimpleExpressions");
}

TEST_F(RowExpressionOptimizerTest, switchSpecialForm) {
  // Files SwitchSpecialFormExpressions{Input|Expected}.json contain the input
  // and expected JSON representing the RowExpressions resulting from the
  // following queries:
  // Simple form:
  // 1. select case 1 when 1 then 32 + 1 when 1 then 34 end;
  // 2. select case null when true then 32 else 33 end;
  // 3. select case 33 when 0 then 0 when 33 then 1 when unbound_long then 2
  //      else 1 end from tmp;
  // Searched form:
  // 1. select case when true then 32 + 1 end;
  // 2. select case when false then 1 else 34 - 1 end;
  // 3. select case when ARRAY[CAST(1 AS BIGINT)] = ARRAY[CAST(1 AS BIGINT)]
  //      then 'matched' else 'not_matched' end;
  // Evaluation of both simple and searched forms of `SWITCH` special form
  // expression is verified here.
  testFile("SwitchSpecialFormExpressions");
}

TEST_F(RowExpressionOptimizerTest, inSpecialForm) {
  // Files InSpecialFormExpressions{Input|Expected}.json contain the input
  // and expected JSON representing the RowExpressions resulting from the
  // following queries:
  // 1. select 3 in (2, null, 3, 5);
  // 2. select 'foo' in ('bar', 'baz', 'buz', 'blah');
  // 3. select null in (2, null, 3, 5);
  // 4. select ROW(1) IN (ROW(2), ROW(1), ROW(2));
  // 5. select MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2],
  //    ARRAY[2, null]), null);
  // 6. select MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 3],
  //    ARRAY[1, null]));
  // Evaluation of `IN` special form expression for primitive and complex types
  // is verified here.
  testFile("InSpecialFormExpressions");
}

TEST_F(RowExpressionOptimizerTest, specialFormRewrites) {
  // Files SpecialFormExpressionRewrites{Input|Expected}.json contain the input
  // and expected JSON representing the RowExpressions resulting from the
  // following queries:
  // 1. select if(1 < 2, 2, 3);
  // 2. select (1 < 2) and (2 < 3);
  // 3. select (1 < 2) or (2 < 3);
  // Special form expression rewrites are verified here.
  testFile("SpecialFormExpressionRewrites");
}

TEST_F(RowExpressionOptimizerTest, betweenCallExpression) {
  // Files BetweenCallExpressions{Input|Expected}.json contain the input and
  // expected JSON representing the RowExpressions resulting from the following
  // queries:
  // 1. select 2 between 3 and 4;
  // 2. select null between 2 and 4;
  // 3. select 'cc' between 'b' and 'd';
  // Evaluation of `BETWEEN` call expression is verified here.
  testFile("BetweenCallExpressions");
}
