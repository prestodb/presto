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
#include "presto_cpp/main/types/VeloxToPrestoExpr.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

// Unit tests are added only for simple expressions here. End-to-end tests that
// validate Velox core::TypedExpr to Presto protocol::RowExpression conversion
// for more exhaustive cases are added in TestNativeExpressionInterpreter.java.
class VeloxToPrestoExprTest : public ::testing::Test,
                              public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void testFile(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::string& fileName) {
    expression::VeloxToPrestoExprConverter veloxToPrestoExprConverter =
        expression::VeloxToPrestoExprConverter(pool_.get());
    json::array_t expected = json::parse(
        slurp(facebook::presto::test::utils::getDataPath(fileName)));
    auto numExpr = inputs.size();
    json j;
    for (auto i = 0; i < numExpr; i++) {
      const auto converted =
          veloxToPrestoExprConverter.getRowExpression(inputs[i]);
      protocol::to_json(j, converted);
      ASSERT_EQ(j, expected[i]);
    }
  }
};

TEST_F(VeloxToPrestoExprTest, constant) {
  auto inputs = std::vector<core::TypedExprPtr>{
      std::make_shared<core::ConstantTypedExpr>(makeConstant<bool>(true, 1)),
      std::make_shared<core::ConstantTypedExpr>(makeConstant<int8_t>(12, 1)),
      std::make_shared<core::ConstantTypedExpr>(makeConstant<int16_t>(123, 1)),
      std::make_shared<core::ConstantTypedExpr>(makeConstant<int32_t>(1234, 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<int64_t>(12345, 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<int64_t>(std::numeric_limits<int64_t>::max(), 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<float>(123.456, 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<double>(1234.5678, 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<double>(std::numeric_limits<double>::quiet_NaN(), 1)),
      std::make_shared<core::ConstantTypedExpr>(
          makeConstant<StringView>("abcd", 1))};
  testFile(inputs, "ConstantExpressionConversion.json");
}

TEST_F(VeloxToPrestoExprTest, rowConstructor) {
  auto inputs = std::vector<core::TypedExprPtr>{
      std::make_shared<core::ConstantTypedExpr>(makeConstantRow(
          ROW({BIGINT(), REAL(), VARCHAR()}),
          variant::row(
              {static_cast<int64_t>(123456),
               static_cast<float>(12.345),
               "abc"}),
          1)),
      std::make_shared<core::ConstantTypedExpr>(makeConstantRow(
          ROW({REAL(), VARCHAR(), INTEGER()}),
          variant::row(
              {std::numeric_limits<float>::quiet_NaN(),
               "",
               std::numeric_limits<int32_t>::max()}),
          1))};
  testFile(inputs, "RowConstructorExpressionConversion.json");
}

TEST_F(VeloxToPrestoExprTest, variable) {
  auto inputs = std::vector<core::TypedExprPtr>{
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(ARRAY(BIGINT()), "c1"),
      std::make_shared<core::FieldAccessTypedExpr>(
          MAP(INTEGER(), REAL()), "c2"),
      std::make_shared<core::FieldAccessTypedExpr>(
          ROW({SMALLINT(), VARCHAR(), DOUBLE()}), "c3"),
      std::make_shared<core::FieldAccessTypedExpr>(
          MAP(ARRAY(TINYINT()), BOOLEAN()), "c4"),
  };
  testFile(inputs, "VariableExpressionConversion.json");
}

TEST_F(VeloxToPrestoExprTest, special) {
  auto inputs = std::vector<core::TypedExprPtr>{
      std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>(
              {std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c1"),
               std::make_shared<core::ConstantTypedExpr>(
                   makeConstant(true, 1))}),
          "or"),
      std::make_shared<core::CallTypedExpr>(
          BOOLEAN(),
          std::vector<core::TypedExprPtr>(
              {std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c2"),
               std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c3")}),
          "and")};
  testFile(inputs, "CallAndSpecialFormExpressionConversion.json");
}
