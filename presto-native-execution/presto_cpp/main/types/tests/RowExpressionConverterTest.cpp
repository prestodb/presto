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
#include "presto_cpp/main/types/RowExpressionConverter.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/expression/CastExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

// RowExpressionConverter is used only by RowExpressionOptimizer so unit tests
// are only added for simple expressions here. End-to-end tests to validate
// velox expression to Presto RowExpression conversion for different types of
// expressions can be found in TestDelegatingExpressionOptimizer.java in
// presto-native-sidecar-plugin.
class RowExpressionConverterTest
    : public ::testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rowExpressionConverter_ =
        std::make_unique<expression::RowExpressionConverter>(pool_.get());
  }

  void testFile(
      const std::vector<exec::ExprPtr>& inputs,
      const std::string& fileName) {
    json::array_t expected = json::parse(
        slurp(facebook::presto::test::utils::getDataPath(fileName)));
    auto numExpr = inputs.size();
    for (auto i = 0; i < numExpr; i++) {
      RowExpressionPtr inputRowExpr;
      // The input row expression is provided by RowExpressionOptimizer in the
      // sidecar. Since RowExpressionConverter is being tested here, we use the
      // expected row expression to mock the input. The input row expression is
      // only used for conversion to special form and call expressions.
      protocol::from_json(expected[i], inputRowExpr);
      EXPECT_EQ(
          rowExpressionConverter_->veloxToPrestoRowExpression(
              inputs[i], inputRowExpr),
          expected[i]);
    }
  }

  void testConstantExpressionConversion(
      const std::vector<exec::ExprPtr>& inputs) {
    json::array_t expected =
        json::parse(slurp(facebook::presto::test::utils::getDataPath(
            "ConstantExpressionConversion.json")));
    auto numExpr = inputs.size();
    for (auto i = 0; i < numExpr; i++) {
      auto constantExpr =
          std::dynamic_pointer_cast<exec::ConstantExpr>(inputs[i]);
      EXPECT_EQ(
          rowExpressionConverter_->getConstantRowExpression(constantExpr),
          expected[i]);
    }
  }

  std::unique_ptr<expression::RowExpressionConverter> rowExpressionConverter_;
};

TEST_F(RowExpressionConverterTest, constant) {
  auto inputs = std::vector<exec::ExprPtr>{
      std::make_shared<exec::ConstantExpr>(makeConstant<bool>(true, 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<int8_t>(12, 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<int16_t>(123, 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<int32_t>(1234, 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<int64_t>(12345, 1)),
      std::make_shared<exec::ConstantExpr>(
          makeConstant<int64_t>(std::numeric_limits<int64_t>::max(), 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<float>(123.456, 1)),
      std::make_shared<exec::ConstantExpr>(makeConstant<double>(1234.5678, 1)),
      std::make_shared<exec::ConstantExpr>(
          makeConstant<double>(std::numeric_limits<double>::quiet_NaN(), 1)),
      std::make_shared<exec::ConstantExpr>(
          makeConstant<StringView>("abcd", 1))};
  testFile(inputs, "ConstantExpressionConversion.json");
  testConstantExpressionConversion(inputs);
}

TEST_F(RowExpressionConverterTest, rowConstructor) {
  auto inputs = std::vector<exec::ExprPtr>{
      std::make_shared<exec::ConstantExpr>(makeConstantRow(
          ROW({BIGINT(), REAL(), VARCHAR()}),
          variant::row(
              {static_cast<int64_t>(123456),
               static_cast<float>(12.345),
               "abc"}),
          1)),
      std::make_shared<exec::ConstantExpr>(makeConstantRow(
          ROW({REAL(), VARCHAR(), INTEGER()}),
          variant::row(
              {std::numeric_limits<float>::quiet_NaN(),
               "",
               std::numeric_limits<int32_t>::max()}),
          1))};
  testFile(inputs, "RowConstructorExpressionConversion.json");
}

TEST_F(RowExpressionConverterTest, variable) {
  auto inputs = std::vector<exec::ExprPtr>{
      std::make_shared<exec::FieldReference>(
          VARCHAR(), std::vector<exec::ExprPtr>{}, "c0"),
      std::make_shared<exec::FieldReference>(
          ARRAY(BIGINT()), std::vector<exec::ExprPtr>{}, "c1"),
      std::make_shared<exec::FieldReference>(
          MAP(INTEGER(), REAL()), std::vector<exec::ExprPtr>{}, "c2"),
      std::make_shared<exec::FieldReference>(
          ROW({SMALLINT(), VARCHAR(), DOUBLE()}),
          std::vector<exec::ExprPtr>{},
          "c3"),
      std::make_shared<exec::FieldReference>(
          MAP(ARRAY(TINYINT()), BOOLEAN()), std::vector<exec::ExprPtr>{}, "c4"),
  };
  testFile(inputs, "VariableExpressionConversion.json");
}

TEST_F(RowExpressionConverterTest, special) {
  auto castExpr = std::make_shared<exec::CastCallToSpecialForm>();
  auto orExpr = std::make_shared<exec::ConjunctCallToSpecialForm>(false);
  auto andExpr = std::make_shared<exec::ConjunctCallToSpecialForm>(true);
  auto inputs = std::vector<exec::ExprPtr>{
      castExpr->constructSpecialForm(
          VARCHAR(),
          {std::make_shared<exec::FieldReference>(
              BIGINT(), std::vector<exec::ExprPtr>{}, "c0")},
          false,
          core::QueryConfig({})),
      orExpr->constructSpecialForm(
          BOOLEAN(),
          {std::make_shared<exec::FieldReference>(
               BOOLEAN(), std::vector<exec::ExprPtr>{}, "c1"),
           std::make_shared<exec::ConstantExpr>(makeConstant(true, 1))},
          false,
          core::QueryConfig({})),
      andExpr->constructSpecialForm(
          BOOLEAN(),
          {castExpr->constructSpecialForm(
               BOOLEAN(),
               {std::make_shared<exec::FieldReference>(
                   INTEGER(), std::vector<exec::ExprPtr>{}, "c2")},
               false,
               core::QueryConfig({})),
           std::make_shared<exec::FieldReference>(
               BOOLEAN(), std::vector<exec::ExprPtr>{}, "c3")},
          false,
          core::QueryConfig({}))};
  testFile(inputs, "CallAndSpecialFormExpressionConversion.json");
}
