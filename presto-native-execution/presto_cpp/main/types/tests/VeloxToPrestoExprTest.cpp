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
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

// VeloxToPrestoExprConverter is used only by RowExpressionOptimizer so unit
// tests are only added for simple expressions here. End-to-end tests to
// validate velox expression to Presto RowExpression conversion for different
// types of expressions can be found in TestDelegatingExpressionOptimizer.java
// in presto-native-sidecar-plugin.
class VeloxToPrestoExprTest : public ::testing::Test,
                              public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    veloxToPrestoExprConverter_ =
        std::make_unique<expression::VeloxToPrestoExprConverter>(pool_.get());
  }

  void testFile(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::string& fileName) {
    json::array_t expected = json::parse(
        slurp(facebook::presto::test::utils::getDataPath(fileName)));
    auto numExpr = inputs.size();
    for (auto i = 0; i < numExpr; i++) {
      RowExpressionPtr inputRowExpr;
      // The input row expression is provided by RowExpressionOptimizer in the
      // sidecar. Since VeloxToPrestoExprConverter is being tested here, we use
      // the expected row expression to mock the input. The input row expression
      // is only used for conversion to special form and call expressions.
      protocol::from_json(expected[i], inputRowExpr);
      EXPECT_EQ(
          veloxToPrestoExprConverter_->getRowExpression(
              inputs[i], inputRowExpr),
          inputRowExpr);
    }
  }

  std::unique_ptr<expression::VeloxToPrestoExprConverter>
      veloxToPrestoExprConverter_;
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
