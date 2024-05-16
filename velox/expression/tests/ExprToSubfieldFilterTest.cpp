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

#include "velox/expression/ExprToSubfieldFilter.h"
#include <gtest/gtest.h>
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::exec {
namespace {

using namespace facebook::velox::common;

void validateSubfield(
    const Subfield& subfield,
    const std::vector<std::string>& expectedPath) {
  ASSERT_EQ(subfield.path().size(), expectedPath.size());
  for (int i = 0; i < expectedPath.size(); ++i) {
    ASSERT_TRUE(subfield.path()[i]);
    ASSERT_EQ(*subfield.path()[i], Subfield::NestedField(expectedPath[i]));
  }
}

class ExprToSubfieldFilterTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    memory::MemoryManager::testingSetInstance({});
  }

  core::TypedExprPtr parseExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    return core::Expressions::inferTypes(
        parse::parseExpr(expr, {}), type, pool_.get());
  }

  core::CallTypedExprPtr parseCallExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        parseExpr(expr, type));
    VELOX_CHECK_NOT_NULL(call);
    return call;
  }

  core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  SimpleExpressionEvaluator evaluator_{queryCtx_.get(), pool_.get()};
};

TEST_F(ExprToSubfieldFilterTest, eq) {
  auto call = parseCallExpr("a = 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, eqExpr) {
  auto call = parseCallExpr("a = 21 * 2", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, eqSubfield) {
  auto call = parseCallExpr("a.b = 42", ROW({{"a", ROW({{"b", BIGINT()}})}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a", "b"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());
}

TEST_F(ExprToSubfieldFilterTest, neq) {
  auto call = parseCallExpr("a <> 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, lte) {
  auto call = parseCallExpr("a <= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, lt) {
  auto call = parseCallExpr("a < 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gte) {
  auto call = parseCallExpr("a >= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gt) {
  auto call = parseCallExpr("a > 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, between) {
  auto call = parseCallExpr("a between 40 and 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), 40 <= i && i <= 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, in) {
  auto call = parseCallExpr("a in (40, 42)", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), i == 40 || i == 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, isNull) {
  auto call = parseCallExpr("a is null", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(0));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, isNotNull) {
  auto call = parseCallExpr("a is not null", ROW({{"a", BIGINT()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(0));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, like) {
  auto call = parseCallExpr("a like 'foo%'", ROW({{"a", VARCHAR()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, nonConstant) {
  auto call =
      parseCallExpr("a = b + 1", ROW({{"a", BIGINT()}, {"b", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, userError) {
  auto call = parseCallExpr("a = 1 / 0", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, dereferenceWithEmptyField) {
  auto call = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::DereferenceTypedExpr>(
              REAL(),
              std::make_shared<core::FieldAccessTypedExpr>(
                  ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}}),
                  std::make_shared<core::InputTypedExpr>(ROW(
                      {{"c0",
                        ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}})}})),
                  "c0"),
              1)},
      "is_null");
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

} // namespace
} // namespace facebook::velox::exec
