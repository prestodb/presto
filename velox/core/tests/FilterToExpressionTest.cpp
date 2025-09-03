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
#include "velox/core/FilterToExpression.h"
#include <gtest/gtest.h>
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::core::test {

class FilterToExpressionTest : public testing::Test,
                               public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  // Helper method to create a row type for testing
  RowTypePtr createTestRowType() {
    return ROW(
        {"a", "b", "c", "d", "e", "f"},
        {BIGINT(), DOUBLE(), VARCHAR(), BOOLEAN(), REAL(), TIMESTAMP()});
  }

  // Helper method to verify expression type and structure
  void verifyExpr(
      const TypedExprPtr& expr,
      const std::string& expectedType,
      const std::string& expectedName) {
    ASSERT_TRUE(expr != nullptr);
    ASSERT_EQ(expr->type()->toString(), expectedType);

    auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
    ASSERT_TRUE(callExpr != nullptr);
    ASSERT_EQ(callExpr->name(), expectedName);
  }

  // Helper method for round trip testing
  void testRoundTrip(
      const std::string& fieldName,
      std::unique_ptr<common::Filter> filter);

  core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  exec::SimpleExpressionEvaluator evaluator_{queryCtx_.get(), pool_.get()};
};

TEST_F(FilterToExpressionTest, AlwaysTrue) {
  auto filter = std::make_unique<common::AlwaysTrue>();
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  ASSERT_TRUE(expr != nullptr);
  ASSERT_EQ(expr->type()->toString(), "BOOLEAN");

  auto constantExpr = std::dynamic_pointer_cast<const ConstantTypedExpr>(expr);
  ASSERT_TRUE(constantExpr != nullptr);
  ASSERT_TRUE(constantExpr->value().value<TypeKind::BOOLEAN>());
}

TEST_F(FilterToExpressionTest, AlwaysFalse) {
  auto filter = std::make_unique<common::AlwaysFalse>();
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  ASSERT_TRUE(expr != nullptr);
  ASSERT_EQ(expr->type()->toString(), "BOOLEAN");

  auto constantExpr = std::dynamic_pointer_cast<const ConstantTypedExpr>(expr);
  ASSERT_TRUE(constantExpr != nullptr);
  ASSERT_FALSE(constantExpr->value().value<TypeKind::BOOLEAN>());
}

TEST_F(FilterToExpressionTest, IsNull) {
  auto filter = std::make_unique<common::IsNull>();
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "is_null");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 1);
}

TEST_F(FilterToExpressionTest, IsNotNull) {
  auto filter = std::make_unique<common::IsNotNull>();
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "not");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 1);

  // Verify the inner expression is an IS_NULL operation
  auto isNullExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(isNullExpr != nullptr);
  ASSERT_EQ(isNullExpr->name(), "is_null");
}

TEST_F(FilterToExpressionTest, BoolValue) {
  auto filter = std::make_unique<common::BoolValue>(true, false);
  common::Subfield subfield("d");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "eq");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  // First input should be the field access expression
  auto fieldExpr = callExpr->inputs()[0];
  ASSERT_TRUE(fieldExpr != nullptr);

  // Second input should be the boolean constant
  auto constantExpr =
      std::dynamic_pointer_cast<const ConstantTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(constantExpr != nullptr);
  ASSERT_EQ(constantExpr->value().value<TypeKind::BOOLEAN>(), true);
}

TEST_F(FilterToExpressionTest, BigintRangeSingleValue) {
  auto filter = std::make_unique<common::BigintRange>(42, 42, false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "eq");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto constantExpr =
      std::dynamic_pointer_cast<const ConstantTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(constantExpr != nullptr);
  ASSERT_EQ(constantExpr->value().value<TypeKind::BIGINT>(), 42);
}

TEST_F(FilterToExpressionTest, BigintRangeWithRange) {
  auto filter = std::make_unique<common::BigintRange>(10, 20, false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto greaterOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(greaterOrEqual != nullptr);
  ASSERT_EQ(greaterOrEqual->name(), "gte");

  auto lessOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(lessOrEqual != nullptr);
  ASSERT_EQ(lessOrEqual->name(), "lte");
}

TEST_F(FilterToExpressionTest, NegatedBigintRangeSingleValue) {
  auto filter = std::make_unique<common::NegatedBigintRange>(42, 42, false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  // The implementation now uses getNonNegated() which creates a NOT expression
  // even for single values, so we expect "not" instead of "neq"
  verifyExpr(expr, "BOOLEAN", "not");
  auto notExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(notExpr->inputs().size(), 1);

  // The inner expression might be an OR expression due to handleNullAllowed
  auto innerExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(notExpr->inputs()[0]);
  ASSERT_TRUE(innerExpr != nullptr);

  if (innerExpr->name() == "or") {
    // If it's an OR expression, the first input should be the EQ operation
    ASSERT_EQ(innerExpr->inputs().size(), 2);
    auto eqExpr =
        std::dynamic_pointer_cast<const CallTypedExpr>(innerExpr->inputs()[0]);
    ASSERT_TRUE(eqExpr != nullptr);
    ASSERT_EQ(eqExpr->name(), "eq");
    ASSERT_EQ(eqExpr->inputs().size(), 2);

    // Verify the constant value is 42
    auto constantExpr =
        std::dynamic_pointer_cast<const ConstantTypedExpr>(eqExpr->inputs()[1]);
    ASSERT_TRUE(constantExpr != nullptr);
    ASSERT_EQ(constantExpr->value().value<TypeKind::BIGINT>(), 42);
  } else if (innerExpr->name() == "eq") {
    // If it's directly an EQ expression
    ASSERT_EQ(innerExpr->inputs().size(), 2);

    // Verify the constant value is 42
    auto constantExpr = std::dynamic_pointer_cast<const ConstantTypedExpr>(
        innerExpr->inputs()[1]);
    ASSERT_TRUE(constantExpr != nullptr);
    ASSERT_EQ(constantExpr->value().value<TypeKind::BIGINT>(), 42);
  } else {
    FAIL() << "Expected either 'or' or 'eq' expression, got: "
           << innerExpr->name();
  }
}

TEST_F(FilterToExpressionTest, DoubleRange) {
  auto filter = std::make_unique<common::DoubleRange>(
      1.5, false, false, 3.5, false, false, false);
  common::Subfield subfield("b");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto greaterOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(greaterOrEqual != nullptr);
  ASSERT_EQ(greaterOrEqual->name(), "gte");

  auto lessOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(lessOrEqual != nullptr);
  ASSERT_EQ(lessOrEqual->name(), "lte");
}

TEST_F(FilterToExpressionTest, FloatRange) {
  auto filter = std::make_unique<common::FloatRange>(
      1.5f, false, true, 3.5f, false, true, false);
  common::Subfield subfield("e");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto greaterThan =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(greaterThan != nullptr);
  ASSERT_EQ(greaterThan->name(), "gt");

  auto lessThan =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(lessThan != nullptr);
  ASSERT_EQ(lessThan->name(), "lt");
}

TEST_F(FilterToExpressionTest, BytesRange) {
  auto filter = std::make_unique<common::BytesRange>(
      "apple", false, false, "orange", false, false, false);
  common::Subfield subfield("c");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto greaterOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(greaterOrEqual != nullptr);
  ASSERT_EQ(greaterOrEqual->name(), "gte");

  auto lessOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(lessOrEqual != nullptr);
  ASSERT_EQ(lessOrEqual->name(), "lte");
}

TEST_F(FilterToExpressionTest, BigintValuesUsingHashTable) {
  std::vector<int64_t> values = {10, 20, 30};
  auto filter = common::createBigintValues(values, false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  // The implementation creates an optimized expression: (range check) AND (in
  // check)
  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  // First input should be the range check (field >= min AND field <= max)
  auto rangeCheckExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(rangeCheckExpr != nullptr);
  ASSERT_EQ(rangeCheckExpr->name(), "and");

  // Second input should be the IN expression
  auto inExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(inExpr != nullptr);
  ASSERT_EQ(inExpr->name(), "in");
  ASSERT_EQ(inExpr->inputs().size(), 2);

  auto arrayExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(inExpr->inputs()[1]);
  ASSERT_TRUE(arrayExpr != nullptr);
  ASSERT_EQ(arrayExpr->name(), "array_constructor");
  ASSERT_EQ(arrayExpr->inputs().size(), 3);
}

TEST_F(FilterToExpressionTest, BytesValues) {
  std::vector<std::string> values = {"apple", "banana", "orange"};
  auto filter = std::make_unique<common::BytesValues>(values, false);
  common::Subfield subfield("c");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "in");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto arrayExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(arrayExpr != nullptr);
  ASSERT_EQ(arrayExpr->name(), "array_constructor");
  ASSERT_EQ(arrayExpr->inputs().size(), 3);
}

TEST_F(FilterToExpressionTest, NegatedBytesValues) {
  std::vector<std::string> values = {"apple", "banana", "orange"};
  auto filter = std::make_unique<common::NegatedBytesValues>(values, false);
  common::Subfield subfield("c");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "not");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 1);

  auto containsExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(containsExpr != nullptr);

  ASSERT_TRUE(containsExpr->name() == "in" || containsExpr->name() == "or");
}

TEST_F(FilterToExpressionTest, NegatedBigintValuesUsingHashTable) {
  std::vector<int64_t> values = {10, 20, 30};
  auto filter = std::make_unique<common::NegatedBigintValuesUsingHashTable>(
      10, 30, values, false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  // The implementation creates a NOT expression for the optimized IN check
  verifyExpr(expr, "BOOLEAN", "not");
  auto notExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(notExpr->inputs().size(), 1);

  // The input should be an OR expression
  auto orExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(notExpr->inputs()[0]);
  ASSERT_TRUE(orExpr != nullptr);
  ASSERT_EQ(orExpr->name(), "or");
  ASSERT_EQ(orExpr->inputs().size(), 2);

  // First input of OR should be range check
  auto rangeCheckExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(orExpr->inputs()[0]);
  ASSERT_TRUE(rangeCheckExpr != nullptr);
  ASSERT_EQ(rangeCheckExpr->name(), "and");

  // Second input of OR should be IS_NULL expression
  auto isNullExpr =
      std::dynamic_pointer_cast<const CallTypedExpr>(orExpr->inputs()[1]);
  ASSERT_TRUE(isNullExpr != nullptr);
  ASSERT_EQ(isNullExpr->name(), "is_null");
}

TEST_F(FilterToExpressionTest, TimestampRange) {
  auto timestamp1 = Timestamp::fromMillis(1609459200000); // 2021-01-01
  auto timestamp2 = Timestamp::fromMillis(1640995200000); // 2022-01-01
  auto filter =
      std::make_unique<common::TimestampRange>(timestamp1, timestamp2, false);
  common::Subfield subfield("f");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "and");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);

  auto greaterOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(greaterOrEqual != nullptr);
  ASSERT_EQ(greaterOrEqual->name(), "gte");

  auto lessOrEqual =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(lessOrEqual != nullptr);
  ASSERT_EQ(lessOrEqual->name(), "lte");
}

TEST_F(FilterToExpressionTest, BigintMultiRange) {
  std::vector<std::unique_ptr<common::BigintRange>> ranges;
  ranges.push_back(std::make_unique<common::BigintRange>(10, 20, false));
  ranges.push_back(std::make_unique<common::BigintRange>(30, 40, false));
  auto filter =
      std::make_unique<common::BigintMultiRange>(std::move(ranges), false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  verifyExpr(expr, "BOOLEAN", "or");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 2);
}

TEST_F(FilterToExpressionTest, MultiRange) {
  // Create a MultiRange filter with compatible filters for BIGINT field
  std::vector<std::unique_ptr<common::Filter>> filters;

  // Add a BigintRange filter
  filters.push_back(std::make_unique<common::BigintRange>(10, 20, false));

  // Add an IsNull filter
  filters.push_back(std::make_unique<common::IsNull>());

  // Add another BigintRange filter instead of BytesRange to avoid type mismatch
  filters.push_back(std::make_unique<common::BigintRange>(30, 40, false));

  auto filter = std::make_unique<common::MultiRange>(std::move(filters), false);
  common::Subfield subfield("a");
  auto rowType = createTestRowType();

  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());

  // Verify the top-level expression is an OR
  verifyExpr(expr, "BOOLEAN", "or");
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_EQ(callExpr->inputs().size(), 3);

  // Verify the first input is a BigintRange expression (AND of
  // greater_than_or_equal and less_than_or_equal)
  auto firstInput =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
  ASSERT_TRUE(firstInput != nullptr);
  ASSERT_EQ(firstInput->name(), "and");
  ASSERT_EQ(firstInput->inputs().size(), 2);

  // Verify the second input is an IsNull expression
  auto secondInput =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
  ASSERT_TRUE(secondInput != nullptr);
  ASSERT_EQ(secondInput->name(), "is_null");

  // Verify the third input is another BigintRange expression (AND of
  // greater_than_or_equal and less_than_or_equal)
  auto thirdInput =
      std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[2]);
  ASSERT_TRUE(thirdInput != nullptr);
  ASSERT_EQ(thirdInput->name(), "and");
  ASSERT_EQ(thirdInput->inputs().size(), 2);
}

// Helper method for round trip testing
void FilterToExpressionTest::testRoundTrip(
    const std::string& fieldName,
    std::unique_ptr<common::Filter> filter) {
  // Step 1: Convert filter to expression
  common::Subfield subfield(fieldName);
  auto rowType = createTestRowType();
  auto expr = filterToExpr(subfield, filter.get(), rowType, pool());
  ASSERT_TRUE(expr != nullptr);

  // Step 2: Convert expression back to filter
  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  if (!callExpr) {
    // Some filters like AlwaysTrue/AlwaysFalse convert to ConstantTypedExpr
    // which can't be converted back to a filter
    return;
  }

  // Special handling for BoolValue filter
  if (filter->kind() == common::FilterKind::kBoolValue) {
    // For BoolValue, we need to extract the eq expression from the and
    // expression
    if (callExpr->name() == "and" && callExpr->inputs().size() == 2) {
      auto eqExpr =
          std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
      if (eqExpr && eqExpr->name() == "eq") {
        callExpr = eqExpr;
      }
    }
  }

  // Special handling for "in" with array_constructor
  if (callExpr->name() == "in" && callExpr->inputs().size() == 2) {
    auto arrayExpr =
        std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);
    if (arrayExpr && arrayExpr->name() == "array_constructor") {
      // Use toSubfieldFilter for array_constructor expressions
      auto [roundTripSubfield, roundTripFilter] =
          exec::toSubfieldFilter(expr, evaluator());

      // Step 3: Verify the round-tripped filter and subfield
      ASSERT_TRUE(roundTripFilter != nullptr);
      ASSERT_EQ(roundTripSubfield.toString(), subfield.toString());

      // Compare filter properties - this will vary based on filter type
      // For this test we'll just verify the filter kind is the same
      ASSERT_EQ(roundTripFilter->kind(), filter->kind());
      return;
    }
  }

  // Special handling for range filters (and expressions)
  if (callExpr->name() == "and" && callExpr->inputs().size() == 2) {
    auto firstInput =
        std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[0]);
    auto secondInput =
        std::dynamic_pointer_cast<const CallTypedExpr>(callExpr->inputs()[1]);

    if (firstInput && secondInput && firstInput->name() == "gte" &&
        secondInput->name() == "lte") {
      // Extract the field and bounds
      auto field = firstInput->inputs()[0];
      auto lowerBound = firstInput->inputs()[1];
      auto upperBound = secondInput->inputs()[1];

      // Create a between expression
      auto betweenExpr = std::make_shared<CallTypedExpr>(
          callExpr->type(), "between", field, lowerBound, upperBound);

      common::Subfield roundTripSubfield;
      auto roundTripFilter =
          exec::ExprToSubfieldFilterParser::getInstance()
              ->leafCallToSubfieldFilter(
                  *betweenExpr, roundTripSubfield, evaluator(), false);

      // Step 3: Verify the round-tripped filter and subfield
      ASSERT_TRUE(roundTripFilter != nullptr);
      ASSERT_EQ(roundTripSubfield.toString(), subfield.toString());

      // Compare filter properties - this will vary based on filter type
      // For this test we'll just verify the filter kind is the same
      ASSERT_EQ(roundTripFilter->kind(), filter->kind());
      return;
    }
  }

  // For all other expressions, use leafCallToSubfieldFilter directly
  common::Subfield roundTripSubfield;
  auto roundTripFilter =
      exec::ExprToSubfieldFilterParser::getInstance()->leafCallToSubfieldFilter(
          *callExpr, roundTripSubfield, evaluator(), false);

  // Step 3: Verify the round-tripped filter and subfield
  ASSERT_TRUE(roundTripFilter != nullptr);
  ASSERT_EQ(roundTripSubfield.toString(), subfield.toString());

  // Compare filter properties - this will vary based on filter type
  // For this test we'll just verify the filter kind is the same
  ASSERT_EQ(roundTripFilter->kind(), filter->kind());
}

// Round trip tests for various filter types
TEST_F(FilterToExpressionTest, RoundTripBigintRangeSingleValue) {
  auto filter = std::make_unique<common::BigintRange>(42, 42, false);
  testRoundTrip("a", std::move(filter));
}

TEST_F(FilterToExpressionTest, RoundTripBigintRangeWithRange) {
  auto filter = std::make_unique<common::BigintRange>(10, 20, false);
  testRoundTrip("a", std::move(filter));
}

TEST_F(FilterToExpressionTest, RoundTripIsNull) {
  auto filter = std::make_unique<common::IsNull>();
  testRoundTrip("a", std::move(filter));
}

TEST_F(FilterToExpressionTest, RoundTripBoolValue) {
  auto filter = std::make_unique<common::BoolValue>(true, false);
  testRoundTrip("d", std::move(filter));
}

TEST_F(FilterToExpressionTest, RoundTripBytesRange) {
  auto filter = std::make_unique<common::BytesRange>(
      "apple", false, false, "orange", false, false, false);
  testRoundTrip("c", std::move(filter));
}

} // namespace facebook::velox::core::test
