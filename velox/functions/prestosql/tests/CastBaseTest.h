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
#pragma once

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/tests/TestingDictionaryFunction.h"

namespace facebook::velox::functions::test {

using namespace facebook::velox::test;

class CastBaseTest : public FunctionBaseTest {
 protected:
  CastBaseTest() {
    exec::registerVectorFunction(
        "testing_dictionary",
        test::TestingDictionaryFunction::signatures(),
        std::make_unique<test::TestingDictionaryFunction>());
  }

  // Build an ITypedExpr for cast(fromType as toType).
  core::TypedExprPtr buildCastExpr(
      const TypePtr& fromType,
      const TypePtr& toType,
      bool isTryCast) {
    core::TypedExprPtr inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(fromType, "c0");
    return std::make_shared<const core::CastTypedExpr>(
        toType, inputField, isTryCast);
  }

  // Evaluate cast(fromType as toType) and return the result vector.
  VectorPtr evaluateCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      bool isTryCast = false) {
    auto castExpr = buildCastExpr(fromType, toType, isTryCast);
    exec::ExprSet exprSet({castExpr}, &execCtx_);
    exec::EvalCtx context(&execCtx_, &exprSet, input.get());

    std::vector<VectorPtr> result(1);
    SelectivityVector rows(input->size());
    exprSet.eval(rows, context, result);
    EXPECT_FALSE(context.errors());
    return result[0];
  }

  // Evaluate cast(fromType as toType) and verify the result matches the
  // expected one.
  void evaluateAndVerify(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected,
      bool isTryCast = false) {
    auto result = evaluateCast(fromType, toType, input, isTryCast);
    assertEqualVectors(expected, result);
  }

  // Build an ITypedExpr for cast(testing_dictionary(fromType) as toType).
  core::TypedExprPtr buildCastExprWithDictionaryInput(
      const TypePtr& fromType,
      const TypePtr& toType,
      bool isTryCast) {
    core::TypedExprPtr inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(fromType, "c0");

    // It is not sufficient to wrap input in a dictionary as it will be peeled
    // off before calling "cast". Apply testing_dictionary function to input to
    // ensure that "cast" receives dictionary input.
    core::TypedExprPtr callExpr = std::make_shared<const core::CallTypedExpr>(
        fromType,
        std::vector<core::TypedExprPtr>{inputField},
        "testing_dictionary");
    return std::make_shared<const core::CastTypedExpr>(
        toType, callExpr, isTryCast);
  }

  // Evaluate cast(testing_dictionary(fromType) as toType) and verify the result
  // matches the expected one. Values in expected should correspond to values in
  // input at the same rows.
  void evaluateAndVerifyDictEncoding(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected,
      bool isTryCast = false) {
    auto castExpr =
        buildCastExprWithDictionaryInput(fromType, toType, isTryCast);

    VectorPtr result;
    result = evaluate(castExpr, input);

    auto indices = test::makeIndicesInReverse(expected->size(), pool());
    assertEqualVectors(wrapInDictionary(indices, expected), result);
  }

  // Evaluate try(cast(testing_dictionary(fromType) as toType)) and verify the
  // result matches the expected one. Values in expected should correspond to
  // values in input at the same rows.
  void evaluateAndVerifyCastInTryDictEncoding(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected) {
    auto castExpr = buildCastExprWithDictionaryInput(fromType, toType, false);
    core::TypedExprPtr tryExpr = std::make_shared<const core::CallTypedExpr>(
        toType, std::vector<core::TypedExprPtr>{castExpr}, "try");

    auto result = evaluate(tryExpr, input);
    auto indices = test::makeIndicesInReverse(expected->size(), pool());
    assertEqualVectors(wrapInDictionary(indices, expected), result);
  }

  /**
   * @tparam From Source type for cast.
   * @tparam To Destination type for cast.
   * @param typeString Cast type in string.
   * @param input Input vector of type From.
   * @param expectedResult Expected output vector of type To.
   */
  template <typename TFrom, typename TTo>
  void testCast(
      const std::string& typeString,
      const std::vector<std::optional<TFrom>>& input,
      const std::vector<std::optional<TTo>>& expectedResult,
      const TypePtr& fromType = CppToType<TFrom>::create(),
      const TypePtr& toType = CppToType<TTo>::create()) {
    auto result = evaluate(
        fmt::format("cast(c0 as {})", typeString),
        makeRowVector({makeNullableFlatVector(input, fromType)}));
    auto expected = makeNullableFlatVector<TTo>(expectedResult, toType);
    assertEqualVectors(expected, result);
  }

  /**
   * @tparam From Source type for cast.
   * @tparam To Destination type for cast.
   * @param typeString Cast type in string.
   * @param input Input vector of type From.
   * @param expectedResult Expected output vector of type To.
   */
  template <typename TFrom, typename TTo>
  void testTryCast(
      const std::string& typeString,
      const std::vector<std::optional<TFrom>>& input,
      const std::vector<std::optional<TTo>>& expectedResult,
      const TypePtr& fromType = CppToType<TFrom>::create(),
      const TypePtr& toType = CppToType<TTo>::create()) {
    auto result = evaluate(
        fmt::format("try_cast(c0 as {})", typeString),
        makeRowVector({makeNullableFlatVector(input, fromType)}));
    auto expected = makeNullableFlatVector<TTo>(expectedResult, toType);
    assertEqualVectors(expected, result);
  }

  /**
   * @tparam From Source type for cast.
   * @param typeString Cast type in string.
   * @param input Input vector of type From.
   */
  template <typename TFrom>
  void testInvalidCast(
      const std::string& typeString,
      const std::vector<std::optional<TFrom>>& input,
      const std::string& expectedErrorMessage,
      const TypePtr& fromType = CppToType<TFrom>::create()) {
    VELOX_ASSERT_THROW(
        evaluate(
            fmt::format("cast(c0 as {})", typeString),
            makeRowVector({makeNullableFlatVector(input, fromType)})),
        expectedErrorMessage);
  }

  void testCast(
      const VectorPtr& input,
      const VectorPtr& expected,
      std::optional<bool> isTryCast = std::nullopt) {
    const auto& fromType = input->type();
    const auto& toType = expected->type();
    SCOPED_TRACE(fmt::format(
        "Cast from {} to {}", fromType->toString(), toType->toString()));
    const auto copy = createCopy(input);
    // Test with flat encoding.
    {
      SCOPED_TRACE("Flat encoding");
      if (isTryCast.has_value()) {
        evaluateAndVerify(
            fromType,
            toType,
            makeRowVector({input}),
            expected,
            isTryCast.value());
      } else {
        evaluateAndVerify(fromType, toType, makeRowVector({input}), expected);
        evaluateAndVerify(
            fromType, toType, makeRowVector({input}), expected, true);
      }

      // Make sure the input vector does not change.
      assertEqualVectors(input, copy);
    }

    // Test with constant encoding that repeats the first element five times.
    {
      SCOPED_TRACE("Constant encoding");
      const auto constantRow =
          makeRowVector({BaseVector::wrapInConstant(5, 0, input)});
      const auto localCopy = createCopy(constantRow);
      const auto constExpected = BaseVector::wrapInConstant(5, 0, expected);

      if (isTryCast.has_value()) {
        evaluateAndVerify(
            fromType, toType, constantRow, constExpected, isTryCast.value());
      } else {
        evaluateAndVerify(fromType, toType, constantRow, constExpected);
        evaluateAndVerify(fromType, toType, constantRow, constExpected, true);
      }

      // Make sure the input vector does not change.
      assertEqualVectors(constantRow, localCopy);
      assertEqualVectors(input, copy);
    }

    // Test with dictionary encoding that reverses the indices.
    {
      SCOPED_TRACE("Dictionary encoding");
      if (isTryCast.has_value()) {
        evaluateAndVerifyDictEncoding(
            fromType,
            toType,
            makeRowVector({input}),
            expected,
            isTryCast.value());
      } else {
        evaluateAndVerifyDictEncoding(
            fromType, toType, makeRowVector({input}), expected);
        evaluateAndVerifyDictEncoding(
            fromType, toType, makeRowVector({input}), expected, true);
      }

      // Make sure the input vector does not change.
      assertEqualVectors(input, copy);
    }
  }

  template <typename TFrom, typename TTo>
  void testCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      std::vector<std::optional<TFrom>> input,
      std::vector<std::optional<TTo>> expected) {
    auto inputVector = makeNullableFlatVector<TFrom>(input, fromType);
    auto expectedVector = makeNullableFlatVector<TTo>(expected, toType);

    testCast(inputVector, expectedVector);
  }

  template <typename TFrom>
  void testThrow(
      const TypePtr& fromType,
      const TypePtr& toType,
      const std::vector<std::optional<TFrom>>& input,
      const std::string& expectedErrorMessage) {
    VELOX_ASSERT_THROW(
        evaluateCast(
            fromType,
            toType,
            makeRowVector({makeNullableFlatVector<TFrom>(input, fromType)})),
        expectedErrorMessage);
  }

  VectorPtr createCopy(const VectorPtr& input) {
    VectorPtr result;
    SelectivityVector rows(input->size());
    BaseVector::ensureWritable(rows, input->type(), input->pool(), result);
    result->copy(input.get(), rows, nullptr);
    return result;
  }

  std::shared_ptr<core::CastTypedExpr> makeCastExpr(
      const core::TypedExprPtr& input,
      const TypePtr& toType,
      bool nullOnFailure) {
    std::vector<core::TypedExprPtr> inputs = {input};
    return std::make_shared<core::CastTypedExpr>(toType, inputs, nullOnFailure);
  }

  const float kInf = std::numeric_limits<float>::infinity();
  const float kNan = std::numeric_limits<float>::quiet_NaN();
};

} // namespace facebook::velox::functions::test
