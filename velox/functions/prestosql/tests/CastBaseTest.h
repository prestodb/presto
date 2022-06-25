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

#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/vector/tests/TestingDictionaryFunction.h"

namespace facebook::velox::functions::test {

using namespace facebook::velox::test;

class CastBaseTest : public FunctionBaseTest {
 protected:
  CastBaseTest() {
    exec::registerVectorFunction(
        "testing_dictionary",
        ::facebook::velox::test::TestingDictionaryFunction::signatures(),
        std::make_unique<::facebook::velox::test::TestingDictionaryFunction>());
  }

  template <typename TTo>
  VectorPtr evaluateCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      bool tryCast = false) {
    std::shared_ptr<const core::ITypedExpr> inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(fromType, "c0");
    std::shared_ptr<const core::ITypedExpr> castExpr =
        std::make_shared<const core::CastTypedExpr>(
            toType,
            std::vector<std::shared_ptr<const core::ITypedExpr>>{inputField},
            tryCast);

    if constexpr (std::is_same_v<TTo, ComplexType>) {
      return evaluate(castExpr, input);
    } else {
      return evaluate<SimpleVector<EvalType<TTo>>>(castExpr, input);
    }
  }

  template <typename TTo>
  void evaluateAndVerify(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected,
      bool tryCast = false) {
    auto result = evaluateCast<TTo>(fromType, toType, input, tryCast);
    assertEqualVectors(expected, result);
  }

  template <typename TTo>
  void evaluateAndVerifyDictEncoding(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected,
      bool tryCast = false) {
    std::shared_ptr<const core::ITypedExpr> inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(fromType, "c0");
    std::shared_ptr<const core::ITypedExpr> callExpr =
        std::make_shared<const core::CallTypedExpr>(
            fromType,
            std::vector<std::shared_ptr<const core::ITypedExpr>>{inputField},
            "testing_dictionary");
    std::shared_ptr<const core::ITypedExpr> castExpr =
        std::make_shared<const core::CastTypedExpr>(
            toType,
            std::vector<std::shared_ptr<const core::ITypedExpr>>{callExpr},
            tryCast);

    VectorPtr result;
    if constexpr (std::is_same_v<TTo, ComplexType>) {
      result = evaluate(castExpr, input);
    } else {
      result = evaluate<SimpleVector<EvalType<TTo>>>(castExpr, input);
    }

    auto indices =
        ::facebook::velox::test::makeIndicesInReverse(expected->size(), pool());
    assertEqualVectors(
        wrapInDictionary(indices, expected->size(), expected), result);
  }

  template <typename TTo>
  void testCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      const VectorPtr& input,
      const VectorPtr& expected) {
    // Test with flat encoding.
    evaluateAndVerify<TTo>(fromType, toType, makeRowVector({input}), expected);
    evaluateAndVerify<TTo>(
        fromType, toType, makeRowVector({input}), expected, true);

    // Test with constant encoding that repeats the first element five times.
    auto constInput = BaseVector::wrapInConstant(5, 0, input);
    auto constExpected = BaseVector::wrapInConstant(5, 0, expected);

    evaluateAndVerify<TTo>(
        fromType, toType, makeRowVector({constInput}), constExpected);
    evaluateAndVerify<TTo>(
        fromType, toType, makeRowVector({constInput}), constExpected, true);

    // Test with dictionary encoding that reverses the indices.
    evaluateAndVerifyDictEncoding<TTo>(
        fromType, toType, makeRowVector({input}), expected);
    evaluateAndVerifyDictEncoding<TTo>(
        fromType, toType, makeRowVector({input}), expected, true);
  }

  template <typename TFrom, typename TTo>
  void testCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      std::vector<std::optional<TFrom>> input,
      std::vector<std::optional<TTo>> expected) {
    auto inputVector = makeNullableFlatVector<TFrom>(input, fromType);
    auto expectedVector = makeNullableFlatVector<TTo>(expected, toType);

    testCast<TTo>(fromType, toType, inputVector, expectedVector);
  }

  template <typename TFrom, typename TTo>
  void testThrow(
      const TypePtr& fromType,
      const TypePtr& toType,
      std::vector<std::optional<TFrom>> input) {
    EXPECT_THROW(
        evaluateCast<TTo>(
            fromType,
            toType,
            makeRowVector({makeNullableFlatVector<TFrom>(input, fromType)})),
        VeloxException);
  }
};

} // namespace facebook::velox::functions::test
