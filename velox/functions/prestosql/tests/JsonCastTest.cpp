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

#include <cstdint>

#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/vector/tests/TestingDictionaryFunction.h"

using namespace facebook::velox;

class JsonCastTest : public functions::test::FunctionBaseTest {
 protected:
  JsonCastTest() {
    exec::registerVectorFunction(
        "testing_dictionary",
        test::TestingDictionaryFunction::signatures(),
        std::make_unique<test::TestingDictionaryFunction>());
  }

  template <typename TTo>
  void evaluateCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected) {
    std::shared_ptr<const core::ITypedExpr> inputField =
        std::make_shared<const core::FieldAccessTypedExpr>(fromType, "c0");
    std::shared_ptr<const core::ITypedExpr> castExpr =
        std::make_shared<const core::CastTypedExpr>(
            toType,
            std::vector<std::shared_ptr<const core::ITypedExpr>>{inputField},
            false);

    auto result = evaluate<SimpleVector<EvalType<TTo>>>(castExpr, input);

    assertEqualVectors(expected, result);
  }

  template <typename TTo>
  void evaluateCastDictEncoding(
      const TypePtr& fromType,
      const TypePtr& toType,
      const RowVectorPtr& input,
      const VectorPtr& expected) {
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
            false);

    auto indices = test::makeIndicesInReverse(input->size(), pool());

    auto result = evaluate<SimpleVector<EvalType<TTo>>>(castExpr, input);
    assertEqualVectors(
        wrapInDictionary(indices, input->size(), expected), result);
  }

  template <typename TTo>
  void testCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      const VectorPtr& input,
      const VectorPtr& expected) {
    // Test with flat encoding.
    evaluateCast<TTo>(fromType, toType, makeRowVector({input}), expected);

    // Test with constant encoding that repeats the first element five times.
    auto constInput = BaseVector::wrapInConstant(5, 0, input);
    auto constExpected = BaseVector::wrapInConstant(5, 0, expected);

    evaluateCast<TTo>(
        fromType, toType, makeRowVector({constInput}), constExpected);

    // Test with dictionary encoding that reverses the indices.
    evaluateCastDictEncoding<TTo>(
        fromType, toType, makeRowVector({input}), expected);
  }

  template <typename TFrom, typename TTo>
  void testCast(
      const TypePtr& fromType,
      const TypePtr& toType,
      std::vector<std::optional<TFrom>> input,
      std::vector<std::optional<TTo>> expected) {
    auto inputVector = makeNullableFlatVector<TFrom>(input);
    auto expectedVector = makeNullableFlatVector<TTo>(expected);

    testCast<TTo>(fromType, toType, inputVector, expectedVector);
  }
};

TEST_F(JsonCastTest, fromBigint) {
  testCast<int64_t, Json>(
      BIGINT(),
      JSON(),
      {1, -3, 0, INT64_MAX, INT64_MIN, std::nullopt},
      {"1"_sv,
       "-3"_sv,
       "0"_sv,
       "9223372036854775807"_sv,
       "-9223372036854775808"_sv,
       std::nullopt});
  testCast<int64_t, Json>(
      BIGINT(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, unsupportedTypes) {
  auto mapVector = makeMapVector<int64_t, int64_t>({{}});
  auto expectedForMap = makeNullableFlatVector<Json>({"{}"});
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(BIGINT(), BIGINT()),
          JSON(),
          makeRowVector({mapVector}),
          expectedForMap),
      VeloxException);

  auto rowVector = makeRowVector({mapVector});
  auto expectedForRow = makeNullableFlatVector<Json>({"[{}]"});
  EXPECT_THROW(
      evaluateCast<Json>(
          ROW({MAP(BIGINT(), BIGINT())}),
          JSON(),
          makeRowVector({rowVector}),
          expectedForRow),
      VeloxException);
}
