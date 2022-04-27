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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox {

using namespace facebook::velox::test;

class TryExprTest : public functions::test::FunctionBaseTest {};

TEST_F(TryExprTest, tryExpr) {
  auto a = makeFlatVector<int32_t>({10, 20, 30, 20, 50, 30});
  auto b = makeFlatVector<int32_t>({1, 0, 3, 4, 0, 6});
  {
    auto result = evaluate("try(c0 / c1)", makeRowVector({a, b}));

    auto expectedResult = makeNullableFlatVector<int32_t>(
        {10, std::nullopt, 10, 5, std::nullopt, 5});
    assertEqualVectors(expectedResult, result);
  }

  auto c = makeNullableFlatVector<StringView>({"1", "2x", "3", "4", "5y"});
  {
    auto result = evaluate("try(cast(c0 as integer))", makeRowVector({c}));
    auto expectedResult =
        makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, std::nullopt});
    assertEqualVectors(expectedResult, result);
  }
}

// Returns the number of times this function has been called so far.
template <typename T>
struct CountCallsFunction {
  int64_t numCalls = 0;

  bool callNullable(int64_t& out, const int64_t*) {
    out = numCalls++;

    return true;
  }
};

TEST_F(TryExprTest, skipExecution) {
  registerFunction<CountCallsFunction, int64_t, int64_t>(
      {"count_calls"}, BIGINT());

  std::vector<std::optional<int64_t>> expected{
      0, std::nullopt, 1, std::nullopt, 2};
  auto flatVector = makeFlatVector<StringView>(expected.size(), [&](auto row) {
    return expected[row].has_value() ? "1" : "a";
  });
  auto result = evaluate<FlatVector<int64_t>>(
      "try(count_calls(cast(c0 as integer)))", makeRowVector({flatVector}));

  assertEqualVectors(makeNullableFlatVector(expected), result);
}

TEST_F(TryExprTest, nestedTryChildErrors) {
  // This tests that with nested TRY expressions, the parent TRY does not see
  // errors the child TRY already handled.

  // Put "a" wherever we want an exception, as casting it to an integer will
  // throw.
  auto flatVector = makeFlatVector<StringView>(
      5, [&](auto row) { return row % 2 == 0 ? "1" : "a"; });
  auto result = evaluate<FlatVector<int32_t>>(
      "try(coalesce(try(cast(c0 as integer)), cast(3 as integer)))",
      makeRowVector({flatVector}));

  assertEqualVectors(
      // Every other row throws an exception, which should get caught and
      // coalesced to 3.
      makeFlatVector<int32_t>({1, 3, 1, 3, 1}),
      result);
}

TEST_F(TryExprTest, nestedTryParentErrors) {
  // This tests that with nested TRY expressions, the child TRY does not see
  // errros the parent TRY is supposed to handle.

  vector_size_t size = 10;
  // Put "a" wherever we want an exception, as casting it to an integer will
  // throw.
  auto col0 = makeFlatVector<StringView>(
      size, [&](auto row) { return row % 3 == 0 ? "a" : "1"; });
  auto col1 = makeFlatVector<StringView>(
      size, [&](auto row) { return row % 3 == 1 ? "a" : "1"; });
  auto result = evaluate<FlatVector<int32_t>>(
      "try(cast(c1 as integer) + coalesce(try(cast(c0 as integer)), cast(3 as integer)))",
      makeRowVector({col0, col1}));

  assertEqualVectors(
      makeFlatVector<int32_t>(
          size,
          [&](auto row) {
            if (row % 3 == 0) {
              // col0 produced an error, so coalesce returned 3.
              return 4;
            }

            return 2;
          },
          [&](auto row) {
            if (row % 3 == 1) {
              // col1 produced an error, so the whole expression is NULL.
              return true;
            }

            return false;
          }),
      result);
}

namespace {
// A function that sets result to be a ConstantVector and then throws an
// exception.
class CreateConstantAndThrow : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    *result =
        BaseVector::createConstant((int64_t)1, rows.end(), context->pool());

    rows.applyToSelected([&](int row) {
      context->setError(
          row, std::make_exception_ptr(std::invalid_argument("expected")));
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("bigint")
                .build()};
  }
};
} // namespace

TEST_F(TryExprTest, constant) {
  // Test a TRY around an expression that sets result to be a ConstantVector
  // and then throws an exception.
  exec::registerVectorFunction(
      "create_constant_and_throw",
      CreateConstantAndThrow::signatures(),
      std::make_unique<CreateConstantAndThrow>());

  auto constant = makeConstant<int64_t>(0, 10);

  // This should return a ConstantVector of NULLs since every row throws an
  // exception.
  auto result = evaluate<ConstantVector<int64_t>>(
      "try(create_constant_and_throw(c0))", makeRowVector({constant}));

  assertEqualVectors(makeNullConstant(TypeKind::BIGINT, 10), result);
}

TEST_F(TryExprTest, evalSimplified) {
  registerFunction<CountCallsFunction, int64_t, int64_t>(
      {"count_calls"}, BIGINT());

  std::vector<std::optional<int64_t>> expected{0, 1, 2, 3};
  auto constant = makeConstant<int64_t>(0, 4);
  // Test that count_calls is called with evalSimplified, not eval.
  // If eval were to be invoked then count_calls would be invoked once and the
  // result would be a constant vector with the value 0 repeated 4 times.
  auto result = evaluateSimplified<FlatVector<int64_t>>(
      "try(count_calls(c0))", makeRowVector({constant}));

  assertEqualVectors(makeNullableFlatVector(expected), result);
}
} // namespace facebook::velox
