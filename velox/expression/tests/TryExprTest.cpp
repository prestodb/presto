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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/tests/TestingAlwaysThrowsFunction.h"

namespace facebook::velox {

using namespace common::testutil;
using namespace facebook::velox::test;

class TryExprTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    FunctionBaseTest::SetUpTestCase();
    TestValue::enable();
  }

  VectorPtr evaluateWithCustomMemoryPool(
      const std::string& sql,
      const RowVectorPtr& data,
      memory::MemoryPool* pool) {
    auto typedExpr = makeTypedExpr(sql, asRowType(data->type()));

    core::ExecCtx execCtx{pool, queryCtx_.get()};
    exec::ExprSet exprSet({typedExpr}, &execCtx);
    exec::EvalCtx context(&execCtx, &exprSet, data.get());

    std::vector<VectorPtr> result(1);
    SelectivityVector allRows(data->size());
    exprSet.eval(allRows, context, result);
    return result[0];
  }
};

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
  registerFunction<CountCallsFunction, int64_t, int64_t>({"count_calls"});

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

TEST_F(TryExprTest, skipExecutionEvalSimplified) {
  registerFunction<CountCallsFunction, int64_t, int64_t>({"count_calls"});

  // Test that when a subset of the inputs to a function wrapped in a TRY throw
  // exceptions, that function is only evaluated on the inputs that did not
  // throw exceptions.
  auto flatVector = makeFlatVector<StringView>({"1", "a", "1", "a", "1"});
  auto result = evaluateSimplified<FlatVector<int64_t>>(
      "try(count_calls(cast(c0 as integer)))", makeRowVector({flatVector}));

  auto expected =
      makeNullableFlatVector<int64_t>({0, std::nullopt, 1, std::nullopt, 2});
  assertEqualVectors(expected, result);
}

TEST_F(TryExprTest, skipExecutionWholeBatchEvalSimplified) {
  registerFunction<CountCallsFunction, int64_t, int64_t>({"count_calls"});

  // Test that when all the inputs to a function wrapped in a TRY throw
  // exceptions, that function isn't evaluated and a NULL constant is returned
  // directly.
  auto flatVector = makeFlatVector<StringView>({"a", "b", "c"});
  auto result = evaluateSimplified<ConstantVector<int64_t>>(
      "try(count_calls(cast(c0 as integer)))", makeRowVector({flatVector}));

  auto expected = makeNullConstant(TypeKind::BIGINT, 3);
  assertEqualVectors(expected, result);
}

/// Verify that subsequent inputs to a non-default-null-behavior function are
/// not evaluated if previous inputs generated errors.
TEST_F(TryExprTest, skipExecutionOnInputErrors) {
  // Fail all rows.
  auto input = makeRowVector({
      makeFlatVector<StringView>({"a"_sv, "b"_sv, "c"_sv}),
  });

  auto [result, stats] = evaluateWithStats(
      "try(array_constructor(cast(c0 as bigint), length(c0)))", input);
  auto expected =
      BaseVector::createNullConstant(ARRAY(BIGINT()), input->size(), pool());

  assertEqualVectors(expected, result);

  EXPECT_EQ(3, stats.at("cast").numProcessedRows);
  EXPECT_EQ(3, stats.at("try").numProcessedRows);
  EXPECT_EQ(0, stats.count("array_constructor"));
  EXPECT_EQ(0, stats.count("length"));

  // Fail some rows.
  input = makeRowVector({
      makeFlatVector<StringView>({"a"_sv, "100"_sv, "c"_sv, "1000"_sv}),
  });

  std::tie(result, stats) = evaluateWithStats(
      "try(array_constructor(cast(c0 as bigint), length(c0)))", input);
  expected = makeNullableArrayVector<int64_t>({
      std::nullopt,
      {{100, 3}},
      std::nullopt,
      {{1000, 4}},
  });

  assertEqualVectors(expected, result);

  EXPECT_EQ(4, stats.at("cast").numProcessedRows);
  EXPECT_EQ(4, stats.at("try").numProcessedRows);
  EXPECT_EQ(2, stats.at("array_constructor").numProcessedRows);
  EXPECT_EQ(2, stats.at("length").numProcessedRows);
}

namespace {
// A function that sets result to be a ConstantVector and then throws an
// exception. The constructor parameter throwOnFirstRow controls whether the
// function throws an exception only at the first row or at every row it is
// evaluated on.
class CreateConstantAndThrow : public exec::VectorFunction {
 public:
  CreateConstantAndThrow(bool throwOnFirstRow = false)
      : throwOnFirstRow_{throwOnFirstRow} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    result = BaseVector::createConstant(
        BIGINT(), (int64_t)1, rows.end(), context.pool());

    if (throwOnFirstRow_) {
      context.setError(
          0, std::make_exception_ptr(std::invalid_argument("expected")));
    } else {
      rows.applyToSelected([&](int row) {
        context.setError(
            row, std::make_exception_ptr(std::invalid_argument("expected")));
      });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("bigint")
                .build()};
  }

 private:
  const bool throwOnFirstRow_;
};
} // namespace

TEST_F(TryExprTest, constant) {
  // Test a TRY around an expression that sets result to be a ConstantVector
  // and then throws an exception.
  {
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

  // Test a TRY over an expression that returns constant result vector but with
  // exceptions on a subset of rows.
  {
    exec::registerVectorFunction(
        "create_constant_and_throw_on_first_row",
        CreateConstantAndThrow::signatures(),
        std::make_unique<CreateConstantAndThrow>(true));

    auto input = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
    VELOX_ASSERT_THROW(
        evaluate<SimpleVector<int64_t>>(
            "create_constant_and_throw_on_first_row(c0)",
            makeRowVector({input})),
        "expected");
    auto result = evaluate<SimpleVector<int64_t>>(
        "try(create_constant_and_throw_on_first_row(c0))",
        makeRowVector({input}));
    assertEqualVectors(
        makeNullableFlatVector<int64_t>({std::nullopt, 1, 1, 1, 1}), result);
  }
}

TEST_F(TryExprTest, evalSimplified) {
  registerFunction<CountCallsFunction, int64_t, int64_t>({"count_calls"});

  std::vector<std::optional<int64_t>> expected{0, 1, 2, 3};
  auto constant = makeConstant<int64_t>(0, 4);
  // Test that count_calls is called with evalSimplified, not eval.
  // If eval were to be invoked then count_calls would be invoked once and the
  // result would be a constant vector with the value 0 repeated 4 times.
  auto result = evaluateSimplified<FlatVector<int64_t>>(
      "try(count_calls(c0))", makeRowVector({constant}));

  assertEqualVectors(makeNullableFlatVector(expected), result);
}

TEST_F(TryExprTest, nonDefaultNulls) {
  // Create dictionary on this input with nulls which will be processed
  // by a non default null function.
  auto dictionaryInput = BaseVector::wrapInDictionary(
      makeNulls({false, false, true, true, false, false}),
      makeIndices({0, 1, 2, 3, 4, 5, 6}),
      6,
      makeConstant("abc", 6));

  auto input = makeRowVector({dictionaryInput});

  // Ensure that expression fails without Try.
  VELOX_ASSERT_THROW(
      evaluateSimplified<SimpleVector<bool>>(
          "distinct_from(10::INTEGER, codepoint(c0))", input),
      "(3 vs. 1) Unexpected parameters (varchar(3)) for function codepoint. Expected: codepoint(varchar(1))");

  // First try simple eval, and ensure only the null rows will be processed.
  auto result = evaluateSimplified<SimpleVector<bool>>(
      "try(distinct_from(10::INTEGER, codepoint(c0)))", input);

  auto expected = makeNullableFlatVector<bool>(
      {std::nullopt, std::nullopt, true, true, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);

  // Again ensure that only null rows are processed in common eval.
  auto commonResult = evaluate<SimpleVector<bool>>(
      "try(distinct_from(10::INTEGER, codepoint(c0)))", input);
  assertEqualVectors(expected, commonResult);
}

TEST_F(TryExprTest, branchingSpecialForm) {
  registerFunction<TestingAlwaysThrowsFunction, bool, bool>({"always_throws"});
  auto data = makeRowVector(
      {makeNullableFlatVector<bool>({true, true, std::nullopt}),
       makeFlatVector<double>({1.1, 2.2, 3.3}),
       makeFlatVector<double>({1.1, 2.1, 3.1}),
       makeNullableFlatVector<bool>({false, false, std::nullopt}),
       makeNullableFlatVector<bool>({std::nullopt, std::nullopt, std::nullopt}),
       makeFlatVector<bool>({true, true, true}),
       makeNullableFlatVector<bool>({std::nullopt, std::nullopt, true}),
       makeConstant<double>(1.1, 3)});

  // Test Conjunct with pre-existing errors in Coalesce and Switch. Pre-existing
  // error should be preserved.
  auto result = evaluate(
      "try(coalesce(always_throws(c0), is_nan(c1) or is_nan(c2)))", data);
  auto expected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, false});
  assertEqualVectors(expected, result);

  result = evaluate(
      "try(switch(always_throws(c0), c3, is_nan(c1) or is_finite(c2)))", data);
  auto switchExpected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, true});
  assertEqualVectors(switchExpected, result);

  // Test Coalesce and Switch over two Conjuncts on the same rows where the
  // first Conjunct throws. The errors from the first Conjunct should be
  // preserved.
  result = evaluate(
      "try(coalesce(always_throws(c0) or always_throws(c3), is_nan(c1) or is_nan(c2)))",
      data);
  assertEqualVectors(expected, result);

  result = evaluate(
      "try(switch(always_throws(c0) or always_throws(c3), is_nan(c2) or is_finite(c1), is_nan(c1) or is_finite(c2)))",
      data);
  assertEqualVectors(switchExpected, result);

  // Test Coalesce and Switch in Conjunct on rows that has pre-existing errors.
  // Coalesce and Switch should be evaluated on these rows and these rows should
  // not throw.
  result = evaluate("always_throws(c0) or coalesce(c4, is_finite(c1))", data);
  expected = makeFlatVector<bool>({true, true, true});
  assertEqualVectors(expected, result);

  result = evaluate("always_throws(c0) or switch(c4, c3, is_finite(c1))", data);
  assertEqualVectors(expected, result);

  // Test Switch where the condition has errors on all rows.
  result = evaluate(
      "try(switch(always_throws(c5), is_finite(c1), is_finite(c2)))", data);
  expected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result);

  // Test Switch where then and else clauses only evaluate on the first two
  // rows.
  result = evaluate(
      "try(switch(always_throws(c6), is_finite(c1), is_finite(c7)))", data);
  expected = makeNullableFlatVector<bool>({true, true, std::nullopt});
  assertEqualVectors(expected, result);
}

TEST_F(TryExprTest, earlyTerminationWithEmptyRows) {
  registerFunction<TestingAlwaysThrowsFunction, bool, bool>({"always_throws"});
  auto data = makeRowVector({
      makeFlatVector<bool>({true, true, true}),
      makeNullableFlatVector<bool>({true, true, std::nullopt}),
      makeFlatVector<double>({1.1, 2.2, 3.3}),
  });
  auto expected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, std::nullopt});

  auto result =
      evaluate("try(switch(always_throws(c0), is_finite(c2), c0))", data);
  assertEqualVectors(expected, result);

  result = evaluate("try(switch(c0, always_throws(c0), c0))", data);
  assertEqualVectors(expected, result);

  result = evaluate("try(coalesce(always_throws(c0), is_finite(c2)))", data);
  assertEqualVectors(expected, result);

  result = evaluate("try(always_throws(c0) and is_finite(c2))", data);
  assertEqualVectors(expected, result);

  // Test Switch where the first case produces a partial result while the second
  // case condition throws at all remaining rows. SwitchExpr may still evaluate
  // the second then clause with an empty selectivity vector. This test case
  // ensures that the evaluation of the second then clause doesn't overwrite the
  // existing partial results from the first case.
  result = evaluate(
      "try(switch(c1, is_nan(c2), always_throws(c0), is_finite(c2), c0))",
      data);
  assertEqualVectors(
      makeNullableFlatVector<bool>({false, false, std::nullopt}), result);
}

TEST_F(TryExprTest, doesNotMutateSharedResults) {
  auto input = makeFlatVector<StringView>({"1", "", ""});

  auto data = makeRowVector({input});
  // The cast here will throw an error, the result of the switch will be c0, but
  // then the try will set errors on top of c0 for all rows. c0 vector must not
  // be changed.
  auto result = evaluate("try(switch(cast(c0 as bool), c0, '1'))", data);
  // Make sure input did not change.
  assertEqualVectors(input, makeFlatVector<StringView>({"1", "", ""}));
}

TEST_F(TryExprTest, decimalDivideByZero) {
  options_.parseDecimalAsDouble = false;
  auto shortDecimalFlatVector =
      makeFlatVector<int64_t>({10, 20, 30}, DECIMAL(10, 2));
  auto longDecimalFlatVector =
      makeFlatVector<int128_t>({10100, 20202, 30300}, DECIMAL(30, 10));

  auto result =
      evaluate("try(c0 / 0.0)", makeRowVector({shortDecimalFlatVector}));
  auto expectedShort = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt}, DECIMAL(11, 2));
  assertEqualVectors(expectedShort, result);

  result = evaluate("try(c0 / 0.0)", makeRowVector({longDecimalFlatVector}));
  auto expectedLong = makeNullableFlatVector<int128_t>(
      {std::nullopt, std::nullopt, std::nullopt}, DECIMAL(31, 10));
  assertEqualVectors(expectedLong, result);
}

// This test must be DEBUG_ONLY because it uses SCOPED_TESTVALUE_SET.
DEBUG_ONLY_TEST_F(TryExprTest, errorRestoringContext) {
  registerFunction<TestingAlwaysThrowsFunction, bool, bool>({"always_throws"});
  // Use a constant input so the encoding is peeled, triggering the EvalCtx to
  // be saved and restored.
  auto data = makeRowVector({makeConstant(true, 3)});

  // EvalCtx::restore calls addError to propagate errors to the original
  // context, this can resize the ErrorVector leading to exceptions through a
  // variety of paths.  For the sake of simplicity, the TestValue is used here
  // to simulate an OOM.
  std::string exceptionMessage =
      "Expected exception. Pretend we're out of memory.";
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::EvalCtx::restore",
      std::function<void(exec::EvalCtx*)>(
          ([&](exec::EvalCtx*) { VELOX_FAIL(exceptionMessage) })));

  VELOX_ASSERT_THROW(
      evaluate("try(always_throws(c0))", data), exceptionMessage);
}

// Verify memory usage increase from wrapping an expression in TRY.
//
// When wrapping an expression memory usage increase should not exceed
// the amount of memory needed to store 1 bit per row (to track whether an
// error occurred or not).
TEST_F(TryExprTest, memoryUsage) {
  vector_size_t size = 10'000;

  auto data = makeRowVector({makeFlatVector<int64_t>(size, folly::identity)});

  // Measure memory usage without TRY.
  int64_t baseline;

  {
    auto pool = rootPool_->addLeafChild("test-memory-usage");
    auto result = evaluateWithCustomMemoryPool("c0 + 1", data, pool.get());
    ASSERT_FALSE(result->mayHaveNulls());

    baseline = pool->peakBytes();
  }

  // Measure memory usage with TRY over non-throwing expression.
  {
    // Memory allocation is not precisise. There is some padding, rounding and
    // quantizing that makes it hard to tell exactly how much memory is
    // allocated. Given that we only need 1 bit per row, allowing 4 bits per row
    // seems conservative enough.
    const auto expectedIncrease = size / 2;

    auto pool = rootPool_->addLeafChild("test-memory-usage");
    auto result = evaluateWithCustomMemoryPool("try(c0 + 1)", data, pool.get());
    ASSERT_FALSE(result->mayHaveNulls());

    ASSERT_GE(pool->peakBytes(), baseline);
    ASSERT_LE(pool->peakBytes(), baseline + expectedIncrease)
        << (pool->peakBytes() - baseline);
  }

  // Measure memory usage with TRY over expression that throws from every row.
  {
    const auto expectedIncrease = size / 2;

    auto pool = rootPool_->addLeafChild("test-memory-usage");
    auto result = evaluateWithCustomMemoryPool("try(c0 / 0)", data, pool.get());
    ASSERT_TRUE(result->mayHaveNulls());

    ASSERT_GE(pool->peakBytes(), baseline);
    ASSERT_LE(pool->peakBytes(), baseline + expectedIncrease)
        << (pool->peakBytes() - baseline);
  }
}

} // namespace facebook::velox
