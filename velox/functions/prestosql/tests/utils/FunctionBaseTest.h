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
#include <utility>

#include "velox/expression/Expr.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::test {

class FunctionBaseTest : public testing::Test,
                         public velox::test::VectorTestBase {
 public:
  using IntegralTypes =
      ::testing::Types<TinyintType, SmallintType, IntegerType, BigintType>;

  using FloatingPointTypes = ::testing::Types<DoubleType, RealType>;

  using FloatingPointAndIntegralTypes = ::testing::Types<
      TinyintType,
      SmallintType,
      IntegerType,
      BigintType,
      DoubleType,
      RealType>;

 protected:
  static void SetUpTestCase();

  static std::function<vector_size_t(vector_size_t row)> modN(int n) {
    return [n](vector_size_t row) { return row % n; };
  }

  static RowTypePtr rowType(const std::string& name, const TypePtr& type) {
    return ROW({name}, {type});
  }

  static RowTypePtr rowType(
      const std::string& name,
      const TypePtr& type,
      const std::string& name2,
      const TypePtr& type2) {
    return ROW({name, name2}, {type, type2});
  }

  core::TypedExprPtr makeTypedExpr(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text, options_);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
  }

  // Use this directly if you want to evaluate a manually-constructed expression
  // tree and don't want it to cast the returned vector.
  VectorPtr evaluate(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    return evaluateImpl<exec::ExprSet>(typedExpr, data, rows);
  }

  // Use this directly if you don't want it to cast the returned vector.
  VectorPtr evaluate(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));

    return evaluate(typedExpr, data, rows);
  }

  /// Evaluates the expression on specified inputs and returns a pair of result
  /// vector and evaluation statistics. The statistics are reported per function
  /// or special form. If a function or a special form occurs in the expression
  /// multiple times, the returned statistics will contain values aggregated
  /// across all calls. Statistics will be missing for functions and
  /// special forms that didn't get evaluated.
  std::pair<VectorPtr, std::unordered_map<std::string, exec::ExprStats>>
  evaluateWithStats(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt);

  // Use this function if you want to evaluate a manually-constructed expression
  // tree.
  template <typename T>
  std::shared_ptr<T> evaluate(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      const TypePtr& resultType = nullptr) {
    auto result = evaluate(typedExpr, data, rows);
    return castEvaluateResult<T>(result, typedExpr->toString(), resultType);
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      const TypePtr& resultType = nullptr) {
    auto result = evaluate(expression, data, rows);
    return castEvaluateResult<T>(result, expression, resultType);
  }

  template <typename T>
  std::shared_ptr<T> evaluateSimplified(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      const TypePtr& resultType = nullptr) {
    auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));
    auto result = evaluateImpl<exec::ExprSetSimplified>(typedExpr, data, rows);

    return castEvaluateResult<T>(result, expression, resultType);
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      RowVectorPtr data,
      const SelectivityVector& rows,
      VectorPtr& result,
      const TypePtr& resultType = nullptr) {
    exec::ExprSet exprSet(
        {makeTypedExpr(expression, asRowType(data->type()))}, &execCtx_);

    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results{std::move(result)};
    exprSet.eval(rows, evalCtx, results);
    result = results[0];

    // reinterpret_cast is used for functions that return logical types like
    // DATE().
    if (resultType != nullptr) {
      return std::reinterpret_pointer_cast<T>(results[0]);
    }
    return std::dynamic_pointer_cast<T>(results[0]);
  }

  /// Evaluate a given expression over a single row of input returning the
  /// result as a std::optional C++ value. Prefer to use the `evaluateOnce()`
  /// helper methods below when testing simple functions instead of manually
  /// handling input and output vectors.
  ///
  /// Arguments should be referenced using c0, c1, .. cn.  Supports integers,
  /// floats, booleans, strings, timestamps, and other primitive types.
  ///
  /// The template parameter type `TReturn` is mandatory and used to cast the
  /// returned value. Always use the C++ type for the returned value (e.g.
  /// `double` not `DOUBLE()`). Either StringView or std::string can be used for
  /// VARCHAR() and VARBINARY().
  ///
  /// If the function returns a custom type, use the physical type that
  /// represent the custom type (e.g. `CustomType<T>::type`). For example, use
  /// `int64_t` to return TIMESTAMP_WITH_TIME_ZONE()
  ///
  /// Input and output parameters should always be wrapped in an std::optional
  /// to allow the representation of null values.
  ///
  /// Example:
  ///
  ///   std::optional<double> exp(std::optional<double> a) {
  ///     return evaluateOnce<double>("exp(c0)", a);
  ///   }
  ///   EXPECT_EQ(1, exp(0));
  ///   EXPECT_EQ(std::nullopt, exp(std::nullopt));
  ///
  ///
  /// Multiple parameters are supported:
  ///
  ///   std::optional<int64_t> xxhash64(
  ///       std::optional<std::string> val,
  ///       std::optional<int64_t> seed) {
  ///     return evaluateOnce<int64_t>("fb_xxhash64(c0, c1)", val, seed);
  ///   }
  ///
  ///
  /// If not specified, input logical types are derived from the C++ type
  /// provided as input using CppToType. In the example above, the `std::string`
  /// C++ type will be translated to `VARCHAR` logical type, and `int64_t` to
  /// `BIGINT`.
  ///
  /// To override the input logical type, use:
  ///
  ///   std::optional<int64_t> hour(std::optional<int32_t> date) {
  ///     return evaluateOnce<int64_t>("hour(c0)", DATE(), date);
  ///   }
  ///
  /// Custom types like TIMESTAMP_WITH_TIMEZONE() and HYPERLOGLOG() are also
  /// supported. For multiple parameters, use a list of logical types:
  ///
  ///   return evaluateOnce<std::string>(
  ///       "hmac_sha1(c0, c1)", {VARBINARY(), VARBINARY()}, arg, key);
  ///
  ///
  /// One can also manually specify additional template parameters, in case
  /// there are problems with template type deduction:
  ///
  ///   return evaluateOnce<int64_t, std::string, int64_t>(
  ///       "fb_xxhash64(c0, c1)", val, seed);
  ///
  template <typename TReturn, typename... TArgs>
  std::optional<TReturn> evaluateOnce(
      const std::string& expr,
      const std::initializer_list<TypePtr>& types,
      std::optional<TArgs>... args) {
    return evaluateOnce<TReturn>(
        expr,
        makeRowVector(unpackEvaluateParams<std::optional<TArgs>...>(
            std::vector<TypePtr>{types},
            std::forward<std::optional<TArgs>>(std::move(args))...)));
  }

  template <typename TReturn, typename... TArgs>
  std::optional<TReturn> evaluateOnce(
      const std::string& expr,
      std::optional<TArgs>... args) {
    return evaluateOnce<TReturn>(
        expr,
        makeRowVector(unpackEvaluateParams<std::optional<TArgs>...>(
            {}, std::forward<std::optional<TArgs>>(std::move(args))...)));
  }

  // Convenience version to allow API users to specify a single type for
  // functions that take a single parameter, instead of having to wrap them in a
  // initializer_list. For example, it allows users to do:
  //
  //  auto a = evaluateOnce<int64_t>("hour(c0)", DATE(), date);
  //
  // instead of:
  //
  //  auto a = evaluateOnce<int64_t>("hour(c0)", {DATE()}, date);
  //
  // Note that if types are specified, evaluateOnce() will throws if the number
  // of types and parameter do not match.
  template <typename TReturn, typename... TArgs>
  std::optional<TReturn> evaluateOnce(
      const std::string& expr,
      const TypePtr& type,
      std::optional<TArgs>... args) {
    return evaluateOnce<TReturn>(
        expr,
        makeRowVector(unpackEvaluateParams<std::optional<TArgs>...>(
            {type}, std::forward<std::optional<TArgs>>(std::move(args))...)));
  }

  template <typename TReturn>
  std::optional<TReturn> evaluateOnce(
      const std::string& expr,
      const RowVectorPtr rowVectorPtr,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      const TypePtr& resultType = nullptr) {
    auto result =
        evaluate<SimpleVector<facebook::velox::test::EvalType<TReturn>>>(
            expr, rowVectorPtr, rows, resultType);
    return result->isNullAt(0) ? std::optional<TReturn>{}
                               : TReturn(result->valueAt(0));
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text, options_);
    return core::Expressions::inferTypes(untyped, rowType, pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(std::move(expressions), &execCtx_);
  }

  std::unique_ptr<exec::ExprSet> compileExpressions(
      const std::vector<std::string>& exprs,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions;
    expressions.reserve(exprs.size());
    for (const auto& expr : exprs) {
      expressions.emplace_back(parseExpression(expr, rowType));
    }
    return std::make_unique<exec::ExprSet>(std::move(expressions), &execCtx_);
  }

  VectorPtr evaluate(
      exec::ExprSet& exprSet,
      const RowVectorPtr& input,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    exec::EvalCtx context(&execCtx_, &exprSet, input.get());

    std::vector<VectorPtr> result(1);
    if (rows.has_value()) {
      exprSet.eval(*rows, context, result);
    } else {
      SelectivityVector defaultRows(input->size());
      exprSet.eval(defaultRows, context, result);
    }
    return result[0];
  }

  /// Parses a timestamp string into Timestamp.
  /// Accepts strings formatted as 'YYYY-MM-DD HH:mm:ss[.nnn]'.
  static Timestamp parseTimestamp(const std::string& text) {
    return util::fromTimestampString(
               text.data(), text.size(), util::TimestampParseMode::kPrestoCast)
        .thenOrThrow(folly::identity, [&](const Status& status) {
          VELOX_USER_FAIL("{}", status.message());
        });
  }

  /// Parses a date string into days since epoch.
  /// Accepts strings formatted as 'YYYY-MM-DD'.
  static int32_t parseDate(const std::string& text) {
    return DATE()->toDays(text);
  }

  /// Returns a vector of signatures for the given function name and return
  /// type.
  /// @param returnType The name of expected return type defined in function
  /// signature.
  static std::vector<const exec::FunctionSignature*> getSignatures(
      const std::string& functionName,
      const std::string& returnType);

  /// Returns a set of signatures for a given function serialized to strings.
  static std::unordered_set<std::string> getSignatureStrings(
      const std::string& functionName);

  /// Given an expression, a list of inputs and expected results, generate
  /// dictionary-encoded and constant-encoded vectors, evaluate the expression
  /// and verify the results.
  void testEncodings(
      const core::TypedExprPtr& expr,
      const std::vector<VectorPtr>& inputs,
      const VectorPtr& expected);

  std::shared_ptr<core::QueryCtx> queryCtx_{
      core::QueryCtx::create(executor_.get())};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  parse::ParseOptions options_;

 private:
  template <typename T>
  std::shared_ptr<T> castEvaluateResult(
      const VectorPtr& result,
      const std::string& expression,
      const TypePtr& expectedType = nullptr) {
    // reinterpret_cast is used for functions that return logical types
    // like DATE().
    VELOX_CHECK(result, "Expression evaluation result is null: {}", expression);
    if (expectedType != nullptr) {
      VELOX_CHECK_EQ(
          result->type()->kind(),
          expectedType->kind(),
          "Expression evaluation result is not of expected type kind: {} -> {} vector of type {}",
          expression,
          result->encoding(),
          result->type()->kindName());
      auto castedResult = std::reinterpret_pointer_cast<T>(result);
      return castedResult;
    }

    auto castedResult = std::dynamic_pointer_cast<T>(result);
    VELOX_CHECK(
        castedResult,
        "Expression evaluation result is not of expected type: {} -> {} vector of type {}",
        expression,
        result->encoding(),
        result->type()->toString());
    return castedResult;
  }

  template <typename ExprSet>
  VectorPtr evaluateImpl(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    ExprSet exprSet({typedExpr}, &execCtx_);
    return evaluate(exprSet, data, rows);
  }

  // Unpack parameters for evaluateOnce(). Base recursion case.
  template <typename...>
  std::vector<VectorPtr> unpackEvaluateParams(
      const std::vector<TypePtr>& types) {
    VELOX_CHECK(types.empty(), "Wrong number of types passed to evaluateOnce.");
    return {};
  }

  // Recursively unpack input values and types into vectors.
  template <typename T, typename... TArgs>
  std::vector<VectorPtr> unpackEvaluateParams(
      const std::vector<TypePtr>& types,
      T&& value,
      TArgs&&... args) {
    // If there are no input types, let makeNullable figure it out.
    auto output = std::vector<VectorPtr>{
        types.empty()
            ? makeNullableFlatVector(std::vector{value})
            : makeNullableFlatVector(std::vector{value}, types.front()),
    };

    // Recurse starting from the second parameter.
    auto result = unpackEvaluateParams<TArgs...>(
        types.size() > 1 ? std::vector<TypePtr>{types.begin() + 1, types.end()}
                         : std::vector<TypePtr>{},
        std::forward<TArgs>(std::move(args))...);
    output.insert(output.end(), result.begin(), result.end());
    return output;
  }
};

} // namespace facebook::velox::functions::test
