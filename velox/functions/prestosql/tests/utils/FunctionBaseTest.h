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

#include "velox/expression/Expr.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::test {

class FunctionBaseTest : public testing::Test,
                         public velox::test::VectorTestBase {
 public:
  // This class generates test name suffixes based on the type.
  // We use the type's toString() return value as the test name.
  // Used as the third argument for GTest TYPED_TEST_SUITE.
  class TypeNames {
   public:
    template <typename T>
    static std::string GetName(int) {
      T type;
      return type.toString();
    }
  };

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
      const RowVectorPtr& data) {
    return evaluateImpl<exec::ExprSet>(typedExpr, data);
  }

  // Use this directly if you don't want it to cast the returned vector.
  VectorPtr evaluate(const std::string& expression, const RowVectorPtr& data) {
    auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));

    return evaluate(typedExpr, data);
  }

  /// Evaluates the expression on specified inputs and returns a pair of result
  /// vector and evaluation statistics. The statistics are reported per function
  /// or special form. If a function or a special form occurs in the expression
  /// multiple times, the returned statistics will contain values aggregated
  /// across all calls. Statistics will be missing for functions and
  /// special forms that didn't get evaluated.
  std::pair<VectorPtr, std::unordered_map<std::string, exec::ExprStats>>
  evaluateWithStats(const std::string& expression, const RowVectorPtr& data);

  // Use this function if you want to evaluate a manually-constructed expression
  // tree.
  template <typename T>
  std::shared_ptr<T> evaluate(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data) {
    auto result = evaluate(typedExpr, data);
    return castEvaluateResult<T>(result, typedExpr->toString());
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto result = evaluate(expression, data);
    return castEvaluateResult<T>(result, expression);
  }

  template <typename T>
  std::shared_ptr<T> evaluateSimplified(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));
    auto result = evaluateImpl<exec::ExprSetSimplified>(typedExpr, data);

    return castEvaluateResult<T>(result, expression);
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      RowVectorPtr data,
      const SelectivityVector& rows,
      VectorPtr& result) {
    exec::ExprSet exprSet(
        {makeTypedExpr(expression, asRowType(data->type()))}, &execCtx_);

    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results{std::move(result)};
    exprSet.eval(rows, evalCtx, results);
    result = results[0];

    return std::dynamic_pointer_cast<T>(results[0]);
  }

  // Evaluate the given expression once, returning the result as a std::optional
  // C++ value. Arguments should be referenced using c0, c1, .. cn.  Supports
  // integers, floats, booleans, and strings.
  //
  // Example:
  //   std::optional<double> exp(std::optional<double> a) {
  //     return evaluateOnce<double>("exp(c0)", a);
  //   }
  //   EXPECT_EQ(1, exp(0));
  //   EXPECT_EQ(std::nullopt, exp(std::nullopt));
  template <typename ReturnType, typename... Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const std::optional<Args>&... args) {
    return evaluateOnce<ReturnType>(
        expr,
        makeRowVector(std::vector<VectorPtr>{
            makeNullableFlatVector(std::vector{args})...}));
  }

  template <typename ReturnType, typename Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const std::vector<std::optional<Args>>& args,
      const std::vector<TypePtr>& types) {
    std::vector<VectorPtr> flatVectors;
    for (vector_size_t i = 0; i < args.size(); ++i) {
      flatVectors.emplace_back(makeNullableFlatVector(
          std::vector<std::optional<Args>>{args[i]}, types[i]));
    }
    auto rowVectorPtr = makeRowVector(flatVectors);
    return evaluateOnce<ReturnType>(expr, rowVectorPtr);
  }

  template <typename ReturnType, typename... Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const RowVectorPtr rowVectorPtr) {
    auto result =
        evaluate<SimpleVector<EvalType<ReturnType>>>(expr, rowVectorPtr);
    return result->isNullAt(0) ? std::optional<ReturnType>{}
                               : ReturnType(result->valueAt(0));
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

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(&execCtx_, &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  /// Returns a set of signatures for a given function serialized to strings.
  static std::unordered_set<std::string> getSignatureStrings(
      const std::string& functionName);

  std::shared_ptr<core::QueryCtx> queryCtx_{
      std::make_shared<core::QueryCtx>(executor_.get())};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  parse::ParseOptions options_;

 private:
  template <typename T>
  std::shared_ptr<T> castEvaluateResult(
      const VectorPtr& result,
      const std::string& expression) {
    VELOX_CHECK(result, "Expression evaluation result is null: {}", expression);

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
      const RowVectorPtr& data) {
    SelectivityVector rows(data->size());
    std::vector<VectorPtr> results(1);

    ExprSet exprSet({typedExpr}, &execCtx_);
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    exprSet.eval(rows, evalCtx, results);

    return results[0];
  }
};

} // namespace facebook::velox::functions::test
