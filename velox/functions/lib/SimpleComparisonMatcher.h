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

#include "velox/core/Expressions.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::functions {

struct SimpleComparison {
  core::TypedExprPtr expr;
  bool isLessThen;
};

class Matcher {
 public:
  virtual bool match(const core::TypedExprPtr& expr) = 0;

  virtual ~Matcher() = default;

  // Checks if all expressions match the corresponding matchers.
  static bool allMatch(
      const std::vector<core::TypedExprPtr>& exprs,
      std::vector<std::shared_ptr<Matcher>>& matchers);
};

// Matches the if expression that children match the given matchers.
class IfMatcher : public Matcher {
 public:
  explicit IfMatcher(std::vector<std::shared_ptr<Matcher>> inputMatchers)
      : inputMatchers_{std::move(inputMatchers)} {
    VELOX_CHECK_EQ(3, inputMatchers_.size());
  }

  bool match(const core::TypedExprPtr& expr) override;

 private:
  std::vector<std::shared_ptr<Matcher>> inputMatchers_;
};

// Matches comparison expression that represents `eq`, `lt`, or `gt`.
class ComparisonMatcher : public Matcher {
 public:
  ComparisonMatcher(
      const std::string& prefix,
      const std::vector<std::shared_ptr<Matcher>>& inputMatchers,
      std::string* op)
      : prefix_{prefix}, inputMatchers_{inputMatchers}, op_{op} {
    VELOX_CHECK_EQ(2, inputMatchers_.size());
  }

  /// Checks if the given name specifies a comparison expression. Can be
  /// overriden to use different function names for Spark.
  virtual bool exprNameMatch(const std::string& name) {
    return name == prefix_ + "eq" || name == prefix_ + "lt" ||
        name == prefix_ + "gt";
  }

  bool match(const core::TypedExprPtr& expr) override;

 protected:
  const std::string prefix_;

 private:
  std::vector<std::shared_ptr<Matcher>> inputMatchers_;
  std::string* op_;
};

class AnySingleInputMatcher : public Matcher {
 public:
  AnySingleInputMatcher(
      core::TypedExprPtr* expr,
      core::FieldAccessTypedExprPtr* input)
      : expr_{expr}, input_{input} {}

  bool match(const core::TypedExprPtr& expr) override;

 private:
  static void collectInputs(
      const core::TypedExprPtr& expr,
      std::unordered_set<core::FieldAccessTypedExprPtr>& inputs);

  core::TypedExprPtr* const expr_;
  core::FieldAccessTypedExprPtr* const input_;
};

/// Matches constant expression that represents values 1, 0, or -1 of type
/// BIGINT.
class ComparisonConstantMatcher : public Matcher {
 public:
  explicit ComparisonConstantMatcher(int64_t* value) : value_{value} {}

  bool match(const core::TypedExprPtr& expr) override;

 private:
  static std::optional<int64_t> asConstant(const core::ITypedExpr* expr);

  int64_t* const value_;
};

class SimpleComparisonChecker {
 protected:
  std::shared_ptr<Matcher> ifelse(
      const std::shared_ptr<Matcher>& condition,
      const std::shared_ptr<Matcher>& thenClause,
      const std::shared_ptr<Matcher>& elseClause) {
    return std::make_shared<IfMatcher>(std::vector<std::shared_ptr<Matcher>>{
        condition, thenClause, elseClause});
  }

  std::shared_ptr<Matcher> anySingleInput(
      core::TypedExprPtr* expr,
      core::FieldAccessTypedExprPtr* input) {
    return std::make_shared<AnySingleInputMatcher>(expr, input);
  }

  std::shared_ptr<Matcher> comparisonConstant(int64_t* value) {
    return std::make_shared<ComparisonConstantMatcher>(value);
  }

  std::string invert(const std::string& prefix, const std::string& op) {
    return op == ltName(prefix) ? gtName(prefix) : ltName(prefix);
  }

  // Returns true for a < b -> -1.
  bool isLessThen(
      const std::string& prefix,
      const std::string& operation,
      const core::FieldAccessTypedExprPtr& left,
      int64_t result,
      const std::string& inputLeft);

  virtual std::shared_ptr<Matcher> comparison(
      const std::string& prefix,
      const std::shared_ptr<Matcher>& left,
      const std::shared_ptr<Matcher>& right,
      std::string* op) {
    return std::make_shared<ComparisonMatcher>(
        prefix, std::vector<std::shared_ptr<Matcher>>{left, right}, op);
  }

  virtual std::string eqName(const std::string& prefix) {
    return prefix + "eq";
  }

  virtual std::string ltName(const std::string& prefix) {
    return prefix + "lt";
  }

  virtual std::string gtName(const std::string& prefix) {
    return prefix + "gt";
  }

 public:
  virtual ~SimpleComparisonChecker() = default;

  /// Given a lambda expression, checks it if represents a simple comparator and
  /// returns the summary of the same.
  ///
  /// For example, identifies
  ///     (x, y) -> if(length(x) < length(y), -1, if(length(x) > length(y), 1,
  ///     0))
  /// expression as a "less than" comparison over length(x).
  ///
  /// Recognizes different variations of this expression, e.g.
  ///
  ///     (x, y) -> if(expr(x) = expr(y), 0, if(expr(x) < expr(y), -1, 1))
  ///     (x, y) -> if(expr(x) = expr(y), 0, if(expr(y) > expr(x), -1, 1))
  ///
  /// Returns std::nullopt if expression is not recognized as a simple
  /// comparator.
  ///
  /// Can be used to re-write generic lambda expressions passed to array_sort
  /// into simpler ones that can be evaluated more efficiently.
  std::optional<SimpleComparison> isSimpleComparison(
      const std::string& prefix,
      const core::LambdaTypedExpr& expr);
};

} // namespace facebook::velox::functions
