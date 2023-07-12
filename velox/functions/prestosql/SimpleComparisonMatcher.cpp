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

#include "velox/functions/prestosql/SimpleComparisonMatcher.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::functions::prestosql {

namespace {

class Matcher {
 public:
  virtual bool match(const core::TypedExprPtr& expr) = 0;

  virtual ~Matcher() = default;

  static bool allMatch(
      const std::vector<core::TypedExprPtr>& exprs,
      std::vector<std::shared_ptr<Matcher>>& matchers) {
    for (auto i = 0; i < exprs.size(); ++i) {
      if (!matchers[i]->match(exprs[i])) {
        return false;
      }
    }
    return true;
  }
};

using MatcherPtr = std::shared_ptr<Matcher>;

class IfMatcher : public Matcher {
 public:
  explicit IfMatcher(std::vector<MatcherPtr> inputMatchers)
      : inputMatchers_{std::move(inputMatchers)} {
    VELOX_CHECK_EQ(3, inputMatchers_.size());
  }

  bool match(const core::TypedExprPtr& expr) override {
    if (auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
      if (call->name() == "if" && allMatch(call->inputs(), inputMatchers_)) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<MatcherPtr> inputMatchers_;
};

using IfMatcherPtr = std::shared_ptr<IfMatcher>;

class ComparisonMatcher : public Matcher {
 public:
  ComparisonMatcher(
      const std::string& prefix,
      std::vector<MatcherPtr> inputMatchers,
      std::string* op)
      : prefix_{prefix}, inputMatchers_{std::move(inputMatchers)}, op_{op} {
    VELOX_CHECK_EQ(2, inputMatchers_.size());
  }

  bool match(const core::TypedExprPtr& expr) override {
    if (auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
      const auto& name = call->name();
      if (name == prefix_ + "eq" || name == prefix_ + "lt" ||
          name == prefix_ + "gt") {
        if (allMatch(call->inputs(), inputMatchers_)) {
          *op_ = name;
          return true;
        }
      }
    }
    return false;
  }

 private:
  const std::string prefix_;
  std::vector<MatcherPtr> inputMatchers_;
  std::string* op_;
};

using ComparisonMatcherPtr = std::shared_ptr<ComparisonMatcher>;

class AnySingleInputMatcher : public Matcher {
 public:
  AnySingleInputMatcher(
      core::TypedExprPtr* expr,
      core::FieldAccessTypedExprPtr* input)
      : expr_{expr}, input_{input} {}

  bool match(const core::TypedExprPtr& expr) override {
    // Check if 'expr' depends on a single column.
    std::unordered_set<core::FieldAccessTypedExprPtr> inputs;
    collectInputs(expr, inputs);

    if (inputs.size() == 1) {
      *expr_ = expr;
      *input_ = *inputs.begin();
      return true;
    }

    return false;
  }

 private:
  static void collectInputs(
      const core::TypedExprPtr& expr,
      std::unordered_set<core::FieldAccessTypedExprPtr>& inputs) {
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
      if (field->isInputColumn()) {
        inputs.insert(field);
        return;
      }
    }

    for (const auto& input : expr->inputs()) {
      collectInputs(input, inputs);
    }
  }

  core::TypedExprPtr* const expr_;
  core::FieldAccessTypedExprPtr* const input_;
};

/// Matches constant expression that represents values 1, 0, or -1 of type
/// BIGINT.
class ComparisonConstantMatcher : public Matcher {
 public:
  explicit ComparisonConstantMatcher(int64_t* value) : value_{value} {}

  bool match(const core::TypedExprPtr& expr) override {
    if (auto constant = asConstant(expr.get())) {
      auto v = constant.value();
      if (v == 0 || v == 1 || v == -1) {
        *value_ = v;
        return true;
      }
    }
    return false;
  }

 private:
  static std::optional<int64_t> asConstant(const core::ITypedExpr* expr) {
    if (auto constant = dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
      if (constant->hasValueVector()) {
        auto constantVector =
            constant->valueVector()->as<SimpleVector<int64_t>>();
        if (!constantVector->isNullAt(0)) {
          return constantVector->valueAt(0);
        }
      } else {
        if (!constant->value().isNull()) {
          if (constant->value().kind() == TypeKind::BIGINT) {
            return constant->value().value<int64_t>();
          }

          if (constant->value().kind() == TypeKind::INTEGER) {
            return constant->value().value<int32_t>();
          }
        }
      }
    }

    return std::nullopt;
  }

  int64_t* const value_;
};

using ComparisonConstantMatcherPtr = std::shared_ptr<ComparisonConstantMatcher>;

MatcherPtr ifelse(
    const MatcherPtr& condition,
    const MatcherPtr& thenClause,
    const MatcherPtr& elseClause) {
  return std::make_shared<IfMatcher>(
      std::vector<MatcherPtr>{condition, thenClause, elseClause});
}

MatcherPtr comparison(
    const std::string& prefix,
    const MatcherPtr& left,
    const MatcherPtr& right,
    std::string* op) {
  return std::make_shared<ComparisonMatcher>(
      prefix, std::vector<MatcherPtr>{left, right}, op);
}

MatcherPtr anySingleInput(
    core::TypedExprPtr* expr,
    core::FieldAccessTypedExprPtr* input) {
  return std::make_shared<AnySingleInputMatcher>(expr, input);
}

MatcherPtr comparisonConstant(int64_t* value) {
  return std::make_shared<ComparisonConstantMatcher>(value);
}

std::string invert(const std::string& prefix, const std::string& op) {
  return op == prefix + "lt" ? prefix + "gt" : prefix + "lt";
}

/// Returns true for a < b -> -1.
bool isLessThen(
    const std::string& prefix,
    const std::string& operation,
    const core::FieldAccessTypedExprPtr& left,
    int64_t result,
    const std::string& inputLeft) {
  std::string op =
      (left->name() == inputLeft) ? operation : invert(prefix, operation);

  if (op == prefix + "lt") {
    return result < 0;
  }

  return result > 0;
}

} // namespace

std::optional<SimpleComparison> isSimpleComparison(
    const std::string& prefix,
    const core::LambdaTypedExpr& expr) {
  // First, check the shape of the expression.
  // if (x(a) < y(b), c1, if (u(c) > v(d), c2, c3))
  core::FieldAccessTypedExprPtr a, b, c, d;
  core::TypedExprPtr x, y, u, v;
  std::string op1, op2;
  int64_t c1, c2, c3;

  auto matcher = ifelse(
      comparison(prefix, anySingleInput(&x, &a), anySingleInput(&y, &b), &op1),
      comparisonConstant(&c1),
      ifelse(
          comparison(
              prefix, anySingleInput(&u, &c), anySingleInput(&v, &d), &op2),
          comparisonConstant(&c2),
          comparisonConstant(&c3)));

  if (!matcher->match(expr.body())) {
    return std::nullopt;
  }

  // Verify that a != b, c != d.
  if (a == b || c == d) {
    return std::nullopt;
  }

  // Verify that x, y, u, v are the same (except for input column).
  std::unordered_map<std::string, core::TypedExprPtr> inputMapping;
  inputMapping.emplace(
      a->name(),
      std::make_shared<core::FieldAccessTypedExpr>(a->type(), b->name()));
  const auto xRewritten = x->rewriteInputNames(inputMapping);
  if (!(*xRewritten == *y->rewriteInputNames(inputMapping) &&
        *xRewritten == *u->rewriteInputNames(inputMapping) &&
        *xRewritten == *v->rewriteInputNames(inputMapping))) {
    return std::nullopt;
  }

  // Verify all constants are different.
  if (c1 == c2 || c2 == c3 || c1 == c3) {
    return std::nullopt;
  }

  const auto eq = prefix + "eq";

  // Verify that equality comparisons return zero.
  // if (x(a) = y(a), 0,..) is good. if (x(a) = y(a), 1,..) is not good.
  // Also, verify that non-equality comparisons return non-zerp.
  // if (x(a) < y(a), 1,..) is good. if (x(a) < y(a), 0,..) is not good.
  if ((op1 == eq && c1 != 0) || (op1 != eq && c1 == 0)) {
    return std::nullopt;
  }

  if ((op2 == eq && c2 != 0) || (op2 != eq && c2 == 0)) {
    return std::nullopt;
  }

  const auto left = expr.signature()->nameOf(0);

  const auto transform = a->name() == left ? x : y;

  if (op1 == eq) {
    // if (x(a) = y(b), 0,...)
    return {{transform, isLessThen(prefix, op2, c, c2, left)}};
  }

  if (op2 == eq) {
    return {{transform, isLessThen(prefix, op1, a, c1, left)}};
  }

  // Make sure op1 and op2 are aligned.
  auto b1 = isLessThen(prefix, op1, a, c1, left);
  auto b2 = isLessThen(prefix, op2, c, c2, left);
  if (b1 != b2) {
    return std::nullopt;
  }

  return {{transform, b1}};
}

} // namespace facebook::velox::functions::prestosql
