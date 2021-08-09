/*
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
#include "velox/experimental/codegen/ast/ASTAnalysis.h"
#include "boost/logic/tribool.hpp"
#include "velox/experimental/codegen/ast/ASTNode.h"
#include "velox/type/Type.h"

namespace facebook::velox::codegen::analysis {

bool isDefaultNull(const ASTNode& node) {
  if (node.as<const InputRefExpr>() || node.isConstantExpression()) {
    return true;
  }

  if (node.getNullMode() == ExpressionNullMode::NullInNullOut) {
    for (auto child : node.children()) {
      if (!isDefaultNull(*child)) {
        return false;
      }
    }
    return true;
  }
  return false;
}

bool isDefaultNullStrict(const ASTNode& node) {
  if (node.as<const InputRefExpr>()) {
    return true;
  }

  if (node.isConstantExpression()) {
    // maybe null for constant expressions is preset.
    return !node.maybeNull();
  }

  if (auto udfCallExpr = node.as<const UDFCallExpr>();
      node.getNullMode() == ExpressionNullMode::NullInNullOut &&
      (udfCallExpr == nullptr || !udfCallExpr->isNullableOutput())) {
    for (auto child : node.children()) {
      if (!isDefaultNullStrict(*child)) {
        return false;
      }
    }
    return true;
  }

  return false;
}

namespace {
// default null clauses connected by logical operators
bool isDefaultNullOrLogical(const ASTNode& node) {
  // logical operators
  if (node.as<const NotExpr>() || node.as<const LogicalAnd>() ||
      node.as<const LogicalOr>() || node.as<const IsNullExpr>()) {
    for (auto child : node.children()) {
      if (!isDefaultNullOrLogical(*child)) {
        return false;
      }
    }
    return true;
  }

  // when node is not logical, the entire subtree must be default null
  return isDefaultNull(node);
}

using namespace boost::logic;

// needed to put tribool in a set
struct triboolLT {
  bool operator()(const tribool& l, const tribool& r) const {
    return l.value < r.value;
  }
};

bool isConstExpr(const ASTNode& node) {
  if (node.as<const InputRefExpr>()) {
    return false;
  }

  auto ret = true;
  for (const auto& child : node.children()) {
    ret = ret && isConstExpr(*child);
  }
  return ret;
}

/// return the set of all possible logical result given at least one input
/// variable is null. Example: "(a>b) AND (c>d)" returns {null, false}
/// the logic:
/// a or b is null => lhs is null => null AND {true, null, false} gets {null,
/// false};
/// c or d is null => rhs is null => {true, null, false} AND null gets {null,
/// false};
/// then the union of the 2 is the final output {null, false} U {null, false} =
/// {null, false}
std::set<tribool, triboolLT> outputIfNullIn(const ASTNode& node) {
  const static std::set<tribool, triboolLT> fullSet = {
      true, false, indeterminate}; // indeterminate == null
  std::set<tribool, triboolLT> out;
  if (node.as<const LogicalAnd>()) {
    std::set<tribool, triboolLT> childOut;
    for (auto& child : node.children()) {
      childOut.merge(outputIfNullIn(*child));
    }
    for (auto& val : childOut) {
      /* assuming if the null variable comes from one side then the other side
       can be any truth value (same below for LogicalOr). this doesn't
       guarantee the most accurate result (e.g. when a variable appears in
       both sides), but it ensures no non-default null expression will be
       treated as default null. */
      for (auto& otherVal : fullSet) {
        out.insert(val && otherVal);
      }
    }
    return out;
  }

  if (node.as<const LogicalOr>()) {
    std::set<tribool, triboolLT> childOut;
    for (auto& child : node.children()) {
      childOut.merge(outputIfNullIn(*child));
    }
    for (auto& val : childOut) {
      for (auto& otherVal : fullSet) {
        out.insert(val || otherVal);
      }
    }
    return out;
  }

  if (auto notExpr = node.as<const NotExpr>()) {
    for (auto& val : outputIfNullIn(*notExpr->child())) {
      out.insert(!val);
    }
    return out;
  }

  if (auto isNullExpr = node.as<const IsNullExpr>()) {
    auto childOut = outputIfNullIn(*isNullExpr->child());
    if (childOut.count(indeterminate)) {
      // (null) IS NULL => true
      out.insert(true);
    }
    if (childOut.count(true) || childOut.count(false)) {
      // (anything not null) IS NULL => false
      out.insert(false);
    }
    return out;
  }

  // not logical operator, we know it's default null boolean expr at this point

  // if it's a default null constant expression, the result should be a fixed
  // value, but we don't know without evaluation, so just return fullSet
  if (isConstExpr(node)) {
    return fullSet;
  }

  // a default null boolean expr that's not constant (has variable), then it
  // only evaluates to null if any input is null
  return {indeterminate};
}
} // namespace

/*
Filter Default Null: an extended default null definition for filter.
A boolean expression that eventually evaluates to false or null are treated the
same - both as false by the filter, therefore the default null analysis can be
stretched for filter: "If at least one input variable is null, then the
expression cannot evaluate to true".
*/
bool isFilterDefaultNull(const ASTNode& node) {
  if (isDefaultNull(node)) {
    // a regular default null is also an extended default null
    return true;
  }

  if (!isDefaultNullOrLogical(node)) {
    return false;
  }

  auto outSet = outputIfNullIn(node);
  return outSet.find(true) == outSet.end();
}
} // namespace facebook::velox::codegen::analysis
