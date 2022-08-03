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

#include "velox/expression/ExprCompiler.h"
#include "velox/expression/CastExpr.h"
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/LambdaExpr.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/expression/TryExpr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::exec {

namespace {

using core::ITypedExpr;
using core::TypedExprPtr;

const char* const kAnd = "and";
const char* const kOr = "or";
const char* const kTry = "try";
const char* const kSwitch = "switch";
const char* const kIf = "if";
const char* const kRowConstructor = "row_constructor";

struct ITypedExprHasher {
  size_t operator()(const ITypedExpr* expr) const {
    return expr->hash();
  }
};

struct ITypedExprComparer {
  bool operator()(const ITypedExpr* lhs, const ITypedExpr* rhs) const {
    return *lhs == *rhs;
  }
};

// Map for deduplicating ITypedExpr trees.
using ExprDedupMap = folly::F14FastMap<
    const ITypedExpr*,
    std::shared_ptr<Expr>,
    ITypedExprHasher,
    ITypedExprComparer>;

/// Represents a lexical scope. A top level scope corresponds to a top
/// level Expr and is shared among the Exprs of the ExprSet. Each
/// lambda introduces a new Scope where the 'locals' are the formal
/// parameters of the lambda. References to variables not defined in
/// a lambda's Scope are detected and added as captures to the
/// lambda. Common subexpression elimination can only take place
/// within one Scope.
struct Scope {
  // Names of variables declared in this Scope, i.e. formal parameters of a
  // lambda. Empty for a top level Scope.
  const std::vector<std::string> locals;

  // The enclosing scope, nullptr if top level scope.
  Scope* parent{nullptr};
  ExprSet* exprSet{nullptr};

  // Field names of an enclosing scope referenced from this or an inner scope.
  std::vector<std::string> capture;
  // Corresponds 1:1 to 'capture'.
  std::vector<FieldReference*> captureReferences;
  // Corresponds 1:1 to 'capture'.
  std::vector<const ITypedExpr*> captureFieldAccesses;
  // Deduplicatable ITypedExprs. Only applies within the one scope.
  ExprDedupMap visited;

  Scope(std::vector<std::string>&& _locals, Scope* _parent, ExprSet* _exprSet)
      : locals(_locals), parent(_parent), exprSet(_exprSet) {}

  void addCapture(FieldReference* reference, const ITypedExpr* fieldAccess) {
    capture.emplace_back(reference->field());
    captureReferences.emplace_back(reference);
    captureFieldAccesses.emplace_back(fieldAccess);
  }
};

const std::string* isAndOrOr(const TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    if (call->name() == kAnd || call->name() == kOr) {
      return &call->name();
    }
  }
  return nullptr;
}

bool isCall(const TypedExprPtr& expr, const std::string& name) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    return call->name() == name;
  }
  return false;
}

// Flattens nested ANDs or ORs into a vector of conjuncts
// Examples:
// in: a AND (b AND (c AND d))
// out: [a, b, c, d]
//
// in: (a OR b) OR (c OR d)
// out: [a, b, c, d]
void flattenInput(
    const TypedExprPtr& input,
    const std::string& flattenCall,
    std::vector<TypedExprPtr>& flat) {
  if (isCall(input, flattenCall)) {
    for (auto& child : input->inputs()) {
      flattenInput(child, flattenCall, flat);
    }
  } else {
    flat.emplace_back(input);
  }
}

ExprPtr getAlreadyCompiled(const ITypedExpr* expr, ExprDedupMap* visited) {
  auto iter = visited->find(expr);
  return iter == visited->end() ? nullptr : iter->second;
}

ExprPtr compileExpression(
    const TypedExprPtr& expr,
    Scope* scope,
    const core::QueryConfig& config,
    memory::MemoryPool* pool,
    bool enableConstantFolding);

std::vector<ExprPtr> compileInputs(
    const TypedExprPtr& expr,
    Scope* scope,
    const core::QueryConfig& config,
    memory::MemoryPool* pool,
    bool enableConstantFolding) {
  std::vector<ExprPtr> compiledInputs;
  const std::string* flattenIf = isAndOrOr(expr);
  for (auto& input : expr->inputs()) {
    if (dynamic_cast<const core::InputTypedExpr*>(input.get())) {
      VELOX_CHECK(
          dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get()),
          "An InputReference can only occur under a FieldReference");
    } else {
      if (flattenIf) {
        std::vector<TypedExprPtr> flat;
        flattenInput(input, *flattenIf, flat);
        for (auto& input : flat) {
          compiledInputs.push_back(compileExpression(
              input, scope, config, pool, enableConstantFolding));
        }
      } else {
        compiledInputs.push_back(compileExpression(
            input, scope, config, pool, enableConstantFolding));
      }
    }
  }
  return compiledInputs;
}

std::vector<TypePtr> getTypes(const std::vector<ExprPtr>& exprs) {
  std::vector<TypePtr> types;
  types.reserve(exprs.size());
  for (auto& expr : exprs) {
    types.emplace_back(expr->type());
  }
  return types;
}

ExprPtr getRowConstructorExpr(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage) {
  static auto rowConstructorVectorFunction =
      vectorFunctionFactories().withRLock([](auto& functionMap) {
        auto functionIterator = functionMap.find(exec::kRowConstructor);
        return functionIterator->second.factory(exec::kRowConstructor, {});
      });

  return std::make_shared<Expr>(
      type,
      std::move(compiledChildren),
      rowConstructorVectorFunction,
      "row",
      trackCpuUsage);
}

ExprPtr getSpecialForm(
    const std::string& name,
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage) {
  if (name == kIf || name == kSwitch) {
    bool inputsSupportFlatNoNullsFastPath =
        Expr::allSupportFlatNoNullsFastPath(compiledChildren);
    return std::make_shared<SwitchExpr>(
        type, std::move(compiledChildren), inputsSupportFlatNoNullsFastPath);
  }
  if (name == kCast) {
    VELOX_CHECK_EQ(compiledChildren.size(), 1);
    return std::make_shared<CastExpr>(
        type,
        std::move(compiledChildren[0]),
        trackCpuUsage,
        false /* nullOnFailure */);
  }
  if (name == kAnd) {
    bool inputsSupportFlatNoNullsFastPath =
        Expr::allSupportFlatNoNullsFastPath(compiledChildren);
    return std::make_shared<ConjunctExpr>(
        type,
        std::move(compiledChildren),
        true /* isAnd */,
        inputsSupportFlatNoNullsFastPath);
  }
  if (name == kOr) {
    bool inputsSupportFlatNoNullsFastPath =
        Expr::allSupportFlatNoNullsFastPath(compiledChildren);
    return std::make_shared<ConjunctExpr>(
        type,
        std::move(compiledChildren),
        false /* isAnd */,
        inputsSupportFlatNoNullsFastPath);
  }
  if (name == kTry) {
    VELOX_CHECK_EQ(compiledChildren.size(), 1);
    return std::make_shared<TryExpr>(type, std::move(compiledChildren[0]));
  }
  if (name == kCoalesce) {
    bool inputsSupportFlatNoNullsFastPath =
        Expr::allSupportFlatNoNullsFastPath(compiledChildren);
    return std::make_shared<CoalesceExpr>(
        type, std::move(compiledChildren), inputsSupportFlatNoNullsFastPath);
  }
  if (name == kRowConstructor) {
    return getRowConstructorExpr(
        type, std::move(compiledChildren), trackCpuUsage);
  }
  return nullptr;
}

void captureFieldReference(
    FieldReference* reference,
    const ITypedExpr* fieldAccess,
    Scope* const referenceScope) {
  auto& field = reference->field();
  for (auto* scope = referenceScope; scope->parent; scope = scope->parent) {
    const auto& locals = scope->locals;
    auto& capture = scope->capture;
    if (std::find(locals.begin(), locals.end(), field) != locals.end() ||
        std::find(capture.begin(), capture.end(), field) != capture.end()) {
      // Return if the field is defined or captured in this scope.
      return;
    }
    scope->addCapture(reference, fieldAccess);
  }
}

std::shared_ptr<Expr> compileLambda(
    const core::LambdaTypedExpr* lambda,
    Scope* scope,
    const core::QueryConfig& config,
    memory::MemoryPool* pool,
    bool enableConstantFolding) {
  auto signature = lambda->signature();
  auto parameterNames = signature->names();
  Scope lambdaScope(std::move(parameterNames), scope, scope->exprSet);
  auto body = compileExpression(
      lambda->body(), &lambdaScope, config, pool, enableConstantFolding);

  // The lambda depends on the captures. For a lambda caller to be
  // able to peel off encodings, the captures too must be peelable.
  std::vector<std::shared_ptr<FieldReference>> captureReferences;
  captureReferences.reserve(lambdaScope.capture.size());
  for (auto i = 0; i < lambdaScope.capture.size(); ++i) {
    auto expr = lambdaScope.captureFieldAccesses[i];
    auto reference = getAlreadyCompiled(expr, &scope->visited);
    if (!reference) {
      auto inner = lambdaScope.captureReferences[i];
      reference = std::make_shared<FieldReference>(
          inner->type(), std::vector<ExprPtr>{}, inner->field());
      scope->visited[expr] = reference;
    }
    captureReferences.emplace_back(
        std::static_pointer_cast<FieldReference>(reference));
  }

  auto functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>(signature->children()), body->type());
  return std::make_shared<LambdaExpr>(
      std::move(functionType),
      std::move(signature),
      std::move(captureReferences),
      std::move(body),
      config.exprTrackCpuUsage());
}

ExprPtr tryFoldIfConstant(const ExprPtr& expr, Scope* scope) {
  if (expr->isDeterministic() && !expr->inputs().empty() &&
      scope->exprSet->execCtx()) {
    try {
      // Check that all inputs are literals.
      for (auto& input : expr->inputs()) {
        if (!dynamic_cast<ConstantExpr*>(input.get())) {
          return expr;
        }
      }
      auto rowType = ROW({}, {});
      auto execCtx = scope->exprSet->execCtx();
      auto row = BaseVector::create(rowType, 1, execCtx->pool());
      EvalCtx context(
          execCtx, scope->exprSet, dynamic_cast<RowVector*>(row.get()));
      VectorPtr result;
      SelectivityVector rows(1);
      expr->eval(rows, context, result);
      auto constantVector = BaseVector::wrapInConstant(1, 0, result);

      return std::make_shared<ConstantExpr>(constantVector);
    }
    // Constant folding has a subtle gotcha: if folding a constant expression
    // deterministically throws, we can't throw at expression compilation time
    // yet because we can't guarantee that this expression would actually need
    // to be evaluated.
    //
    // So, here, if folding an expression throws an exception, we just ignore it
    // and leave the expression as-is. If this expression is hit at execution
    // time and needs to be evaluated, it will throw and fail the query anyway.
    // If not, in case this expression is never hit at execution time (for
    // instance, if other arguments are all null in a function with default null
    // behavior), the query won't fail.
    catch (const std::exception&) {
    }
  }
  return expr;
}

/// Returns a vector aligned with exprs vector where elements that correspond to
/// constant expressions are set to constant values of these expressions.
/// Elements that correspond to non-constant expressions are set to null.
std::vector<VectorPtr> getConstantInputs(const std::vector<ExprPtr>& exprs) {
  std::vector<VectorPtr> constants;
  constants.reserve(exprs.size());
  for (auto& expr : exprs) {
    if (auto constantExpr = std::dynamic_pointer_cast<ConstantExpr>(expr)) {
      constants.emplace_back(constantExpr->value());
    } else {
      constants.emplace_back(nullptr);
    }
  }
  return constants;
}

ExprPtr compileExpression(
    const TypedExprPtr& expr,
    Scope* scope,
    const core::QueryConfig& config,
    memory::MemoryPool* pool,
    bool enableConstantFolding) {
  ExprPtr alreadyCompiled = getAlreadyCompiled(expr.get(), &scope->visited);
  if (alreadyCompiled) {
    if (!alreadyCompiled->isMultiplyReferenced()) {
      scope->exprSet->addToReset(alreadyCompiled);
    }
    alreadyCompiled->setMultiplyReferenced();
    return alreadyCompiled;
  }

  const bool trackCpuUsage = config.exprTrackCpuUsage();

  ExprPtr result;
  auto resultType = expr->type();
  auto compiledInputs =
      compileInputs(expr, scope, config, pool, enableConstantFolding);
  auto inputTypes = getTypes(compiledInputs);

  if (dynamic_cast<const core::ConcatTypedExpr*>(expr.get())) {
    result = getRowConstructorExpr(
        resultType, std::move(compiledInputs), trackCpuUsage);
  } else if (auto cast = dynamic_cast<const core::CastTypedExpr*>(expr.get())) {
    VELOX_CHECK(!compiledInputs.empty());
    result = std::make_shared<CastExpr>(
        resultType,
        std::move(compiledInputs[0]),
        trackCpuUsage,
        cast->nullOnFailure());
  } else if (auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
    if (auto specialForm = getSpecialForm(
            call->name(),
            resultType,
            std::move(compiledInputs),
            trackCpuUsage)) {
      result = specialForm;
    } else if (
        auto func = getVectorFunction(
            call->name(), inputTypes, getConstantInputs(compiledInputs))) {
      result = std::make_shared<Expr>(
          resultType,
          std::move(compiledInputs),
          func,
          call->name(),
          trackCpuUsage);
    } else if (
        auto simpleFunctionEntry =
            SimpleFunctions().resolveFunction(call->name(), inputTypes)) {
      auto metadata = simpleFunctionEntry->getMetadata();
      VELOX_USER_CHECK(
          resultType->kindEquals(metadata->returnType()),
          "Found incompatible return types for '{}' ({} vs. {}) "
          "for input types ({}).",
          call->name(),
          metadata->returnType(),
          resultType,
          folly::join(", ", inputTypes));
      auto func = simpleFunctionEntry->createFunction()->createVectorFunction(
          config, getConstantInputs(compiledInputs));
      result = std::make_shared<Expr>(
          resultType,
          std::move(compiledInputs),
          std::move(func),
          call->name(),
          trackCpuUsage);
    } else {
      VELOX_FAIL(
          "Scalar function not registered: {} ({})",
          call->name(),
          folly::join(", ", inputTypes));
    }
  } else if (
      auto access =
          dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
    auto fieldReference = std::make_shared<FieldReference>(
        expr->type(), move(compiledInputs), access->name());
    if (access->isInputColumn()) {
      // We only want to capture references to top level fields, not struct
      // fields.
      captureFieldReference(fieldReference.get(), expr.get(), scope);
    }
    result = fieldReference;
  } else if (auto row = dynamic_cast<const core::InputTypedExpr*>(expr.get())) {
    VELOX_UNSUPPORTED("InputTypedExpr '{}' is not supported", row->toString());
  } else if (
      auto constant =
          dynamic_cast<const core::ConstantTypedExpr*>(expr.get())) {
    if (constant->hasValueVector()) {
      result = std::make_shared<ConstantExpr>(constant->valueVector());
    } else {
      if (constant->value().isNull()) {
        result = std::make_shared<ConstantExpr>(
            BaseVector::createNullConstant(constant->type(), 1, pool));
      } else {
        result = std::make_shared<ConstantExpr>(
            BaseVector::createConstant(constant->value(), 1, pool));
      }
    }
  } else if (
      auto lambda = dynamic_cast<const core::LambdaTypedExpr*>(expr.get())) {
    result = compileLambda(lambda, scope, config, pool, enableConstantFolding);
  } else {
    VELOX_UNSUPPORTED("Unknown typed expression");
  }

  result->computeMetadata();

  auto folded =
      enableConstantFolding ? tryFoldIfConstant(result, scope) : result;
  scope->visited[expr.get()] = folded;
  return folded;
}

} // namespace

std::vector<std::shared_ptr<Expr>> compileExpressions(
    std::vector<TypedExprPtr>&& sources,
    core::ExecCtx* execCtx,
    ExprSet* exprSet,
    bool enableConstantFolding) {
  Scope scope({}, nullptr, exprSet);
  std::vector<std::shared_ptr<Expr>> exprs;
  exprs.reserve(sources.size());

  for (auto& source : sources) {
    exprs.push_back(compileExpression(
        source,
        &scope,
        execCtx->queryCtx()->config(),
        execCtx->pool(),
        enableConstantFolding));
  }
  return exprs;
}

} // namespace facebook::velox::exec
