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
#include "velox/expression/LambdaExpr.h"

#include "velox/expression/FieldReference.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::exec {

namespace {

// Represents an interpreted lambda expression. 'signature' describes
// the parameters passed by the caller. 'capture' is a row with a
// leading nullptr for each element in 'signature' followed by the
// vectors for the captures from the lambda's definition scope.
class ExprCallable : public Callable {
 public:
  ExprCallable(
      RowTypePtr signature,
      RowVectorPtr capture,
      std::shared_ptr<Expr> body,
      std::vector<std::shared_ptr<Expr>> sharedExprsToReset)
      : signature_(std::move(signature)),
        capture_(std::move(capture)),
        body_(std::move(body)),
        sharedExprsToReset_(std::move(sharedExprsToReset)) {}

  bool hasCapture() const override {
    return capture_->childrenSize() > signature_->size();
  }

  void apply(
      const SelectivityVector& rows,
      const SelectivityVector* validRowsInReusedResult,
      const BufferPtr& wrapCapture,
      EvalCtx* context,
      const std::vector<VectorPtr>& args,
      const BufferPtr& elementToTopLevelRows,
      VectorPtr* result) override {
    auto row = createRowVector(context, wrapCapture, args, rows.end());
    EvalCtx lambdaCtx = createLambdaCtx(context, row, validRowsInReusedResult);
    ScopedVarSetter throwOnError(
        lambdaCtx.mutableThrowOnError(), context->throwOnError());
    resetSharedExprs();
    body_->eval(rows, lambdaCtx, *result);
    transformErrorVector(lambdaCtx, context, rows, elementToTopLevelRows);
  }

  void applyNoThrow(
      const SelectivityVector& rows,
      const SelectivityVector* validRowsInReusedResult,
      const BufferPtr& wrapCapture,
      EvalCtx* context,
      const std::vector<VectorPtr>& args,
      ErrorVectorPtr& elementErrors,
      VectorPtr* result) override {
    auto row = createRowVector(context, wrapCapture, args, rows.end());
    EvalCtx lambdaCtx = createLambdaCtx(context, row, validRowsInReusedResult);
    ScopedVarSetter throwOnError(lambdaCtx.mutableThrowOnError(), false);
    resetSharedExprs();
    body_->eval(rows, lambdaCtx, *result);
    lambdaCtx.swapErrors(elementErrors);
  }

 private:
  void resetSharedExprs() {
    for (auto& expr : sharedExprsToReset_) {
      expr->reset();
    }
  }

  EvalCtx createLambdaCtx(
      EvalCtx* context,
      std::shared_ptr<RowVector>& row,
      const SelectivityVector* validRowsInReusedResult) {
    EvalCtx lambdaCtx{context->execCtx(), context->exprSet(), row.get()};
    if (validRowsInReusedResult != nullptr) {
      *lambdaCtx.mutableIsFinalSelection() = false;
      *lambdaCtx.mutableFinalSelection() = validRowsInReusedResult;
    }
    return lambdaCtx;
  }

  // Transform error vector to map element rows back to top-level rows.
  void transformErrorVector(
      EvalCtx& lambdaCtx,
      EvalCtx* context,
      const SelectivityVector& rows,
      const BufferPtr& elementToTopLevelRows) {
    // Transform error vector to map element rows back to top-level rows.
    if (elementToTopLevelRows) {
      lambdaCtx.addElementErrorsToTopLevel(
          rows, elementToTopLevelRows, *context->errorsPtr());
    } else {
      lambdaCtx.addErrors(rows, *lambdaCtx.errorsPtr(), *context->errorsPtr());
    }
  }

  std::shared_ptr<RowVector> createRowVector(
      EvalCtx* context,
      const BufferPtr& wrapCapture,
      const std::vector<VectorPtr>& args,
      vector_size_t size) {
    VELOX_CHECK_EQ(signature_->size(), args.size())
    std::vector<VectorPtr> allVectors = args;
    for (auto index = args.size(); index < capture_->childrenSize(); ++index) {
      auto values = capture_->childAt(index);
      VELOX_DCHECK(!isLazyNotLoaded(*values));
      if (wrapCapture) {
        values = BaseVector::wrapInDictionary(
            BufferPtr(nullptr), wrapCapture, size, values);
      }
      allVectors.push_back(values);
    }

    auto row = std::make_shared<RowVector>(
        context->pool(),
        capture_->type(),
        BufferPtr(nullptr),
        size,
        std::move(allVectors));
    return row;
  }

  RowTypePtr signature_;
  RowVectorPtr capture_;
  std::shared_ptr<Expr> body_;
  // List of Shared Exprs that are decendants of 'body_' for which reset() needs
  // to be called before calling `body_->eval()`.
  std::vector<std::shared_ptr<Expr>> sharedExprsToReset_;
};

void extractSharedExpressions(
    const ExprPtr& expr,
    std::unordered_set<ExprPtr>& shared) {
  for (const auto& input : expr->inputs()) {
    if (input->isMultiplyReferenced()) {
      shared.insert(input);
      continue;
    }
    extractSharedExpressions(input, shared);
  }
}

} // namespace

LambdaExpr::LambdaExpr(
    TypePtr type,
    RowTypePtr&& signature,
    std::vector<std::shared_ptr<FieldReference>>&& capture,
    std::shared_ptr<Expr>&& body,
    bool trackCpuUsage)
    : SpecialForm(
          std::move(type),
          std::vector<std::shared_ptr<Expr>>(),
          "lambda",
          false /* supportsFlatNoNullsFastPath */,
          trackCpuUsage),
      signature_(std::move(signature)),
      body_(std::move(body)),
      capture_(std::move(capture)) {
  std::unordered_set<ExprPtr> shared;
  extractSharedExpressions(body_, shared);
  for (auto& expr : shared) {
    sharedExprsToReset_.push_back(expr);
  }
}

void LambdaExpr::computeDistinctFields() {
  SpecialForm::computeDistinctFields();
  std::vector<FieldReference*> capturedFields;
  for (auto& field : capture_) {
    capturedFields.push_back(field.get());
  }
  mergeFields(distinctFields_, multiplyReferencedFields_, capturedFields);
}

std::string LambdaExpr::toString(bool recursive) const {
  if (!recursive) {
    return name_;
  }

  std::string inputs;
  for (int i = 0; i < signature_->size(); ++i) {
    inputs.append(signature_->nameOf(i));
    if (!inputs.empty()) {
      inputs.append(", ");
    }
  }

  for (const auto& field : capture_) {
    inputs.append(field->field());
    if (!inputs.empty()) {
      inputs.append(", ");
    }
  }
  inputs.pop_back();
  inputs.pop_back();

  return fmt::format("({}) -> {}", inputs, body_->toString());
}

std::string LambdaExpr::toSql(std::vector<VectorPtr>* complexConstants) const {
  std::ostringstream out;
  out << "(";
  // Inputs.
  for (auto i = 0; i < signature_->size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signature_->nameOf(i);
  }
  out << ") -> " << body_->toSql(complexConstants);

  return out.str();
}

void LambdaExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!typeWithCapture_) {
    makeTypeWithCapture(context);
  }
  std::vector<VectorPtr> values(typeWithCapture_->size());
  for (auto i = 0; i < captureChannels_.size(); ++i) {
    assert(!values.empty());
    // Ensure all captured fields are loaded.
    const auto& rowsToLoad =
        context.isFinalSelection() ? rows : *context.finalSelection();
    context.ensureFieldLoaded(captureChannels_[i], rowsToLoad);
    values[signature_->size() + i] = context.getField(captureChannels_[i]);
  }
  auto capture = std::make_shared<RowVector>(
      context.pool(),
      typeWithCapture_,
      BufferPtr(nullptr),
      rows.end(),
      values,
      0);
  auto callable = std::make_shared<ExprCallable>(
      signature_, capture, body_, sharedExprsToReset_);
  std::shared_ptr<FunctionVector> functions;
  if (!result) {
    functions = std::make_shared<FunctionVector>(context.pool(), type_);
    result = functions;
  } else {
    VELOX_CHECK(result->encoding() == VectorEncoding::Simple::FUNCTION);
    functions = std::static_pointer_cast<FunctionVector>(result);
  }
  functions->addFunction(callable, rows);
}

void LambdaExpr::makeTypeWithCapture(EvalCtx& context) {
  // On first use, compose the type of parameters + capture and set
  // the indices of captures in the context row.
  if (capture_.empty()) {
    typeWithCapture_ = signature_;
  } else {
    auto& contextType = context.row()->type()->as<TypeKind::ROW>();
    auto parameterNames = signature_->names();
    auto parameterTypes = signature_->children();
    for (auto& reference : capture_) {
      auto& name = reference->field();
      auto channel = contextType.getChildIdx(name);
      captureChannels_.push_back(channel);
      parameterNames.push_back(name);
      parameterTypes.push_back(contextType.childAt(channel));
    }
    typeWithCapture_ =
        ROW(std::move(parameterNames), std::move(parameterTypes));
  }
}

void LambdaExpr::extractSubfieldsImpl(
    folly::F14FastMap<std::string, int32_t>* shadowedNames,
    std::vector<common::Subfield>* subfields) const {
  for (auto& name : signature_->names()) {
    (*shadowedNames)[name]++;
  }
  body_->extractSubfieldsImpl(shadowedNames, subfields);
  for (auto& name : signature_->names()) {
    auto it = shadowedNames->find(name);
    if (--it->second == 0) {
      shadowedNames->erase(it);
    }
  }
}

} // namespace facebook::velox::exec
