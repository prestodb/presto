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
      std::shared_ptr<const RowType> signature,
      std::shared_ptr<RowVector> capture,
      std::shared_ptr<Expr> body)
      : signature_(std::move(signature)),
        capture_(std::move(capture)),
        body_(std::move(body)) {}

  bool hasCapture() const override {
    return capture_->childrenSize() > signature_->size();
  }

  void apply(
      const SelectivityVector& rows,
      BufferPtr wrapCapture,
      EvalCtx* context,
      const std::vector<VectorPtr>& args,
      VectorPtr* result) override {
    std::vector<VectorPtr> allVectors = args;
    for (auto index = args.size(); index < capture_->childrenSize(); ++index) {
      auto values = capture_->childAt(index);
      if (wrapCapture) {
        values = BaseVector::wrapInDictionary(
            BufferPtr(nullptr), wrapCapture, rows.end(), values);
      }
      allVectors.push_back(values);
    }
    auto row = std::make_shared<RowVector>(
        context->pool(),
        capture_->type(),
        BufferPtr(nullptr),
        rows.end(),
        std::move(allVectors));
    EvalCtx lambdaCtx(context->execCtx(), context->exprSet(), row.get());
    if (!context->isFinalSelection()) {
      *lambdaCtx.mutableIsFinalSelection() = false;
      *lambdaCtx.mutableFinalSelection() = context->finalSelection();
    }
    body_->eval(rows, lambdaCtx, *result);
  }

 private:
  std::shared_ptr<const RowType> signature_;
  RowVectorPtr capture_;
  std::shared_ptr<Expr> body_;
};

} // namespace

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

void LambdaExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  if (!typeWithCapture_) {
    makeTypeWithCapture(context);
  }
  std::vector<VectorPtr> values(typeWithCapture_->size());
  for (auto i = 0; i < captureChannels_.size(); ++i) {
    assert(!values.empty());
    values[signature_->size() + i] = context.getField(captureChannels_[i]);
  }
  auto capture = std::make_shared<RowVector>(
      context.pool(),
      typeWithCapture_,
      BufferPtr(nullptr),
      rows.end(),
      values,
      0);
  auto callable = std::make_shared<ExprCallable>(signature_, capture, body_);
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
} // namespace facebook::velox::exec
