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
#include "velox/exec/FilterProject.h"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"

namespace facebook::velox::exec {
namespace {
bool checkAddIdentityProjection(
    const core::TypedExprPtr& projection,
    const RowTypePtr& inputType,
    column_index_t outputChannel,
    std::vector<IdentityProjection>& identityProjections) {
  if (auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
          projection)) {
    const auto& inputs = field->inputs();
    if (inputs.empty() ||
        (inputs.size() == 1 &&
         dynamic_cast<const core::InputTypedExpr*>(inputs[0].get()))) {
      const auto inputChannel = inputType->getChildIdx(field->name());
      identityProjections.emplace_back(inputChannel, outputChannel);
      return true;
    }
  }

  return false;
}
} // namespace

FilterProject::FilterProject(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::FilterNode>& filter,
    const std::shared_ptr<const core::ProjectNode>& project)
    : Operator(
          driverCtx,
          project ? project->outputType() : filter->outputType(),
          operatorId,
          project ? project->id() : filter->id(),
          "FilterProject"),
      hasFilter_(filter != nullptr) {
  std::vector<core::TypedExprPtr> allExprs;
  if (hasFilter_) {
    allExprs.push_back(filter->filter());
  }
  if (project) {
    auto inputType = project->sources()[0]->outputType();
    for (column_index_t i = 0; i < project->projections().size(); i++) {
      auto projection = project->projections()[i];
      bool identityProjection = checkAddIdentityProjection(
          projection, inputType, i, identityProjections_);
      if (!identityProjection) {
        allExprs.push_back(projection);
        resultProjections_.emplace_back(allExprs.size() - 1, i);
      }
    }
  } else {
    for (column_index_t i = 0; i < outputType_->size(); ++i) {
      identityProjections_.emplace_back(i, i);
    }
    isIdentityProjection_ = true;
  }
  numExprs_ = allExprs.size();
  exprs_ = makeExprSetFromFlag(std::move(allExprs), operatorCtx_->execCtx());

  if (numExprs_ > 0 && !identityProjections_.empty()) {
    auto inputType = project ? project->sources()[0]->outputType()
                             : filter->sources()[0]->outputType();
    std::unordered_set<uint32_t> distinctFieldIndices;
    for (auto field : exprs_->distinctFields()) {
      auto fieldIndex = inputType->getChildIdx(field->name());
      distinctFieldIndices.insert(fieldIndex);
    }
    for (auto identityField : identityProjections_) {
      if (distinctFieldIndices.find(identityField.inputChannel) !=
          distinctFieldIndices.end()) {
        multiplyReferencedFieldIndices_.push_back(identityField.inputChannel);
      }
    }
  }
}

void FilterProject::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  numProcessedInputRows_ = 0;
  if (!resultProjections_.empty()) {
    results_.resize(resultProjections_.back().inputChannel + 1);
    for (auto& result : results_) {
      if (result && result.unique() && result->isFlatEncoding()) {
        BaseVector::prepareForReuse(result, 0);
      } else {
        result.reset();
      }
    }
  }
}

bool FilterProject::allInputProcessed() {
  if (!input_) {
    return true;
  }
  if (numProcessedInputRows_ == input_->size()) {
    input_ = nullptr;
    return true;
  }
  return false;
}

bool FilterProject::isFinished() {
  return noMoreInput_ && allInputProcessed();
}

RowVectorPtr FilterProject::getOutput() {
  if (allInputProcessed()) {
    return nullptr;
  }

  vector_size_t size = input_->size();
  LocalSelectivityVector localRows(*operatorCtx_->execCtx(), size);
  auto* rows = localRows.get();
  VELOX_DCHECK_NOT_NULL(rows)
  rows->setAll();
  EvalCtx evalCtx(operatorCtx_->execCtx(), exprs_.get(), input_.get());

  // Pre-load lazy vectors which are referenced by both expressions and identity
  // projections.
  for (auto fieldIdx : multiplyReferencedFieldIndices_) {
    evalCtx.ensureFieldLoaded(fieldIdx, *rows);
  }

  if (!hasFilter_) {
    numProcessedInputRows_ = size;
    VELOX_CHECK(!isIdentityProjection_);
    project(*rows, evalCtx);

    if (results_.size() > 0) {
      auto outCol = results_[0];
      if (outCol && outCol->isCodegenOutput()) {
        // codegen can output different size when it merged filter + projection
        size = outCol->size();
        if (size == 0) { // all filtered out
          return nullptr;
        }
      }
    }

    return fillOutput(size, nullptr);
  }

  // evaluate filter
  auto numOut = filter(evalCtx, *rows);
  numProcessedInputRows_ = size;
  if (numOut == 0) { // no rows passed the filer
    input_ = nullptr;
    return nullptr;
  }

  bool allRowsSelected = (numOut == size);

  // evaluate projections (if present)
  if (!isIdentityProjection_) {
    if (!allRowsSelected) {
      rows->setFromBits(filterEvalCtx_.selectedBits->as<uint64_t>(), size);
    }
    project(*rows, evalCtx);
  }

  return fillOutput(
      numOut, allRowsSelected ? nullptr : filterEvalCtx_.selectedIndices);
}

void FilterProject::project(const SelectivityVector& rows, EvalCtx& evalCtx) {
  exprs_->eval(
      hasFilter_ ? 1 : 0, numExprs_, !hasFilter_, rows, evalCtx, results_);
}

vector_size_t FilterProject::filter(
    EvalCtx& evalCtx,
    const SelectivityVector& allRows) {
  exprs_->eval(0, 1, true, allRows, evalCtx, results_);
  return processFilterResults(results_[0], allRows, filterEvalCtx_, pool());
}
} // namespace facebook::velox::exec
