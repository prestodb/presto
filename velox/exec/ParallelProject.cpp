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

#include "velox/exec/ParallelProject.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

ParallelProject::ParallelProject(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const core::ParallelProjectNodePtr& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "ParallelProject"),
      node_(node) {}

namespace {
bool checkAddIdentityProjection(
    const core::TypedExprPtr& projection,
    const RowTypePtr& inputType,
    column_index_t outputChannel,
    std::vector<IdentityProjection>& identityProjections) {
  if (auto field = core::TypedExprs::asFieldAccess(projection)) {
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

void ParallelProject::initialize() {
  Operator::initialize();
  std::vector<core::TypedExprPtr> allExprs;

  const auto& inputType = node_->sources()[0]->outputType();
  auto& exprs = node_->exprs();
  int32_t unitIdx = 0;
  int32_t exprIdx = 0;
  int32_t unitSize = exprs[unitIdx].size();
  work_.emplace_back();
  work_.back().execCtx = std::make_unique<core::ExecCtx>(
      operatorCtx_->pool(), operatorCtx_->driverCtx()->task->queryCtx().get());

  std::vector<core::TypedExprPtr> unitExprs;
  for (column_index_t i = 0; i < node_->exprNames().size(); i++) {
    auto& projection = exprs[unitIdx][exprIdx];
    bool identityProjection = checkAddIdentityProjection(
        projection, inputType, i, identityProjections_);
    if (!identityProjection) {
      unitExprs.push_back(projection);
      work_.back().resultProjections.emplace_back(unitExprs.size() - 1, i);
    } else {
      work_.back().loadOnly.push_back(identityProjections_.back().inputChannel);
    }
    ++exprIdx;
    if (exprIdx == unitSize) {
      // It may be that the only work is loading lazies.
      auto tempExprs =
          makeExprSetFromFlag(std::move(unitExprs), operatorCtx_->execCtx());
      std::shared_ptr<ExprSet> shared(tempExprs.release());
      work_.back().exprSet = std::move(shared);
      ++unitIdx;
      exprIdx = 0;
      if (unitIdx == exprs.size()) {
        break;
      }
      unitSize = exprs[unitIdx].size();
      work_.emplace_back();
      work_.back().execCtx = std::make_unique<core::ExecCtx>(
          operatorCtx_->pool(),
          operatorCtx_->driverCtx()->task->queryCtx().get());
    }
  }

  int32_t outputIdx = node_->exprNames().size();
  auto sourceType = node_->sources()[0]->outputType();
  for (auto& name : node_->noLoadIdentities()) {
    auto idx = sourceType->getChildIdx(name);
    identityProjections_.emplace_back(idx, outputIdx++);
  }
}

void ParallelProject::addInput(RowVectorPtr input) {
  input_ = std::move(input);
  numProcessedInputRows_ = 0;
}

bool ParallelProject::allInputProcessed() {
  if (!input_) {
    return true;
  }
  if (numProcessedInputRows_ == input_->size()) {
    input_ = nullptr;
    return true;
  }
  return false;
}

bool ParallelProject::isFinished() {
  return noMoreInput_ && allInputProcessed();
}

RowVectorPtr ParallelProject::getOutput() {
  if (allInputProcessed()) {
    return nullptr;
  }

  vector_size_t size = input_->size();
  allRows_.resize(size);
  allRows_.setAll();
  std::vector<std::shared_ptr<AsyncSource<WorkResult>>> pending;
  std::vector<VectorPtr> results(outputType_->size());

  for (auto i = 0; i < work_.size(); ++i) {
    pending.push_back(std::make_shared<AsyncSource<WorkResult>>(
        [i, &results, this]() { return doWork(i, results); }));
    auto item = pending.back();
    operatorCtx_->task()->queryCtx()->executor()->add(
        [item]() { item->prepare(); });
  }
  std::exception_ptr error;
  for (auto i = 0; i < pending.size(); ++i) {
    auto result = pending[i]->move();
    stats_.wlock()->getOutputTiming.add(pending[i]->prepareTiming());
    if (!error && result->error) {
      error = result->error;
    }
  }
  if (error) {
    std::rethrow_exception(error);
  }

  for (auto& projection : identityProjections_) {
    results[projection.outputChannel] =
        input_->childAt(projection.inputChannel);
  }
  numProcessedInputRows_ = size;
  input_.reset();
  return std::make_shared<RowVector>(
      operatorCtx_->pool(), outputType_, nullptr, size, std::move(results));
}

std::unique_ptr<ParallelProject::WorkResult> ParallelProject::doWork(
    int32_t workIdx,
    std::vector<VectorPtr>& results) {
  auto& work = work_[workIdx];
  EvalCtx evalCtx(work.execCtx.get(), work.exprSet.get(), input_.get());
  try {
    for (auto channel : work.loadOnly) {
      evalCtx.ensureFieldLoaded(channel, allRows_);
    }

    std::vector<VectorPtr> localResults;
    work.exprSet->eval(
        0, work.exprSet->exprs().size(), true, allRows_, evalCtx, localResults);
    for (auto& projection : work.resultProjections) {
      results[projection.outputChannel] =
          std::move(localResults[projection.inputChannel]);
    }
  } catch (const std::exception&) {
    return std::make_unique<WorkResult>(std::current_exception());
  }
  return std::make_unique<WorkResult>(nullptr);
}

} // namespace facebook::velox::exec
