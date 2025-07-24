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

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

class ParallelProject : public Operator {
 public:
  ParallelProject(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const core::ParallelProjectNodePtr& node);

  bool isFilter() const override {
    return false;
  }

  bool preservesOrder() const override {
    return true;
  }

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    for (auto& work : work_) {
      if (work.exprSet) {
        work.exprSet->clear();
      }
    }
  }

  void initialize() override;

 private:
  struct WorkUnit {
    // Maps from result channel of exprSet to channel in the node's output type.
    std::vector<IdentityProjection> resultProjections;
    // Positions in input which are to be loaded by this group.
    std::vector<column_index_t> loadOnly;
    std::unique_ptr<core::ExecCtx> execCtx;
    std::shared_ptr<ExprSet> exprSet;
  };

  struct WorkResult {
    WorkResult(std::exception_ptr e) : error(std::move(e)) {}
    std::exception_ptr error;
  };

  // Tests if 'numProcessedRows_' equals to the length of input_ and clears
  // outstanding references to input_ if done. Returns true if getOutput
  // should return nullptr.
  bool allInputProcessed();

  std::unique_ptr<WorkResult> doWork(
      int32_t workIdx,
      std::vector<VectorPtr>& result);

  // Cached ParallelProject node for lazy initialization. After
  // initialization, they will be reset, and initialized_ will be set to true.
  const core::ParallelProjectNodePtr node_;

  bool initialized_{false};

  std::vector<WorkUnit> work_;
  SelectivityVector allRows_;
  int32_t numProcessedInputRows_{0};
};

} // namespace facebook::velox::exec
