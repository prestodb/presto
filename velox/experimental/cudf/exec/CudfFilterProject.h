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

#include "velox/experimental/cudf/exec/ExpressionEvaluator.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/Operator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::cudf_velox {

// TODO: Does not support Filter yet.
class CudfFilterProject : public exec::Operator, public NvtxHelper {
 public:
  CudfFilterProject(
      int32_t operatorId,
      velox::exec::DriverCtx* driverCtx,
      const velox::exec::FilterProject::Export& info,
      std::vector<velox::exec::IdentityProjection> identityProjections,
      const std::shared_ptr<const core::FilterNode>& filter,
      const std::shared_ptr<const core::ProjectNode>& project);

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  void filter(
      std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
      rmm::cuda_stream_view stream);

  std::vector<std::unique_ptr<cudf::column>> project(
      std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
      rmm::cuda_stream_view stream);

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    projectEvaluator_.close();
    filterEvaluator_.close();
  }

 private:
  bool allInputProcessed();
  // If true exprs_[0] is a filter and the other expressions are projections
  const bool hasFilter_{false};
  // Cached filter and project node for lazy initialization. After
  // initialization, they will be reset, and initialized_ will be set to true.
  std::shared_ptr<const core::ProjectNode> project_;
  std::shared_ptr<const core::FilterNode> filter_;
  ExpressionEvaluator projectEvaluator_;
  ExpressionEvaluator filterEvaluator_;

  std::vector<velox::exec::IdentityProjection> resultProjections_;
  std::vector<velox::exec::IdentityProjection> identityProjections_;
};

} // namespace facebook::velox::cudf_velox
