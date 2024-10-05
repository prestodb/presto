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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/exec/QueryDataReader.h"

namespace facebook::velox::exec::trace {
/// This is a scan operator for query replay. It uses traced data from a
/// specific directory path, which is
/// $traceRoot/$taskId/$nodeId/$pipelineId/$driverId.
///
/// A plan node can be split into multiple pipelines, and each pipeline can be
/// divided into multiple operators. Each operator corresponds to a driver,
/// which is a thread of execution. Pipeline IDs and driver IDs are sequential
/// numbers starting from zero.
///
/// For a single plan node, there can be multiple traced data files. To find the
/// right input data file for replaying, we need to use both the pipeline ID and
/// driver ID.
///
/// The trace data directory up to the $nodeId, which is $root/$taskId/$nodeId.
/// It can be found from the QueryReplayScanNode. However the pipeline ID and
/// driver ID are only known during operator creation, so we need to figure out
/// the input traced data file and the output type dynamically.
class QueryTraceScan final : public SourceOperator {
 public:
  QueryTraceScan(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::QueryTraceScanNode>&
          queryTraceScanNode);

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  std::unique_ptr<QueryDataReader> traceReader_;
  bool finished_{false};
};

} // namespace facebook::velox::exec::trace
