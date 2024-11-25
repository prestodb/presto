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

#include "velox/exec/OperatorTraceScan.h"

#include "velox/exec/TraceUtil.h"

namespace facebook::velox::exec::trace {

OperatorTraceScan::OperatorTraceScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TraceScanNode>& traceScanNode)
    : SourceOperator(
          driverCtx,
          traceScanNode->outputType(),
          operatorId,
          traceScanNode->id(),
          "OperatorTraceScan") {
  traceReader_ = std::make_unique<OperatorTraceInputReader>(
      getOpTraceDirectory(
          traceScanNode->traceDir(),
          traceScanNode->pipelineId(),
          traceScanNode->driverIds().at(driverCtx->driverId)),
      traceScanNode->outputType(),
      memory::MemoryManager::getInstance()->tracePool());
}

RowVectorPtr OperatorTraceScan::getOutput() {
  RowVectorPtr batch;
  if (traceReader_->read(batch)) {
    return batch;
  }
  finished_ = true;
  return nullptr;
}

bool OperatorTraceScan::isFinished() {
  return finished_;
}

} // namespace facebook::velox::exec::trace
