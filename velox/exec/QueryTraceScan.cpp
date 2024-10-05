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

#include "velox/exec/QueryTraceScan.h"

#include "QueryTraceUtil.h"

namespace facebook::velox::exec::trace {

QueryTraceScan::QueryTraceScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::QueryTraceScanNode>& queryTraceScanNode)
    : SourceOperator(
          driverCtx,
          queryTraceScanNode->outputType(),
          operatorId,
          queryTraceScanNode->id(),
          "QueryReplayScan") {
  const auto dataDir = getDataDir(
      queryTraceScanNode->traceDir(),
      driverCtx->pipelineId,
      driverCtx->driverId);
  traceReader_ = std::make_unique<QueryDataReader>(
      dataDir,
      queryTraceScanNode->outputType(),
      memory::MemoryManager::getInstance()->tracePool());
}

RowVectorPtr QueryTraceScan::getOutput() {
  RowVectorPtr batch;
  if (traceReader_->read(batch)) {
    return batch;
  }
  finished_ = true;
  return nullptr;
}

bool QueryTraceScan::isFinished() {
  return finished_;
}

} // namespace facebook::velox::exec::trace
