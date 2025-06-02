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

#include "velox/experimental/cudf/exec/NvtxHelper.h"

#include "velox/exec/LocalPartition.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::cudf_velox {

class CudfLocalPartition : public exec::Operator, public NvtxHelper {
 public:
  CudfLocalPartition(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::LocalPartitionNode>& planNode);

  std::string toString() const override {
    return fmt::format("CudfLocalPartition({})", numPartitions_);
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  /// Always true but the caller will check isBlocked before adding input, hence
  /// the blocked state does not accumulate input.
  bool needsInput() const override {
    return true;
  }

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  void noMoreInput() override;

  bool isFinished() override;

 protected:
  const std::vector<std::shared_ptr<exec::LocalExchangeQueue>> queues_;
  const size_t numPartitions_;

  std::vector<exec::BlockingReason> blockingReasons_;
  std::vector<ContinueFuture> futures_;

  std::vector<column_index_t> partitionKeyIndices_;
};

} // namespace facebook::velox::cudf_velox
