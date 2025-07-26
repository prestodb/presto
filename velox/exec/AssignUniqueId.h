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
#include "velox/exec/Operator.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

class AssignUniqueId : public Operator {
 public:
  AssignUniqueId(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AssignUniqueIdNode>& planNode,
      int32_t uniqueTaskId,
      std::shared_ptr<std::atomic_int64_t> rowIdPool);

  bool isFilter() const override {
    return true;
  }

  bool preservesOrder() const override {
    return true;
  }

  bool needsInput() const override {
    return input_ == nullptr;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool startDrain() override {
    // No need to drain for assignUniqueId operator.
    return false;
  }

  bool isFinished() override;

 private:
  void generateIdColumn(vector_size_t size);

  void requestRowIds();

  const int64_t kRowIdsPerRequest = 1L << 20;
  const int64_t kMaxRowId = 1L << 40;
  const int64_t kTaskUniqueIdLimit = 1L << 24;

  int64_t uniqueValueMask_;
  int64_t rowIdCounter_;
  int64_t maxRowIdCounterValue_;

  std::shared_ptr<std::atomic_int64_t> rowIdPool_;
};
} // namespace facebook::velox::exec
