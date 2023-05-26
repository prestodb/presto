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

#include "velox/exec/JoinBridge.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class NestedLoopJoinBridge : public JoinBridge {
 public:
  void setData(std::vector<VectorPtr> buildVectors);

  std::optional<std::vector<VectorPtr>> dataOrFuture(ContinueFuture* future);

 private:
  std::optional<std::vector<VectorPtr>> buildVectors_;
};

class NestedLoopJoinBuild : public Operator {
 public:
  NestedLoopJoinBuild(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::NestedLoopJoinNode> joinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override {
    dataVectors_.clear();
    Operator::close();
  }

 private:
  std::vector<VectorPtr> dataVectors_;

  // Future for synchronizing with other Drivers of the same pipeline. All build
  // Drivers must be completed before making data available for the probe side.
  ContinueFuture future_{ContinueFuture::makeEmpty()};
};

} // namespace facebook::velox::exec
