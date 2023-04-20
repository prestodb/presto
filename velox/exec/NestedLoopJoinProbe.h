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

#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
class NestedLoopJoinProbe : public Operator {
 public:
  NestedLoopJoinProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !input_ && !buildSideEmpty_;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override;

 private:
  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  std::vector<IdentityProjection> buildProjections_;

  std::optional<std::vector<VectorPtr>> buildData_;

  // Index into buildData_ for the build side vector to process on next call to
  // getOutput().
  size_t buildIndex_{0};

  // Input row to process on next call to getOutput().
  vector_size_t probeRow_{0};

  bool buildSideEmpty_{false};
};
} // namespace facebook::velox::exec
