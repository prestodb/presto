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

#include "velox/exec/Operator.h"

namespace facebook::velox::cudf_velox {
class CudfLimit : public exec::Operator, public NvtxHelper {
 public:
  CudfLimit(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::LimitNode>& limitNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_ || (noMoreInput_ && input_ == nullptr);
  }

 private:
  int64_t remainingOffset_;
  int64_t remainingLimit_;
  bool finished_{false};
};
} // namespace facebook::velox::cudf_velox
