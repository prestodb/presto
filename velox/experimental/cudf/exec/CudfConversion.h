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
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/vector/ComplexVector.h"

#include <deque>
#include <memory>
#include <vector>

namespace facebook::velox::cudf_velox {

class CudfFromVelox : public exec::Operator, public NvtxHelper {
 public:
  static constexpr const char* kGpuBatchSizeRows =
      "velox.cudf.gpu_batch_size_rows";

  CudfFromVelox(
      int32_t operatorId,
      RowTypePtr outputType,
      exec::DriverCtx* driverCtx,
      std::string planNodeId);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void close() override;

 private:
  std::vector<RowVectorPtr> inputs_;
  std::size_t currentOutputSize_ = 0;
  bool finished_ = false;
};

class CudfToVelox : public exec::Operator, public NvtxHelper {
 public:
  static constexpr const char* kPassthroughMode =
      "velox.cudf.to_velox.passthrough_mode";

  CudfToVelox(
      int32_t operatorId,
      RowTypePtr outputType,
      exec::DriverCtx* driverCtx,
      std::string planNodeId);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

  void close() override;

 private:
  bool isPassthroughMode() const;
  std::optional<uint64_t> averageRowSize();
  std::optional<uint64_t> averageRowSize_;
  std::deque<CudfVectorPtr> inputs_;
  bool finished_ = false;
};

} // namespace facebook::velox::cudf_velox
