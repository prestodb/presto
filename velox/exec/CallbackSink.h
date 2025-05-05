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
#include "velox/exec/OperatorUtils.h"

namespace facebook::velox::exec {

class CallbackSink : public Operator {
 public:
  CallbackSink(
      int32_t operatorId,
      DriverCtx* driverCtx,
      Consumer consumeCb,
      std::function<BlockingReason(ContinueFuture*)> startedCb = nullptr)
      : Operator(driverCtx, nullptr, operatorId, "N/A", "CallbackSink"),
        startedCb_{std::move(startedCb)},
        consumeCb_{std::move(consumeCb)} {}

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool startDrain() override;

  bool needsInput() const override {
    return consumeCb_ != nullptr;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  void close() override;

  ContinueFuture future_;
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  std::function<BlockingReason(ContinueFuture*)> startedCb_;
  Consumer consumeCb_;
};

} // namespace facebook::velox::exec
