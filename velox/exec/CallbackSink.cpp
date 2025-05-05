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

#include "velox/exec/CallbackSink.h"

namespace facebook::velox::exec {
void CallbackSink::addInput(RowVectorPtr input) {
  loadColumns(input, *operatorCtx_->execCtx());
  blockingReason_ = consumeCb_(std::move(input), false, &future_);
}

RowVectorPtr CallbackSink::getOutput() {
  if (FOLLY_LIKELY(!isDraining())) {
    return nullptr;
  }
  const auto blockingReason = consumeCb_(nullptr, /*drained=*/true, &future_);
  VELOX_CHECK_EQ(blockingReason, BlockingReason::kNotBlocked);
  finishDrain();
  return nullptr;
}

bool CallbackSink::startDrain() {
  VELOX_CHECK(isDraining());
  if (consumeCb_ == nullptr) {
    return false;
  }
  return true;
}

void CallbackSink::noMoreInput() {
  Operator::noMoreInput();
  close();
}

BlockingReason CallbackSink::isBlocked(ContinueFuture* future) {
  if (startedCb_ != nullptr) {
    blockingReason_ = startedCb_(&future_);
    startedCb_ = nullptr;
  }
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = BlockingReason::kNotBlocked;
    return BlockingReason::kWaitForConsumer;
  }
  return BlockingReason::kNotBlocked;
}

void CallbackSink::close() {
  if (consumeCb_ == nullptr) {
    return;
  }
  consumeCb_(nullptr, false, nullptr);
  consumeCb_ = nullptr;
  Operator::close();
}
} // namespace facebook::velox::exec
