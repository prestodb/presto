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

#include "velox/common/memory/ArbitrationOperation.h"
#include <mutex>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/time/Timer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {
using namespace facebook::velox::memory;

ArbitrationOperation::ArbitrationOperation(
    ScopedArbitrationParticipant&& participant,
    uint64_t requestBytes,
    uint64_t timeoutMs)
    : requestBytes_(requestBytes),
      timeoutMs_(timeoutMs),
      createTimeMs_(getCurrentTimeMs()),
      participant_(std::move(participant)) {
  VELOX_CHECK_GT(requestBytes_, 0);
}

ArbitrationOperation::~ArbitrationOperation() {
  VELOX_CHECK_NE(
      state_,
      State::kRunning,
      "Unexpected arbitration operation state on destruction");
}

std::string ArbitrationOperation::stateName(State state) {
  switch (state) {
    case State::kInit:
      return "init";
    case State::kWaiting:
      return "waiting";
    case State::kRunning:
      return "running";
    case State::kFinished:
      return "finished";
    default:
      return fmt::format("unknown state: {}", static_cast<int>(state));
  }
}

void ArbitrationOperation::setState(State state) {
  switch (state) {
    case State::kWaiting:
      VELOX_CHECK_EQ(state_, State::kInit);
      break;
    case State::kRunning:
      VELOX_CHECK(this->state_ == State::kWaiting || state_ == State::kInit);
      break;
    case State::kFinished:
      VELOX_CHECK_EQ(this->state_, State::kRunning);
      break;
    default:
      VELOX_UNREACHABLE(
          "Unexpected state transition from {} to {}", state_, state);
      break;
  }
  state_ = state;
}

void ArbitrationOperation::start() {
  VELOX_CHECK_EQ(state_, State::kInit);
  participant_->startArbitration(this);
  setState(ArbitrationOperation::State::kRunning);
  VELOX_CHECK_EQ(startTimeMs_, 0);
  startTimeMs_ = getCurrentTimeMs();
}

void ArbitrationOperation::finish() {
  setState(State::kFinished);
  VELOX_CHECK_EQ(finishTimeMs_, 0);
  finishTimeMs_ = getCurrentTimeMs();
  participant_->finishArbitration(this);
}

bool ArbitrationOperation::aborted() const {
  return participant_->aborted();
}

size_t ArbitrationOperation::executionTimeMs() const {
  if (state_ == State::kFinished) {
    VELOX_CHECK_GE(finishTimeMs_, createTimeMs_);
    return finishTimeMs_ - createTimeMs_;
  } else {
    const auto currentTimeMs = getCurrentTimeMs();
    VELOX_CHECK_GE(currentTimeMs, createTimeMs_);
    return currentTimeMs - createTimeMs_;
  }
}

bool ArbitrationOperation::hasTimeout() const {
  return state_ != State::kFinished && timeoutMs() <= 0;
}

size_t ArbitrationOperation::timeoutMs() const {
  if (state_ == State::kFinished) {
    return 0;
  }
  const auto execTimeMs = executionTimeMs();
  if (execTimeMs >= timeoutMs_) {
    return 0;
  }
  return timeoutMs_ - execTimeMs;
}

void ArbitrationOperation::setGrowTargets() {
  // We shall only set grow targets once after start execution.
  VELOX_CHECK_EQ(state_, State::kRunning);
  VELOX_CHECK(
      maxGrowBytes_ == 0 && minGrowBytes_ == 0,
      "Arbitration operation grow targets have already been set: {}/{}",
      succinctBytes(maxGrowBytes_),
      succinctBytes(minGrowBytes_));
  participant_->getGrowTargets(requestBytes_, maxGrowBytes_, minGrowBytes_);
  VELOX_CHECK_LE(requestBytes_, maxGrowBytes_);
}

ArbitrationOperation::Stats ArbitrationOperation::stats() const {
  VELOX_CHECK_EQ(state_, State::kFinished);
  VELOX_CHECK_NE(startTimeMs_, 0);

  const uint64_t executionTimeMs = this->executionTimeMs();

  VELOX_CHECK_GE(startTimeMs_, createTimeMs_);
  const uint64_t localArbitrationWaitTimeMs = startTimeMs_ - createTimeMs_;
  if (globalArbitrationStartTimeMs_ == 0) {
    return {
        localArbitrationWaitTimeMs,
        finishTimeMs_ - startTimeMs_,
        0,
        executionTimeMs};
  }

  VELOX_CHECK_GE(globalArbitrationStartTimeMs_, startTimeMs_);
  const uint64_t localArbitrationExecTimeMs =
      globalArbitrationStartTimeMs_ - startTimeMs_;
  return {
      localArbitrationWaitTimeMs,
      localArbitrationExecTimeMs,
      finishTimeMs_ - globalArbitrationStartTimeMs_,
      executionTimeMs};
}

std::ostream& operator<<(std::ostream& out, ArbitrationOperation::State state) {
  out << ArbitrationOperation::stateName(state);
  return out;
}
} // namespace facebook::velox::memory
