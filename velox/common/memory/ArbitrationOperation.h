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

#include "velox/common/base/Counters.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/ArbitrationParticipant.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {

/// Manages the execution of a memory arbitration request within the arbitrator.
class ArbitrationOperation {
 public:
  ArbitrationOperation(
      ScopedArbitrationParticipant&& pool,
      uint64_t requestBytes,
      uint64_t timeoutMs);

  ~ArbitrationOperation();

  enum class State {
    kInit,
    kWaiting,
    kRunning,
    kFinished,
  };

  State state() const {
    return state_;
  }

  static std::string stateName(State state);

  /// Returns the corresponding arbitration participant.
  const ScopedArbitrationParticipant& participant() {
    return participant_;
  }

  /// Invoked to start arbitration execution on the arbitration participant. The
  /// latter ensures the serialized execution of arbitration operations from the
  /// same query with one at a time. So this method blocks until all the prior
  /// arbitration operations finish.
  void start();

  /// Invoked to finish arbitration execution on the arbitration participant. It
  /// also resumes the next waiting arbitration operation to execute if there is
  /// one.
  void finish();

  /// Returns true if the corresponding arbitration participant has been
  /// aborted.
  bool aborted() const;

  /// Invoked to set the grow targets for this arbitration operation based on
  /// the request size.
  ///
  /// NOTE: this should be called once after the arbitration operation is
  /// started.
  void setGrowTargets();

  uint64_t requestBytes() const {
    return requestBytes_;
  }

  /// Returns the max grow bytes for this arbitration operation which could be
  /// larger than the request bytes for exponential growth.
  uint64_t maxGrowBytes() const {
    return maxGrowBytes_;
  }

  /// Returns the min grow bytes for this arbitration operation to ensure the
  /// arbitration participant has the minimum amount of memory capacity. The
  /// arbitrator might allocate memory from the reserved memory capacity pool
  /// for the min grow bytes.
  uint64_t minGrowBytes() const {
    return minGrowBytes_;
  }

  /// Returns the allocated bytes by this arbitration operation.
  uint64_t& allocatedBytes() {
    return allocatedBytes_;
  }

  /// Returns the remaining execution time for this operation before time out.
  /// If the operation has already finished, this returns zero.
  size_t timeoutMs() const;

  /// Returns true if this operation has timed out.
  bool hasTimeout() const;

  /// Returns the execution time of this arbitration operation since creation.
  size_t executionTimeMs() const;

  /// Getters/Setters of the wait time in (local) arbitration paritcipant wait
  /// queue or (global) arbitrator request wait queue.
  void setLocalArbitrationWaitTimeUs(uint64_t waitTimeUs) {
    VELOX_CHECK_EQ(localArbitrationWaitTimeUs_, 0);
    VELOX_CHECK_EQ(state_, State::kWaiting);
    localArbitrationWaitTimeUs_ = waitTimeUs;
  }

  uint64_t localArbitrationWaitTimeUs() const {
    return localArbitrationWaitTimeUs_;
  }

  void setGlobalArbitrationWaitTimeUs(uint64_t waitTimeUs) {
    VELOX_CHECK_EQ(globalArbitrationWaitTimeUs_, 0);
    VELOX_CHECK_EQ(state_, State::kRunning);
    globalArbitrationWaitTimeUs_ = waitTimeUs;
  }

  uint64_t globalArbitrationWaitTimeUs() const {
    return globalArbitrationWaitTimeUs_;
  }

 private:
  void setState(State state);

  const uint64_t requestBytes_;
  const uint64_t timeoutMs_;

  // The start time of this arbitration operation.
  const uint64_t createTimeMs_;
  const ScopedArbitrationParticipant participant_;

  State state_{State::kInit};

  uint64_t finishTimeMs_{0};

  uint64_t maxGrowBytes_{0};
  uint64_t minGrowBytes_{0};

  // The actual bytes allocated from arbitrator based on the request bytes and
  // grow targets. It is either zero on failure or between 'requestBytes_' and
  // 'maxGrowBytes_' on success.
  uint64_t allocatedBytes_{0};

  // The time that waits in local arbitration queue.
  uint64_t localArbitrationWaitTimeUs_{0};

  // The time that waits for global arbitration queue.
  uint64_t globalArbitrationWaitTimeUs_{0};

  friend class ArbitrationParticipant;
};

std::ostream& operator<<(std::ostream& out, ArbitrationOperation::State state);
} // namespace facebook::velox::memory

template <>
struct fmt::formatter<facebook::velox::memory::ArbitrationOperation::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::memory::ArbitrationOperation::State state,
      format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::memory::ArbitrationOperation::stateName(state), ctx);
  }
};
