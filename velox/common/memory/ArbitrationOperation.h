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
      uint64_t timeoutNs);

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

  /// Returns the remaining execution time for this operation before time out.
  /// If the operation has already finished, this returns zero.
  uint64_t timeoutNs() const;

  /// Returns true if this operation has timed out.
  bool hasTimeout() const;

  /// Returns the execution time of this arbitration operation since creation.
  uint64_t executionTimeNs() const;

  /// Invoked to mark the start of global arbitration. This is used to measure
  /// how much time spent in waiting for global arbitration.
  void recordGlobalArbitrationStartTime() {
    VELOX_CHECK_EQ(globalArbitrationStartTimeNs_, 0);
    VELOX_CHECK_EQ(state_, State::kRunning);
    globalArbitrationStartTimeNs_ = getCurrentTimeNano();
  }

  /// The execution stats of this arbitration operation after completion.
  struct Stats {
    uint64_t localArbitrationWaitTimeNs{0};
    uint64_t localArbitrationExecTimeNs{0};
    uint64_t globalArbitrationWaitTimeNs{0};
    uint64_t executionTimeNs{0};
  };

  /// NOTE: should only called after this arbitration operation finishes.
  Stats stats() const;

 private:
  void setState(State state);

  const uint64_t requestBytes_;
  const uint64_t timeoutNs_;

  // The start time of this arbitration operation.
  const uint64_t createTimeNs_;
  const ScopedArbitrationParticipant participant_;

  State state_{State::kInit};

  uint64_t startTimeNs_{0};
  uint64_t finishTimeNs_{0};

  uint64_t maxGrowBytes_{0};
  uint64_t minGrowBytes_{0};

  // The time that starts global arbitration wait
  uint64_t globalArbitrationStartTimeNs_{};

  friend class ArbitrationParticipant;
};

std::ostream& operator<<(std::ostream& out, ArbitrationOperation::State state);
} // namespace facebook::velox::memory

template <>
struct fmt::formatter<facebook::velox::memory::ArbitrationOperation::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::memory::ArbitrationOperation::State state,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::memory::ArbitrationOperation::stateName(state), ctx);
  }
};
