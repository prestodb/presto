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

#include "velox/connectors/Connector.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/Exchange.h"
#include "velox/runner/MultiFragmentPlan.h"

/// Base classes for multifragment Velox query execution.
namespace facebook::velox::runner {

/// Iterator for obtaining splits for a scan. One is created for each table
/// scan.
class SplitSource {
 public:
  static constexpr uint32_t kUngroupedGroupId =
      std::numeric_limits<uint32_t>::max();

  /// Result of getSplits. Each split belongs to a group. A nullptr split for
  /// group means that there are on more splits for the group. In ungrouped
  /// execution, the group is kUngroupedGroupId.
  struct SplitAndGroup {
    std::shared_ptr<connector::ConnectorSplit> split;
    uint32_t group{kUngroupedGroupId};
  };

  virtual ~SplitSource() = default;

  /// Returns a set of splits that cover up to 'targetBytes' of data.
  virtual std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) = 0;
};

/// A factory for getting a SplitSource for each TableScan. The splits produced
/// may depend on partition keys, buckets etc mentioned by each tableScan.
class SplitSourceFactory {
 public:
  virtual ~SplitSourceFactory() = default;

  /// Returns a splitSource for one TableScan across all Tasks of
  /// the fragment. The source will be invoked to produce splits for
  /// each individual worker running the scan.
  virtual std::shared_ptr<SplitSource> splitSourceForScan(
      const core::TableScanNode& scan) = 0;
};

/// Base class for executing multifragment Velox queries. One instance
/// of a Runner coordinates the execution of one multifragment
/// query. Different derived classes can support different shuffles
/// and different scheduling either in process or in a cluster. Unless
/// otherwise stated, the member functions are thread safe as long as
/// the caller holds an owning reference to the runner.
class Runner {
 public:
  enum class State { kInitialized, kRunning, kFinished, kError, kCancelled };

  static std::string stateString(Runner::State state);

  virtual ~Runner() = default;

  /// Returns the next batch of results. Returns nullptr when no more results.
  /// Throws any execution time errors. The result is allocated in the pool of
  /// QueryCtx given to the Runner implementation. The caller is responsible for
  /// serializing calls from different threads.
  virtual RowVectorPtr next() = 0;

  /// Returns Task stats for each fragment of the plan. The stats correspond 1:1
  /// to the stages in the MultiFragmentPlan. This may be called at any time.
  /// before waitForCompletion() or abort().
  virtual std::vector<exec::TaskStats> stats() const = 0;

  /// Returns the state of execution.
  virtual State state() const = 0;

  /// Cancels the possibly pending execution. Returns immediately, thus before
  /// the execution is actually finished. Use waitForCompletion() to wait for
  /// all execution resources to be freed. May be called from any thread without
  /// serialization.
  virtual void abort() = 0;

  /// Waits up to 'maxWaitMicros' for all activity of the execution to cease.
  /// This is used in tests to ensure that all pools are empty and unreferenced
  /// before teradown.

  virtual void waitForCompletion(int32_t maxWaitMicros) = 0;
};

} // namespace facebook::velox::runner

template <>
struct fmt::formatter<facebook::velox::runner::Runner::State>
    : formatter<std::string> {
  auto format(facebook::velox::runner::Runner::State state, format_context& ctx)
      const {
    return formatter<std::string>::format(
        facebook::velox::runner::Runner::stateString(state), ctx);
  }
};
