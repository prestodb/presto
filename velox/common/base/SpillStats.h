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
#include <stdint.h>
#include <string.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include "velox/common/compression/Compression.h"

namespace facebook::velox::common {
/// Provides the fine-grained spill execution stats.
struct SpillStats {
  /// The number of times that spilling runs on an operator.
  uint64_t spillRuns{0};
  /// The number of bytes in memory to spill
  uint64_t spilledInputBytes{0};
  /// The number of bytes spilled to disks.
  ///
  /// NOTE: if compression is enabled, this counts the compressed bytes.
  uint64_t spilledBytes{0};
  /// The number of spilled rows.
  uint64_t spilledRows{0};
  /// NOTE: when we sum up the stats from a group of spill operators, it is
  /// the total number of spilled partitions X number of operators.
  uint32_t spilledPartitions{0};
  /// The number of spilled files.
  uint64_t spilledFiles{0};
  /// The time spent on filling rows for spilling.
  uint64_t spillFillTimeUs{0};
  /// The time spent on sorting rows for spilling.
  uint64_t spillSortTimeUs{0};
  /// The time spent on serializing rows for spilling.
  uint64_t spillSerializationTimeUs{0};
  /// The number of spill writer flushes, equivalent to number of write calls to
  /// underlying filesystem.
  uint64_t spillWrites{0};
  // TODO(jtan6): Remove after presto native lands
  uint64_t spillDiskWrites{0};
  /// The time spent on copy out serialized rows for disk write. If compression
  /// is enabled, this includes the compression time.
  uint64_t spillFlushTimeUs{0};
  /// The time spent on writing spilled rows to disk.
  uint64_t spillWriteTimeUs{0};
  /// The number of times that an hash build operator exceeds the max spill
  /// limit.
  uint64_t spillMaxLevelExceededCount{0};

  SpillStats(
      uint64_t _spillRuns,
      uint64_t _spilledInputBytes,
      uint64_t _spilledBytes,
      uint64_t _spilledRows,
      uint32_t _spilledPartitions,
      uint64_t _spilledFiles,
      uint64_t _spillFillTimeUs,
      uint64_t _spillSortTimeUs,
      uint64_t _spillSerializationTimeUs,
      uint64_t _spillWrites,
      uint64_t _spillFlushTimeUs,
      uint64_t _spillWriteTimeUs,
      uint64_t _spillMaxLevelExceededCount);

  SpillStats() = default;

  bool empty() const {
    return spilledBytes == 0;
  }

  SpillStats& operator+=(const SpillStats& other);
  SpillStats operator-(const SpillStats& other) const;
  bool operator==(const SpillStats& other) const;
  bool operator!=(const SpillStats& other) const {
    return !(*this == other);
  }
  bool operator>(const SpillStats& other) const;
  bool operator<(const SpillStats& other) const;
  bool operator>=(const SpillStats& other) const;
  bool operator<=(const SpillStats& other) const;

  void reset();

  std::string toString() const;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& o,
    const common::SpillStats& stats) {
  return o << stats.toString();
}

/// The utilities to update the process wide spilling stats.
/// Updates the number of spill runs.
void updateGlobalSpillRunStats(uint64_t numRuns);

/// Updates the stats of new append spilled rows including the number of spilled
/// rows and the serializaion time.
void updateGlobalSpillAppendStats(
    uint64_t numRows,
    uint64_t serializaionTimeUs);

/// Increments the number of spilled partitions.
void incrementGlobalSpilledPartitionStats();

/// Updates the time spent on filling rows to spill.
void updateGlobalSpillFillTime(uint64_t timeUs);

/// Updates the time spent on sorting rows to spill.
void updateGlobalSpillSortTime(uint64_t timeUs);

/// Updates the stats for disk write including the number of disk writes,
/// the written bytes, the time spent on copying out (compression) for disk
/// writes, the time spent on disk writes.
void updateGlobalSpillWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeUs,
    uint64_t writeTimeUs);

/// Increment the spill memory bytes.
void updateGlobalSpillMemoryBytes(uint64_t spilledInputBytes);

/// Increments the spilled files by one.
void incrementGlobalSpilledFiles();

/// Increments the exceeded max spill level count.
void updateGlobalMaxSpillLevelExceededCount(
    uint64_t maxSpillLevelExceededCount);

/// Gets the cumulative global spill stats.
SpillStats globalSpillStats();
} // namespace facebook::velox::common
