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

#define VELOX_SPILL_LIMIT_EXCEEDED(errorMessage)                    \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kSpillLimitExceeded.c_str(),   \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

/// Defining type for a callback function that returns the spill directory path.
/// Implementations can use it to ensure the path exists before returning.
using GetSpillDirectoryPathCB = std::function<const std::string&()>;

/// The callback used to update the aggregated spill bytes of a query. If the
/// query spill limit is set, the callback throws if the aggregated spilled
/// bytes exceed the set limit.
using UpdateAndCheckSpillLimitCB = std::function<void(uint64_t)>;

/// Specifies the config for spilling.
struct SpillConfig {
  SpillConfig() = default;
  SpillConfig(
      GetSpillDirectoryPathCB _getSpillDirPathCb,
      UpdateAndCheckSpillLimitCB _updateAndCheckSpillLimitCb,
      std::string _filePath,
      uint64_t _maxFileSize,
      uint64_t _writeBufferSize,
      uint64_t _minSpillRunSize,
      folly::Executor* _executor,
      int32_t _minSpillableReservationPct,
      int32_t _spillableReservationGrowthPct,
      uint8_t _startPartitionBit,
      uint8_t _numPartitionBits,
      int32_t _maxSpillLevel,
      uint64_t _maxSpillRunRows,
      uint64_t _writerFlushThresholdSize,
      const std::string& _compressionKind,
      const std::string& _fileCreateConfig = {});

  /// Returns the spilling level with given 'startBitOffset' and
  /// 'numPartitionBits'.
  ///
  /// NOTE: we advance (or right shift) the partition bit offset when goes to
  /// the next level of recursive spilling.
  int32_t spillLevel(uint8_t startBitOffset) const;

  /// Checks if the given 'startBitOffset' and 'numPartitionBits' has exceeded
  /// the max hash join spill limit.
  bool exceedSpillLevelLimit(uint8_t startBitOffset) const;

  /// A callback function that returns the spill directory path. Implementations
  /// can use it to ensure the path exists before returning.
  GetSpillDirectoryPathCB getSpillDirPathCb;

  /// The callback used to update the aggregated spill bytes of a query. If the
  /// query spill limit is set, the callback throws if the aggregated spilled
  /// bytes exceed the set limit.
  UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb;

  /// Prefix for spill files.
  std::string fileNamePrefix;

  /// The max spill file size. If it is zero, there is no limit on the spill
  /// file size.
  uint64_t maxFileSize;

  /// Specifies the size to buffer the serialized spill data before write to
  /// storage system for io efficiency.
  uint64_t writeBufferSize;

  /// The min spill run size (bytes) limit used to select partitions for
  /// spilling. The spiller tries to spill a previously spilled partitions if
  /// its data size exceeds this limit, otherwise it spills the partition with
  /// most data. If the limit is zero, then the spiller always spill a
  /// previously spilled partition if it has any data. This is to avoid spill
  /// from a partition with a small amount of data which might result in
  /// generating too many small spilled files.
  uint64_t minSpillRunSize;

  /// Executor for spilling. If nullptr spilling writes on the Driver's thread.
  folly::Executor* executor; // Not owned.

  /// The minimal spillable memory reservation in percentage of the current
  /// memory usage.
  int32_t minSpillableReservationPct;

  /// The spillable memory reservation growth in percentage of the current
  /// memory usage.
  int32_t spillableReservationGrowthPct;

  /// The start partition bit offset of the top (the first level) partitions.
  uint8_t startPartitionBit;

  /// Used to calculate the spill hash partition number for hash join and
  /// RowNumber with 'startPartitionBit'.
  uint8_t numPartitionBits;

  /// The max allowed spilling level with zero being the initial spilling
  /// level. This only applies for hash build spilling which needs recursive
  /// spilling when the build table is too big. If it is set to -1, then there
  /// is no limit and then some extreme large query might run out of spilling
  /// partition bits at the end.
  int32_t maxSpillLevel;

  /// The max row numbers to fill and spill for each spill run. This is used to
  /// cap the memory used for spilling. If it is zero, then there is no limit
  /// and spilling might run out of memory.
  uint64_t maxSpillRunRows;

  /// Minimum memory footprint size required to reclaim memory from a file
  /// writer by flushing its buffered data to disk.
  uint64_t writerFlushThresholdSize;

  /// CompressionKind when spilling, CompressionKind_NONE means no compression.
  common::CompressionKind compressionKind;

  /// Custom options passed to velox::FileSystem to create spill WriteFile.
  std::string fileCreateConfig;
};
} // namespace facebook::velox::common
