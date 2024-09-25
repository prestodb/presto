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

#include "velox/common/base/SpillStats.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::velox::common {
namespace {
std::vector<folly::Synchronized<SpillStats>>& allSpillStats() {
  static std::vector<folly::Synchronized<SpillStats>> spillStatsList(
      std::thread::hardware_concurrency());
  return spillStatsList;
}

folly::Synchronized<SpillStats>& localSpillStats() {
  const auto idx = std::hash<std::thread::id>{}(std::this_thread::get_id());
  auto& spillStatsVector = allSpillStats();
  return spillStatsVector[idx % spillStatsVector.size()];
}
} // namespace

SpillStats::SpillStats(
    uint64_t _spillRuns,
    uint64_t _spilledInputBytes,
    uint64_t _spilledBytes,
    uint64_t _spilledRows,
    uint32_t _spilledPartitions,
    uint64_t _spilledFiles,
    uint64_t _spillFillTimeNanos,
    uint64_t _spillSortTimeNanos,
    uint64_t _spillExtractVectorTimeNanos,
    uint64_t _spillSerializationTimeNanos,
    uint64_t _spillWrites,
    uint64_t _spillFlushTimeNanos,
    uint64_t _spillWriteTimeNanos,
    uint64_t _spillMaxLevelExceededCount,
    uint64_t _spillReadBytes,
    uint64_t _spillReads,
    uint64_t _spillReadTimeNanos,
    uint64_t _spillDeserializationTimeNanos)
    : spillRuns(_spillRuns),
      spilledInputBytes(_spilledInputBytes),
      spilledBytes(_spilledBytes),
      spilledRows(_spilledRows),
      spilledPartitions(_spilledPartitions),
      spilledFiles(_spilledFiles),
      spillFillTimeNanos(_spillFillTimeNanos),
      spillSortTimeNanos(_spillSortTimeNanos),
      spillExtractVectorTimeNanos(_spillExtractVectorTimeNanos),
      spillSerializationTimeNanos(_spillSerializationTimeNanos),
      spillWrites(_spillWrites),
      spillFlushTimeNanos(_spillFlushTimeNanos),
      spillWriteTimeNanos(_spillWriteTimeNanos),
      spillMaxLevelExceededCount(_spillMaxLevelExceededCount),
      spillReadBytes(_spillReadBytes),
      spillReads(_spillReads),
      spillReadTimeNanos(_spillReadTimeNanos),
      spillDeserializationTimeNanos(_spillDeserializationTimeNanos) {}

SpillStats& SpillStats::operator+=(const SpillStats& other) {
  spillRuns += other.spillRuns;
  spilledInputBytes += other.spilledInputBytes;
  spilledBytes += other.spilledBytes;
  spilledRows += other.spilledRows;
  spilledPartitions += other.spilledPartitions;
  spilledFiles += other.spilledFiles;
  spillFillTimeNanos += other.spillFillTimeNanos;
  spillSortTimeNanos += other.spillSortTimeNanos;
  spillExtractVectorTimeNanos += other.spillExtractVectorTimeNanos;
  spillSerializationTimeNanos += other.spillSerializationTimeNanos;
  spillWrites += other.spillWrites;
  spillFlushTimeNanos += other.spillFlushTimeNanos;
  spillWriteTimeNanos += other.spillWriteTimeNanos;
  spillMaxLevelExceededCount += other.spillMaxLevelExceededCount;
  spillReadBytes += other.spillReadBytes;
  spillReads += other.spillReads;
  spillReadTimeNanos += other.spillReadTimeNanos;
  spillDeserializationTimeNanos += other.spillDeserializationTimeNanos;
  return *this;
}

SpillStats SpillStats::operator-(const SpillStats& other) const {
  SpillStats result;
  result.spillRuns = spillRuns - other.spillRuns;
  result.spilledInputBytes = spilledInputBytes - other.spilledInputBytes;
  result.spilledBytes = spilledBytes - other.spilledBytes;
  result.spilledRows = spilledRows - other.spilledRows;
  result.spilledPartitions = spilledPartitions - other.spilledPartitions;
  result.spilledFiles = spilledFiles - other.spilledFiles;
  result.spillFillTimeNanos = spillFillTimeNanos - other.spillFillTimeNanos;
  result.spillSortTimeNanos = spillSortTimeNanos - other.spillSortTimeNanos;
  result.spillExtractVectorTimeNanos =
      spillExtractVectorTimeNanos - other.spillExtractVectorTimeNanos;
  result.spillDeserializationTimeNanos =
      spillExtractVectorTimeNanos - other.spillExtractVectorTimeNanos;
  result.spillSerializationTimeNanos =
      spillSerializationTimeNanos - other.spillSerializationTimeNanos;
  result.spillWrites = spillWrites - other.spillWrites;
  result.spillFlushTimeNanos = spillFlushTimeNanos - other.spillFlushTimeNanos;
  result.spillWriteTimeNanos = spillWriteTimeNanos - other.spillWriteTimeNanos;
  result.spillMaxLevelExceededCount =
      spillMaxLevelExceededCount - other.spillMaxLevelExceededCount;
  result.spillReadBytes = spillReadBytes - other.spillReadBytes;
  result.spillReads = spillReads - other.spillReads;
  result.spillReadTimeNanos = spillReadTimeNanos - other.spillReadTimeNanos;
  result.spillDeserializationTimeNanos =
      spillDeserializationTimeNanos - other.spillDeserializationTimeNanos;
  return result;
}

bool SpillStats::operator<(const SpillStats& other) const {
  uint32_t gtCount{0};
  uint32_t ltCount{0};
#define UPDATE_COUNTER(counter)           \
  do {                                    \
    if (counter < other.counter) {        \
      ++ltCount;                          \
    } else if (counter > other.counter) { \
      ++gtCount;                          \
    }                                     \
  } while (0);

  UPDATE_COUNTER(spillRuns);
  UPDATE_COUNTER(spilledInputBytes);
  UPDATE_COUNTER(spilledBytes);
  UPDATE_COUNTER(spilledRows);
  UPDATE_COUNTER(spilledPartitions);
  UPDATE_COUNTER(spilledFiles);
  UPDATE_COUNTER(spillFillTimeNanos);
  UPDATE_COUNTER(spillSortTimeNanos);
  UPDATE_COUNTER(spillExtractVectorTimeNanos);
  UPDATE_COUNTER(spillSerializationTimeNanos);
  UPDATE_COUNTER(spillWrites);
  UPDATE_COUNTER(spillFlushTimeNanos);
  UPDATE_COUNTER(spillWriteTimeNanos);
  UPDATE_COUNTER(spillMaxLevelExceededCount);
  UPDATE_COUNTER(spillReadBytes);
  UPDATE_COUNTER(spillReads);
  UPDATE_COUNTER(spillReadTimeNanos);
  UPDATE_COUNTER(spillDeserializationTimeNanos);
#undef UPDATE_COUNTER
  VELOX_CHECK(
      !((gtCount > 0) && (ltCount > 0)),
      "gtCount {} ltCount {}",
      gtCount,
      ltCount);
  return ltCount > 0;
}

bool SpillStats::operator>(const SpillStats& other) const {
  return !(*this < other) && (*this != other);
}

bool SpillStats::operator>=(const SpillStats& other) const {
  return !(*this < other);
}

bool SpillStats::operator<=(const SpillStats& other) const {
  return !(*this > other);
}

bool SpillStats::operator==(const SpillStats& other) const {
  return std::tie(
             spillRuns,
             spilledInputBytes,
             spilledBytes,
             spilledRows,
             spilledPartitions,
             spilledFiles,
             spillFillTimeNanos,
             spillSortTimeNanos,
             spillExtractVectorTimeNanos,
             spillSerializationTimeNanos,
             spillWrites,
             spillFlushTimeNanos,
             spillWriteTimeNanos,
             spillMaxLevelExceededCount,
             spillReadBytes,
             spillReads,
             spillReadTimeNanos,
             spillDeserializationTimeNanos) ==
      std::tie(
             other.spillRuns,
             other.spilledInputBytes,
             other.spilledBytes,
             other.spilledRows,
             other.spilledPartitions,
             other.spilledFiles,
             other.spillFillTimeNanos,
             other.spillSortTimeNanos,
             other.spillExtractVectorTimeNanos,
             other.spillSerializationTimeNanos,
             other.spillWrites,
             other.spillFlushTimeNanos,
             other.spillWriteTimeNanos,
             spillMaxLevelExceededCount,
             spillReadBytes,
             spillReads,
             spillReadTimeNanos,
             spillDeserializationTimeNanos);
}

void SpillStats::reset() {
  spillRuns = 0;
  spilledInputBytes = 0;
  spilledBytes = 0;
  spilledRows = 0;
  spilledPartitions = 0;
  spilledFiles = 0;
  spillFillTimeNanos = 0;
  spillSortTimeNanos = 0;
  spillExtractVectorTimeNanos = 0;
  spillSerializationTimeNanos = 0;
  spillWrites = 0;
  spillFlushTimeNanos = 0;
  spillWriteTimeNanos = 0;
  spillMaxLevelExceededCount = 0;
  spillReadBytes = 0;
  spillReads = 0;
  spillReadTimeNanos = 0;
  spillDeserializationTimeNanos = 0;
}

std::string SpillStats::toString() const {
  return fmt::format(
      "spillRuns[{}] spilledInputBytes[{}] spilledBytes[{}] spilledRows[{}] "
      "spilledPartitions[{}] spilledFiles[{}] spillFillTimeNanos[{}] "
      "spillSortTimeNanos[{}] spillExtractVectorTime[{}] spillSerializationTimeNanos[{}] spillWrites[{}] "
      "spillFlushTimeNanos[{}] spillWriteTimeNanos[{}] maxSpillExceededLimitCount[{}] "
      "spillReadBytes[{}] spillReads[{}] spillReadTimeNanos[{}] "
      "spillReadDeserializationTimeNanos[{}]",
      spillRuns,
      succinctBytes(spilledInputBytes),
      succinctBytes(spilledBytes),
      spilledRows,
      spilledPartitions,
      spilledFiles,
      succinctNanos(spillFillTimeNanos),
      succinctNanos(spillSortTimeNanos),
      succinctNanos(spillExtractVectorTimeNanos),
      succinctNanos(spillSerializationTimeNanos),
      spillWrites,
      succinctNanos(spillFlushTimeNanos),
      succinctNanos(spillWriteTimeNanos),
      spillMaxLevelExceededCount,
      succinctBytes(spillReadBytes),
      spillReads,
      succinctNanos(spillReadTimeNanos),
      succinctNanos(spillDeserializationTimeNanos));
}

void updateGlobalSpillRunStats(uint64_t numRuns) {
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spillRuns += numRuns;
}

void updateGlobalSpillAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeNs) {
  RECORD_METRIC_VALUE(kMetricSpilledRowsCount, numRows);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricSpillSerializationTimeMs, serializationTimeNs / 1'000'000);
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeNanos += serializationTimeNs;
}

void incrementGlobalSpilledPartitionStats() {
  ++localSpillStats().wlock()->spilledPartitions;
}

void updateGlobalSpillFillTime(uint64_t timeNs) {
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillFillTimeMs, timeNs / 1'000'000);
  localSpillStats().wlock()->spillFillTimeNanos += timeNs;
}

void updateGlobalSpillSortTime(uint64_t timeNs) {
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillSortTimeMs, timeNs / 1'000'000);
  localSpillStats().wlock()->spillSortTimeNanos += timeNs;
}

void updateGlobalSpillExtractVectorTime(uint64_t timeNs) {
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricSpillExtractVectorTimeMs, timeNs / 1'000'000);
  localSpillStats().wlock()->spillExtractVectorTimeNanos += timeNs;
}

void updateGlobalSpillWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeNs,
    uint64_t writeTimeNs) {
  RECORD_METRIC_VALUE(kMetricSpillWritesCount);
  RECORD_METRIC_VALUE(kMetricSpilledBytes, spilledBytes);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricSpillFlushTimeMs, flushTimeNs / 1'000'000);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricSpillWriteTimeMs, writeTimeNs / 1'000'000);
  auto statsLocked = localSpillStats().wlock();
  ++statsLocked->spillWrites;
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeNanos += flushTimeNs;
  statsLocked->spillWriteTimeNanos += writeTimeNs;
}

void updateGlobalSpillReadStats(
    uint64_t spillReads,
    uint64_t spillReadBytes,
    uint64_t spillReadTimeNs) {
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spillReads += spillReads;
  statsLocked->spillReadBytes += spillReadBytes;
  statsLocked->spillReadTimeNanos += spillReadTimeNs;
}

void updateGlobalSpillMemoryBytes(uint64_t spilledInputBytes) {
  RECORD_METRIC_VALUE(kMetricSpilledInputBytes, spilledInputBytes);
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spilledInputBytes += spilledInputBytes;
}

void incrementGlobalSpilledFiles() {
  RECORD_METRIC_VALUE(kMetricSpilledFilesCount);
  ++localSpillStats().wlock()->spilledFiles;
}

void updateGlobalMaxSpillLevelExceededCount(
    uint64_t maxSpillLevelExceededCount) {
  localSpillStats().wlock()->spillMaxLevelExceededCount +=
      maxSpillLevelExceededCount;
}

void updateGlobalSpillDeserializationTimeNs(uint64_t timeNs) {
  localSpillStats().wlock()->spillDeserializationTimeNanos += timeNs;
}

SpillStats globalSpillStats() {
  SpillStats gSpillStats;
  for (auto& spillStats : allSpillStats()) {
    gSpillStats += spillStats.copy();
  }
  return gSpillStats;
}
} // namespace facebook::velox::common
