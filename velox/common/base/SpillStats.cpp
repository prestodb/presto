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
    uint64_t _spillFillTimeUs,
    uint64_t _spillSortTimeUs,
    uint64_t _spillSerializationTimeUs,
    uint64_t _spillDiskWrites,
    uint64_t _spillFlushTimeUs,
    uint64_t _spillWriteTimeUs,
    uint64_t _spillMaxLevelExceededCount)
    : spillRuns(_spillRuns),
      spilledInputBytes(_spilledInputBytes),
      spilledBytes(_spilledBytes),
      spilledRows(_spilledRows),
      spilledPartitions(_spilledPartitions),
      spilledFiles(_spilledFiles),
      spillFillTimeUs(_spillFillTimeUs),
      spillSortTimeUs(_spillSortTimeUs),
      spillSerializationTimeUs(_spillSerializationTimeUs),
      spillDiskWrites(_spillDiskWrites),
      spillFlushTimeUs(_spillFlushTimeUs),
      spillWriteTimeUs(_spillWriteTimeUs),
      spillMaxLevelExceededCount(_spillMaxLevelExceededCount) {}

SpillStats& SpillStats::operator+=(const SpillStats& other) {
  spillRuns += other.spillRuns;
  spilledInputBytes += other.spilledInputBytes;
  spilledBytes += other.spilledBytes;
  spilledRows += other.spilledRows;
  spilledPartitions += other.spilledPartitions;
  spilledFiles += other.spilledFiles;
  spillFillTimeUs += other.spillFillTimeUs;
  spillSortTimeUs += other.spillSortTimeUs;
  spillSerializationTimeUs += other.spillSerializationTimeUs;
  spillDiskWrites += other.spillDiskWrites;
  spillFlushTimeUs += other.spillFlushTimeUs;
  spillWriteTimeUs += other.spillWriteTimeUs;
  spillMaxLevelExceededCount += other.spillMaxLevelExceededCount;
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
  result.spillFillTimeUs = spillFillTimeUs - other.spillFillTimeUs;
  result.spillSortTimeUs = spillSortTimeUs - other.spillSortTimeUs;
  result.spillSerializationTimeUs =
      spillSerializationTimeUs - other.spillSerializationTimeUs;
  result.spillDiskWrites = spillDiskWrites - other.spillDiskWrites;
  result.spillFlushTimeUs = spillFlushTimeUs - other.spillFlushTimeUs;
  result.spillWriteTimeUs = spillWriteTimeUs - other.spillWriteTimeUs;
  result.spillMaxLevelExceededCount =
      spillMaxLevelExceededCount - other.spillMaxLevelExceededCount;
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
  UPDATE_COUNTER(spillFillTimeUs);
  UPDATE_COUNTER(spillSortTimeUs);
  UPDATE_COUNTER(spillSerializationTimeUs);
  UPDATE_COUNTER(spillDiskWrites);
  UPDATE_COUNTER(spillFlushTimeUs);
  UPDATE_COUNTER(spillWriteTimeUs);
  UPDATE_COUNTER(spillMaxLevelExceededCount);
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
             spillFillTimeUs,
             spillSortTimeUs,
             spillSerializationTimeUs,
             spillDiskWrites,
             spillFlushTimeUs,
             spillWriteTimeUs,
             spillMaxLevelExceededCount) ==
      std::tie(
             other.spillRuns,
             other.spilledInputBytes,
             other.spilledBytes,
             other.spilledRows,
             other.spilledPartitions,
             other.spilledFiles,
             other.spillFillTimeUs,
             other.spillSortTimeUs,
             other.spillSerializationTimeUs,
             other.spillDiskWrites,
             other.spillFlushTimeUs,
             other.spillWriteTimeUs,
             spillMaxLevelExceededCount);
}

void SpillStats::reset() {
  spillRuns = 0;
  spilledInputBytes = 0;
  spilledBytes = 0;
  spilledRows = 0;
  spilledPartitions = 0;
  spilledFiles = 0;
  spillFillTimeUs = 0;
  spillSortTimeUs = 0;
  spillSerializationTimeUs = 0;
  spillDiskWrites = 0;
  spillFlushTimeUs = 0;
  spillWriteTimeUs = 0;
  spillMaxLevelExceededCount = 0;
}

std::string SpillStats::toString() const {
  return fmt::format(
      "spillRuns[{}] spilledInputBytes[{}] spilledBytes[{}] spilledRows[{}] spilledPartitions[{}] spilledFiles[{}] spillFillTimeUs[{}] spillSortTime[{}] spillSerializationTime[{}] spillDiskWrites[{}] spillFlushTime[{}] spillWriteTime[{}] maxSpillExceededLimitCount[{}]",
      spillRuns,
      succinctBytes(spilledInputBytes),
      succinctBytes(spilledBytes),
      spilledRows,
      spilledPartitions,
      spilledFiles,
      succinctMicros(spillFillTimeUs),
      succinctMicros(spillSortTimeUs),
      succinctMicros(spillSerializationTimeUs),
      spillDiskWrites,
      succinctMicros(spillFlushTimeUs),
      succinctMicros(spillWriteTimeUs),
      spillMaxLevelExceededCount);
}

void updateGlobalSpillRunStats(uint64_t numRuns) {
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spillRuns += numRuns;
}

void updateGlobalSpillAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeUs) {
  RECORD_METRIC_VALUE(kMetricSpilledRowsCount, numRows);
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricSpillSerializationTimeMs, serializationTimeUs / 1'000);
  auto statsLocked = localSpillStats().wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeUs += serializationTimeUs;
}

void incrementGlobalSpilledPartitionStats() {
  ++localSpillStats().wlock()->spilledPartitions;
}

void updateGlobalSpillFillTime(uint64_t timeUs) {
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillFillTimeMs, timeUs / 1'000);
  localSpillStats().wlock()->spillFillTimeUs += timeUs;
}

void updateGlobalSpillSortTime(uint64_t timeUs) {
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillSortTimeMs, timeUs / 1'000);
  localSpillStats().wlock()->spillSortTimeUs += timeUs;
}

void updateGlobalSpillWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeUs,
    uint64_t writeTimeUs) {
  RECORD_METRIC_VALUE(kMetricSpillDiskWritesCount);
  RECORD_METRIC_VALUE(kMetricSpilledBytes, spilledBytes);
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillFlushTimeMs, flushTimeUs / 1'000);
  RECORD_HISTOGRAM_METRIC_VALUE(kMetricSpillWriteTimeMs, writeTimeUs / 1'000);
  auto statsLocked = localSpillStats().wlock();
  ++statsLocked->spillDiskWrites;
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeUs += flushTimeUs;
  statsLocked->spillWriteTimeUs += writeTimeUs;
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

SpillStats globalSpillStats() {
  SpillStats gSpillStats;
  for (auto& spillStats : allSpillStats()) {
    gSpillStats += spillStats.copy();
  }
  return gSpillStats;
}
} // namespace facebook::velox::common
