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

#include "velox/exec/Spiller.h"
#include <folly/ScopeGuard.h>
#include "velox/common/base/AsyncSource.h"
#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/PrefixSort.h"
#include "velox/external/timsort/TimSort.hpp"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

SpillerBase::SpillerBase(
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    uint64_t targetFileSize,
    uint64_t maxSpillRunRows,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : container_(container),
      executor_(spillConfig->executor),
      bits_(bits),
      rowType_(rowType),
      maxSpillRunRows_(maxSpillRunRows),
      spillStats_(spillStats),
      state_(
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          bits.numPartitions(),
          numSortingKeys,
          sortCompareFlags,
          targetFileSize,
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->prefixSortConfig,
          memory::spillMemoryPool(),
          spillStats,
          spillConfig->fileCreateConfig) {
  TestValue::adjust("facebook::velox::exec::SpillerBase", this);

  spillRuns_.reserve(state_.maxPartitions());
  for (int i = 0; i < state_.maxPartitions(); ++i) {
    spillRuns_.emplace_back(*memory::spillMemoryPool());
  }
}

NoRowContainerSpiller::NoRowContainerSpiller(
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : SpillerBase(
          nullptr,
          std::move(rowType),
          bits,
          0,
          {},
          spillConfig->maxFileSize,
          0,
          spillConfig,
          spillStats) {}

void SpillerBase::spill(const RowContainerIterator* startRowIter) {
  VELOX_CHECK(!finalized_);

  markAllPartitionsSpilled();

  RowContainerIterator rowIter;
  if (startRowIter != nullptr) {
    rowIter = *startRowIter;
  }

  bool lastRun{false};
  do {
    lastRun = fillSpillRuns(&rowIter);
    runSpill(lastRun);
  } while (!lastRun);

  checkEmptySpillRuns();
}

bool SpillerBase::fillSpillRuns(RowContainerIterator* iterator) {
  checkEmptySpillRuns();

  bool lastRun{false};
  uint64_t execTimeNs{0};
  {
    NanosecondTimer timer(&execTimeNs);

    // Number of rows to hash and divide into spill partitions at a time.
    constexpr int32_t kHashBatchSize = 4096;
    std::vector<uint64_t> hashes(kHashBatchSize);
    std::vector<char*> rows(kHashBatchSize);
    const bool isSinglePartition = bits_.numPartitions() == 1;

    uint64_t totalRows{0};
    for (;;) {
      const auto numRows = container_->listRows(
          iterator, rows.size(), RowContainer::kUnlimited, rows.data());
      if (numRows == 0) {
        lastRun = true;
        break;
      }

      // Calculate hashes for this batch of spill candidates.
      auto rowSet = folly::Range<char**>(rows.data(), numRows);

      if (!isSinglePartition) {
        for (auto i = 0; i < container_->keyTypes().size(); ++i) {
          container_->hash(i, rowSet, i > 0, hashes.data());
        }
      }

      // Put each in its run.
      for (auto i = 0; i < numRows; ++i) {
        // TODO: consider to cache the hash bits in row container so we only
        // need to calculate them once.
        const auto partition = isSinglePartition
            ? 0
            : bits_.partition(hashes[i], state_.maxPartitions());
        VELOX_DCHECK_GE(partition, 0);
        spillRuns_[partition].rows.push_back(rows[i]);
        spillRuns_[partition].numBytes += container_->rowSize(rows[i]);
      }

      totalRows += numRows;
      if (maxSpillRunRows_ > 0 && totalRows >= maxSpillRunRows_) {
        break;
      }
    }
  }
  updateSpillFillTime(execTimeNs);

  return lastRun;
}

void SpillerBase::runSpill(bool lastRun) {
  ++spillStats_->wlock()->spillRuns;

  std::vector<std::shared_ptr<AsyncSource<SpillStatus>>> writes;
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    VELOX_CHECK(
        state_.isPartitionSpilled(partition),
        "Partition {} is not marked as spilled",
        partition);
    if (spillRuns_[partition].rows.empty()) {
      continue;
    }
    writes.push_back(memory::createAsyncMemoryReclaimTask<SpillStatus>(
        [partition, this]() { return writeSpill(partition); }));
    if ((writes.size() > 1) && executor_ != nullptr) {
      executor_->add([source = writes.back()]() { source->prepare(); });
    }
  }
  auto sync = folly::makeGuard([&]() {
    for (auto& write : writes) {
      // We consume the result for the pending writes. This is a
      // cleanup in the guard and must not throw. The first error is
      // already captured before this runs.
      try {
        write->move();
      } catch (const std::exception&) {
      }
    }
  });

  std::vector<std::unique_ptr<SpillStatus>> results;
  results.reserve(writes.size());
  for (auto& write : writes) {
    results.push_back(write->move());
  }
  for (auto& result : results) {
    if (result->error != nullptr) {
      std::rethrow_exception(result->error);
    }
    const auto numWritten = result->rowsWritten;
    auto partition = result->partition;
    auto& run = spillRuns_[partition];
    VELOX_CHECK_EQ(numWritten, run.rows.size());
    run.clear();
    // When a sorted run ends, we start with a new file next time.
    if (needSort()) {
      state_.finishFile(partition);
    }
  }
}

std::unique_ptr<SpillerBase::SpillStatus> SpillerBase::writeSpill(
    int32_t partition) {
  // Target size of a single vector of spilled content. One of
  // these will be materialized at a time for each stream of the
  // merge.
  constexpr int32_t kTargetBatchBytes = 1 << 18; // 256K
  constexpr int32_t kTargetBatchRows = 64;

  RowVectorPtr spillVector;
  auto& run = spillRuns_[partition];
  try {
    ensureSorted(run);
    size_t written = 0;
    while (written < run.rows.size()) {
      extractSpillVector(
          run.rows, kTargetBatchRows, kTargetBatchBytes, spillVector, written);
      state_.appendToPartition(partition, spillVector);
    }
    return std::make_unique<SpillStatus>(partition, written, nullptr);
  } catch (const std::exception&) {
    // The exception is passed to the caller thread which checks this in
    // advanceSpill().
    return std::make_unique<SpillStatus>(
        partition, 0, std::current_exception());
  }
}

void SpillerBase::ensureSorted(SpillRun& run) {
  // The spill data of a hash join doesn't need to be sorted.
  if (run.sorted || !needSort()) {
    return;
  }

  uint64_t sortTimeNs{0};
  {
    NanosecondTimer timer(&sortTimeNs);

    if (!state_.prefixSortConfig().has_value()) {
      gfx::timsort(
          run.rows.begin(),
          run.rows.end(),
          [&](const char* left, const char* right) {
            return container_->compareRows(
                       left, right, state_.sortCompareFlags()) < 0;
          });
    } else {
      PrefixSort::sort(
          container_,
          state_.sortCompareFlags(),
          state_.prefixSortConfig().value(),
          memory::spillMemoryPool(),
          run.rows);
    }

    run.sorted = true;
  }

  // NOTE: Always set a non-zero sort time to avoid flakiness in tests which
  // check sort time.
  updateSpillSortTime(std::max<uint64_t>(1, sortTimeNs));
}

int64_t SpillerBase::extractSpillVector(
    SpillRows& rows,
    int32_t maxRows,
    int64_t maxBytes,
    RowVectorPtr& spillVector,
    size_t& nextBatchIndex) {
  uint64_t extractNs{0};
  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  VELOX_CHECK(!rows.empty());
  int32_t numRows = 0;
  int64_t bytes = 0;
  {
    NanosecondTimer timer(&extractNs);
    for (; numRows < limit; ++numRows) {
      bytes += container_->rowSize(rows[nextBatchIndex + numRows]);
      if (bytes > maxBytes) {
        // Increment because the row that went over the limit is part
        // of the result. We must spill at least one row.
        ++numRows;
        break;
      }
    }
    extractSpill(folly::Range(&rows[nextBatchIndex], numRows), spillVector);
    nextBatchIndex += numRows;
  }
  updateSpillExtractVectorTime(extractNs);
  return bytes;
}

void SpillerBase::extractSpill(
    folly::Range<char**> rows,
    RowVectorPtr& resultPtr) {
  if (resultPtr == nullptr) {
    resultPtr = BaseVector::create<RowVector>(
        rowType_, rows.size(), memory::spillMemoryPool());
  } else {
    resultPtr->prepareForReuse();
    resultPtr->resize(rows.size());
  }

  auto* result = resultPtr.get();
  const auto& types = container_->columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container_->extractColumn(rows.data(), rows.size(), i, result->childAt(i));
  }
  const auto& accumulators = container_->accumulators();
  column_index_t accumulatorColumnOffset = types.size();
  for (auto i = 0; i < accumulators.size(); ++i) {
    accumulators[i].extractForSpill(
        rows, result->childAt(i + accumulatorColumnOffset));
  }
}

void SpillerBase::updateSpillExtractVectorTime(uint64_t timeNs) {
  spillStats_->wlock()->spillExtractVectorTimeNanos += timeNs;
  common::updateGlobalSpillExtractVectorTime(timeNs);
}

void SpillerBase::updateSpillSortTime(uint64_t timeNs) {
  spillStats_->wlock()->spillSortTimeNanos += timeNs;
  common::updateGlobalSpillSortTime(timeNs);
}

void SpillerBase::checkEmptySpillRuns() const {
  for (const auto& spillRun : spillRuns_) {
    VELOX_CHECK(spillRun.rows.empty());
  }
}

void SpillerBase::updateSpillFillTime(uint64_t timeNs) {
  spillStats_->wlock()->spillFillTimeNanos += timeNs;
  common::updateGlobalSpillFillTime(timeNs);
}

void SpillerBase::finishSpill(SpillPartitionSet& partitionSet) {
  finalizeSpill();

  for (auto& partition : state_.spilledPartitionSet()) {
    const SpillPartitionId partitionId(bits_.begin(), partition);
    if (partitionSet.count(partitionId) == 0) {
      partitionSet.emplace(
          partitionId,
          std::make_unique<SpillPartition>(
              partitionId, state_.finish(partition)));
    } else {
      partitionSet[partitionId]->addFiles(state_.finish(partition));
    }
  }
}

common::SpillStats SpillerBase::stats() const {
  return spillStats_->copy();
}

std::string SpillerBase::toString() const {
  return fmt::format(
      "{}\t{}\tMAX_PARTITIONS:{}\tFINALIZED:{}",
      type(),
      rowType_->toString(),
      state_.maxPartitions(),
      finalized_);
}

void SpillerBase::finalizeSpill() {
  VELOX_CHECK(!finalized_);
  finalized_ = true;
}

void SpillerBase::markAllPartitionsSpilled() {
  for (auto partition = 0; partition < state_.maxPartitions(); ++partition) {
    if (!state_.isPartitionSpilled(partition)) {
      state_.setPartitionSpilled(partition);
    }
  }
}

void NoRowContainerSpiller::spill(
    uint32_t partition,
    const RowVectorPtr& spillVector) {
  VELOX_CHECK(!finalized_);
  if (FOLLY_UNLIKELY(!state_.isPartitionSpilled(partition))) {
    VELOX_FAIL(
        "Can't spill vector to a non-spilling partition: {}, {}",
        partition,
        toString());
  }
  VELOX_DCHECK(spillRuns_[partition].rows.empty());

  if (FOLLY_UNLIKELY(spillVector == nullptr)) {
    return;
  }

  state_.appendToPartition(partition, spillVector);
}

void SortInputSpiller::spill() {
  SpillerBase::spill(nullptr);
}

SortOutputSpiller::SortOutputSpiller(
    RowContainer* container,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : SpillerBase(
          container,
          std::move(rowType),
          HashBitRange{},
          0,
          {},
          std::numeric_limits<uint64_t>::max(),
          spillConfig->maxSpillRunRows,
          spillConfig,
          spillStats) {}

void SortOutputSpiller::spill(SpillRows& rows) {
  VELOX_CHECK(!finalized_);
  VELOX_CHECK(!rows.empty());

  markAllPartitionsSpilled();

  VELOX_CHECK_EQ(bits_.numPartitions(), 1);
  checkEmptySpillRuns();
  uint64_t execTimeNs{0};
  {
    NanosecondTimer timer(&execTimeNs);
    spillRuns_[0].rows =
        SpillRows(rows.begin(), rows.end(), spillRuns_[0].rows.get_allocator());
    for (const auto* row : rows) {
      spillRuns_[0].numBytes += container_->rowSize(row);
    }
  }
  updateSpillFillTime(execTimeNs);
  runSpill(true);
  checkEmptySpillRuns();
}

void SortOutputSpiller::runSpill(bool lastRun) {
  SpillerBase::runSpill(lastRun);
  if (lastRun) {
    for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
      state_.finishFile(partition);
    }
  }
}
} // namespace facebook::velox::exec
