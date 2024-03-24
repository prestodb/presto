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
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/external/timsort/TimSort.hpp"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {
#define CHECK_NOT_FINALIZED() \
  VELOX_CHECK(!finalized_, "Spiller has been finalized")

#define CHECK_FINALIZED() \
  VELOX_CHECK(finalized_, "Spiller hasn't been finalized yet");
} // namespace

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : Spiller(
          type,
          container,
          std::move(rowType),
          HashBitRange{},
          numSortingKeys,
          sortCompareFlags,
          false,
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          std::numeric_limits<uint64_t>::max(),
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->fileCreateConfig,
          spillStats) {
  VELOX_CHECK(
      type_ == Type::kOrderByInput || type_ == Type::kAggregateInput,
      "Unexpected spiller type: {}",
      typeName(type_));
  VELOX_CHECK_EQ(state_.maxPartitions(), 1);
  VELOX_CHECK_EQ(state_.targetFileSize(), std::numeric_limits<uint64_t>::max());
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : Spiller(
          type,
          container,
          std::move(rowType),
          HashBitRange{},
          0,
          {},
          false,
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          std::numeric_limits<uint64_t>::max(),
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->fileCreateConfig,
          spillStats) {
  VELOX_CHECK(
      type_ == Type::kAggregateOutput || type_ == Type::kOrderByOutput,
      "Unexpected spiller type: {}",
      typeName(type_));
  VELOX_CHECK_EQ(state_.maxPartitions(), 1);
  VELOX_CHECK_EQ(state_.targetFileSize(), std::numeric_limits<uint64_t>::max());
}

Spiller::Spiller(
    Type type,
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : Spiller(
          type,
          nullptr,
          std::move(rowType),
          bits,
          0,
          {},
          false,
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          spillConfig->maxFileSize,
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->executor,
          0,
          spillConfig->fileCreateConfig,
          spillStats) {
  VELOX_CHECK_EQ(
      type_,
      Type::kHashJoinProbe,
      "Unexpected spiller type: {}",
      typeName(type_));
}

Spiller::Spiller(
    Type type,
    core::JoinType joinType,
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : Spiller(
          type,
          container,
          std::move(rowType),
          bits,
          0,
          {},
          needRightSideJoin(joinType),
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          spillConfig->maxFileSize,
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->fileCreateConfig,
          spillStats) {
  VELOX_CHECK_EQ(type_, Type::kHashJoinBuild);
  VELOX_CHECK(isHashJoinTableSpillType(rowType_, joinType));
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : Spiller(
          type,
          container,
          std::move(rowType),
          bits,
          0,
          {},
          false,
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          spillConfig->maxFileSize,
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->executor,
          spillConfig->maxSpillRunRows,
          spillConfig->fileCreateConfig,
          spillStats) {
  VELOX_CHECK_EQ(type_, Type::kRowNumber);
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    bool recordProbedFlag,
    const common::GetSpillDirectoryPathCB& getSpillDirPathCb,
    const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
    const std::string& fileNamePrefix,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    common::CompressionKind compressionKind,
    folly::Executor* executor,
    uint64_t maxSpillRunRows,
    const std::string& fileCreateConfig,
    folly::Synchronized<common::SpillStats>* spillStats)
    : type_(type),
      container_(container),
      executor_(executor),
      bits_(bits),
      rowType_(std::move(rowType)),
      spillProbedFlag_(recordProbedFlag),
      maxSpillRunRows_(maxSpillRunRows),
      spillStats_(spillStats),
      state_(
          getSpillDirPathCb,
          updateAndCheckSpillLimitCb,
          fileNamePrefix,
          bits.numPartitions(),
          numSortingKeys,
          sortCompareFlags,
          targetFileSize,
          writeBufferSize,
          compressionKind,
          memory::spillMemoryPool(),
          spillStats,
          fileCreateConfig) {
  TestValue::adjust(
      "facebook::velox::exec::Spiller", const_cast<HashBitRange*>(&bits_));

  VELOX_CHECK(!spillProbedFlag_ || type_ == Type::kHashJoinBuild);
  VELOX_CHECK_EQ(container_ == nullptr, type_ == Type::kHashJoinProbe);
  spillRuns_.reserve(state_.maxPartitions());
  for (int i = 0; i < state_.maxPartitions(); ++i) {
    spillRuns_.emplace_back(*memory::spillMemoryPool());
  }
}

void Spiller::extractSpill(folly::Range<char**> rows, RowVectorPtr& resultPtr) {
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
  if (spillProbedFlag_) {
    container_->extractProbedFlags(
        rows.data(), rows.size(), false, false, result->childAt(types.size()));
    ++accumulatorColumnOffset;
  }
  for (auto i = 0; i < accumulators.size(); ++i) {
    accumulators[i].extractForSpill(
        rows, result->childAt(i + accumulatorColumnOffset));
  }
}

int64_t Spiller::extractSpillVector(
    SpillRows& rows,
    int32_t maxRows,
    int64_t maxBytes,
    RowVectorPtr& spillVector,
    size_t& nextBatchIndex) {
  VELOX_CHECK_NE(type_, Type::kHashJoinProbe);

  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  VELOX_CHECK(!rows.empty());
  int32_t numRows = 0;
  int64_t bytes = 0;
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
  return bytes;
}

namespace {
// A stream of ordered rows being read from the in memory
// container. This is the part of a spillable range that is not yet
// spilled when starting to produce output. This is only used for
// sorted spills since for hash join spilling we just use the data in
// the RowContainer as is.
class RowContainerSpillMergeStream : public SpillMergeStream {
 public:
  RowContainerSpillMergeStream(
      int32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      Spiller::SpillRows&& rows,
      Spiller& spiller)
      : numSortKeys_(numSortKeys),
        sortCompareFlags_(sortCompareFlags),
        rows_(std::move(rows)),
        spiller_(spiller) {
    if (!rows_.empty()) {
      nextBatch();
    }
  }

  uint32_t id() const override {
    // Returns the max uint32_t as the special id for in-memory spill merge
    // stream.
    return std::numeric_limits<uint32_t>::max();
  }

 private:
  int32_t numSortKeys() const override {
    return numSortKeys_;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const override {
    return sortCompareFlags_;
  }

  void nextBatch() override {
    // Extracts up to 64 rows at a time. Small batch size because may
    // have wide data and no advantage in large size for narrow data
    // since this is all processed row by row.
    static constexpr vector_size_t kMaxRows = 64;
    constexpr uint64_t kMaxBytes = 1 << 18;
    if (nextBatchIndex_ >= rows_.size()) {
      index_ = 0;
      size_ = 0;
      return;
    }
    spiller_.extractSpillVector(
        rows_, kMaxRows, kMaxBytes, rowVector_, nextBatchIndex_);
    size_ = rowVector_->size();
    index_ = 0;
  }

  const int32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;

  Spiller::SpillRows rows_;
  Spiller& spiller_;
  size_t nextBatchIndex_ = 0;
};
} // namespace

std::unique_ptr<SpillMergeStream> Spiller::spillMergeStreamOverRows(
    int32_t partition) {
  CHECK_FINALIZED();
  VELOX_CHECK_LT(partition, state_.maxPartitions());

  if (!state_.isPartitionSpilled(partition)) {
    return nullptr;
  }
  // Skip the merge stream from row container if it is empty.
  if (spillRuns_[partition].rows.empty()) {
    return nullptr;
  }
  ensureSorted(spillRuns_[partition]);
  return std::make_unique<RowContainerSpillMergeStream>(
      container_->keyTypes().size(),
      state_.sortCompareFlags(),
      std::move(spillRuns_[partition].rows),
      *this);
}

void Spiller::ensureSorted(SpillRun& run) {
  // The spill data of a hash join doesn't need to be sorted.
  if (run.sorted || !needSort()) {
    return;
  }

  uint64_t sortTimeUs{0};
  {
    MicrosecondTimer timer(&sortTimeUs);
    gfx::timsort(
        run.rows.begin(),
        run.rows.end(),
        [&](const char* left, const char* right) {
          return container_->compareRows(
                     left, right, state_.sortCompareFlags()) < 0;
        });
    run.sorted = true;
  }

  // NOTE: Always set a non-zero sort time to avoid flakiness in tests which
  // check sort time.
  updateSpillSortTime(std::max<uint64_t>(1, sortTimeUs));
}

std::unique_ptr<Spiller::SpillStatus> Spiller::writeSpill(int32_t partition) {
  VELOX_CHECK_NE(type_, Type::kHashJoinProbe);
  // Target size of a single vector of spilled content. One of
  // these will be materialized at a time for each stream of the
  // merge.
  constexpr int32_t kTargetBatchBytes = 1 << 18; // 256K
  constexpr int32_t kTargetBatchRows = 64;

  RowVectorPtr spillVector;
  auto& run = spillRuns_[partition];
  try {
    ensureSorted(run);
    int64_t totalBytes = 0;
    size_t written = 0;
    while (written < run.rows.size()) {
      extractSpillVector(
          run.rows, kTargetBatchRows, kTargetBatchBytes, spillVector, written);
      totalBytes += state_.appendToPartition(partition, spillVector);
      if (totalBytes > state_.targetFileSize()) {
        VELOX_CHECK(!needSort());
        state_.finishFile(partition);
      }
    }
    return std::make_unique<SpillStatus>(partition, written, nullptr);
  } catch (const std::exception&) {
    // The exception is passed to the caller thread which checks this in
    // advanceSpill().
    return std::make_unique<SpillStatus>(
        partition, 0, std::current_exception());
  }
}

void Spiller::runSpill(bool lastRun) {
  ++spillStats_->wlock()->spillRuns;
  VELOX_CHECK(type_ != Spiller::Type::kOrderByOutput || lastRun);

  std::vector<std::shared_ptr<AsyncSource<SpillStatus>>> writes;
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    VELOX_CHECK(
        state_.isPartitionSpilled(partition),
        "Partition {} is not marked as spilled",
        partition);
    if (spillRuns_[partition].rows.empty()) {
      continue;
    }
    writes.push_back(std::make_shared<AsyncSource<SpillStatus>>(
        [partition, this]() { return writeSpill(partition); }));
    if (executor_) {
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

  // For aggregation output / orderby output spiller, we expect only one spill
  // call to spill all the rows starting from the specified row offset.
  if (lastRun &&
      (type_ == Spiller::Type::kAggregateOutput ||
       type_ == Spiller::Type::kOrderByOutput)) {
    for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
      state_.finishFile(partition);
    }
  }
}

void Spiller::updateSpillFillTime(uint64_t timeUs) {
  spillStats_->wlock()->spillFillTimeUs += timeUs;
  common::updateGlobalSpillFillTime(timeUs);
}

void Spiller::updateSpillSortTime(uint64_t timeUs) {
  spillStats_->wlock()->spillSortTimeUs += timeUs;
  common::updateGlobalSpillSortTime(timeUs);
}

bool Spiller::needSort() const {
  return type_ != Type::kHashJoinProbe && type_ != Type::kHashJoinBuild &&
      type_ != Type::kRowNumber && type_ != Type::kAggregateOutput &&
      type_ != Type::kOrderByOutput;
}

void Spiller::spill() {
  return spill(nullptr);
}

void Spiller::spill(const RowContainerIterator& startRowIter) {
  VELOX_CHECK_EQ(type_, Type::kAggregateOutput);
  return spill(&startRowIter);
}

void Spiller::spill(const RowContainerIterator* startRowIter) {
  CHECK_NOT_FINALIZED();
  VELOX_CHECK_NE(type_, Type::kHashJoinProbe);
  VELOX_CHECK_NE(type_, Type::kOrderByOutput);

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

void Spiller::spill(std::vector<char*>& rows) {
  CHECK_NOT_FINALIZED();
  VELOX_CHECK_EQ(type_, Type::kOrderByOutput);
  VELOX_CHECK(!rows.empty());

  markAllPartitionsSpilled();

  fillSpillRun(rows);
  runSpill(true);
  checkEmptySpillRuns();
}

void Spiller::checkEmptySpillRuns() const {
  for (const auto& spillRun : spillRuns_) {
    VELOX_CHECK(spillRun.rows.empty());
  }
}

void Spiller::markAllPartitionsSpilled() {
  for (auto partition = 0; partition < state_.maxPartitions(); ++partition) {
    if (!state_.isPartitionSpilled(partition)) {
      state_.setPartitionSpilled(partition);
    }
  }
}

void Spiller::spill(uint32_t partition, const RowVectorPtr& spillVector) {
  CHECK_NOT_FINALIZED();
  VELOX_CHECK(
      type_ == Type::kHashJoinProbe || type_ == Type::kHashJoinBuild ||
          type_ == Type::kRowNumber,
      "Unexpected spiller type: {}",
      typeName(type_));
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

void Spiller::finishSpill(SpillPartitionSet& partitionSet) {
  finalizeSpill();

  for (auto& partition : state_.spilledPartitionSet()) {
    const SpillPartitionId partitionId(bits_.begin(), partition);
    if (FOLLY_UNLIKELY(partitionSet.count(partitionId) == 0)) {
      partitionSet.emplace(
          partitionId,
          std::make_unique<SpillPartition>(
              partitionId, state_.finish(partition)));
    } else {
      partitionSet[partitionId]->addFiles(state_.finish(partition));
    }
  }
}

SpillPartition Spiller::finishSpill() {
  VELOX_CHECK_EQ(state_.maxPartitions(), 1);
  VELOX_CHECK(state_.isPartitionSpilled(0));

  finalizeSpill();
  return SpillPartition(SpillPartitionId{bits_.begin(), 0}, state_.finish(0));
}

void Spiller::finalizeSpill() {
  CHECK_NOT_FINALIZED();
  finalized_ = true;
}

bool Spiller::fillSpillRuns(RowContainerIterator* iterator) {
  checkEmptySpillRuns();

  bool lastRun{false};
  uint64_t execTimeUs{0};
  {
    MicrosecondTimer timer(&execTimeUs);

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
  updateSpillFillTime(execTimeUs);

  return lastRun;
}

void Spiller::fillSpillRun(std::vector<char*>& rows) {
  VELOX_CHECK_EQ(bits_.numPartitions(), 1);
  checkEmptySpillRuns();
  uint64_t execTimeUs{0};
  {
    MicrosecondTimer timer(&execTimeUs);
    spillRuns_[0].rows =
        SpillRows(rows.begin(), rows.end(), spillRuns_[0].rows.get_allocator());
    for (const auto* row : rows) {
      spillRuns_[0].numBytes += container_->rowSize(row);
    }
  }
  updateSpillFillTime(execTimeUs);
}

std::string Spiller::toString() const {
  return fmt::format(
      "{}\t{}\tMAX_PARTITIONS:{}\tFINALIZED:{}",
      typeName(type_),
      rowType_->toString(),
      state_.maxPartitions(),
      finalized_);
}

// static
std::string Spiller::typeName(Type type) {
  switch (type) {
    case Type::kOrderByInput:
      return "ORDER_BY_INPUT";
    case Type::kOrderByOutput:
      return "ORDER_BY_OUTPUT";
    case Type::kHashJoinBuild:
      return "HASH_JOIN_BUILD";
    case Type::kHashJoinProbe:
      return "HASH_JOIN_PROBE";
    case Type::kAggregateInput:
      return "AGGREGATE_INPUT";
    case Type::kAggregateOutput:
      return "AGGREGATE_OUTPUT";
    case Type::kRowNumber:
      return "ROW_NUMBER";
    default:
      VELOX_UNREACHABLE("Unknown type: {}", static_cast<int>(type));
  }
}

common::SpillStats Spiller::stats() const {
  return spillStats_->copy();
}
} // namespace facebook::velox::exec
