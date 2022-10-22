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

#include "velox/common/base/AsyncSource.h"

#include <folly/ScopeGuard.h>
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

constexpr int kLogEveryN = 32;

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowContainer::Eraser eraser,
    RowTypePtr rowType,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const std::string& path,
    int64_t targetFileSize,
    memory::MemoryPool& pool,
    folly::Executor* executor)
    : Spiller(
          type,
          container,
          eraser,
          std::move(rowType),
          HashBitRange{},
          numSortingKeys,
          sortCompareFlags,
          path,
          targetFileSize,
          pool,
          executor) {
  VELOX_CHECK_EQ(type_, Type::kOrderBy);
}

Spiller::Spiller(
    Type type,
    RowTypePtr rowType,
    HashBitRange bits,
    const std::string& path,
    int64_t targetFileSize,
    memory::MemoryPool& pool,
    folly::Executor* FOLLY_NULLABLE executor)
    : Spiller(
          type,
          nullptr,
          nullptr,
          std::move(rowType),
          bits,
          0,
          {},
          path,
          targetFileSize,
          pool,
          executor) {
  VELOX_CHECK_EQ(type_, Type::kHashJoinProbe);
}

Spiller::Spiller(
    Type type,
    RowContainer* container,
    RowContainer::Eraser eraser,
    RowTypePtr rowType,
    HashBitRange bits,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const std::string& path,
    int64_t targetFileSize,
    memory::MemoryPool& pool,
    folly::Executor* executor)
    : type_(type),
      container_(container),
      eraser_(eraser),
      bits_(bits),
      rowType_(std::move(rowType)),
      state_(
          path,
          bits.numPartitions(),
          numSortingKeys,
          sortCompareFlags,
          targetFileSize,
          pool,
          spillMappedMemory()),
      pool_(pool),
      executor_(executor) {
  TestValue::adjust(
      "facebook::velox::exec::Spiller", const_cast<HashBitRange*>(&bits_));

  VELOX_CHECK_EQ(container_ == nullptr, type_ == Type::kHashJoinProbe);
  // kOrderBy spiller type must only have one partition.
  VELOX_CHECK((type_ != Type::kOrderBy) || (state_.maxPartitions() == 1));
  spillRuns_.reserve(state_.maxPartitions());
  for (int i = 0; i < state_.maxPartitions(); ++i) {
    spillRuns_.emplace_back(spillMappedMemory());
  }
}

void Spiller::extractSpill(folly::Range<char**> rows, RowVectorPtr& resultPtr) {
  if (!resultPtr) {
    resultPtr =
        BaseVector::create<RowVector>(rowType_, rows.size(), &spillPool());
  } else {
    resultPtr->prepareForReuse();
    resultPtr->resize(rows.size());
  }
  auto result = resultPtr.get();
  auto& types = container_->columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container_->extractColumn(rows.data(), rows.size(), i, result->childAt(i));
  }
  auto& aggregates = container_->aggregates();
  auto numKeys = types.size();
  for (auto i = 0; i < aggregates.size(); ++i) {
    aggregates[i]->finalize(rows.data(), rows.size());
    aggregates[i]->extractAccumulators(
        rows.data(), rows.size(), &result->childAt(i + numKeys));
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
  assert(!rows.empty());
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
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      Spiller::SpillRows&& rows,
      Spiller& spiller)
      : numSortingKeys_(numSortingKeys),
        sortCompareFlags_(sortCompareFlags),
        rows_(std::move(rows)),
        spiller_(spiller) {
    if (!rows_.empty()) {
      nextBatch();
    }
  }

 private:
  int32_t numSortingKeys() const override {
    return numSortingKeys_;
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

  const int32_t numSortingKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;

  Spiller::SpillRows rows_;
  Spiller& spiller_;
  size_t nextBatchIndex_ = 0;
};
} // namespace

std::unique_ptr<SpillMergeStream> Spiller::spillMergeStreamOverRows(
    int32_t partition) {
  VELOX_CHECK(spillFinalized_);
  VELOX_CHECK_LT(partition, state_.maxPartitions());

  if (!state_.isPartitionSpilled(partition)) {
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
  if (!run.sorted && needSort()) {
    std::sort(
        run.rows.begin(),
        run.rows.end(),
        [&](const char* left, const char* right) {
          return container_->compareRows(
                     left, right, state_.sortCompareFlags()) < 0;
        });
    run.sorted = true;
  }
}

std::unique_ptr<Spiller::SpillStatus> Spiller::writeSpill(int32_t partition) {
  VELOX_CHECK_NE(type_, Type::kHashJoinProbe);
  VELOX_CHECK_EQ(pendingSpillPartitions_.count(partition), 1);
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
      totalBytes += extractSpillVector(
          run.rows, kTargetBatchRows, kTargetBatchBytes, spillVector, written);
      state_.appendToPartition(partition, spillVector);
      if (totalBytes > state_.targetFileSize()) {
        break;
      }
    }
    return std::make_unique<SpillStatus>(partition, written, nullptr);
  } catch (const std::exception& e) {
    // The exception is passed to the caller thread which checks this in
    // advanceSpill().
    return std::make_unique<SpillStatus>(
        partition, 0, std::current_exception());
  }
}

void Spiller::advanceSpill() {
  std::vector<std::shared_ptr<AsyncSource<SpillStatus>>> writes;
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    if (pendingSpillPartitions_.count(partition) == 0) {
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
      } catch (const std::exception& e) {
      }
    }
  });

  std::vector<std::unique_ptr<SpillStatus>> results;
  results.reserve(writes.size());
  for (auto& write : writes) {
    results.push_back(write->move());
  }
  for (auto& result : results) {
    if (result->error) {
      std::rethrow_exception(result->error);
    }
    auto numWritten = result->rowsWritten;
    spilledRows_ += numWritten;
    auto partition = result->partition;
    auto& run = spillRuns_[partition];
    auto spilled = folly::Range<char**>(run.rows.data(), numWritten);
    eraser_(spilled);
    if (!container_->numRows()) {
      // If the container became empty, free its memory.
      container_->clear();
    }
    run.rows.erase(run.rows.begin(), run.rows.begin() + numWritten);
    if (run.rows.empty()) {
      // Run ends, start with a new file next time.
      run.clear();
      state_.finishWrite(partition);
      pendingSpillPartitions_.erase(partition);
    }
  }
}

bool Spiller::needSort() const {
  return type_ != Type::kHashJoinProbe && type_ != Type::kHashJoinBuild;
}

void Spiller::spill(uint64_t targetRows, uint64_t targetBytes) {
  VELOX_CHECK(!spillFinalized_);

  if (type_ == Type::kHashJoinBuild || type_ == Type::kHashJoinProbe) {
    VELOX_FAIL("Don't support incremental spill on type: {}", typeName(type_));
  }

  bool hasFilledRuns = false;
  for (;;) {
    auto rowsLeft = container_->numRows();
    auto spaceLeft = container_->stringAllocator().retainedSize() -
        container_->stringAllocator().freeSpace();
    if (rowsLeft == 0 || (rowsLeft <= targetRows && spaceLeft <= targetBytes)) {
      break;
    }
    if (!pendingSpillPartitions_.empty()) {
      advanceSpill();
      // Check if we have released sufficient memory after spilling.
      continue;
    }

    // Fill the spill runs once per each spill run.
    //
    // NOTE: there might be some leftover from previous spill run so that we
    // finish spilling them first.
    if (!hasFilledRuns) {
      fillSpillRuns();
      hasFilledRuns = true;
    }

    while (rowsLeft > 0 && (rowsLeft > targetRows || spaceLeft > targetBytes)) {
      const int32_t partition = pickNextPartitionToSpill();
      if (partition == -1) {
        VELOX_FAIL(
            "No partition has spillable data but still doesn't reach the spill target, target rows {}, target bytes {}, rows left {}, bytes left {}",
            targetRows,
            targetBytes,
            rowsLeft,
            spaceLeft);
        break;
      }
      if (!state_.isPartitionSpilled(partition)) {
        state_.setPartitionSpilled(partition);
      }
      VELOX_DCHECK_EQ(pendingSpillPartitions_.count(partition), 0);
      pendingSpillPartitions_.insert(partition);
      rowsLeft =
          std::max<int64_t>(0, rowsLeft - spillRuns_[partition].rows.size());
      spaceLeft =
          std::max<int64_t>(0, spaceLeft - spillRuns_[partition].numBytes);
    }
    // Quit this spill run if we have spilled all the partitions.
    if (pendingSpillPartitions_.empty()) {
      LOG_EVERY_N(WARNING, kLogEveryN)
          << spaceLeft << " bytes and " << rowsLeft
          << " rows left after spilled all partitions, spiller: " << toString();
      break;
    }
  }

  // Clear the non-spilling runs on exit.
  clearNonSpillingRuns();
}

void Spiller::spill(const SpillPartitionNumSet& partitions) {
  VELOX_CHECK(!spillFinalized_);

  if (FOLLY_UNLIKELY(type_ == Type::kHashJoinProbe)) {
    VELOX_FAIL("There is no row container for {}", typeName(type_));
  }
  if (FOLLY_UNLIKELY(!pendingSpillPartitions_.empty())) {
    VELOX_FAIL(
        "There are pending spilling operations on partitions: {}",
        folly::join(",", pendingSpillPartitions_));
  }

  for (auto partition : partitions) {
    if (FOLLY_LIKELY(!state_.isPartitionSpilled(partition))) {
      state_.setPartitionSpilled(partition);
    }
    if (FOLLY_LIKELY(!spillRuns_[partition].rows.empty())) {
      pendingSpillPartitions_.insert(partition);
    }
  }

  while (!pendingSpillPartitions_.empty()) {
    advanceSpill();
  }
}

void Spiller::spill(uint32_t partition, const RowVectorPtr& spillVector) {
  VELOX_CHECK(!spillFinalized_);

  if (FOLLY_UNLIKELY(needSort())) {
    VELOX_FAIL(
        "Do not support to spill vector for sorted spiller: {}", toString());
  }
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

int32_t Spiller::pickNextPartitionToSpill() {
  VELOX_DCHECK_EQ(spillRuns_.size(), state_.maxPartitions());

  // Sort the partitions based on spiller type to pick.
  std::vector<int32_t> partitionIndices(spillRuns_.size());
  std::iota(partitionIndices.begin(), partitionIndices.end(), 0);
  std::sort(
      partitionIndices.begin(),
      partitionIndices.end(),
      [&](int32_t lhs, int32_t rhs) {
        return spillRuns_[lhs].numBytes > spillRuns_[rhs].numBytes;
      });
  for (auto partition : partitionIndices) {
    if (pendingSpillPartitions_.count(partition) != 0) {
      continue;
    }
    if (spillRuns_[partition].numBytes == 0) {
      continue;
    }
    return partition;
  }
  return -1;
}

Spiller::SpillRows Spiller::finishSpill() {
  VELOX_CHECK(!spillFinalized_);
  spillFinalized_ = true;

  SpillRows rowsFromNonSpillingPartitions(
      0, memory::StlMappedMemoryAllocator<char*>(&spillMappedMemory()));
  if (type_ != Spiller::Type::kHashJoinProbe) {
    fillSpillRuns(&rowsFromNonSpillingPartitions);
  }
  return rowsFromNonSpillingPartitions;
}

void Spiller::finishSpill(SpillPartitionSet& partitionSet) {
  VELOX_CHECK(!spillFinalized_);
  spillFinalized_ = true;

  for (auto& partition : state_.spilledPartitionSet()) {
    const SpillPartitionId partitionId(bits_.begin(), partition);
    if (FOLLY_UNLIKELY(partitionSet.count(partitionId) == 0)) {
      partitionSet.emplace(
          partitionId,
          std::make_unique<SpillPartition>(
              partitionId, state_.files(partition)));
    } else {
      partitionSet[partitionId]->addFiles(state_.files(partition));
    }
  }
}

void Spiller::clearSpillRuns() {
  for (auto& run : spillRuns_) {
    run.clear();
  }
}

void Spiller::fillSpillRuns(SpillRows* rowsFromNonSpillingPartitions) {
  clearSpillRuns();

  RowContainerIterator iterator;
  // Number of rows to hash and divide into spill partitions at a time.
  constexpr int32_t kHashBatchSize = 4096;
  std::vector<uint64_t> hashes(kHashBatchSize);
  std::vector<char*> rows(kHashBatchSize);
  for (;;) {
    auto numRows = container_->listRows(
        &iterator, rows.size(), RowContainer::kUnlimited, rows.data());
    // Calculate hashes for this batch of spill candidates.
    auto rowSet = folly::Range<char**>(rows.data(), numRows);
    for (auto i = 0; i < container_->keyTypes().size(); ++i) {
      container_->hash(i, rowSet, i > 0, hashes.data());
    }

    // Put each in its run.
    for (auto i = 0; i < numRows; ++i) {
      // TODO: consider to cache the hash bits in row container so we only need
      // to calculate them once.
      const auto partition = (type_ == Type::kOrderBy)
          ? 0
          : bits_.partition(hashes[i], state_.maxPartitions());
      VELOX_DCHECK_GE(partition, 0);
      // If 'rowsFromNonSpillingPartitions' is not null, it is used to collect
      // the rows from non-spilling partitions when finishes spilling.
      if (FOLLY_UNLIKELY(
              rowsFromNonSpillingPartitions != nullptr &&
              !state_.isPartitionSpilled(partition))) {
        rowsFromNonSpillingPartitions->push_back(rows[i]);
        continue;
      }
      spillRuns_[partition].rows.push_back(rows[i]);
      spillRuns_[partition].numBytes += container_->rowSize(rows[i]);
    }
    if (numRows == 0) {
      break;
    }
  }
}

void Spiller::clearNonSpillingRuns() {
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    if (pendingSpillPartitions_.count(partition) == 0) {
      spillRuns_[partition].clear();
    }
  }
}

std::string Spiller::toString() const {
  return fmt::format(
      "{}\t{}\tMAX_PARTITIONS:{}\tFINALIZED:{}",
      typeName(type_),
      rowType_->toString(),
      state_.maxPartitions(),
      spillFinalized_);
}

int32_t Spiller::Config::spillLevel(uint8_t startBitOffset) const {
  const auto numPartitionBits = hashBitRange.numBits();
  VELOX_CHECK_LE(
      startBitOffset + numPartitionBits,
      64,
      "startBitOffset:{} numPartitionsBits:{}",
      startBitOffset,
      numPartitionBits);
  const int32_t deltaBits = startBitOffset - hashBitRange.begin();
  VELOX_CHECK_GE(deltaBits, 0, "deltaBits:{}", deltaBits);
  VELOX_CHECK_EQ(
      deltaBits % numPartitionBits,
      0,
      "deltaBits:{} numPartitionsBits{}",
      deltaBits,
      numPartitionBits);
  return deltaBits / numPartitionBits;
}

bool Spiller::Config::exceedSpillLevelLimit(uint8_t startBitOffset) const {
  if (startBitOffset + hashBitRange.numBits() > 64) {
    return true;
  }
  if (maxSpillLevel == -1) {
    return false;
  }
  return spillLevel(startBitOffset) > maxSpillLevel;
}

// static
std::string Spiller::typeName(Type type) {
  switch (type) {
    case Type::kOrderBy:
      return "ORDER_BY";
    case Type::kHashJoinBuild:
      return "HASH_JOIN_BUILD";
    case Type::kHashJoinProbe:
      return "HASH_JOIN_PROBE";
    case Type::kAggregate:
      return "AGGREGATE";
    default:
      VELOX_UNREACHABLE("Unknown type: {}", static_cast<int>(type));
      return fmt::format("UNKNOWN TYPE: {}", static_cast<int>(type));
  }
}

void Spiller::fillSpillRuns(std::vector<SpillableStats>& statsList) {
  if (FOLLY_UNLIKELY(type_ == Type::kHashJoinProbe)) {
    VELOX_FAIL("There is no row container for {}", typeName(type_));
  }
  statsList.resize(state_.maxPartitions());
  if (isAllSpilled()) {
    return;
  }
  fillSpillRuns(nullptr);
  for (int partitionNum = 0; partitionNum < state_.maxPartitions();
       ++partitionNum) {
    const auto& spillRun = spillRuns_[partitionNum];
    statsList[partitionNum].numBytes += spillRun.numBytes;
    statsList[partitionNum].numRows += spillRun.rows.size();
  }
}

// static
memory::MappedMemory& Spiller::spillMappedMemory() {
  // Return the top level instance. Since this too may be full,
  // another possibility is to return an emergency instance that
  // delegates to the process wide one and makes a file-backed mmap
  // if the allocation fails.
  return *memory::MappedMemory::getInstance();
}

// static
memory::MemoryPool& Spiller::spillPool() {
  static auto pool = memory::getDefaultScopedMemoryPool();
  return *pool;
}

} // namespace facebook::velox::exec
