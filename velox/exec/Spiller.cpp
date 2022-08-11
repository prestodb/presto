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
    RowContainer& container,
    RowContainer::Eraser eraser,
    RowTypePtr rowType,
    int32_t numSortingKeys,
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
          path,
          targetFileSize,
          pool,
          executor) {}

Spiller::Spiller(
    Type type,
    RowContainer& container,
    RowContainer::Eraser eraser,
    RowTypePtr rowType,
    HashBitRange bits,
    int32_t numSortingKeys,
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
          targetFileSize,
          pool,
          spillMappedMemory()),
      pool_(pool),
      executor_(executor) {
  // TODO: add to support kHashJoin type later.
  VELOX_CHECK_NE(
      type_,
      Type::kHashJoin,
      "Spiller type:{} is not supported yet",
      typeName(type_));
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
  auto& types = container_.columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container_.extractColumn(rows.data(), rows.size(), i, result->childAt(i));
  }
  auto& aggregates = container_.aggregates();
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
  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  assert(!rows.empty());
  int32_t numRows = 0;
  int64_t bytes = 0;
  for (; numRows < limit; ++numRows) {
    bytes += container_.rowSize(rows[nextBatchIndex + numRows]);
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
class RowContainerSpillStream : public SpillStream {
 public:
  RowContainerSpillStream(
      RowTypePtr type,
      int32_t numSortingKeys,
      memory::MemoryPool& pool,
      Spiller::SpillRows&& rows,
      Spiller& spiller)
      : SpillStream(std::move(type), numSortingKeys, pool),
        rows_(std::move(rows)),
        spiller_(spiller) {
    if (!rows_.empty()) {
      nextBatch();
    }
  }

  uint64_t size() const override {
    // 0 means that 'this' does not own spilled data in files.
    return 0;
  }

 private:
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

  Spiller::SpillRows rows_;
  Spiller& spiller_;
  size_t nextBatchIndex_ = 0;
};
} // namespace

std::unique_ptr<SpillStream> Spiller::spillStreamOverRows(int32_t partition) {
  VELOX_CHECK(spillFinalized_);
  VELOX_CHECK_LT(partition, state_.maxPartitions());

  if (!state_.isPartitionSpilled(partition)) {
    return nullptr;
  }
  ensureSorted(spillRuns_[partition]);
  return std::make_unique<RowContainerSpillStream>(
      rowType_,
      container_.keyTypes().size(),
      pool_,
      std::move(spillRuns_[partition].rows),
      *this);
}

void Spiller::ensureSorted(SpillRun& run) {
  // The spill data of a hash join doesn't need to be sorted.
  if (!run.sorted && type_ != Type::kHashJoin) {
    std::sort(
        run.rows.begin(),
        run.rows.end(),
        [&](const char* left, const char* right) {
          return container_.compareRows(left, right) < 0;
        });
    run.sorted = true;
  }
}

std::unique_ptr<Spiller::SpillStatus> Spiller::writeSpill(
    int32_t partition,
    uint64_t maxBytes) {
  VELOX_CHECK_EQ(pendingSpillPartitions_.count(partition), 1);
  // Target size of a single vector of spilled content. One of
  // these will be materialized at a time for each stream of the
  // merge.
  constexpr int32_t kTargetBatchBytes = 1 << 18; // 256K

  RowVectorPtr spillVector;
  auto& run = spillRuns_[partition];
  try {
    ensureSorted(run);
    int64_t totalBytes = 0;
    size_t written = 0;
    while (written < run.rows.size()) {
      totalBytes += extractSpillVector(
          run.rows, 64, kTargetBatchBytes, spillVector, written);
      state_.appendToPartition(partition, spillVector);
      if (totalBytes > maxBytes) {
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

void Spiller::advanceSpill(uint64_t maxBytes) {
  std::vector<std::shared_ptr<AsyncSource<SpillStatus>>> writes;
  for (auto partition = 0; partition < spillRuns_.size(); ++partition) {
    if (pendingSpillPartitions_.count(partition) == 0) {
      continue;
    }
    writes.push_back(std::make_shared<AsyncSource<SpillStatus>>(
        [partition, this, maxBytes]() {
          return writeSpill(partition, maxBytes);
        }));
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

  for (auto& write : writes) {
    const auto result = write->move();

    if (result->error) {
      std::rethrow_exception(result->error);
    }
    auto numWritten = result->rowsWritten;
    spilledRows_ += numWritten;
    auto partition = result->partition;
    auto& run = spillRuns_[partition];
    auto spilled = folly::Range<char**>(run.rows.data(), numWritten);
    eraser_(spilled);
    if (!container_.numRows()) {
      // If the container became empty, free its memory.
      container_.clear();
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

void Spiller::spill(uint64_t targetRows, uint64_t targetBytes) {
  VELOX_CHECK(!spillFinalized_);
  bool hasFilledRuns = false;
  for (;;) {
    auto rowsLeft = container_.numRows();
    auto spaceLeft = container_.stringAllocator().retainedSize() -
        container_.stringAllocator().freeSpace();
    if (rowsLeft == 0 || (rowsLeft <= targetRows && spaceLeft <= targetBytes)) {
      break;
    }
    if (!pendingSpillPartitions_.empty()) {
      advanceSpill(state_.targetFileSize());
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
            "No partition has no spillable data but still doesn't reach the spill target, target rows {}, target bytes {}, rows left {}, bytes left {}",
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

int32_t Spiller::pickNextPartitionToSpill() {
  VELOX_DCHECK_EQ(spillRuns_.size(), state_.maxPartitions());

  // Sort the partitions based on spiller type to pick.
  std::vector<int32_t> partitionIndices(spillRuns_.size());
  std::iota(partitionIndices.begin(), partitionIndices.end(), 0);
  std::sort(
      partitionIndices.begin(),
      partitionIndices.end(),
      [&](int32_t lhs, int32_t rhs) {
        // For non kHashJoin type, we always try to spill from the partition
        // with more spillable data first no matter it has spilled or not. For
        // kHashJoin, we will try to stick with the spilling partitions if they
        // have spillable data.
        //
        // NOTE: the picker loop below will skip the spilled partition which has
        // no spillable data.
        if (type_ == Type::kHashJoin &&
            state_.isPartitionSpilled(lhs) != state_.isPartitionSpilled(rhs)) {
          return state_.isPartitionSpilled(lhs);
        }
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
  fillSpillRuns(&rowsFromNonSpillingPartitions);
  return rowsFromNonSpillingPartitions;
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
    auto numRows = container_.listRows(
        &iterator, rows.size(), RowContainer::kUnlimited, rows.data());
    // Calculate hashes for this batch of spill candidates.
    auto rowSet = folly::Range<char**>(rows.data(), numRows);
    for (auto i = 0; i < container_.keyTypes().size(); ++i) {
      container_.hash(i, rowSet, i > 0, hashes.data());
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
      spillRuns_[partition].numBytes += container_.rowSize(rows[i]);
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
      "Type{}\tRowType:{}\tNum Partitions:{}\tFinalized:{}",
      typeName(type_),
      rowType_->toString(),
      state_.maxPartitions(),
      spillFinalized_);
}

// static
std::string Spiller::typeName(Type type) {
  switch (type) {
    case Type::kOrderBy:
      return "ORDER_BY";
    case Type::kHashJoin:
      return "HASH_JOIN";
    case Type::kAggregate:
      return "AGGREGATE";
    default:
      VELOX_UNREACHABLE("Unknown type: {}", static_cast<int>(type));
      return fmt::format("UNKNOWN TYPE: {}", static_cast<int>(type));
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
