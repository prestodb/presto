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

#include "velox/exec/HashTable.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/process/ProcessBase.h"
#include "velox/common/process/TraceContext.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/vector/VectorTypeUtils.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
// static
std::string BaseHashTable::modeString(HashMode mode) {
  switch (mode) {
    case HashMode::kHash:
      return "HASH";
    case HashMode::kArray:
      return "ARRAY";
    case HashMode::kNormalizedKey:
      return "NORMALIZED_KEY";
    default:
      return fmt::format(
          "Unknown HashTable mode:{}", static_cast<int32_t>(mode));
  }
}

template <bool ignoreNullKeys>
HashTable<ignoreNullKeys>::HashTable(
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    const std::vector<Accumulator>& accumulators,
    const std::vector<TypePtr>& dependentTypes,
    bool allowDuplicates,
    bool isJoinBuild,
    bool hasProbedFlag,
    uint32_t minTableSizeForParallelJoinBuild,
    memory::MemoryPool* pool,
    const std::shared_ptr<velox::HashStringAllocator>& stringArena)
    : BaseHashTable(std::move(hashers)),
      minTableSizeForParallelJoinBuild_(minTableSizeForParallelJoinBuild),
      isJoinBuild_(isJoinBuild) {
  std::vector<TypePtr> keys;
  for (auto& hasher : hashers_) {
    keys.push_back(hasher->type());
    if (!VectorHasher::typeKindSupportsValueIds(hasher->typeKind())) {
      hashMode_ = HashMode::kHash;
    }
  }

  rows_ = std::make_unique<RowContainer>(
      keys,
      !ignoreNullKeys,
      accumulators,
      dependentTypes,
      allowDuplicates,
      isJoinBuild,
      hasProbedFlag,
      hashMode_ != HashMode::kHash,
      pool,
      stringArena);
  nextOffset_ = rows_->nextOffset();
}

class ProbeState {
 public:
  enum class Operation { kProbe, kInsert, kErase };
  // Special tag for an erased entry. This counts as occupied for probe and as
  // empty for insert. If a tag word with empties gets an erase, we make the
  // erased tag empty. If the tag word getting the erase has no empties, the
  // erase is marked with a tombstone. A probe always stops with a tag word with
  // empties. Adding an empty to a tag word with no empties would break probes
  // that needed to skip this tag word. This is standard practice for open
  // addressing hash tables. F14 has more sophistication in this but we do not
  // need it here since erase is very rare except spilling and is not expected
  // to change the load factor by much in the expected uses.
  static constexpr uint8_t kTombstoneTag = 0x7f;
  static constexpr uint8_t kEmptyTag = 0x00;
  static constexpr int32_t kFullMask = 0xffff;

  int32_t row() const {
    return row_;
  }

  // Use one instruction to make 16 copies of the tag being searched for
  template <typename Table>
  inline void preProbe(const Table& table, uint64_t hash, int32_t row) {
    row_ = row;
    bucketOffset_ = table.bucketOffset(hash);
    const auto tag = BaseHashTable::hashTag(hash);
    wantedTags_ = BaseHashTable::TagVector::broadcast(tag);
    group_ = nullptr;
    indexInTags_ = kNotSet;
    __builtin_prefetch(
        reinterpret_cast<uint8_t*>(table.table_) + bucketOffset_);
  }

  // Use one instruction to load 16 tags. Use another one instruction
  // to compare the tag being searched for to 16 tags.
  // If there is a match, load corresponding data from the table.
  template <Operation op = Operation::kProbe, typename Table>
  inline void firstProbe(const Table& table, int32_t firstKey) {
    tagsInTable_ = BaseHashTable::loadTags(
        reinterpret_cast<uint8_t*>(table.table_), bucketOffset_);
    table.incrementTagLoads();
    hits_ = simd::toBitMask(tagsInTable_ == wantedTags_);
    if (hits_) {
      loadNextHit<op>(table, firstKey);
    }
  }

  template <Operation op, typename Compare, typename Insert, typename Table>
  inline char* fullProbe(
      Table& table,
      int32_t firstKey,
      Compare compare,
      Insert insert,
      int64_t& numTombstones,
      bool extraCheck,
      TableInsertPartitionInfo* partitionInfo = nullptr) {
    VELOX_DCHECK(partitionInfo == nullptr || op == Operation::kInsert);

    if (group_ && compare(group_, row_)) {
      if (op == Operation::kErase) {
        eraseHit(table, numTombstones);
      }
      table.incrementHits();
      return group_;
    }

    auto* alreadyChecked = group_;
    if (extraCheck) {
      tagsInTable_ = table.loadTags(bucketOffset_);
      hits_ = simd::toBitMask(tagsInTable_ == wantedTags_);
    }

    const int64_t startBucketOffset = bucketOffset_;
    int64_t insertBucketOffset = -1;
    const auto kEmptyGroup = BaseHashTable::TagVector::broadcast(0);
    const auto kTombstoneGroup =
        BaseHashTable::TagVector::broadcast(kTombstoneTag);
    for (int64_t numProbedBuckets = 0; numProbedBuckets < table.numBuckets();
         ++numProbedBuckets) {
      if constexpr (op == Operation::kInsert) {
        if (partitionInfo != nullptr) {
          if (FOLLY_UNLIKELY(!partitionInfo->inRange(bucketOffset_))) {
            // If we have passed the partition boundary, then call insert to put
            // row in overflows.
            return insert(row_, bucketOffset_);
          }
          if (FOLLY_UNLIKELY(
                  (bucketOffset_ <= startBucketOffset) &&
                  (numProbedBuckets > 0))) {
            // If this is the last bucket and wrap around, then call insert to
            // put row in overflows.
            return insert(row_, partitionInfo->end);
          }
        }
      }

      while (hits_ > 0) {
        loadNextHit<op>(table, firstKey);
        if (!(extraCheck && group_ == alreadyChecked) &&
            compare(group_, row_)) {
          if (op == Operation::kErase) {
            eraseHit(table, numTombstones);
          }
          table.incrementHits();
          return group_;
        }
      }

      uint16_t empty = simd::toBitMask(tagsInTable_ == kEmptyGroup) & kFullMask;
      if (empty > 0) {
        if (op == Operation::kProbe) {
          return nullptr;
        }
        if (op == Operation::kErase) {
          VELOX_FAIL("Erasing non-existing entry");
        }
        if (indexInTags_ != kNotSet) {
          // We came to the end of the probe without a hit. We replace the first
          // tombstone on the way.
          --numTombstones;
          return insert(row_, insertBucketOffset + indexInTags_);
        }
        auto pos = bits::getAndClearLastSetBit(empty);
        return insert(row_, bucketOffset_ + pos);
      }
      if (op == Operation::kInsert && indexInTags_ == kNotSet) {
        // We passed through a full group.
        uint16_t tombstones =
            simd::toBitMask(tagsInTable_ == kTombstoneGroup) & kFullMask;
        if (tombstones > 0) {
          insertBucketOffset = bucketOffset_;
          indexInTags_ = bits::getAndClearLastSetBit(tombstones);
        }
      }
      bucketOffset_ = table.nextBucketOffset(bucketOffset_);
      tagsInTable_ = table.loadTags(bucketOffset_);
      hits_ = simd::toBitMask(tagsInTable_ == wantedTags_);
    }
    // Throws here if we have looped through all the buckets in the table.
    VELOX_FAIL(
        "Have looped through all the buckets in table: {}", table.toString());
  }

  template <typename Table>
  FOLLY_ALWAYS_INLINE char* joinNormalizedKeyFullProbe(
      const Table& table,
      const uint64_t* keys) {
    if (group_ && RowContainer::normalizedKey(group_) == keys[row_]) {
      table.incrementHits();
      return group_;
    }
    const auto kEmptyGroup = BaseHashTable::TagVector::broadcast(kEmptyTag);
    for (int64_t numProbedBuckets = 0; numProbedBuckets < table.numBuckets();
         ++numProbedBuckets) {
      if (!hits_) {
        const uint16_t empty = simd::toBitMask(tagsInTable_ == kEmptyGroup);
        if (empty) {
          return nullptr;
        }
      } else {
        loadNextHit<Operation::kProbe>(
            table, -static_cast<int32_t>(sizeof(normalized_key_t)));
        if (RowContainer::normalizedKey(group_) == keys[row_]) {
          table.incrementHits();
          return group_;
        }
        continue;
      }
      bucketOffset_ = table.nextBucketOffset(bucketOffset_);
      tagsInTable_ = BaseHashTable::loadTags(
          reinterpret_cast<uint8_t*>(table.table_), bucketOffset_);
      hits_ = simd::toBitMask(tagsInTable_ == wantedTags_) & kFullMask;
    }
    // Throws here if we have looped through all the buckets in the table.
    VELOX_FAIL("Have looped through all the buckets in table");
  }

 private:
  static constexpr uint8_t kNotSet = 0xff;

  template <Operation op, typename Table>
  inline void loadNextHit(Table& table, int32_t firstKey) {
    const int32_t hit = bits::getAndClearLastSetBit(hits_);

    if (op == Operation::kErase) {
      indexInTags_ = hit;
    }
    group_ = table.row(bucketOffset_, hit);
    __builtin_prefetch(group_ + firstKey);
    table.incrementRowLoads();
  }

  template <typename Table>
  void eraseHit(Table& table, int64_t& numTombstones) {
    const auto kEmptyGroup = BaseHashTable::TagVector::broadcast(kEmptyTag);
    const bool hasEmptyGroup =
        simd::toBitMask(tagsInTable_ == kEmptyGroup) != 0;

    table.bucketAt(bucketOffset_)
        ->setTag(indexInTags_, hasEmptyGroup ? 0 : kTombstoneTag);
    numTombstones += !hasEmptyGroup;
  }

  char* group_;
  BaseHashTable::TagVector wantedTags_;
  BaseHashTable::TagVector tagsInTable_;
  int32_t row_;
  int64_t bucketOffset_;
  BaseHashTable::MaskType hits_;

  // If op is kErase, this is the index of the current hit within the
  // group of 'tagIndex_'. If op is kInsert, this is the index of the
  // first tombstone in the group of 'bucketOffset_'. Insert
  // replaces the first tombstone it finds. If it finds an empty
  // before finding a tombstone, it replaces the empty as soon as it
  // sees it. But the tombstone can be replaced only after finding an
  // empty and thus determining that the item being inserted is not in
  // the table.
  uint8_t indexInTags_ = kNotSet;
};

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::storeKeys(
    HashLookup& lookup,
    vector_size_t row) {
  for (int32_t i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    rows_->store(hasher->decodedVector(), row, lookup.hits[row], i); // NOLINT
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::storeRowPointer(
    uint64_t index,
    uint64_t hash,
    char* row) {
  if (hashMode_ == HashMode::kArray) {
    reinterpret_cast<char**>(table_)[index] = row;
    return;
  }
  const int64_t offset = bucketOffset(index);
  auto* bucket = bucketAt(offset);
  const auto slotIndex = index & (sizeof(TagVector) - 1);
  bucket->setTag(slotIndex, hashTag(hash));
  bucket->setPointer(slotIndex, row);
}

template <bool ignoreNullKeys>
char* HashTable<ignoreNullKeys>::insertEntry(
    HashLookup& lookup,
    uint64_t index,
    vector_size_t row) {
  char* group = rows_->newRow();
  lookup.hits[row] = group; // NOLINT
  storeKeys(lookup, row);
  storeRowPointer(index, lookup.hashes[row], group);
  if (hashMode_ == HashMode::kNormalizedKey) {
    // We store the unique digest of key values (normalized key) in
    // the word below the row. Space was reserved in the allocation
    // unless we have given up on normalized keys.
    RowContainer::normalizedKey(group) = lookup.normalizedKeys[row]; // NOLINT
  }
  ++numDistinct_;
  lookup.newGroups.push_back(row);
  return group;
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::compareKeys(
    const char* group,
    HashLookup& lookup,
    vector_size_t row) {
  int32_t numKeys = lookup.hashers.size();
  // The loop runs at least once. Allow for first comparison to fail
  // before loop end check.
  int32_t i = 0;
  do {
    auto& hasher = lookup.hashers[i];
    if (!rows_->equals<!ignoreNullKeys>(
            group, rows_->columnAt(i), hasher->decodedVector(), row)) {
      return false;
    }
  } while (++i < numKeys);
  return true;
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::compareKeys(
    const char* group,
    const char* inserted) {
  auto numKeys = hashers_.size();
  int32_t i = 0;
  do {
    if (rows_->compare(group, inserted, i, CompareFlags{true, true})) {
      return false;
    }
  } while (++i < numKeys);
  return true;
}

template <bool ignoreNullKeys>
template <bool isJoin, bool isNormalizedKey>
FOLLY_ALWAYS_INLINE void HashTable<ignoreNullKeys>::fullProbe(
    HashLookup& lookup,
    ProbeState& state,
    bool extraCheck) {
  constexpr ProbeState::Operation op =
      isJoin ? ProbeState::Operation::kProbe : ProbeState::Operation::kInsert;
  if constexpr (isNormalizedKey) {
    // NOLINT
    lookup.hits[state.row()] = state.fullProbe<op>(
        *this,
        -static_cast<int32_t>(sizeof(normalized_key_t)),
        [&](char* group, int32_t row) INLINE_LAMBDA {
          return RowContainer::normalizedKey(group) ==
              lookup.normalizedKeys[row];
        },
        [&](int32_t row, uint64_t index) {
          return isJoin ? nullptr : insertEntry(lookup, index, row);
        },
        numTombstones_,
        !isJoin && extraCheck);
    return;
  }
  // NOLINT
  lookup.hits[state.row()] = state.fullProbe<op>(
      *this,
      0,
      [&](char* group, int32_t row) { return compareKeys(group, lookup, row); },
      [&](int32_t row, uint64_t index) {
        return isJoin ? nullptr : insertEntry(lookup, index, row);
      },
      numTombstones_,
      !isJoin && extraCheck);
}

namespace {
// Normalized keys have non0-random bits. Bits need to be propagated
// up to make a tag byte and down so that non-lowest bits of
// normalized key affect the hash table index.
inline uint64_t mixNormalizedKey(uint64_t k, uint8_t bits) {
  return folly::hasher<uint64_t>()(k);
}

void populateNormalizedKeys(HashLookup& lookup, int8_t sizeBits) {
  lookup.normalizedKeys.resize(lookup.rows.back() + 1);
  uint64_t* __restrict hashes = lookup.hashes.data();
  uint64_t* __restrict keys = lookup.normalizedKeys.data();
  int32_t end = lookup.rows.back() + 1;
  if (end / 4 < lookup.rows.size()) {
    // For more than 1/4 of the positions in use, run the loop on all
    // elements, since the loop will do 4 at a time.
    for (auto row = 0; row < end; ++row) {
      auto hash = hashes[row];
      keys[row] = hash; // NOLINT
      hashes[row] = mixNormalizedKey(hash, sizeBits);
    }
    return;
  }
  for (auto row : lookup.rows) {
    auto hash = hashes[row];
    keys[row] = hash; // NOLINT
    hashes[row] = mixNormalizedKey(hash, sizeBits);
  }
}
} // namespace

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::groupProbe(HashLookup& lookup) {
  incrementProbes(lookup.rows.size());

  if (hashMode_ == HashMode::kArray) {
    arrayGroupProbe(lookup);
    return;
  }
  // Do size-based rehash before mixing hashes from normalized keys
  // because the size of the table affects the mixing.
  checkSize(lookup.rows.size(), false);
  if (hashMode_ == HashMode::kNormalizedKey) {
    populateNormalizedKeys(lookup, sizeBits_);
    groupNormalizedKeyProbe(lookup);
    return;
  }
  ProbeState state1;
  ProbeState state2;
  ProbeState state3;
  ProbeState state4;
  int32_t probeIndex = 0;
  int32_t numProbes = lookup.rows.size();
  auto rows = lookup.rows.data();
  for (; probeIndex + 4 <= numProbes; probeIndex += 4) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 1];
    state2.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 2];
    state3.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 3];
    state4.preProbe(*this, lookup.hashes[row], row);

    state1.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
    state2.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
    state3.firstProbe<ProbeState::Operation::kInsert>(*this, 0);
    state4.firstProbe<ProbeState::Operation::kInsert>(*this, 0);

    fullProbe<false>(lookup, state1, false);
    fullProbe<false>(lookup, state2, true);
    fullProbe<false>(lookup, state3, true);
    fullProbe<false>(lookup, state4, true);
  }
  for (; probeIndex < numProbes; ++probeIndex) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    state1.firstProbe(*this, 0);
    fullProbe<false>(lookup, state1, false);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::groupNormalizedKeyProbe(HashLookup& lookup) {
  ProbeState state1;
  ProbeState state2;
  ProbeState state3;
  ProbeState state4;
  int32_t probeIndex = 0;
  int32_t numProbes = lookup.rows.size();
  auto rows = lookup.rows.data();
  constexpr int32_t kKeyOffset =
      -static_cast<int32_t>(sizeof(normalized_key_t));
  for (; probeIndex + 4 <= numProbes; probeIndex += 4) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 1];
    state2.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 2];
    state3.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 3];
    state4.preProbe(*this, lookup.hashes[row], row);
    state1.firstProbe<ProbeState::Operation::kInsert>(*this, kKeyOffset);
    state2.firstProbe<ProbeState::Operation::kInsert>(*this, kKeyOffset);
    state3.firstProbe<ProbeState::Operation::kInsert>(*this, kKeyOffset);
    state4.firstProbe<ProbeState::Operation::kInsert>(*this, kKeyOffset);
    fullProbe<false, true>(lookup, state1, false);
    fullProbe<false, true>(lookup, state2, true);
    fullProbe<false, true>(lookup, state3, true);
    fullProbe<false, true>(lookup, state4, true);
  }
  for (; probeIndex < numProbes; ++probeIndex) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    state1.firstProbe(*this, kKeyOffset);
    fullProbe<false, true>(lookup, state1, false);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::arrayGroupProbe(HashLookup& lookup) {
  VELOX_DCHECK(!lookup.hashes.empty());
  VELOX_DCHECK(!lookup.hits.empty());

  int32_t numProbes = lookup.rows.size();
  const vector_size_t* rows = lookup.rows.data();
  auto hashes = lookup.hashes.data();
  auto groups = lookup.hits.data();
  int32_t i = 0;
  if (process::hasAvx2() && simd::isDense(rows, numProbes)) {
    auto allZero = xsimd::broadcast<int64_t>(0);
    constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
    auto start = rows[0];
    auto end = start + numProbes - kWidth;
    for (i = start; i <= end; i += kWidth) {
      auto loaded = simd::gather(
          reinterpret_cast<const int64_t*>(table_),
          reinterpret_cast<const int64_t*>(hashes + i));
      loaded.store_unaligned(reinterpret_cast<int64_t*>(groups + i));
      auto misses = simd::toBitMask(loaded == allZero);
      if (LIKELY(!misses)) {
        continue;
      }
      for (auto miss = 0; miss < kWidth; ++miss) {
        auto row = i + miss;
        if (!groups[row]) {
          auto index = hashes[row];
          auto hit = table_[index];
          if (!hit) {
            hit = insertEntry(lookup, index, row);
          }
          groups[row] = hit;
        }
      }
    }
    i -= start;
  }
  for (; i < numProbes; ++i) {
    auto row = rows[i];
    uint64_t index = hashes[row];
    VELOX_DCHECK_LT(index, capacity_);
    char* group = table_[index];
    if (UNLIKELY(!group)) {
      group = insertEntry(lookup, index, row);
    }
    groups[row] = group;
    lookup.hits[row] = group; // NOLINT
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::joinProbe(HashLookup& lookup) {
  incrementProbes(lookup.rows.size());
  if (hashMode_ == HashMode::kArray) {
    arrayJoinProbe(lookup);
    return;
  }
  if (hashMode_ == HashMode::kNormalizedKey) {
    populateNormalizedKeys(lookup, sizeBits_);
    joinNormalizedKeyProbe(lookup);
    return;
  }
  int32_t probeIndex = 0;
  int32_t numProbes = lookup.rows.size();
  const vector_size_t* rows = lookup.rows.data();
  ProbeState state1;
  ProbeState state2;
  ProbeState state3;
  ProbeState state4;
  for (; probeIndex + 4 <= numProbes; probeIndex += 4) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 1];
    state2.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 2];
    state3.preProbe(*this, lookup.hashes[row], row);
    row = rows[probeIndex + 3];
    state4.preProbe(*this, lookup.hashes[row], row);
    state1.firstProbe(*this, 0);
    state2.firstProbe(*this, 0);
    state3.firstProbe(*this, 0);
    state4.firstProbe(*this, 0);
    fullProbe<true>(lookup, state1, false);
    fullProbe<true>(lookup, state2, false);
    fullProbe<true>(lookup, state3, false);
    fullProbe<true>(lookup, state4, false);
  }
  for (; probeIndex < numProbes; ++probeIndex) {
    int32_t row = rows[probeIndex];
    state1.preProbe(*this, lookup.hashes[row], row);
    state1.firstProbe(*this, 0);
    fullProbe<true>(lookup, state1, false);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::arrayJoinProbe(HashLookup& lookup) {
  // Rows are nearly always consecutive.
  auto& rows = lookup.rows;
  auto hashes = lookup.hashes.data();
  auto hits = lookup.hits.data();
  auto numRows = rows.size();
  int32_t i = 0;
  constexpr int32_t kBatchSize = xsimd::batch<int64_t>::size;
  constexpr int32_t kStep = kBatchSize * 2;
  // We loop 2 vectors at a time for fewer switches. The rows are in practice
  // always contiguous.
  for (; i + kStep <= numRows; i += kStep) {
    auto firstRow = rows[i];
    if (rows[i + kStep - 1] - firstRow == kStep - 1) {
      // kStep consecutive.
      simd::gather(
          reinterpret_cast<const int64_t*>(table_),
          reinterpret_cast<const int64_t*>(hashes + firstRow))
          .store_unaligned(reinterpret_cast<int64_t*>(hits) + firstRow);
      simd::gather(
          reinterpret_cast<const int64_t*>(table_),
          reinterpret_cast<const int64_t*>(hashes + firstRow + kBatchSize))
          .store_unaligned(
              reinterpret_cast<int64_t*>(hits) + firstRow + kBatchSize);
    } else {
      for (auto j = i; j < i + kStep; ++j) {
        auto row = rows[j];
        auto index = hashes[row];
        VELOX_DCHECK_LT(index, capacity_);
        hits[row] = table_[index]; // NOLINT
      }
    }
  }
  for (; i < numRows; ++i) {
    auto row = rows[i];
    auto index = hashes[row];
    VELOX_DCHECK_LT(index, capacity_);
    hits[row] = table_[index]; // NOLINT
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::joinNormalizedKeyProbe(HashLookup& lookup) {
  int32_t probeIndex = 0;
  int32_t numProbes = lookup.rows.size();
  const vector_size_t* rows = lookup.rows.data();
  constexpr int32_t groupSize = 64;
  ProbeState states[groupSize];
  const uint64_t* keys = lookup.normalizedKeys.data();
  const uint64_t* hashes = lookup.hashes.data();
  char** hits = lookup.hits.data();
  constexpr int32_t kKeyOffset =
      -static_cast<int32_t>(sizeof(normalized_key_t));
  for (; probeIndex + groupSize <= numProbes; probeIndex += groupSize) {
    for (int32_t i = 0; i < groupSize; ++i) {
      int32_t row = rows[probeIndex + i];
      states[i].preProbe(*this, hashes[row], row);
    }
    for (int32_t i = 0; i < groupSize; ++i) {
      states[i].firstProbe(*this, kKeyOffset);
    }
    for (int32_t i = 0; i < groupSize; ++i) {
      hits[states[i].row()] = states[i].joinNormalizedKeyFullProbe(*this, keys);
    }
  }
  for (; probeIndex < numProbes; ++probeIndex) {
    int32_t row = rows[probeIndex];
    states[0].preProbe(*this, lookup.hashes[row], row);
    states[0].firstProbe(*this, 0);
    hits[row] = states[0].joinNormalizedKeyFullProbe(*this, keys);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::allocateTables(uint64_t size) {
  VELOX_CHECK(bits::isPowerOfTwo(size), "Size is not a power of two: {}", size);
  VELOX_CHECK_GT(size, 0);
  capacity_ = size;
  const uint64_t byteSize = capacity_ * tableSlotSize();
  VELOX_CHECK_EQ(byteSize % kBucketSize, 0);
  numTombstones_ = 0;
  sizeMask_ = byteSize - 1;
  numBuckets_ = byteSize / kBucketSize;
  sizeBits_ = __builtin_popcountll(sizeMask_);
  bucketOffsetMask_ = sizeMask_ & ~(kBucketSize - 1);
  // The total size is 8 bytes per slot, in groups of 16 slots with 16 bytes of
  // tags and 16 * 6 bytes of pointers and a padding of 16 bytes to round up the
  // cache line.
  const auto numPages =
      memory::AllocationTraits::numPages(size * tableSlotSize());
  rows_->pool()->allocateContiguous(numPages, tableAllocation_);
  table_ = tableAllocation_.data<char*>();
  memset(table_, 0, capacity_ * sizeof(char*));
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::clear() {
  rows_->clear();
  if (table_) {
    // All modes have 8 bytes per slot.
    memset(table_, 0, capacity_ * sizeof(char*));
  }
  numDistinct_ = 0;
  numTombstones_ = 0;
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::checkSize(
    int32_t numNew,
    bool initNormalizedKeys) {
  // NOTE: the way we decide the table size and trigger rehash, guarantees the
  // table should always have free slots after the insertion.
  VELOX_CHECK(
      capacity_ == 0 || capacity_ > (numDistinct_ + numTombstones_),
      "size {}, numDistinct {}, numTombstoneRows {}, hashMode {}",
      capacity_,
      numDistinct_,
      numTombstones_,
      hashMode_);

  const int64_t newNumDistincts = numNew + numDistinct_;
  if (table_ == nullptr || capacity_ == 0) {
    const auto newSize = newHashTableEntries(numDistinct_, numNew);
    allocateTables(newSize);
    if (numDistinct_ > 0) {
      rehash(initNormalizedKeys);
    }
    // We are not always able to reuse a tombstone slot as a free one for hash
    // collision handling purpose. For example, if all the table slots are
    // either occupied or tombstone, then we can't store any new entry in the
    // table. Also, if there is non-trivial amount of tombstone slots in table,
    // then the table lookup will become slow. Given that, we treat tombstone
    // slot as non-empty slot here to decide whether to trigger rehash or not.
  } else if (newNumDistincts > rehashSize()) {
    // NOTE: we need to plus one here as number itself could be power of two.
    const auto newCapacity = bits::nextPowerOfTwo(
        std::max(newNumDistincts, capacity_ - numTombstones_) + 1);
    allocateTables(newCapacity);
    rehash(initNormalizedKeys);
  }
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::hashRows(
    folly::Range<char**> rows,
    bool initNormalizedKeys,
    raw_vector<uint64_t>& hashes) {
  if (rows.empty()) {
    return true;
  }
  if (!initNormalizedKeys && hashMode_ == HashMode::kNormalizedKey) {
    for (auto i = 0; i < rows.size(); ++i) {
      hashes[i] =
          mixNormalizedKey(RowContainer::normalizedKey(rows[i]), sizeBits_);
    }
    return true;
  }

  for (int32_t i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    if (hashMode_ == HashMode::kHash) {
      rows_->hash(i, rows, i > 0, hashes.data());
    } else {
      // Array or normalized key.
      auto column = rows_->columnAt(i);
      if (!hasher->computeValueIdsForRows(
              rows.data(),
              rows.size(),
              column.offset(),
              column.nullByte(),
              ignoreNullKeys ? 0 : column.nullMask(),
              hashes)) {
        // Must reconsider 'hashMode_' and start over.
        return false;
      }
    }
  }
  if (hashMode_ == HashMode::kNormalizedKey && initNormalizedKeys) {
    for (auto i = 0; i < rows.size(); ++i) {
      RowContainer::normalizedKey(rows[i]) = hashes[i];
      hashes[i] = mixNormalizedKey(hashes[i], sizeBits_);
    }
  }
  return true;
}

namespace {
template <typename Source>
void syncWorkItems(
    std::vector<std::shared_ptr<Source>>& items,
    std::exception_ptr& error,
    CpuWallTiming time,
    bool log = false) {
  // All items must be synced also in case of error because the items
  // hold references to the table and rows which could be destructed
  // if unwinding the stack did not pause to sync.
  for (auto& item : items) {
    try {
      item->move();
      time.add(item->prepareTiming());
    } catch (const std::exception& e) {
      if (log) {
        LOG(ERROR) << "Error in async hash build: " << e.what();
      }
      error = std::current_exception();
    }
  }
}
} // namespace

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::canApplyParallelJoinBuild() const {
  if (!isJoinBuild_ || buildExecutor_ == nullptr) {
    return false;
  }
  if (hashMode_ == HashMode::kArray) {
    return false;
  }
  if (otherTables_.empty()) {
    return false;
  }
  return (capacity_ / (1 + otherTables_.size())) >
      minTableSizeForParallelJoinBuild_;
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::parallelJoinBuild() {
  process::TraceContext trace("HashTable::parallelJoinBuild");
  TestValue::adjust(
      "facebook::velox::exec::HashTable::parallelJoinBuild", rows_->pool());
  VELOX_CHECK_LE(1 + otherTables_.size(), std::numeric_limits<uint8_t>::max());
  const uint8_t numPartitions = 1 + otherTables_.size();
  VELOX_CHECK_GT(
      capacity_ / numPartitions,
      minTableSizeForParallelJoinBuild_,
      "Less than {} entries per partition for parallel build",
      minTableSizeForParallelJoinBuild_);
  buildPartitionBounds_.resize(numPartitions + 1);
  // Pad the tail of buildPartitionBounds_ to max int.
  std::fill(
      buildPartitionBounds_.begin(),
      buildPartitionBounds_.begin() + buildPartitionBounds_.capacity(),
      std::numeric_limits<PartitionBoundIndexType>::max());

  // The partitioning is in terms of ranges of bucket offset.
  for (auto i = 0; i < numPartitions; ++i) {
    // The bounds are the closes tag/row pointer group bound, always cache
    // line aligned.
    buildPartitionBounds_[i] =
        bits::roundUp(((sizeMask_ + 1) / numPartitions) * i, kBucketSize);
    // Bounds must always be positive
    VELOX_CHECK_GE(
        buildPartitionBounds_[i],
        0,
        "Turn on VELOX_ENABLE_INT64_BUILD_PARTITION_BOUND to avoid integer overflow in buildPartitionBounds_");
  }
  buildPartitionBounds_.back() = sizeMask_ + 1;
  std::vector<std::shared_ptr<AsyncSource<bool>>> partitionSteps;
  std::vector<std::shared_ptr<AsyncSource<bool>>> buildSteps;
  // rowPartitions are used in the async threads, so declare them before the
  // sync guard.
  std::vector<std::unique_ptr<RowPartitions>> rowPartitions;
  auto sync = folly::makeGuard([&]() {
    // This is executed on returning path, possibly in unwinding, so must not
    // throw.
    std::exception_ptr error;
    syncWorkItems(partitionSteps, error, offThreadBuildTiming_, true);
    syncWorkItems(buildSteps, error, offThreadBuildTiming_, true);
  });

  const auto getTable = [this](size_t i) INLINE_LAMBDA {
    return i == 0 ? this : otherTables_[i - 1].get();
  };

  // This step can involve large memory allocations, so there is a chance of
  // OOMs here. Do it before any async work is started to reduce the chances of
  // concurrency issues.
  rowPartitions.reserve(numPartitions);
  for (auto i = 0; i < numPartitions; ++i) {
    auto* table = getTable(i);
    rowPartitions.push_back(table->rows()->createRowPartitions(*rows_->pool()));
  }

  // The parallel table partitioning step.
  for (auto i = 0; i < numPartitions; ++i) {
    auto* table = getTable(i);
    partitionSteps.push_back(std::make_shared<AsyncSource<bool>>(
        [this, table, rawRowPartitions = rowPartitions[i].get()]() {
          partitionRows(*table, *rawRowPartitions);
          return std::make_unique<bool>(true);
        }));
    assert(!partitionSteps.empty()); // lint
    buildExecutor_->add([step = partitionSteps.back()]() { step->prepare(); });
  }

  std::exception_ptr error;
  syncWorkItems(partitionSteps, error, offThreadBuildTiming_);
  if (error != nullptr) {
    std::rethrow_exception(error);
  }

  // The parallel table building step.
  std::vector<std::vector<char*>> overflowPerPartition(numPartitions);
  for (auto i = 0; i < numPartitions; ++i) {
    buildSteps.push_back(std::make_shared<AsyncSource<bool>>(
        [this, i, &overflowPerPartition, &rowPartitions]() {
          buildJoinPartition(i, rowPartitions, overflowPerPartition[i]);
          return std::make_unique<bool>(true);
        }));
    VELOX_CHECK(!buildSteps.empty());
    buildExecutor_->add([step = buildSteps.back()]() { step->prepare(); });
  }
  syncWorkItems(buildSteps, error, offThreadBuildTiming_);
  if (error != nullptr) {
    std::rethrow_exception(error);
  }

  raw_vector<uint64_t> hashes;
  for (auto i = 0; i < numPartitions; ++i) {
    auto& overflows = overflowPerPartition[i];
    hashes.resize(overflows.size());
    hashRows(
        folly::Range<char**>(overflows.data(), overflows.size()),
        false,
        hashes);
    insertForJoin(overflows.data(), hashes.data(), overflows.size(), nullptr);
    auto table = i == 0 ? this : otherTables_[i - 1].get();
    VELOX_CHECK_EQ(table->rows()->numRows(), table->numParallelBuildRows_);
  }
}

namespace {
// Returns an index into 'buildPartitionBounds_' given an index into tags of the
// HashTable.
int32_t findPartition(
    PartitionBoundIndexType index,
    const PartitionBoundIndexType* bounds,
    int32_t numPartitions) {
  // The partition bounds are padded to batch size.
  constexpr int32_t kBatch = xsimd::batch<PartitionBoundIndexType>::size;
  auto indexVector = xsimd::batch<PartitionBoundIndexType>::broadcast(index);
  for (auto i = 1; i < numPartitions; i += kBatch) {
    auto bits = simd::toBitMask(
        indexVector <
        xsimd::batch<PartitionBoundIndexType>::load_unaligned(bounds + i));
    if (bits) {
      return i + __builtin_ctz(bits) - 1;
    }
  }
  VELOX_UNREACHABLE("Partition index out of range");
}
} // namespace

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::partitionRows(
    HashTable<ignoreNullKeys>& subtable,
    RowPartitions& rowPartitions) {
  constexpr int32_t kBatch = 1024;
  raw_vector<char*> rows(kBatch);
  raw_vector<uint64_t> hashes(kBatch);
  raw_vector<uint8_t> partitions(kBatch);
  RowContainerIterator iter;
  while (auto numRows = subtable.rows_->listRows(
             &iter, kBatch, RowContainer::kUnlimited, rows.data())) {
    hashRows(folly::Range<char**>(rows.data(), numRows), true, hashes);
    VELOX_DCHECK_EQ(
        0,
        buildPartitionBounds_.capacity() %
            xsimd::batch<PartitionBoundIndexType>::size,
        "partition bounds must be padded to SIMD width");
    for (auto i = 0; i < numRows; ++i) {
      auto index = bucketOffset(hashes[i]);
      partitions[i] = findPartition(
          index, buildPartitionBounds_.data(), buildPartitionBounds_.size());
    }
    rowPartitions.appendPartitions(
        folly::Range<const uint8_t*>(partitions.data(), numRows));
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::buildJoinPartition(
    uint8_t partition,
    const std::vector<std::unique_ptr<RowPartitions>>& rowPartitions,
    std::vector<char*>& overflow) {
  constexpr int32_t kBatch = 1024;
  raw_vector<char*> rows(kBatch);
  raw_vector<uint64_t> hashes(kBatch);
  const int32_t numPartitions = 1 + otherTables_.size();
  TableInsertPartitionInfo partitionInfo{
      buildPartitionBounds_[partition],
      buildPartitionBounds_[partition + 1],
      overflow};
  for (auto i = 0; i < numPartitions; ++i) {
    auto* table = i == 0 ? this : otherTables_[i - 1].get();
    RowContainerIterator iter;
    while (const auto numRows = table->rows_->listPartitionRows(
               iter, partition, kBatch, *rowPartitions[i], rows.data())) {
      hashRows(folly::Range(rows.data(), numRows), false, hashes);
      insertForJoin(rows.data(), hashes.data(), numRows, &partitionInfo);
      table->numParallelBuildRows_ += numRows;
    }
  }
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::insertBatch(
    char** groups,
    int32_t numGroups,
    raw_vector<uint64_t>& hashes,
    bool initNormalizedKeys) {
  if (!hashRows(folly::Range(groups, numGroups), initNormalizedKeys, hashes)) {
    return false;
  }
  if (isJoinBuild_) {
    insertForJoin(groups, hashes.data(), numGroups);
  } else {
    insertForGroupBy(groups, hashes.data(), numGroups);
  }
  return true;
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::insertForGroupBy(
    char** groups,
    uint64_t* hashes,
    int32_t numGroups) {
  if (hashMode_ == HashMode::kArray) {
    for (auto i = 0; i < numGroups; ++i) {
      auto index = hashes[i];
      VELOX_CHECK_LT(index, capacity_);
      VELOX_CHECK_NULL(table_[index]);
      table_[index] = groups[i];
    }
  } else {
    constexpr int32_t kPrefetchDistance = 10;
    for (int32_t i = 0; i < numGroups; ++i) {
      auto hash = hashes[i];
      auto offset = this->bucketOffset(hash);
      auto tagsInTable =
          BaseHashTable::loadTags(reinterpret_cast<uint8_t*>(table_), offset);
      if (i + kPrefetchDistance < numGroups) {
        // Prefetch a future cache line. kPrefetchDistance positions ahead
        // should be beyond the CPU's out of order window.
        __builtin_prefetch(
            reinterpret_cast<char*>(table_) +
            bucketOffset(hashes[i + kPrefetchDistance]));
      }
      bool inserted{false};
      for (int64_t numProbedBuckets = 0; numProbedBuckets < numBuckets();
           ++numProbedBuckets) {
        MaskType free =
            ~simd::toBitMask(
                BaseHashTable::TagVector::batch_bool_type(tagsInTable)) &
            ProbeState::kFullMask;
        if (free) {
          auto freeOffset = bits::getAndClearLastSetBit(free);
          storeRowPointer(offset + freeOffset, hash, groups[i]);
          inserted = true;
          break;
        }
        offset = nextBucketOffset(offset);
        tagsInTable =
            BaseHashTable::loadTags(reinterpret_cast<uint8_t*>(table_), offset);
      }
      // Throws here if we have looped through all the buckets in the table.
      VELOX_CHECK(
          inserted,
          "Have looped through all the buckets in table: {}",
          toString());
    }
  }
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::arrayPushRow(char* row, int32_t index) {
  auto existing = table_[index];
  if (nextOffset_) {
    nextRow(row) = existing;
    if (existing) {
      hasDuplicates_ = true;
    }
  } else if (existing) {
    // Semijoin or a known unique build side ignores a repeat of a key.
    return false;
  }
  table_[index] = row;
  return !existing;
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::pushNext(char* row, char* next) {
  if (nextOffset_ > 0) {
    hasDuplicates_ = true;
    auto previousNext = nextRow(row);
    nextRow(row) = next;
    nextRow(next) = previousNext;
  }
}

template <bool ignoreNullKeys>
FOLLY_ALWAYS_INLINE void HashTable<ignoreNullKeys>::buildFullProbe(
    ProbeState& state,
    uint64_t hash,
    char* inserted,
    bool extraCheck,
    TableInsertPartitionInfo* partitionInfo) {
  auto insertFn = [&](int32_t /*row*/, PartitionBoundIndexType index) {
    if (partitionInfo != nullptr && !partitionInfo->inRange(index)) {
      partitionInfo->addOverflow(inserted);
      return nullptr;
    }
    storeRowPointer(index, hash, inserted);
    return nullptr;
  };

  if (hashMode_ == HashMode::kNormalizedKey) {
    state.fullProbe<ProbeState::Operation::kInsert>(
        *this,
        -static_cast<int32_t>(sizeof(normalized_key_t)),
        [&](char* group, int32_t /*row*/) {
          if (RowContainer::normalizedKey(group) ==
              RowContainer::normalizedKey(inserted)) {
            if (nextOffset_) {
              pushNext(group, inserted);
            }
            return true;
          }
          return false;
        },
        insertFn,
        numTombstones_,
        extraCheck,
        partitionInfo);
  } else {
    state.fullProbe<ProbeState::Operation::kInsert>(
        *this,
        0,
        [&](char* group, int32_t /*row*/) {
          if (compareKeys(group, inserted)) {
            if (nextOffset_ > 0) {
              pushNext(group, inserted);
            }
            return true;
          }
          return false;
        },
        insertFn,
        numTombstones_,
        extraCheck,
        partitionInfo);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::insertForJoin(
    char** groups,
    uint64_t* hashes,
    int32_t numGroups,
    TableInsertPartitionInfo* partitionInfo) {
  // The insertable rows are in the table, all get put in the hash table or
  // array.
  if (hashMode_ == HashMode::kArray) {
    for (auto i = 0; i < numGroups; ++i) {
      auto index = hashes[i];
      VELOX_CHECK_LT(index, capacity_);
      arrayPushRow(groups[i], index);
    }
    return;
  }

  ProbeState state;
  for (auto i = 0; i < numGroups; ++i) {
    state.preProbe(*this, hashes[i], i);
    state.firstProbe(*this, 0);

    buildFullProbe(state, hashes[i], groups[i], i, partitionInfo);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::rehash(bool initNormalizedKeys) {
  ++numRehashes_;
  constexpr int32_t kHashBatchSize = 1024;
  if (canApplyParallelJoinBuild()) {
    parallelJoinBuild();
    return;
  }
  raw_vector<uint64_t> hashes;
  hashes.resize(kHashBatchSize);
  char* groups[kHashBatchSize];
  // A join build can have multiple payload tables. Loop over 'this'
  // and the possible other tables and put all the data in the table
  // of 'this'.
  for (int32_t i = 0; i <= otherTables_.size(); ++i) {
    RowContainerIterator iterator;
    int32_t numGroups;
    do {
      numGroups = (i == 0 ? this : otherTables_[i - 1].get())
                      ->rows()
                      ->listRows(&iterator, kHashBatchSize, groups);
      if (!insertBatch(
              groups, numGroups, hashes, initNormalizedKeys || i != 0)) {
        VELOX_CHECK_NE(hashMode_, HashMode::kHash);
        setHashMode(HashMode::kHash, 0);
        return;
      }
    } while (numGroups > 0);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::setHashMode(HashMode mode, int32_t numNew) {
  VELOX_CHECK_NE(hashMode_, HashMode::kHash);
  TestValue::adjust("facebook::velox::exec::HashTable::setHashMode", &mode);
  if (mode == HashMode::kArray) {
    const auto bytes = capacity_ * tableSlotSize();
    const auto numPages = memory::AllocationTraits::numPages(bytes);
    rows_->pool()->allocateContiguous(numPages, tableAllocation_);
    table_ = tableAllocation_.data<char*>();
    memset(table_, 0, bytes);
    hashMode_ = HashMode::kArray;
    rehash(true);
  } else if (mode == HashMode::kHash) {
    hashMode_ = HashMode::kHash;
    for (auto& hasher : hashers_) {
      hasher->resetStats();
    }
    rows_->disableNormalizedKeys();
    capacity_ = 0;
    // Makes tables of the right size and rehashes.
    checkSize(numNew, true);
  } else if (mode == HashMode::kNormalizedKey) {
    hashMode_ = HashMode::kNormalizedKey;
    capacity_ = 0;
    // Makes tables of the right size and rehashes.
    checkSize(numNew, true);
  }
}

template <bool ignoreNullKeys>
bool HashTable<ignoreNullKeys>::analyze() {
  constexpr int32_t kHashBatchSize = 1024;
  // @lint-ignore CLANGTIDY
  char* groups[kHashBatchSize];
  RowContainerIterator iterator;
  int32_t numGroups;
  do {
    numGroups = rows_->listRows(&iterator, kHashBatchSize, groups);
    for (int32_t i = 0; i < hashers_.size(); ++i) {
      auto& hasher = hashers_[i];
      if (!hasher->isRange()) {
        // A range mode hasher does not know distincts, so need to
        // look. A distinct mode one does know the range. A hash join
        // build is always analyzed.
        continue;
      }
      uint64_t rangeSize;
      uint64_t distinctSize;
      hasher->cardinality(0, rangeSize, distinctSize);
      if (distinctSize == VectorHasher::kRangeTooLarge &&
          rangeSize == VectorHasher::kRangeTooLarge) {
        return false;
      }
      RowColumn column = rows_->columnAt(i);
      hasher->analyze(
          groups,
          numGroups,
          column.offset(),
          ignoreNullKeys ? 0 : column.nullByte(),
          ignoreNullKeys ? 0 : column.nullMask());
    }
  } while (numGroups > 0);
  return true;
}

namespace {
// Multiplies a * b and produces uint64_t max to denote overflow. If
// either a or b is overflow, preserves overflow.
inline uint64_t safeMul(uint64_t a, uint64_t b) {
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  if (a == kMax || b == kMax) {
    return kMax;
  }
  uint64_t result;
  if (__builtin_mul_overflow(a, b, &result)) {
    return kMax;
  }
  return result;
}
} // namespace

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::enableRangeWhereCan(
    const std::vector<uint64_t>& rangeSizes,
    const std::vector<uint64_t>& distinctSizes,
    std::vector<bool>& useRange) {
  // Sort non-range keys by the cardinality increase going from distinct to
  // range.
  std::vector<size_t> indices(rangeSizes.size());
  std::vector<uint64_t> rangeMultipliers(
      rangeSizes.size(), std::numeric_limits<uint64_t>::max());
  for (auto i = 0; i < rangeSizes.size(); i++) {
    indices[i] = i;
    if (!useRange[i]) {
      rangeMultipliers[i] = rangeSizes[i] / distinctSizes[i];
    }
  }

  std::sort(indices.begin(), indices.end(), [&](auto i, auto j) {
    return rangeMultipliers[i] < rangeMultipliers[j];
  });

  auto calculateNewMultipler = [&]() {
    uint64_t multipler = 1;
    for (auto i = 0; i < rangeSizes.size(); ++i) {
      // NOLINT
      multipler =
          safeMul(multipler, useRange[i] ? rangeSizes[i] : distinctSizes[i]);
    }
    return multipler;
  };

  // Switch distinct to range if the cardinality increase does not overflow
  // 64 bits.
  for (auto i = 0; i < rangeSizes.size(); ++i) {
    if (!useRange[indices[i]]) {
      useRange[indices[i]] = true;
      auto newProduct = calculateNewMultipler();
      if (newProduct == VectorHasher::kRangeTooLarge) {
        useRange[indices[i]] = false;
        return;
      }
    }
  }
}

template <bool ignoreNullKeys>
uint64_t HashTable<ignoreNullKeys>::setHasherMode(
    const std::vector<std::unique_ptr<VectorHasher>>& hashers,
    const std::vector<bool>& useRange,
    const std::vector<uint64_t>& rangeSizes,
    const std::vector<uint64_t>& distinctSizes) {
  uint64_t multiplier = 1;
  // A group by leaves 50% space for values not yet seen.
  for (int i = 0; i < hashers.size(); ++i) {
    multiplier = useRange.size() > i && useRange[i]
        ? hashers[i]->enableValueRange(multiplier, reservePct())
        : hashers[i]->enableValueIds(multiplier, reservePct());
    VELOX_CHECK_NE(multiplier, VectorHasher::kRangeTooLarge);
  }
  return multiplier;
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::clearUseRange(std::vector<bool>& useRange) {
  for (auto i = 0; i < hashers_.size(); ++i) {
    useRange[i] = hashers_[i]->typeKind() == TypeKind::BOOLEAN;
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::decideHashMode(
    int32_t numNew,
    bool disableRangeArrayHash) {
  std::vector<uint64_t> rangeSizes(hashers_.size());
  std::vector<uint64_t> distinctSizes(hashers_.size());
  std::vector<bool> useRange(hashers_.size());
  uint64_t bestWithReserve = 1;
  uint64_t distinctsWithReserve = 1;
  uint64_t rangesWithReserve = 1;
  // Permanently turn off kArray hash mode with key value ranges after this is
  // first requested.
  if (disableRangeArrayHash && numNew == 0 && disableRangeArrayHash_) {
    // The option is already set and no new rows are added. Return.
    return;
  }
  disableRangeArrayHash_ |= disableRangeArrayHash;
  if (numDistinct_ && !isJoinBuild_) {
    if (!analyze()) {
      setHashMode(HashMode::kHash, numNew);
      return;
    }
  }
  for (int i = 0; i < hashers_.size(); ++i) {
    hashers_[i]->cardinality(reservePct(), rangeSizes[i], distinctSizes[i]);
    distinctsWithReserve = safeMul(distinctsWithReserve, distinctSizes[i]);
    rangesWithReserve = safeMul(rangesWithReserve, rangeSizes[i]);
    if (distinctSizes[i] == VectorHasher::kRangeTooLarge &&
        rangeSizes[i] != VectorHasher::kRangeTooLarge) {
      useRange[i] = true;
      bestWithReserve = safeMul(bestWithReserve, rangeSizes[i]);
    } else if (
        rangeSizes[i] != VectorHasher::kRangeTooLarge &&
        rangeSizes[i] <= distinctSizes[i] * 20) {
      useRange[i] = true;
      bestWithReserve = safeMul(bestWithReserve, rangeSizes[i]);
    } else {
      bestWithReserve = safeMul(bestWithReserve, distinctSizes[i]);
    }
  }

  if (rangesWithReserve < kArrayHashMaxSize && !disableRangeArrayHash_) {
    std::fill(useRange.begin(), useRange.end(), true);
    capacity_ = setHasherMode(hashers_, useRange, rangeSizes, distinctSizes);
    setHashMode(HashMode::kArray, numNew);
    return;
  }

  if (bestWithReserve < kArrayHashMaxSize ||
      (disableRangeArrayHash_ && bestWithReserve < numDistinct_ * 2)) {
    capacity_ = setHasherMode(hashers_, useRange, rangeSizes, distinctSizes);
    setHashMode(HashMode::kArray, numNew);
    return;
  }
  if (rangesWithReserve != VectorHasher::kRangeTooLarge) {
    std::fill(useRange.begin(), useRange.end(), true);
    setHasherMode(hashers_, useRange, rangeSizes, distinctSizes);
    setHashMode(HashMode::kNormalizedKey, numNew);
    return;
  }
  if (hashers_.size() == 1 && distinctsWithReserve > 10000) {
    // A single part group by that does not go by range or become an array
    // does not make sense as a normalized key unless it is very small.
    setHashMode(HashMode::kHash, numNew);
    return;
  }

  if (distinctsWithReserve < kArrayHashMaxSize) {
    clearUseRange(useRange);
    capacity_ = setHasherMode(hashers_, useRange, rangeSizes, distinctSizes);
    setHashMode(HashMode::kArray, numNew);
    return;
  }
  if (distinctsWithReserve == VectorHasher::kRangeTooLarge &&
      rangesWithReserve == VectorHasher::kRangeTooLarge) {
    setHashMode(HashMode::kHash, numNew);
    return;
  }
  // The key concatenation fits in 64 bits.
  if (bestWithReserve != VectorHasher::kRangeTooLarge) {
    enableRangeWhereCan(rangeSizes, distinctSizes, useRange);
  } else {
    clearUseRange(useRange);
  }
  setHasherMode(hashers_, useRange, rangeSizes, distinctSizes);
  setHashMode(HashMode::kNormalizedKey, numNew);
}

template <bool ignoreNullKeys>
std::string HashTable<ignoreNullKeys>::toString() {
  std::stringstream out;
  out << "[HashTable keys: " << hashers_.size()
      << " hash mode: " << modeString(hashMode_) << " capacity: " << capacity_
      << " distinct count: " << numDistinct_
      << " tombstones count: " << numTombstones_ << "]";
  if (table_ == nullptr) {
    out << " (no table)";
  }

  for (auto& hasher : hashers_) {
    out << std::endl << hasher->toString();
  }
  out << std::endl;

  if (kTrackLoads) {
    out << fmt::format(
               "{} probes {} tag loads {} row loads {} hits",
               numProbes_,
               numTagLoads_,
               numRowLoads_,
               numHits_)
        << std::endl;
  }

  if (hashMode_ == HashMode::kArray) {
    int64_t occupied = 0;
    if (table_ && tableAllocation_.data() && tableAllocation_.size()) {
      // 'size_' and 'table_' may not be set if initializing.
      uint64_t size = std::min<uint64_t>(
          tableAllocation_.size() / sizeof(char*), capacity_);
      for (int32_t i = 0; i < size; ++i) {
        occupied += table_[i] != nullptr;
      }
    }
    out << "Total slots used: " << occupied << std::endl;
  } else {
    int64_t occupied = 0;

    // Count of buckets indexed by the number of non-empty slots.
    // Each bucket has 16 slots. Hence, the number of non-empty slots is between
    // 0 and 16 (17 possible values).
    int64_t numBuckets[sizeof(TagVector) + 1] = {};
    for (int64_t bucketOffset = 0; bucketOffset < sizeMask_;
         bucketOffset += kBucketSize) {
      auto tags = loadTags(bucketOffset);
      auto filled = simd::toBitMask(tags != TagVector::broadcast(0));
      auto numOccupied = __builtin_popcount(filled);

      ++numBuckets[numOccupied];
      occupied += numOccupied;
    }

    out << "Total buckets: " << (sizeMask_ / kBucketSize + 1) << std::endl;
    out << "Total slots used: " << occupied << std::endl;
    for (auto i = 1; i < sizeof(TagVector) + 1; ++i) {
      if (numBuckets[i] > 0) {
        out << numBuckets[i] << " buckets with " << i << " slots used"
            << std::endl;
      }
    }
  }

  return out.str();
}

template <bool ignoreNullKeys>
std::string HashTable<ignoreNullKeys>::toString(
    int64_t startBucket,
    int64_t numBuckets) const {
  if (table_ == nullptr) {
    return "(no table)";
  }

  VELOX_CHECK_GE(startBucket, 0);
  VELOX_CHECK_GT(numBuckets, 0);

  const int64_t totalBuckets = sizeMask_ / kBucketSize + 1;
  if (startBucket >= totalBuckets) {
    return "";
  }

  const int64_t endBucket =
      std::min<int64_t>(startBucket + numBuckets, totalBuckets);

  std::ostringstream out;
  for (int64_t i = startBucket; i < endBucket; ++i) {
    out << std::setw(1 + endBucket / 10) << i << ": ";

    auto bucket = bucketAt(i * kBucketSize);
    for (auto j = 0; j < sizeof(TagVector); ++j) {
      if (j > 0) {
        out << ", ";
      }
      const auto tag = bucket->tagAt(j);
      if (tag == ProbeState::kTombstoneTag) {
        out << std::setw(3) << "T";
      } else if (tag == ProbeState::kEmptyTag) {
        out << std::setw(3) << "E";
      } else {
        out << (int)tag;
      }
    }
    out << std::endl;
  }

  return out.str();
}

namespace {
bool mayUseValueIds(const BaseHashTable& table) {
  if (table.hashMode() == BaseHashTable::HashMode::kHash) {
    return false;
  }
  for (auto& hasher : table.hashers()) {
    if (!hasher->mayUseValueIds()) {
      return false;
    }
  }
  return true;
}
} // namespace

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::prepareJoinTable(
    std::vector<std::unique_ptr<BaseHashTable>> tables,
    folly::Executor* executor,
    int8_t spillInputStartPartitionBit) {
  buildExecutor_ = executor;
  otherTables_.reserve(tables.size());
  for (auto& table : tables) {
    otherTables_.emplace_back(std::unique_ptr<HashTable<ignoreNullKeys>>(
        dynamic_cast<HashTable<ignoreNullKeys>*>(table.release())));
  }
  bool useValueIds = mayUseValueIds(*this);
  if (useValueIds) {
    for (auto& other : otherTables_) {
      if (!mayUseValueIds(*other)) {
        useValueIds = false;
        break;
      }
    }
    if (useValueIds) {
      for (auto& other : otherTables_) {
        for (auto i = 0; i < hashers_.size(); ++i) {
          hashers_[i]->merge(*other->hashers_[i]);
          if (!hashers_[i]->mayUseValueIds()) {
            useValueIds = false;
            break;
          }
        }
        if (!useValueIds) {
          break;
        }
      }
    }
  }
  numDistinct_ = rows()->numRows();
  for (const auto& other : otherTables_) {
    numDistinct_ += other->rows()->numRows();
  }
  if (!useValueIds) {
    if (hashMode_ != HashMode::kHash) {
      setHashMode(HashMode::kHash, 0);
    } else {
      checkSize(0, true);
    }
  } else {
    decideHashMode(0);
  }
  checkHashBitsOverlap(spillInputStartPartitionBit);
}

template <bool ignoreNullKeys>
int32_t HashTable<ignoreNullKeys>::listJoinResults(
    JoinResultIterator& iter,
    bool includeMisses,
    folly::Range<vector_size_t*> inputRows,
    folly::Range<char**> hits) {
  VELOX_CHECK_LE(inputRows.size(), hits.size());
  if (!hasDuplicates_) {
    return listJoinResultsNoDuplicates(iter, includeMisses, inputRows, hits);
  }
  int numOut = 0;
  auto maxOut = inputRows.size();
  while (iter.lastRowIndex < iter.rows->size()) {
    if (!iter.nextHit) {
      auto row = (*iter.rows)[iter.lastRowIndex];
      iter.nextHit = (*iter.hits)[row]; // NOLINT
      if (!iter.nextHit) {
        ++iter.lastRowIndex;

        if (includeMisses) {
          inputRows[numOut] = row; // NOLINT
          hits[numOut] = nullptr;
          ++numOut;
          if (numOut >= maxOut) {
            return numOut;
          }
        }
        continue;
      }
    }
    while (iter.nextHit) {
      char* next = nullptr;
      if (nextOffset_) {
        next = nextRow(iter.nextHit);
        if (next) {
          __builtin_prefetch(reinterpret_cast<char*>(next) + nextOffset_);
        }
      }
      inputRows[numOut] = (*iter.rows)[iter.lastRowIndex]; // NOLINT
      hits[numOut] = iter.nextHit;
      ++numOut;
      iter.nextHit = next;
      if (!iter.nextHit) {
        ++iter.lastRowIndex;
      }
      if (numOut >= maxOut) {
        return numOut;
      }
    }
  }
  return numOut;
}

template <bool ignoreNullKeys>
int32_t HashTable<ignoreNullKeys>::listJoinResultsNoDuplicates(
    JoinResultIterator& iter,
    bool includeMisses,
    folly::Range<vector_size_t*> inputRows,
    folly::Range<char**> hits) {
  int32_t numOut = 0;
  auto maxOut = inputRows.size();
  int32_t i = iter.lastRowIndex;
  auto numRows = iter.rows->size();

  constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
  auto sourceHits = reinterpret_cast<int64_t*>(iter.hits->data());
  auto sourceRows = iter.rows->data();
  // We pass the pointers as int64_t's in 'hitWords'.
  auto resultHits = reinterpret_cast<int64_t*>(hits.data());
  auto resultRows = inputRows.data();
  int32_t outLimit = maxOut - kWidth;
  for (; i + kWidth <= numRows && numOut < outLimit; i += kWidth) {
    auto indices = simd::loadGatherIndices<int64_t, int32_t>(sourceRows + i);
    auto hitWords = simd::gather(sourceHits, indices);
    auto misses = includeMisses ? 0 : simd::toBitMask(hitWords == 0);
    if (misses == 0xf) {
      continue;
    }
    if (!misses) {
      hitWords.store_unaligned(resultHits + numOut);
      indices.store_unaligned(resultRows + numOut);
      numOut += kWidth;
      continue;
    }
    auto matches = misses ^ bits::lowMask(kWidth);
    simd::filter<int64_t>(hitWords, matches, xsimd::default_arch{})
        .store_unaligned(resultHits + numOut);
    simd::filter<int32_t>(indices, matches, xsimd::default_arch{})
        .store_unaligned(resultRows + numOut);
    numOut += __builtin_popcount(matches);
  }
  for (; i < numRows; ++i) {
    auto row = sourceRows[i];
    if (includeMisses || sourceHits[row]) {
      resultHits[numOut] = sourceHits[row];
      resultRows[numOut] = row;
      ++numOut;
      if (numOut >= maxOut) {
        ++i;
        break;
      }
    }
  }

  iter.lastRowIndex = i;
  return numOut;
}

template <bool ignoreNullKeys>
template <RowContainer::ProbeType probeType>
int32_t HashTable<ignoreNullKeys>::listRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  if (iter->hashTableIndex_ == -1) {
    auto numRows = rows_->listRows<probeType>(
        &iter->rowContainerIterator_, maxRows, maxBytes, rows);
    if (numRows) {
      return numRows;
    }
    iter->hashTableIndex_ = 0;
    iter->rowContainerIterator_.reset();
  }
  while (iter->hashTableIndex_ < otherTables_.size()) {
    auto numRows =
        otherTables_[iter->hashTableIndex_]
            ->rows()
            ->template listRows<probeType>(
                &iter->rowContainerIterator_, maxRows, maxBytes, rows);
    if (numRows) {
      return numRows;
    }
    ++iter->hashTableIndex_;
    iter->rowContainerIterator_.reset();
  }

  return 0;
}

template <bool ignoreNullKeys>
int32_t HashTable<ignoreNullKeys>::listNotProbedRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  return listRows<RowContainer::ProbeType::kNotProbed>(
      iter, maxRows, maxBytes, rows);
}

template <bool ignoreNullKeys>
int32_t HashTable<ignoreNullKeys>::listProbedRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  return listRows<RowContainer::ProbeType::kProbed>(
      iter, maxRows, maxBytes, rows);
}

template <bool ignoreNullKeys>
int32_t HashTable<ignoreNullKeys>::listAllRows(
    RowsIterator* iter,
    int32_t maxRows,
    uint64_t maxBytes,
    char** rows) {
  return listRows<RowContainer::ProbeType::kAll>(iter, maxRows, maxBytes, rows);
}

template <>
int32_t HashTable<false>::listNullKeyRows(
    NullKeyRowsIterator* iter,
    int32_t maxRows,
    char** rows) {
  if (!iter->initialized) {
    VELOX_CHECK_GT(nextOffset_, 0);
    VELOX_CHECK_EQ(hashers_.size(), 1);
    HashLookup lookup(hashers_);
    if (hashMode_ == HashMode::kHash) {
      lookup.hashes.push_back(VectorHasher::kNullHash);
    } else {
      lookup.hashes.push_back(0);
    }
    lookup.rows.push_back(0);
    lookup.hits.resize(1);
    joinProbe(lookup);
    iter->nextHit = lookup.hits[0];
    iter->initialized = true;
  }
  int32_t numRows = 0;
  char* hit = iter->nextHit;
  while (numRows < maxRows && hit) {
    rows[numRows++] = hit;
    hit = nextRow(hit);
  }
  iter->nextHit = hit;
  return numRows;
}

template <>
int32_t
HashTable<true>::listNullKeyRows(NullKeyRowsIterator*, int32_t, char**) {
  VELOX_UNREACHABLE();
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::erase(folly::Range<char**> rows) {
  auto numRows = rows.size();
  raw_vector<uint64_t> hashes;
  hashes.resize(numRows);

  for (int32_t i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    if (hashMode_ == HashMode::kHash) {
      rows_->hash(i, rows, i > 0, hashes.data());
    } else {
      auto column = rows_->columnAt(i);
      if (!hasher->computeValueIdsForRows(
              rows.data(),
              numRows,
              column.offset(),
              column.nullByte(),
              ignoreNullKeys ? 0 : column.nullMask(),
              hashes)) {
        VELOX_FAIL("Value ids in erase must exist for all keys");
      }
    }
  }
  eraseWithHashes(rows, hashes.data());
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::eraseWithHashes(
    folly::Range<char**> rows,
    uint64_t* hashes) {
  auto numRows = rows.size();
  if (hashMode_ == HashMode::kArray) {
    for (auto i = 0; i < numRows; ++i) {
      DCHECK(hashes[i] < capacity_);
      table_[hashes[i]] = nullptr;
    }
  } else {
    if (hashMode_ == HashMode::kNormalizedKey) {
      for (auto i = 0; i < numRows; ++i) {
        hashes[i] = mixNormalizedKey(hashes[i], sizeBits_);
      }
    }

    ProbeState state;
    for (auto i = 0; i < numRows; ++i) {
      state.preProbe(*this, hashes[i], i);

      state.firstProbe<ProbeState::Operation::kErase>(*this, 0);
      state.fullProbe<ProbeState::Operation::kErase>(
          *this,
          0,
          [&](const char* group, int32_t row) { return rows[row] == group; },
          [&](int32_t /*index*/, int32_t /*row*/) { return nullptr; },
          numTombstones_,
          false);
    }
  }
  numDistinct_ -= numRows;
  if (!otherTables_.empty()) {
    raw_vector<char*> containerRows;
    containerRows.resize(rows.size());
    for (auto& other : otherTables_) {
      const auto numContainerRows =
          other->rows()->findRows(rows, containerRows.data());
      other->rows()->eraseRows(
          folly::Range(containerRows.data(), numContainerRows));
    }
    const auto numContainerRows = rows_->findRows(rows, containerRows.data());
    rows_->eraseRows(folly::Range(containerRows.data(), numContainerRows));
  } else {
    rows_->eraseRows(rows);
  }
}

template <bool ignoreNullKeys>
void HashTable<ignoreNullKeys>::checkConsistency() const {
  VELOX_CHECK_GE(capacity_, numDistinct_);
  if (hashMode_ == BaseHashTable::HashMode::kArray) {
    return;
  }
  uint64_t numEmpty = 0;
  uint64_t numTombstone = 0;
  for (auto start = 0; start < sizeMask_; start += kBucketSize) {
    auto bucket = bucketAt(start);
    for (auto i = 0; i < sizeof(TagVector); ++i) {
      if (bucket->tagAt(i) == ProbeState::kTombstoneTag) {
        ++numTombstone;
        continue;
      }
      if (bucket->tagAt(i) == ProbeState::kEmptyTag) {
        ++numEmpty;
        continue;
      }
    }
  }
  VELOX_CHECK_EQ(
      numEmpty + numTombstone + numDistinct_,
      capacity_,
      "capacity: {}, numEmpty: {}, numTombstone: {}, numDistinct: {}",
      capacity_,
      numEmpty,
      numTombstone,
      numDistinct_);
}

template class HashTable<true>;
template class HashTable<false>;

namespace {
void populateLookupRows(
    const SelectivityVector& rows,
    raw_vector<vector_size_t>& lookupRows) {
  if (rows.isAllSelected()) {
    std::iota(lookupRows.begin(), lookupRows.end(), 0);
  } else {
    lookupRows.clear();
    rows.applyToSelected([&](auto row) { lookupRows.push_back(row); });
  }
}
} // namespace

void BaseHashTable::prepareForGroupProbe(
    HashLookup& lookup,
    const RowVectorPtr& input,
    SelectivityVector& rows,
    bool ignoreNullKeys,
    int8_t spillInputStartPartitionBit) {
  checkHashBitsOverlap(spillInputStartPartitionBit);
  auto& hashers = lookup.hashers;

  for (auto& hasher : hashers) {
    auto key = input->childAt(hasher->channel())->loadedVector();
    hasher->decode(*key, rows);
  }

  if (ignoreNullKeys) {
    // A null in any of the keys disables the row.
    deselectRowsWithNulls(hashers, rows);
  }

  lookup.reset(rows.end());

  bool rehash = false;
  const auto mode = hashMode();
  for (auto i = 0; i < hashers.size(); ++i) {
    auto& hasher = hashers[i];
    if (mode != BaseHashTable::HashMode::kHash) {
      if (!hasher->computeValueIds(rows, lookup.hashes)) {
        rehash = true;
      }
    } else {
      hasher->hash(rows, i > 0, lookup.hashes);
    }
  }

  if (rehash || capacity() == 0) {
    if (mode != BaseHashTable::HashMode::kHash) {
      decideHashMode(input->size());
      // Do not forward 'ignoreNullKeys' to avoid redundant evaluation of
      // deselectRowsWithNulls.
      prepareForGroupProbe(
          lookup, input, rows, false, spillInputStartPartitionBit);
      return;
    }
  }

  populateLookupRows(rows, lookup.rows);
}

void BaseHashTable::prepareForJoinProbe(
    HashLookup& lookup,
    const RowVectorPtr& input,
    SelectivityVector& rows,
    bool decodeAndRemoveNulls) {
  auto& hashers = lookup.hashers;

  if (decodeAndRemoveNulls) {
    for (auto& hasher : hashers) {
      auto key = input->childAt(hasher->channel())->loadedVector();
      hasher->decode(*key, rows);
    }

    // A null in any of the keys disables the row.
    deselectRowsWithNulls(hashers, rows);
  }

  lookup.reset(rows.end());

  const auto mode = hashMode();
  for (auto i = 0; i < hashers.size(); ++i) {
    auto& hasher = hashers[i];
    if (mode != BaseHashTable::HashMode::kHash) {
      auto& key = input->childAt(hasher->channel());
      hashers_[i]->lookupValueIds(
          *key, rows, lookup.scratchMemory, lookup.hashes);
    } else {
      hasher->hash(rows, i > 0, lookup.hashes);
    }
  }

  populateLookupRows(rows, lookup.rows);
}

} // namespace facebook::velox::exec
