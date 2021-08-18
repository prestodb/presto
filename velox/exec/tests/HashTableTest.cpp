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
#include "velox/common/base/SelectivityInfo.h"
#include "velox/vector/tests/VectorMaker.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

// Test framework for join hash tables. Generates probe keys, of which
// some percent are inserted in a hashTable. The placement of the
// payload is shuffled so as not to correlate with the probe
// order. Tests the presence/correctness of the hit for each key and
// measures the time for computing hashes/value ids vs the time spent
// probing the table. Covers kArray, kNormalizedKey and kHash hash
// modes.
class HashTableTest : public testing::Test {
 protected:
  void testCycle(
      BaseHashTable::HashMode mode,
      int32_t size,
      int32_t numWays,
      TypePtr buildType,
      int32_t numKeys) {
    std::vector<std::unique_ptr<HashTable<true>>> otherTables;
    std::vector<TypePtr> dependentTypes;
    int32_t sequence = 0;
    isInTable_.resize(
        bits::nwords(numWays * size),
        static_cast<const std::vector<
            unsigned long,
            std::allocator<unsigned long>>::value_type>(-1));
    if (insertPct_ != 100) {
      // If we probe with all keys but only mean to insert part, we deselect.
      folly::Random::DefaultGenerator rng;
      rng.seed(1);
      for (auto i = 0; i < size * numWays; ++i) {
        if (folly::Random::rand32(rng) % 100 > insertPct_) {
          bits::clearBit(isInTable_.data(), i);
        }
      }
    }
    int32_t startOffset = 0;
    for (auto way = 0; way < numWays; ++way) {
      std::vector<RowVectorPtr> batches;
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      for (auto channel = 0; channel < numKeys; ++channel) {
        keyHashers.emplace_back(std::make_unique<VectorHasher>(
            buildType->childAt(channel), channel));
      }
      auto table = HashTable<true>::createForJoin(
          std::move(keyHashers), dependentTypes, true, mappedMemory_);

      makeRows(size, 1, sequence, buildType, batches);
      copyVectorsToTable(batches, startOffset, table.get());
      sequence += size;
      if (!topTable_) {
        topTable_ = std::move(table);
      } else {
        otherTables.push_back(std::move(table));
      }
      batches_.insert(batches_.end(), batches.begin(), batches.end());
      startOffset += size;
    }
    topTable_->prepareJoinTable(std::move(otherTables));
    EXPECT_EQ(topTable_->hashMode(), mode);
    LOG(INFO) << "Made table " << describeTable();
    testProbe();
  }

  std::string describeTable() {
    std::stringstream out;
    auto mode = topTable_->hashMode();
    if (mode == BaseHashTable::HashMode::kHash) {
      out << "Multipart key ";
    } else {
      out
          << (mode == BaseHashTable::HashMode::kArray ? "Array "
                                                      : "Normalized key ");
      out << "(";
      for (auto& hasher : topTable_->hashers()) {
        out << (hasher->isRange() ? "range " : "valueIds ");
      }
      out << ") ";
    }
    out << topTable_->numDistinct() << " entries";
    return out.str();
  }

  void copyVectorsToTable(
      std::vector<RowVectorPtr>& batches,
      int32_t tableOffset,
      HashTable<true>* table) {
    int32_t batchSize = batches[0]->size();
    std::vector<uint64_t> dummy(batchSize);
    int32_t batchOffset = 0;
    rowOfKey_.resize(tableOffset + batchSize * batches.size());
    auto rowContainer = table->rows();
    auto& hashers = table->hashers();
    auto numKeys = hashers.size();
    // We init a DecodedVector for each member of the RowVectors in 'batches'.
    std::vector<std::vector<DecodedVector>> decoded;
    SelectivityVector rows(batchSize);
    SelectivityVector insertedRows(batchSize);
    for (auto& batch : batches) {
      // If we are only inserting a fraction of the rows, we set
      // insertedRows to that fraction so that the VectorHashers only
      // see keys that will actually be inserted.
      if (insertPct_ < 100) {
        bits::copyBits(
            isInTable_.data(),
            tableOffset + batchOffset,
            insertedRows.asMutableRange().bits(),
            0,
            batchSize);
      }
      decoded.emplace_back(batch->childrenSize());
      VELOX_CHECK_EQ(batch->size(), batchSize);
      auto& decoders = decoded.back();
      for (auto i = 0; i < batch->childrenSize(); ++i) {
        decoders[i].decode(*batch->childAt(i), rows);
        if (i < numKeys) {
          if (table->hashMode() != BaseHashTable::HashMode::kHash) {
            auto hasher = table->hashers()[i].get();
            if (hasher->mayUseValueIds()) {
              hasher->computeValueIds(*batch->childAt(i), insertedRows, &dummy);
            }
          }
        }
      }
      batchOffset += batchSize;
    }

    auto size = batchSize * batches.size();
    auto powerOfTwo = bits::nextPowerOfTwo(size);
    int32_t mask = powerOfTwo - 1;
    int32_t position = 0;
    int32_t delta = 1;
    int32_t numInserted = 0;
    auto nextOffset = rowContainer->nextOffset();

    // We insert values in a geometric skip order. 1, 2, 4, 7,
    // 11,... where the skip increments by one. We wrap around at the
    // power of two boundary. This sequence hits every place in the
    // power of two range once. Like this, when we probe the data for
    // consecutive keys the hits will have no cache locality.
    for (auto count = 0; count < powerOfTwo; ++count) {
      if (position < size &&
          (insertPct_ == 100 ||
           bits::isBitSet(isInTable_.data(), tableOffset + position))) {
        char* newRow = rowContainer->newRow();
        rowOfKey_[tableOffset + position] = newRow;
        auto batchIndex = position / batchSize;
        auto rowIndex = position % batchSize;
        if (nextOffset) {
          *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
        }
        ++numInserted;
        for (auto i = 0; i < hashers.size(); ++i) {
          rowContainer->store(decoded[batchIndex][i], rowIndex, newRow, i);
        }
      }
      position = (position + delta) & mask;
      ++delta;
    }
  }

  // Makes a vector of 'type' with 'size' unique elements, initialized
  // based on 'sequence'. If 'sequence' is incremented by 'size'
  // between the next call will not overlap with the results of the
  // previous one.
  VectorPtr makeVector(TypePtr type, int32_t size, int32_t sequence) {
    switch (type->kind()) {
      case TypeKind::BIGINT:
        return vectorMaker_->flatVector<int64_t>(
            size,
            [&](vector_size_t row) { return keySpacing_ * (sequence + row); },
            nullptr);

      case TypeKind::VARCHAR: {
        auto strings = std::static_pointer_cast<FlatVector<StringView>>(
            BaseVector::create(VARCHAR(), size, pool_.get()));
        for (auto row = 0; row < size; ++row) {
          auto string = fmt::format("{}", keySpacing_ * (sequence + row));
          // Make strings that overflow the inline limit for 1/10 of
          // the values after 10K,000. Datasets with only
          // range-encodable small strings can be made within the
          // first 10K values.
          if (row > 10000 && row % 10 == 0) {
            string += "----" + string + "----" + string;
          }
          strings->set(row, StringView(string));
        }
        return strings;
      }

      case TypeKind::ROW: {
        std::vector<VectorPtr> children;
        for (auto i = 0; i < type->size(); ++i) {
          children.push_back(makeVector(type->childAt(i), size, sequence));
        }
        return vectorMaker_->rowVector(children);
      }
      default:
        VELOX_FAIL("Unsupported kind for makeVector {}", type->kind());
    }
  }

  void makeRows(
      int32_t batchSize,
      int32_t numBatches,
      int32_t sequence,
      TypePtr buildType,
      std::vector<RowVectorPtr>& batches) {
    for (auto i = 0; i < numBatches; ++i) {
      batches.push_back(std::static_pointer_cast<RowVector>(
          makeVector(buildType, batchSize, sequence)));
      sequence += batchSize;
    }
  }

  void testProbe() {
    auto lookup = std::make_unique<HashLookup>(topTable_->hashers());
    auto batchSize = batches_[0]->size();
    SelectivityVector rows(batchSize);
    auto mode = topTable_->hashMode();
    SelectivityInfo hashTime;
    SelectivityInfo probeTime;
    int32_t numHashed = 0;
    int32_t numProbed = 0;
    int32_t numHit = 0;
    DecodedVector decoded;
    std::vector<uint64_t> scratch;
    auto& hashers = topTable_->hashers();
    for (auto batchIndex = 0; batchIndex < batches_.size(); ++batchIndex) {
      auto batch = batches_[batchIndex];
      lookup->reset(batch->size());
      rows.setAll();
      numHashed += batch->size();
      {
        SelectivityTimer timer(hashTime, 0);
        for (auto i = 0; i < hashers.size(); ++i) {
          auto key = batch->childAt(i);
          if (mode != BaseHashTable::HashMode::kHash) {
            decoded.decode(*key, rows);
            hashers[i]->lookupValueIds(decoded, rows, scratch, &lookup->hashes);
          } else {
            hashers[i]->hash(*key, rows, i > 0, &lookup->hashes);
          }
        }
      }

      lookup->rows.clear();
      if (rows.isAllSelected()) {
        lookup->rows.resize(rows.size());
        std::iota(lookup->rows.begin(), lookup->rows.end(), 0);
      } else {
        constexpr int32_t kPadding = simd::kPadding / sizeof(int32_t);
        lookup->rows.resize(bits::roundUp(rows.size() + kPadding, kPadding));
        auto numRows = simd::indicesOfSetBits(
            rows.asRange().bits(), 0, batch->size(), lookup->rows.data());
        lookup->rows.resize(numRows);
      }
      auto startOffset = batchIndex * batchSize;
      if (lookup->rows.empty()) {
        // the keys disqualify all entries. The table is not consulted.
        for (auto i = startOffset; i < startOffset + batch->size(); ++i) {
          ASSERT_EQ(nullptr, rowOfKey_[i]);
        }
      } else {
        {
          numProbed += lookup->rows.size();
          SelectivityTimer timer(probeTime, 0);
          topTable_->joinProbe(*lookup);
        }
        for (auto i = 0; i < lookup->rows.size(); ++i) {
          auto key = lookup->rows[i];
          numHit += lookup->hits[key] != nullptr;
          ASSERT_EQ(rowOfKey_[startOffset + key], lookup->hits[key]);
        }
      }
    }
    LOG(INFO)
        << fmt::format(
               "Hashed: {} Probed: {} Hit: {} Hash time/row {} probe time/row {}",
               numHashed,
               numProbed,
               numHit,
               hashTime.timeToDropValue() / numHashed,
               probeTime.timeToDropValue() / numProbed)
        << std::endl;
  }

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  memory::MappedMemory* mappedMemory_{memory::MappedMemory::getInstance()};
  std::unique_ptr<test::VectorMaker> vectorMaker_{
      std::make_unique<test::VectorMaker>(pool_.get())};
  // Bitmap of positions in batches_ that end up in the table.
  std::vector<uint64_t> isInTable_;
  // Test payload, keys first.
  std::vector<RowVectorPtr> batches_;

  // Corresponds 1:1 to data in 'batches_'. nullptr if the key is not
  // inserted, otherwise pointer into the RowContainer.
  std::vector<char*> rowOfKey_;
  std::unique_ptr<HashTable<true>> topTable_;
  // Percentage of keys inserted into the table. This is for measuring
  // joins that miss the table part of the time. Used in initializing
  // 'isInTable_'.
  int32_t insertPct_ = 100;
  // Spacing between consecutive generated keys. Affects whether
  // Vectorhashers make ranges or ids of distinct values.
  int32_t keySpacing_ = 1;
};

TEST_F(HashTableTest, int2DenseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_F(HashTableTest, string1DenseArray) {
  auto type = ROW({"k1"}, {VARCHAR()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 1);
}

TEST_F(HashTableTest, string2Normalized) {
  auto type = ROW({"k1", "k2"}, {VARCHAR(), VARCHAR()});
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 50000, 2, type, 2);
}

TEST_F(HashTableTest, int2SparseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_F(HashTableTest, int2SparseNormalized) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 10000, 2, type, 2);
}

TEST_F(HashTableTest, int2SparseNormalizedMostMiss) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  insertPct_ = 10;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 100000, 2, type, 2);
}

TEST_F(HashTableTest, structKey) {
  auto type =
      ROW({"key"}, {ROW({"k1", "k2", "k3"}, {BIGINT(), VARCHAR(), BIGINT()})});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 100000, 2, type, 1);
}

TEST_F(HashTableTest, mixed6Sparse) {
  auto type =
      ROW({"k1", "k2", "k3", "k4", "k5", "k6"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 1000000, 2, type, 6);
}
