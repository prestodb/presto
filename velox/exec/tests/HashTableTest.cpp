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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include <memory>

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
class HashTableTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    if (GetParam()) {
      executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(16);
    }
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void testCycle(
      BaseHashTable::HashMode mode,
      int32_t size,
      int32_t numWays,
      TypePtr buildType,
      int32_t numKeys) {
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
    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    for (auto way = 0; way < numWays; ++way) {
      std::vector<RowVectorPtr> batches;
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      for (auto channel = 0; channel < numKeys; ++channel) {
        keyHashers.emplace_back(std::make_unique<VectorHasher>(
            buildType->childAt(channel), channel));
      }
      auto table = HashTable<true>::createForJoin(
          std::move(keyHashers), dependentTypes, true, false, mappedMemory_);

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
    topTable_->prepareJoinTable(std::move(otherTables), executor_.get());
    EXPECT_EQ(topTable_->hashMode(), mode);
    LOG(INFO) << "Made table " << describeTable();
    testProbe();
    testEraseEveryN(3);
    testProbe();
    testEraseEveryN(4);
    testProbe();
    testGroupBySpill(size, buildType, numKeys);
  }

  // Inserts and deletes rows in a HashTable, similarly to a group by
  // that periodically spills a fraction of the groups.
  void testGroupBySpill(int32_t size, TypePtr tableType, int32_t numKeys) {
    constexpr int32_t kBatchSize = 1000;
    constexpr int32_t kNumErasePerRound = 500;
    int32_t sequence = 0;
    std::vector<RowVectorPtr> batches;
    auto table = createHashTableForAggregation(tableType, numKeys);
    auto lookup = std::make_unique<HashLookup>(table->hashers());
    std::vector<char*> allInserted;
    int32_t numErased = 0;
    // We insert 1000 and delete 500.
    for (auto round = 0; round < size; round += kBatchSize) {
      makeRows(kBatchSize, 1, sequence, tableType, batches);
      sequence += kBatchSize;
      lookup->reset(kBatchSize);
      insertGroups(*batches.back(), *lookup, *table);
      allInserted.insert(
          allInserted.end(), lookup->hits.begin(), lookup->hits.end());

      table->erase(
          folly::Range<char**>(&allInserted[numErased], kNumErasePerRound));
      numErased += kNumErasePerRound;
    }
    int32_t batchStart = 0;
    // We loop over the keys one more time. The first half will be all
    // new rows, the second half will be hits of existing ones.
    int32_t row = 0;
    for (auto i = 0; i < batches.size(); ++i) {
      insertGroups(*batches[0], *lookup, *table);
      for (; row < batchStart + kBatchSize; ++row) {
        if (row < numErased) {
          ASSERT_NE(lookup->hits[row - batchStart], allInserted[row]);
        } else {
          ASSERT_EQ(lookup->hits[row - batchStart], allInserted[row]);
        }
      }
    }
  }

  std::unique_ptr<HashTable<false>> createHashTableForAggregation(
      const TypePtr& tableType,
      int numKeys) {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (auto channel = 0; channel < numKeys; ++channel) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(tableType->childAt(channel), channel));
    }
    static std::vector<std::unique_ptr<Aggregate>> empty;
    return HashTable<false>::createForAggregation(
        std::move(keyHashers), empty, mappedMemory_);
  }

  void insertGroups(
      const RowVector& input,
      HashLookup& lookup,
      HashTable<false>& table) {
    const SelectivityVector rows(input.size());
    insertGroups(input, rows, lookup, table);
  }

  void insertGroups(
      const RowVector& input,
      const SelectivityVector& rows,
      HashLookup& lookup,
      HashTable<false>& table) {
    lookup.reset(rows.end());
    lookup.rows.clear();
    rows.applyToSelected([&](auto row) { lookup.rows.push_back(row); });

    auto& hashers = table.hashers();
    auto mode = table.hashMode();
    bool rehash = false;
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input.childAt(hashers[i]->channel());
      hashers[i]->decode(*key, rows);
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(rows, lookup.hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(rows, i > 0, lookup.hashes);
      }
    }

    if (rehash) {
      if (table.hashMode() != BaseHashTable::HashMode::kHash) {
        table.decideHashMode(input.size());
      }
      insertGroups(input, rows, lookup, table);
      return;
    }
    table.groupProbe(lookup);
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
    raw_vector<uint64_t> dummy(batchSize);
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
              hasher->decode(*batch->childAt(i), insertedRows);
              hasher->computeValueIds(insertedRows, dummy);
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
    auto& hashers = topTable_->hashers();
    VectorHasher::ScratchMemory scratchMemory;
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
            hashers[i]->lookupValueIds(
                *key, rows, scratchMemory, lookup->hashes);
          } else {
            hashers[i]->decode(*key, rows);
            hashers[i]->hash(rows, i > 0, lookup->hashes);
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

  // Erases every strideth non-erased item in the hash table.
  void testEraseEveryN(int32_t stride) {
    std::vector<char*> toErase;
    int32_t counter = 0;
    for (auto i = 0; i < rowOfKey_.size(); ++i) {
      if (rowOfKey_[i] && ++counter % stride == 0) {
        toErase.push_back(rowOfKey_[i]);
        rowOfKey_[i] = nullptr;
      }
    }
    topTable_->erase(folly::Range<char**>(toErase.data(), toErase.size()));
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
  int64_t keySpacing_ = 1;
  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

TEST_P(HashTableTest, int2DenseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_P(HashTableTest, string1DenseArray) {
  auto type = ROW({"k1"}, {VARCHAR()});
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 1);
}

TEST_P(HashTableTest, string2Normalized) {
  auto type = ROW({"k1", "k2"}, {VARCHAR(), VARCHAR()});
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 5000, 19, type, 2);
}

TEST_P(HashTableTest, int2SparseArray) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kArray, 500, 2, type, 2);
}

TEST_P(HashTableTest, int2SparseNormalized) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 10000, 2, type, 2);
}

TEST_P(HashTableTest, int2SparseNormalizedMostMiss) {
  auto type = ROW({"k1", "k2"}, {BIGINT(), BIGINT()});
  keySpacing_ = 1000;
  insertPct_ = 10;
  testCycle(BaseHashTable::HashMode::kNormalizedKey, 100000, 2, type, 2);
}

TEST_P(HashTableTest, structKey) {
  auto type =
      ROW({"key"}, {ROW({"k1", "k2", "k3"}, {BIGINT(), VARCHAR(), BIGINT()})});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 100000, 2, type, 1);
}

TEST_P(HashTableTest, mixed6Sparse) {
  auto type =
      ROW({"k1", "k2", "k3", "k4", "k5", "k6"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
  keySpacing_ = 1000;
  testCycle(BaseHashTable::HashMode::kHash, 100000, 9, type, 6);
}

// It should be safe to call clear() before we insert any data into HashTable
TEST_P(HashTableTest, clear) {
  std::vector<std::unique_ptr<VectorHasher>> keyHashers;
  keyHashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0 /*channel*/));
  std::vector<std::unique_ptr<Aggregate>> aggregates;
  aggregates.push_back(Aggregate::create(
      "sum",
      facebook::velox::core::AggregationNode::Step::kPartial,
      std::vector<TypePtr>{BIGINT()},
      BIGINT()));
  auto table = HashTable<true>::createForAggregation(
      std::move(keyHashers), aggregates, mappedMemory_);
  table->clear();
}

/// Test edge case that used to trigger a rounding error in
/// HashTable::enableRangeWhereCan.
TEST_P(HashTableTest, enableRangeWhereCan) {
  auto rowType = ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), VARCHAR()});
  auto table = createHashTableForAggregation(rowType, 3);
  auto lookup = std::make_unique<HashLookup>(table->hashers());

  // Generate 3 keys with the following ranges and number of distinct values
  // (ndv):
  //  0: range=4409503440398, ndv=25
  //  1: range=18446744073709551615, ndv=748
  //  2: range=18446744073709551615, ndv=1678

  std::vector<int64_t> a;
  for (int i = 1; i < 25; i++) {
    a.push_back(i);
  }
  a.back() = 4409503440398;

  std::vector<std::string> b;
  for (int i = 1; i < 748; i++) {
    b.push_back(std::string(15, '.') + std::to_string(i));
  }

  std::vector<std::string> c;
  for (int i = 1; i < 1678; i++) {
    c.push_back(std::string(15, '.') + std::to_string(i));
  }

  auto data = vectorMaker_->rowVector({
      vectorMaker_->flatVector<int64_t>(
          2'000, [&](auto row) { return a[row % a.size()]; }),
      vectorMaker_->flatVector<StringView>(
          2'000, [&](auto row) { return StringView(b[row % b.size()]); }),
      vectorMaker_->flatVector<StringView>(
          2'000, [&](auto row) { return StringView(c[row % c.size()]); }),
  });

  lookup->reset(data->size());
  insertGroups(*data, *lookup, *table);
}

TEST_P(HashTableTest, arrayProbeNormalizedKey) {
  auto table = createHashTableForAggregation(ROW({"a"}, {BIGINT()}), 1);
  auto lookup = std::make_unique<HashLookup>(table->hashers());

  for (auto i = 0; i < 200; ++i) {
    auto data = vectorMaker_->rowVector({
        vectorMaker_->flatVector<int64_t>(
            10'000, [&](auto row) { return i * 10'000 + row; }),
    });

    SelectivityVector rows(5'000);
    insertGroups(*data, rows, *lookup, *table);

    rows.resize(10'000);
    rows.clearAll();
    rows.setValidRange(5'000, 10'000, true);
    rows.updateBounds();
    insertGroups(*data, rows, *lookup, *table);
  }

  ASSERT_TRUE(table->hashMode() == BaseHashTable::HashMode::kNormalizedKey);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashTableTests,
    HashTableTest,
    testing::Values(true, false));
