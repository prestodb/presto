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

#include "velox/common/base/SelectivityInfo.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <memory>

#include "velox/common/process/Profiler.h"

DEFINE_int64(custom_size, 0, "Custom number of entries");
DEFINE_int32(custom_hit_rate, 0, "Percentage of hits in custom test");
DEFINE_int32(custom_key_spacing, 1, "Spacing between key values");

DEFINE_int32(custom_num_ways, 10, "Number of build threads");

DEFINE_bool(profile, false, "Generate perf profiles and memory stats");

DECLARE_bool(velox_time_allocations);

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct HashTableBenchmarkParams {
  HashTableBenchmarkParams() = default;

  HashTableBenchmarkParams(
      std::string title,
      int64_t size,
      int32_t hitrate,
      int32_t keySpacing = 1,
      int32_t _numWays = 10)
      : title(std::move(title)),
        buildSize(size),
        size(100 * (size / _numWays) / hitrate),
        numWays(_numWays),
        insertPct(hitrate),
        keySpacing(keySpacing) {}

  // Title for reporting
  std::string title;

  // Expected mode.
  BaseHashTable::HashMode mode{BaseHashTable::HashMode::kNormalizedKey};

  int64_t buildSize;

  // Number of distinct probe rows. Not all are necessarily in the table.
  int32_t size;

  // Number of build RowContainers.
  int32_t numWays;

  // Type of build row.
  TypePtr buildType{ROW({"k1"}, {BIGINT()})};

  // Number of leading elements in 'buildType' that make up the key.
  int32_t numKeys{1};

  //  Percentage of the 'size' probe side rows that are inserted into the
  //  table.
  int32_t insertPct;

  // Space between consecutive key values. Affects use of range in
  // VectorHasher.
  int32_t keySpacing{1};

  std::string toString() const {
    return fmt::format(
        "{}: Rows={} Hit%={} NumProbes={}",
        title,
        buildSize,
        insertPct,
        size * numWays);
  }
};

struct HashTableBenchmarkRun {
  HashTableBenchmarkParams params;

  // __rdtsc clocks per hashed probe row. Same for Velox and F14 cases.
  float hashClocks{0};

  // Result in __rdtsc clocks for total probe over number of probed rows.
  float probeClocks{0};

  // Distinct rows in the table.
  int32_t numDistinct;

  // The mode of the table.
  BaseHashTable::HashMode hashMode;

  // Clocks for same operation with F14FastSet if applicable.
  float f14ProbeClocks{-1};

  std::string toString() const {
    std::stringstream out;
    out << params.toString();
    out << " hash/row=" << hashClocks << " probe clocks=" << probeClocks;
    if (f14ProbeClocks != -1) {
      out << " f14Probe=" << f14ProbeClocks << " ("
          << (100 * f14ProbeClocks / probeClocks) << "%)";
    }
    std::string modeString = hashMode == BaseHashTable::HashMode::kArray
        ? "array"
        : hashMode == BaseHashTable::HashMode::kHash ? "hash"
                                                     : "normalized key";
    out << std::endl
        << " numDistinct=" << numDistinct << " mode=" << modeString;
    return out.str();
  }
};

// Test framework for join hash tables. Generates probe keys, of which
// some percent are inserted in a hashTable. The placement of the
// payload is shuffled so as not to correlate with the probe
// order. Tests the presence/correctness of the hit for each key and
// measures the time for computing hashes/value ids vs the time spent
// probing the table. Covers kArray, kNormalizedKey and kHash hash
// modes.
class HashTableBenchmark : public VectorTestBase {
 public:
  void makeData(HashTableBenchmarkParams params) {
    topTable_.reset();
    batches_.clear();
    rowOfKey_.clear();
    isInTable_.clear();
    params_ = params;
    std::vector<TypePtr> dependentTypes;
    int32_t sequence = 0;
    isInTable_.resize(
        bits::nwords(params_.numWays * params_.size),
        static_cast<const std::vector<
            unsigned long,
            std::allocator<unsigned long>>::value_type>(-1));
    if (params_.insertPct != 100) {
      // If we probe with all keys but only mean to insert part, we deselect.
      folly::Random::DefaultGenerator rng;
      rng.seed(1);
      for (auto i = 0; i < params_.size * params_.numWays; ++i) {
        if (folly::Random::rand32(rng) % 100 > params_.insertPct) {
          bits::clearBit(isInTable_.data(), i);
        }
      }
    }
    int32_t startOffset = 0;
    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    for (auto way = 0; way < params_.numWays; ++way) {
      std::vector<RowVectorPtr> batches;
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      for (auto channel = 0; channel < params_.numKeys; ++channel) {
        keyHashers.emplace_back(std::make_unique<VectorHasher>(
            params_.buildType->childAt(channel), channel));
      }
      auto table = HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          true,
          false,
          1'000,
          pool_.get());

      makeRows(params_.size, 1, sequence, params_.buildType, batches);
      copyVectorsToTable(batches, startOffset, table.get());
      sequence += params_.size;
      if (!topTable_) {
        topTable_ = std::move(table);
      } else {
        otherTables.push_back(std::move(table));
      }
      batches_.insert(batches_.end(), batches.begin(), batches.end());
      startOffset += params_.size;
    }
    topTable_->prepareJoinTable(std::move(otherTables), executor_.get());
    LOG(INFO) << "Made table " << topTable_->toString();

    if (topTable_->hashMode() == BaseHashTable::HashMode::kNormalizedKey) {
      f14Table_ = std::make_unique<F14TestTable>(
          1024,
          F14TestHasher(),
          F14TestComparer(),
          memory::StlAllocator<uint64_t*>(*pool_));
      constexpr int32_t kInsertBatch = 1000;
      char* insertRows[kInsertBatch];
      auto& otherTables = topTable_->testingOtherTables();
      for (auto i = 0; i <= otherTables.size(); ++i) {
        auto subtable = i == 0 ? topTable_.get() : otherTables[i - 1].get();
        RowContainerIterator iter;
        while (auto numRows = subtable->rows()->listRows(
                   &iter, kInsertBatch, RowContainer::kUnlimited, insertRows)) {
          for (auto row = 0; row < numRows; ++row) {
            f14Table_->insert(reinterpret_cast<uint64_t*>(insertRows[row]));
          }
        }
      }
    }
  }

  HashTableBenchmarkRun run() {
    HashTableBenchmarkRun result;
    result.params = params_;
    testProbe();
    result.hashClocks = hashClocksPerRow_;
    result.probeClocks = clocksPerRow_;
    result.hashMode = topTable_->hashMode();
    result.numDistinct = topTable_->numDistinct();
    if (topTable_->hashMode() == BaseHashTable::HashMode::kNormalizedKey) {
      testF14Probe();
      result.f14ProbeClocks = clocksPerRow_;
    }
    return result;
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

  void copyVectorsToTable(
      const std::vector<RowVectorPtr>& batches,
      int32_t tableOffset,
      BaseHashTable* table) {
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
      if (params_.insertPct < 100) {
        bits::copyBits(
            isInTable_.data(),
            tableOffset + batchOffset,
            insertedRows.asMutableRange().bits(),
            0,
            batchSize);
        insertedRows.updateBounds();
      }
      decoded.emplace_back(batch->childrenSize());
      VELOX_CHECK_EQ(batch->size(), batchSize);
      auto& decoders = decoded.back();
      for (auto i = 0; i < batch->childrenSize(); ++i) {
        decoders[i].decode(*batch->childAt(i), rows);
        if (i < numKeys) {
          auto hasher = table->hashers()[i].get();
          hasher->decode(*batch->childAt(i), insertedRows);
          if (table->hashMode() != BaseHashTable::HashMode::kHash &&
              hasher->mayUseValueIds()) {
            hasher->computeValueIds(insertedRows, dummy);
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
    auto nextOffset = rowContainer->nextOffset();

    // We insert values in a geometric skip order. 1, 2, 4, 7,
    // 11,... where the skip increments by one. We wrap around at the
    // power of two boundary. This sequence hits every place in the
    // power of two range once. Like this, when we probe the data for
    // consecutive keys the hits will have no cache locality.
    for (auto count = 0; count < powerOfTwo; ++count) {
      if (position < size &&
          (params_.insertPct == 100 ||
           bits::isBitSet(isInTable_.data(), tableOffset + position))) {
        char* newRow = rowContainer->newRow();
        rowOfKey_[tableOffset + position] = newRow;
        auto batchIndex = position / batchSize;
        auto rowIndex = position % batchSize;
        if (nextOffset) {
          *reinterpret_cast<char**>(newRow + nextOffset) = nullptr;
        }
        for (auto i = 0; i < batches[batchIndex]->type()->size(); ++i) {
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
            [&](vector_size_t row) {
              return params_.keySpacing * (sequence + row);
            },
            nullptr);

      case TypeKind::VARCHAR: {
        auto strings = BaseVector::create<FlatVector<StringView>>(
            VARCHAR(), size, pool_.get());
        for (auto row = 0; row < size; ++row) {
          auto string =
              fmt::format("{}", params_.keySpacing * (sequence + row));
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
    hashClocksPerRow_ = hashTime.timeToDropValue() / numHashed;

    clocksPerRow_ = probeTime.timeToDropValue() / numProbed;

    std::cout
        << fmt::format(
               "Hashed: {} Probed: {} Hit: {} Hash time/row {} probe time/row {}",
               numHashed,
               numProbed,
               numHit,
               hashTime.timeToDropValue() / numHashed,
               probeTime.timeToDropValue() / numProbed)
        << std::endl;
  }

  // Same as testProbe for normalized keys, uses F14Set instead.
  void testF14Probe() {
    auto lookup = std::make_unique<HashLookup>(topTable_->hashers());

    auto batchSize = batches_[0]->size();
    SelectivityVector rows(batchSize);
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
          hashers[i]->lookupValueIds(*key, rows, scratchMemory, lookup->hashes);
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
          for (auto row = 0; row < lookup->rows.size(); ++row) {
            auto index = lookup->rows[row];
            uint64_t key = lookup->hashes[index];
            uint64_t* keyPtr = &key + 1;
            auto it = f14Table_->find(keyPtr);
            lookup->hits[index] =
                it == f14Table_->end() ? nullptr : reinterpret_cast<char*>(*it);
          }
        }
        for (auto i = 0; i < lookup->rows.size(); ++i) {
          auto key = lookup->rows[i];
          numHit += lookup->hits[key] != nullptr;
          ASSERT_EQ(rowOfKey_[startOffset + key], lookup->hits[key]);
        }
      }
    }
    hashClocksPerRow_ = hashTime.timeToDropValue() / numHashed;
    clocksPerRow_ = probeTime.timeToDropValue() / numProbed;

    std::cout
        << fmt::format(
               "F14set: Hashed: {} Probed: {} Hit: {} Hash time/row {} probe time/row {}",
               numHashed,
               numProbed,
               numHit,
               hashTime.timeToDropValue() / numHashed,
               probeTime.timeToDropValue() / numProbed)
        << std::endl;
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::unique_ptr<VectorMaker> vectorMaker_{
      std::make_unique<VectorMaker>(pool_.get())};
  // Bitmap of positions in batches_ that end up in the table.
  std::vector<uint64_t> isInTable_;
  // Test payload, keys first.
  std::vector<RowVectorPtr> batches_;

  // Corresponds 1:1 to data in 'batches_'. nullptr if the key is not
  // inserted, otherwise pointer into the RowContainer.
  std::vector<char*> rowOfKey_;
  std::unique_ptr<HashTable<true>> topTable_;
  HashTableBenchmarkParams params_;

  // Timing set by test*Probe().
  float hashClocksPerRow_{0};
  float clocksPerRow_{0};

  // hasher and comparer for F14 comparison test.
  struct F14TestHasher {
    // Same as mixNormalizedKey() in HashTable.cpp.
    size_t operator()(uint64_t* value) const {
      return folly::hasher<uint64_t>()(value[-1]);
    }
  };

  struct F14TestComparer {
    bool operator()(uint64_t* left, uint64_t* right) const {
      return left[-1] == right[-1];
    }
  };

  using F14TestTable = folly::F14FastSet<
      uint64_t*,
      F14TestHasher,
      F14TestComparer,
      memory::StlAllocator<uint64_t*>>;
  std::unique_ptr<F14TestTable> f14Table_;
};

void combineResults(
    std::vector<HashTableBenchmarkRun>& results,
    HashTableBenchmarkRun run) {
  if (!results.empty() && results.back().params.title == run.params.title) {
    return;
  }
  results.push_back(run);
}
} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManagerOptions options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);
  if (FLAGS_profile) {
    auto allocator = memory::MemoryManager::getInstance()->allocator();
    std::function<void()> reportInit;
    std::function<std::string()> report;
    allocator->getTracingHooks(reportInit, report, nullptr);
    FLAGS_profiler_check_interval_seconds = 20;
    FLAGS_profiler_min_cpu_pct = 50;
    FLAGS_profiler_max_sample_seconds = 30;
    FLAGS_velox_time_allocations = true;
    filesystems::registerLocalFileSystem();
    process::Profiler::start("/tmp/hashprof", reportInit, report);
  }
  auto bm = std::make_unique<HashTableBenchmark>();
  std::vector<HashTableBenchmarkRun> results;

  std::vector<HashTableBenchmarkParams> params = {
      HashTableBenchmarkParams("Hit10K", 10000, 100),
      HashTableBenchmarkParams("Miss10K", 10000, 5),

      HashTableBenchmarkParams("HitVid10K", 10000, 100, 1000),
      HashTableBenchmarkParams("MissVid10K", 10000, 5, 1000),

      HashTableBenchmarkParams("Hit4M", 4000000, 100),
      HashTableBenchmarkParams("Miss4M", 4000000, 5),

      HashTableBenchmarkParams("Hit32M", 32000000, 100),
      HashTableBenchmarkParams("Miss32M", 32000000, 5),

      HashTableBenchmarkParams("Hit128M", 128000000, 100)};
  if (FLAGS_custom_size != 0) {
    params.push_back(HashTableBenchmarkParams(
        "Custom",
        FLAGS_custom_size,
        FLAGS_custom_hit_rate,
        FLAGS_custom_key_spacing,
        FLAGS_custom_num_ways));
  }

  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.title, [param, &bm, &results]() {
      std::string lastCase;
      if (lastCase != param.title) {
        lastCase = param.title;
        folly::BenchmarkSuspender suspender;
        bm->makeData(param);
      }
      combineResults(results, bm->run());
      return 1;
    });
  }
  folly::runBenchmarks();
  std::cout << "*** Results:" << std::endl;
  for (auto& result : results) {
    std::cout << result.toString() << std::endl;
  }
  return 0;
}
