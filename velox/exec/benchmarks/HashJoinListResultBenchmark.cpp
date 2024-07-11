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
#include "velox/exec/OperatorUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <iostream>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct HashTableBenchmarkParams {
  HashTableBenchmarkParams() = default;

  // Benchmark params, we need to provide:
  //  -the expect hash mode,
  //  -the build & probe row schema,
  //  -the expected hash table size,
  //  -number of probing rows,
  //  -build key repetition distribution.
  HashTableBenchmarkParams(
      BaseHashTable::HashMode mode,
      const TypePtr& buildType,
      int64_t hashTableSize,
      int64_t probeSize,
      const std::vector<std::pair<int32_t, int32_t>>&
          keyRepeatTimesDistribution,
      bool runErase)
      : mode{mode},
        buildType{buildType},
        hashTableSize{hashTableSize},
        probeSize{probeSize},
        keyRepeatTimesDistribution{keyRepeatTimesDistribution},
        runErase{runErase} {
    int32_t distSum = 0;
    buildSize = 0;
    buildKeyRepeat.reserve(keyRepeatTimesDistribution.size());
    for (auto dist : keyRepeatTimesDistribution) {
      if (dist.first > 100 || dist.first < 0 || dist.second < 0) {
        VELOX_FAIL(
            "Bad distribution: [distribution:{}, key duplicate count:{}]",
            dist.first,
            dist.second);
      }
      buildSize += (hashTableSize * dist.first / 100) * (dist.second + 1);
      distSum += dist.first;
      buildKeyRepeat.emplace_back(
          std::make_pair(hashTableSize * distSum / 100, dist.second));
    }
    VELOX_CHECK_EQ(distSum, 100, "Sum of distributions should be 100");

    if (hashTableSize > BaseHashTable::kArrayHashMaxSize &&
        mode == BaseHashTable::HashMode::kArray) {
      VELOX_FAIL("Bad hash mode.");
    }

    numDependentFields = buildType->size() - 1;
    if (mode == BaseHashTable::HashMode::kNormalizedKey) {
      extraValue = BaseHashTable::kArrayHashMaxSize + 100;
    } else if (mode == BaseHashTable::HashMode::kHash) {
      extraValue = std::numeric_limits<int64_t>::max() - 1;
    } else {
      extraValue = 0;
    }
    std::string modeString = mode == BaseHashTable::HashMode::kArray ? "array"
        : mode == BaseHashTable::HashMode::kHash                     ? "hash"
                                                 : "normalized key";
    std::stringstream distStr;
    for (auto dist : keyRepeatTimesDistribution) {
      distStr << fmt::format("{}%:{};", dist.first, dist.second);
    }
    title = fmt::format(
        "{},probe:{},buildDist:{}", modeString, probeSize, distStr.str());
    if (runErase) {
      title += ",withErase";
    }
  }

  // Expected mode.
  BaseHashTable::HashMode mode;

  // Type of build & probe row.
  TypePtr buildType;

  // Distinct rows in the table.
  int64_t hashTableSize;

  // Number of probe rows.
  int64_t probeSize;

  // Key repeation distribution: {{80, 1}, {20, 0}}: 80% keys have 1
  // duplication, 20$ keys have no duplication.
  std::vector<std::pair<int32_t, int32_t>> keyRepeatTimesDistribution;

  bool runErase;

  // Title for reporting
  std::string title;

  // Number of build rows.
  int64_t buildSize;

  // This parameter controls the hashing mode. It is incorporated into the keys
  // on the build side. If the expected mode is an array, its value is 0. If
  // the expected mode is a normalized key, its value is 'kArrayHashMaxSize' +
  // 100 to make the key range > 'kArrayHashMaxSize'. If the expected mode is a
  // hash, its value is the maximum value of int64_t minus 1 to make the key
  // range  == 'kRangeTooLarge'.
  int64_t extraValue;

  std::vector<std::pair<int32_t, int32_t>> buildKeyRepeat;

  // Number of dependent fields.
  int32_t numDependentFields;

  // Number of the tables: 1 top talbe + 7 other tables.
  int32_t numTables{8};

  std::string toString() const {
    std::stringstream distStr;
    for (auto dist : keyRepeatTimesDistribution) {
      distStr << fmt::format("{}%:{}; ", dist.first, dist.second);
    }
    return fmt::format(
        "HashTableSize:{}, BuildInputSize:{}, ProbeInputSize:{}, ExpectHashMode:{}, KeyRepeatTimesDistribution: {}",
        hashTableSize,
        buildSize,
        probeSize,
        BaseHashTable::modeString(mode),
        distStr.str());
  }
};

struct HashTableBenchmarkResult {
  HashTableBenchmarkParams params;

  // Number of output rows after 'listJoinResult'.
  int64_t numOutput;

  int32_t numIter{1};

  double buildClocks{0};

  double listJoinResultClocks{0};

  double totalClock{0};

  double eraseClock{0};

  // The mode of the table.
  BaseHashTable::HashMode hashMode;

  void merge(HashTableBenchmarkResult other) {
    numIter++;
    buildClocks += other.buildClocks;
    listJoinResultClocks += other.listJoinResultClocks;
    totalClock += other.totalClock;
    eraseClock += other.eraseClock;
  }

  std::string toString() const {
    std::stringstream out;
    out << params.toString();
    out << std::endl
        << " mode=" << BaseHashTable::modeString(hashMode)
        << " numOutput=" << numOutput << " totalClock=" << totalClock
        << " listJoinResultClocks=" << listJoinResultClocks << "("
        << (listJoinResultClocks / totalClock * 100)
        << "%) buildClocks=" << buildClocks << "("
        << (buildClocks / totalClock * 100) << "%)";
    if (params.runErase) {
      out << " eraseClock=" << eraseClock << "("
          << (eraseClock / totalClock * 100) << "%)";
    }
    return out.str();
  }
};

class HashTableListJoinResultBenchmark : public VectorTestBase {
 public:
  HashTableListJoinResultBenchmark()
      : randomEngine_((std::random_device{}())) {}

  HashTableBenchmarkResult run(HashTableBenchmarkParams params) {
    params_ = params;
    HashTableBenchmarkResult result;
    result.params = params_;
    SelectivityInfo totalClock;
    {
      SelectivityTimer timer(totalClock, 0);
      buildTable();
      result.numOutput = probeTableAndListResult();
      result.hashMode = topTable_->hashMode();
      VELOX_CHECK_EQ(result.hashMode, params_.mode);
      if (params.runErase) {
        eraseTable();
        result.eraseClock += eraseTime_;
      }
      topTable_.reset();
    }
    result.buildClocks += buildTime_;
    result.listJoinResultClocks += listJoinResultTime_;
    result.totalClock += totalClock.timeToDropValue();

    return result;
  }

 private:
  // Generate buiild key based on current key and key repetition distribution.
  int64_t getBuildKey(int64_t& buildKey, int32_t& iterTimes, int32_t& repeat) {
    if (iterTimes >= repeat) {
      iterTimes = 0;
      buildKey++;
      for (auto iter : params_.buildKeyRepeat) {
        if (buildKey < iter.first) {
          repeat = iter.second;
          break;
        }
      }
    } else {
      iterTimes++;
    }
    return buildKey;
  }

  // Create the row vector for the build side, where the first column is used
  // as the join key, and the remaining columns are dependent fields.
  // If expect mode is array, the key is within the range [0, hashTableSize];
  // If expect mode is normalized key, the key is within the range
  // [0, hashTableSize] + extraValue(kArrayHashMaxSize + 100);
  // If expect mode is hash, the key is within the range [0, hashTableSize] +
  // extraValue(max_int64 -1);
  RowVectorPtr makeBuildRows(
      int64_t maxKey,
      bool addExtraValue,
      int64_t& buildKey,
      int32_t& iterTimes,
      int32_t& repeat) {
    std::vector<int64_t> data;
    while (buildKey < maxKey) {
      auto key = getBuildKey(buildKey, iterTimes, repeat);
      data.emplace_back(key);
    }
    if (addExtraValue) {
      data[0] = params_.extraValue;
    }

    std::shuffle(data.begin(), data.end(), randomEngine_);
    std::vector<VectorPtr> children;
    children.push_back(makeFlatVector<int64_t>(data));
    for (int32_t i = 0; i < params_.numDependentFields; ++i) {
      children.push_back(makeFlatVector<int64_t>(
          data.size(),
          [&](vector_size_t row) { return row + maxKey; },
          nullptr));
    }
    return makeRowVector(children);
  }

  // Generate the build side data batches, one batch pre table.
  void makeBuildBatches(std::vector<RowVectorPtr>& batches) {
    int64_t buildKey = -1;
    int32_t iterTimes = 0;
    int32_t repeat = 0;
    int64_t maxKey = 0;
    for (auto i = 0; i < params_.numTables; ++i) {
      if (i == params_.numTables - 1) {
        maxKey = params_.hashTableSize;
      } else {
        maxKey += params_.hashTableSize / params_.numTables;
      }
      batches.push_back(makeBuildRows(
          maxKey, i == params_.numTables - 1, buildKey, iterTimes, repeat));
    }
  }

  // Create the row vector for the probe side, where the first column is used
  // as the join key, and the remaining columns are dependent fields.
  // Probe key is within the range [0, hashTableSize].
  RowVectorPtr
  makeProbeVector(int32_t size, int64_t hashTableSize, int64_t& sequence) {
    std::vector<VectorPtr> children;
    children.push_back(makeFlatVector<int64_t>(
        size,
        [&](vector_size_t row) { return (sequence + row) % hashTableSize; },
        nullptr));
    sequence += size;
    for (int32_t i = 0; i < params_.numDependentFields; ++i) {
      children.push_back(makeFlatVector<int64_t>(
          size, [&](vector_size_t row) { return row + size; }, nullptr));
    }
    return makeRowVector(children);
  }

  void copyVectorsToTable(RowVectorPtr batch, BaseHashTable* table) {
    int32_t batchSize = batch->size();
    raw_vector<uint64_t> dummy(batchSize);
    auto rowContainer = table->rows();
    auto& hashers = table->hashers();
    auto numKeys = hashers.size();
    auto numDependentFields = batch->childrenSize() - numKeys;

    std::vector<DecodedVector> decoders;
    decoders.reserve(numDependentFields);
    SelectivityVector rows(batchSize);

    for (auto i = 0; i < batch->childrenSize(); ++i) {
      if (i < numKeys) {
        auto hasher = table->hashers()[i].get();
        hasher->decode(*batch->childAt(i), rows);
        if (table->hashMode() != BaseHashTable::HashMode::kHash &&
            hasher->mayUseValueIds()) {
          hasher->computeValueIds(rows, dummy);
        }
      } else {
        decoders[i - numKeys].decode(*batch->childAt(i), rows);
      }
    }
    rows.applyToSelected([&](auto rowIndex) {
      char* newRow = rowContainer->newRow();
      *reinterpret_cast<char**>(newRow + rowContainer->nextOffset()) = nullptr;
      for (auto i = 0; i < numKeys; ++i) {
        rowContainer->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
      }
      for (auto i = 0; i < numDependentFields; ++i) {
        rowContainer->store(decoders[i], rowIndex, newRow, i + numKeys);
      }
    });
  }

  // Prepare join table.
  void buildTable() {
    std::vector<TypePtr> dependentTypes;
    std::vector<std::unique_ptr<BaseHashTable>> otherTables;
    std::vector<RowVectorPtr> batches;
    makeBuildBatches(batches);
    for (auto i = 0; i < params_.numTables; ++i) {
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(params_.buildType->childAt(0), 0));
      auto table = HashTable<true>::createForJoin(
          std::move(keyHashers),
          dependentTypes,
          true,
          false,
          1'000,
          pool_.get());

      copyVectorsToTable(batches[i], table.get());
      if (i == 0) {
        topTable_ = std::move(table);
      } else {
        otherTables.push_back(std::move(table));
      }
    }
    SelectivityInfo buildClocks;
    {
      SelectivityTimer timer(buildClocks, 0);
      topTable_->prepareJoinTable(std::move(otherTables), executor_.get());
    }
    buildTime_ = buildClocks.timeToDropValue();
  }

  void probeTable(
      HashLookup& lookup,
      const RowVectorPtr& batch,
      const int64_t batchSize) {
    auto& hashers = topTable_->hashers();
    auto mode = topTable_->hashMode();
    SelectivityVector rows(batchSize);
    VectorHasher::ScratchMemory scratchMemory;
    lookup.reset(batch->size());
    for (auto i = 0; i < hashers.size(); ++i) {
      auto key = batch->childAt(i);
      if (mode != BaseHashTable::HashMode::kHash) {
        hashers[i]->lookupValueIds(*key, rows, scratchMemory, lookup.hashes);
      } else {
        hashers[i]->decode(*key, rows);
        hashers[i]->hash(rows, i > 0, lookup.hashes);
      }
    }
    lookup.rows.resize(rows.size());
    std::iota(lookup.rows.begin(), lookup.rows.end(), 0);
    topTable_->joinProbe(lookup);
  }

  // Hash probe andd list join result.
  int64_t probeTableAndListResult() {
    auto lookup = std::make_unique<HashLookup>(topTable_->hashers());
    auto numBatch = params_.probeSize / params_.hashTableSize;
    auto batchSize = params_.hashTableSize;
    SelectivityInfo listJoinResultClocks;
    BaseHashTable::JoinResultIterator results;
    BufferPtr outputRowMapping;
    auto outputBatchSize = batchSize;
    std::vector<char*> outputTableRows;
    int64_t sequence = 0;
    int64_t numJoinListResult = 0;
    for (auto i = 0; i < numBatch; ++i) {
      auto batch = makeProbeVector(batchSize, params_.hashTableSize, sequence);
      probeTable(*lookup, batch, batchSize);
      results.reset(*lookup);
      auto mapping = initializeRowNumberMapping(
          outputRowMapping, outputBatchSize, pool_.get());
      outputTableRows.resize(outputBatchSize);
      {
        SelectivityTimer timer(listJoinResultClocks, 0);
        while (!results.atEnd()) {
          numJoinListResult += topTable_->listJoinResults(
              results,
              false,
              mapping,
              folly::Range(outputTableRows.data(), outputTableRows.size()));
        }
      }
    }
    listJoinResultTime_ = listJoinResultClocks.timeToDropValue();
    return numJoinListResult;
  }

  void eraseTable() {
    auto lookup = std::make_unique<HashLookup>(topTable_->hashers());
    auto batchSize = 10000;
    auto mode = topTable_->hashMode();
    SelectivityInfo eraseClock;
    BaseHashTable::JoinResultIterator results;
    BufferPtr outputRowMapping;
    auto outputBatchSize = topTable_->rows()->numRows() + 2;
    std::vector<char*> outputTableRows;
    int64_t sequence = 0;
    auto batch = makeProbeVector(batchSize, batchSize, sequence);
    probeTable(*lookup, batch, batchSize);
    results.reset(*lookup);
    auto mapping = initializeRowNumberMapping(
        outputRowMapping, outputBatchSize, pool_.get());
    outputTableRows.resize(outputBatchSize);
    auto num = topTable_->listJoinResults(
        results,
        false,
        mapping,
        folly::Range(outputTableRows.data(), outputTableRows.size()));
    {
      SelectivityTimer timer(eraseClock, 0);
      topTable_->rows()->eraseRows(
          folly::Range<char**>(outputTableRows.data(), num));
    }
    eraseTime_ += eraseClock.timeToDropValue();
  }

  std::default_random_engine randomEngine_;
  std::unique_ptr<HashTable<true>> topTable_;
  HashTableBenchmarkParams params_;

  double buildTime_{0};
  double eraseTime_{0};
  double listJoinResultTime_{0};
};

void combineResults(
    std::vector<HashTableBenchmarkResult>& results,
    HashTableBenchmarkResult run) {
  if (!results.empty() && results.back().params.title == run.params.title) {
    results.back().merge(run);
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

  auto bm = std::make_unique<HashTableListJoinResultBenchmark>();
  std::vector<HashTableBenchmarkResult> results;

  auto hashTableSize = (2L << 20) - 3;
  auto probeRowSize = 100000000L;

  TypePtr onlyKeyType{ROW({"k1"}, {BIGINT()})};

  TypePtr keyAndDependentType{
      ROW({"k1", "d1", "d2"}, {BIGINT(), BIGINT(), BIGINT()})};

  std::vector<BaseHashTable::HashMode> hashModes = {
      BaseHashTable::HashMode::kArray,
      BaseHashTable::HashMode::kNormalizedKey,
      BaseHashTable::HashMode::kHash};

  std::vector<std::vector<std::pair<int32_t, int32_t>>> keyRepeatDists = {
      // 20% of the rows are repeated only once, and 80% of the rows are not
      // repeated.
      {{20, 1}, {80, 0}},
      {{20, 5}, {80, 0}},
      {{20, 10}, {80, 0}},
      {{20, 20}, {80, 0}},
      {{20, 50}, {80, 0}},
      {{10, 5}, {10, 1}, {80, 0}},
      {{10, 10}, {10, 5}, {10, 1}, {70, 0}},
      {{10, 20}, {10, 10}, {10, 5}, {10, 1}, {60, 0}},
      {{10, 50}, {10, 20}, {10, 10}, {10, 5}, {10, 1}, {50, 0}},
      {{100, 1}},
      {{100, 5}},
      {{100, 10}},
      {{100, 15}},
      {{100, 20}},
      {{100, 25}}};
  std::vector<HashTableBenchmarkParams> params;
  for (auto withErase : {false, true}) {
    for (auto mode : hashModes) {
      for (auto& dist : keyRepeatDists) {
        params.emplace_back(HashTableBenchmarkParams(
            mode, onlyKeyType, hashTableSize, probeRowSize, dist, withErase));
      }
    }
  }

  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.title, [param, &bm, &results]() {
      combineResults(results, bm->run(param));
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
