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
  //  -the build row schema,
  //  -the expected hash table size,
  //  -number of building rows,
  //  -number of build RowContainers.
  HashTableBenchmarkParams(
      BaseHashTable::HashMode mode,
      const TypePtr& buildType,
      int64_t hashTableSize,
      int64_t buildSize,
      int32_t numWays)
      : mode{mode},
        buildType{buildType},
        hashTableSize{hashTableSize},
        buildSize{buildSize},
        numWays{numWays} {
    VELOX_CHECK_LE(hashTableSize, buildSize);
    VELOX_CHECK_GE(numWays, 1);

    if (hashTableSize > BaseHashTable::kArrayHashMaxSize &&
        mode == BaseHashTable::HashMode::kArray) {
      VELOX_FAIL("Bad hash mode.");
    }

    numFields = buildType->size();
    if (mode == BaseHashTable::HashMode::kNormalizedKey) {
      extraValue = BaseHashTable::kArrayHashMaxSize + 100;
    } else if (mode == BaseHashTable::HashMode::kHash) {
      extraValue = std::numeric_limits<int64_t>::max() - 1;
    } else {
      extraValue = 0;
    }

    title = fmt::format(
        "Size:{},parallel:{},withDup:{},{}",
        buildSize,
        numWays > 1,
        buildSize > hashTableSize,
        BaseHashTable::modeString(mode));
  }

  // Expected mode.
  BaseHashTable::HashMode mode;

  // Type of build & probe row.
  TypePtr buildType;

  // Distinct rows in the table.
  int64_t hashTableSize;

  // Number of build rows.
  int64_t buildSize;

  // Number of build RowContainers.
  int32_t numWays;

  // Title for reporting
  std::string title;

  // This parameter controls the hashing mode. It is incorporated into the keys
  // on the build side. If the expected mode is an array, its value is 0. If
  // the expected mode is a normalized key, its value is 'kArrayHashMaxSize' +
  // 100 to make the key range > 'kArrayHashMaxSize'. If the expected mode is a
  // hash, its value is the maximum value of int64_t minus 1 to make the key
  // range  == 'kRangeTooLarge'.
  int64_t extraValue;

  // Number of fields.
  int32_t numFields;

  std::string toString() const {
    return fmt::format(
        "HashTableSize:{}, BuildInputSize:{}, ExpectHashMode:{}",
        hashTableSize,
        buildSize,
        BaseHashTable::modeString(mode));
  }
};

class HashJoinPrepareJoinTableBenchmark : public VectorTestBase {
 public:
  HashJoinPrepareJoinTableBenchmark()
      : randomEngine_((std::random_device{}())) {}

  // Create join tables.
  void prepare(HashTableBenchmarkParams params) {
    params_ = params;
    topTable_.reset();
    otherTables_.clear();
    createTable();
  }

  // Run 'prepareJoinTable'.
  void run() {
    topTable_->prepareJoinTable(std::move(otherTables_), executor_.get());
    VELOX_CHECK_EQ(topTable_->hashMode(), params_.mode);
  }

 private:
  // Create the row vector for the build side, where the first column is used
  // as the join key, and the remaining columns are dependent fields.
  // If expect mode is array, the key is within the range [0, hashTableSize];
  // If expect mode is normalized key, the key is within the range
  // [0, hashTableSize] + extraValue(kArrayHashMaxSize + 100);
  // If expect mode is hash, the key is within the range [0, hashTableSize] +
  // extraValue(max_int64 -1);
  RowVectorPtr makeBuildRows(
      int64_t numKeys,
      int64_t maxKey,
      int64_t& buildKey,
      bool addExtraValue) {
    std::vector<int64_t> data;
    auto makeData = [&]() {
      data.clear();
      for (auto i = 0; i < numKeys; ++i) {
        data.emplace_back((buildKey++) % maxKey);
      }
      if (addExtraValue) {
        data[0] = params_.extraValue;
      }
      std::shuffle(data.begin(), data.end(), randomEngine_);
    };

    std::vector<VectorPtr> children;
    for (int32_t i = 0; i < params_.numFields; ++i) {
      makeData();
      children.push_back(makeFlatVector<int64_t>(data));
    }
    return makeRowVector(children);
  }

  // Generate the build side data batches, one batch pre table.
  void makeBuildBatches(std::vector<RowVectorPtr>& batches) {
    int64_t buildKey = 0;
    for (auto i = 0; i < params_.numWays; ++i) {
      batches.push_back(makeBuildRows(
          params_.buildSize / params_.numWays,
          params_.hashTableSize,
          buildKey,
          i == params_.numWays - 1));
    }
  }

  void copyVectorsToTable(RowVectorPtr batch, BaseHashTable* table) {
    int32_t batchSize = batch->size();
    raw_vector<uint64_t> dummy(batchSize);
    auto rowContainer = table->rows();
    auto& hashers = table->hashers();
    auto numKeys = hashers.size();
    SelectivityVector rows(batchSize);

    for (auto i = 0; i < batch->childrenSize(); ++i) {
      auto hasher = table->hashers()[i].get();
      hasher->decode(*batch->childAt(i), rows);
      if (table->hashMode() != BaseHashTable::HashMode::kHash &&
          hasher->mayUseValueIds()) {
        hasher->computeValueIds(rows, dummy);
      }
    }
    rows.applyToSelected([&](auto rowIndex) {
      char* newRow = rowContainer->newRow();
      *reinterpret_cast<char**>(newRow + rowContainer->nextOffset()) = nullptr;
      for (auto i = 0; i < numKeys; ++i) {
        rowContainer->store(hashers[i]->decodedVector(), rowIndex, newRow, i);
      }
    });
  }

  // Create join table.
  void createTable() {
    std::vector<TypePtr> dependentTypes;
    std::vector<RowVectorPtr> batches;
    makeBuildBatches(batches);
    for (auto i = 0; i < params_.numWays; ++i) {
      std::vector<std::unique_ptr<VectorHasher>> keyHashers;
      for (int j = 0; j < params_.numFields; ++j) {
        keyHashers.emplace_back(
            std::make_unique<VectorHasher>(params_.buildType->childAt(j), j));
      }
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
        otherTables_.push_back(std::move(table));
      }
    }
  }

  std::default_random_engine randomEngine_;
  std::unique_ptr<HashTable<true>> topTable_;
  std::vector<std::unique_ptr<BaseHashTable>> otherTables_;
  HashTableBenchmarkParams params_;
};

void initArrayModeBenchmarkParams(
    std::vector<HashTableBenchmarkParams>& params) {
  TypePtr oneKeyType{ROW({"k1"}, {BIGINT()})};
  std::vector<int64_t> buildSizeVector = {100000, (2L << 20) - 3};
  std::vector<int64_t> numTablesVector = {1, 8};
  std::vector<int64_t> dupFactorVector = {1, 8};
  for (auto buildSize : buildSizeVector) {
    for (auto dupFactor : dupFactorVector) {
      for (auto numTables : numTablesVector) {
        params.push_back(HashTableBenchmarkParams(
            BaseHashTable::HashMode::kArray,
            oneKeyType,
            buildSize / dupFactor,
            buildSize,
            numTables));
      }
    }
  }
}

void initNormalizedKeyModeBenchmarkParams(
    std::vector<HashTableBenchmarkParams>& params) {
  TypePtr twoKeyType{ROW({"k1", "k2"}, {BIGINT(), BIGINT()})};
  std::vector<int64_t> buildSizeVector = {100000, (2L << 20) - 3, 2L << 23};
  std::vector<int64_t> numTablesVector = {1, 8};
  std::vector<int64_t> dupFactorVector = {1, 8};
  for (auto buildSize : buildSizeVector) {
    for (auto dupFactor : dupFactorVector) {
      for (auto numTables : numTablesVector) {
        params.push_back(HashTableBenchmarkParams(
            BaseHashTable::HashMode::kNormalizedKey,
            twoKeyType,
            buildSize / dupFactor,
            buildSize,
            numTables));
      }
    }
  }
}

void initHashModeBenchmarkParams(
    std::vector<HashTableBenchmarkParams>& params) {
  TypePtr threeKeyType{ROW({"k1", "k2", "k3"}, {BIGINT(), BIGINT(), BIGINT()})};
  std::vector<int64_t> buildSizeVector = {(2L << 20) - 3, 2L << 23};
  std::vector<int64_t> numTablesVector = {1, 8};
  std::vector<int64_t> dupFactorVector = {1, 8};
  for (auto buildSize : buildSizeVector) {
    for (auto dupFactor : dupFactorVector) {
      for (auto numTables : numTablesVector) {
        params.push_back(HashTableBenchmarkParams(
            BaseHashTable::HashMode::kHash,
            threeKeyType,
            buildSize / dupFactor,
            buildSize,
            numTables));
      }
    }
  }
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

  auto bm = std::make_unique<HashJoinPrepareJoinTableBenchmark>();
  std::vector<HashTableBenchmarkParams> params;
  // initArrayModeBenchmarkParams(params);
  initNormalizedKeyModeBenchmarkParams(params);
  initHashModeBenchmarkParams(params);

  for (auto& param : params) {
    folly::addBenchmark(__FILE__, param.title, [param, &bm]() {
      folly::BenchmarkSuspender suspender;
      bm->prepare(param);
      suspender.dismiss();
      bm->run();
      return 1;
    });
  }
  folly::runBenchmarks();
  return 0;
}
