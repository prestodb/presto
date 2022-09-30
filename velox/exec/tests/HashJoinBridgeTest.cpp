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
#include "velox/exec/HashJoinBridge.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using facebook::velox::exec::test::TempDirectoryPath;

struct TestParam {
  int32_t numProbers{1};
  int32_t numBuilders{1};
};

class HashJoinBridgeTest : public testing::Test,
                           public testing::WithParamInterface<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    return std::vector<TestParam>(
        {TestParam{1, 4}, TestParam{4, 1}, TestParam{4, 4}, TestParam{1, 1}});
  }

 protected:
  static void SetUpTestCase() {
    filesystems::registerLocalFileSystem();
  }

  HashJoinBridgeTest()
      : rowType_(ROW({"k1", "k2"}, {BIGINT(), BIGINT()})),
        numBuilders_(GetParam().numBuilders),
        numProbers_(GetParam().numProbers) {}

  void SetUp() override {
    rng_.seed(1245);
    tempDir_ = exec::test::TempDirectoryPath::create();
  }

  void TearDown() override {}

  uint32_t randInt(uint32_t n) {
    std::lock_guard<std::mutex> l(mutex_);
    return folly::Random().rand64(rng_) % (n + 1);
  }

  bool oneIn(uint32_t n) {
    std::lock_guard<std::mutex> l(mutex_);
    return folly::Random().oneIn(n, rng_);
  }

  std::shared_ptr<HashJoinBridge> createJoinBridge() const {
    return std::make_shared<HashJoinBridge>();
  }

  std::unique_ptr<BaseHashTable> createFakeHashTable() {
    std::vector<std::unique_ptr<VectorHasher>> keyHashers;
    for (auto channel = 0; channel < rowType_->size(); ++channel) {
      keyHashers.emplace_back(
          std::make_unique<VectorHasher>(rowType_->childAt(channel), channel));
    }
    return HashTable<true>::createForJoin(
        std::move(keyHashers), {}, true, false, mappedMemory_);
  }

  std::vector<ContinueFuture> createEmptyFutures(int32_t count) {
    std::vector<ContinueFuture> futures;
    futures.reserve(count);
    for (int32_t i = 0; i < count; ++i) {
      futures.push_back(ContinueFuture::makeEmpty());
    }
    return futures;
  }

  void createFile(const std::string& filePath) {
    auto fs = filesystems::getFileSystem(filePath, nullptr);
    // File object dtor will close the file.
    auto file = fs->openFileForWrite(filePath);
  }

  SpillFiles makeFakeSpillFiles(int32_t numFiles) {
    SpillFiles files;
    files.reserve(numFiles);
    for (int32_t i = 0; i < numFiles; ++i) {
      files.push_back(std::make_unique<SpillFile>(
          rowType_,
          1,
          std::vector<CompareFlags>({}),
          tempDir_->path + "/Spill",
          *pool_));
      // Create a fake file to avoid too many exception logs in test when spill
      // file deletion fails.
      createFile(files.back()->testingFilePath());
    }
    return files;
  }

  SpillPartitionSet makeFakeSpillPartitionSet(uint8_t partitionBitOffset) {
    SpillPartitionSet partitionSet;
    const int32_t numPartitions =
        std::max<int32_t>(1, randInt(maxNumPartitions_));
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
      const SpillPartitionId id(partitionBitOffset, partition);
      partitionSet.emplace(
          id,
          std::make_unique<SpillPartition>(
              id, makeFakeSpillFiles(numSpillFilesPerPartition_)));
    }
    return partitionSet;
  }

  int32_t getSpillLevel(uint8_t partitionBitOffset) {
    return (partitionBitOffset - startPartitionBitOffset_) / numPartitionBits_;
  }

  const RowTypePtr rowType_;
  const int32_t numBuilders_;
  const int32_t numProbers_;
  const uint8_t startPartitionBitOffset_{0};
  const uint32_t numPartitionBits_{2};
  const uint32_t maxNumPartitions_{8};
  const uint32_t numSpillFilesPerPartition_{20};

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  memory::MappedMemory* mappedMemory_{memory::MappedMemory::getInstance()};
  std::shared_ptr<TempDirectoryPath> tempDir_;

  std::mutex mutex_;
  folly::Random::DefaultGenerator rng_;
};

TEST_P(HashJoinBridgeTest, withoutSpill) {
  for (const bool hasNullKeys : {false, true}) {
    SCOPED_TRACE(fmt::format("hasNullKeys: {}", hasNullKeys));
    auto futures = createEmptyFutures(numProbers_);

    auto joinBridge = createJoinBridge();
    // Can't call any other APIs except addBuilder() before start a join bridge
    // first.
    ASSERT_ANY_THROW(joinBridge->setHashTable(createFakeHashTable(), {}));
    ASSERT_ANY_THROW(joinBridge->setAntiJoinHasNullKeys());
    ASSERT_ANY_THROW(joinBridge->probeFinished());
    ASSERT_ANY_THROW(joinBridge->tableOrFuture(&futures[0]));
    ASSERT_ANY_THROW(joinBridge->spillInputOrFuture(&futures[0]));

    // Can't start a bridge without any builders.
    ASSERT_ANY_THROW(joinBridge->start());

    joinBridge = createJoinBridge();

    for (int32_t i = 0; i < numBuilders_; ++i) {
      joinBridge->addBuilder();
    }
    joinBridge->start();

    for (int32_t i = 0; i < numProbers_; ++i) {
      auto tableOr = joinBridge->tableOrFuture(&futures[i]);
      ASSERT_FALSE(tableOr.has_value());
      ASSERT_TRUE(futures[i].valid());
    }

    BaseHashTable* rawTable = nullptr;
    if (hasNullKeys) {
      joinBridge->setAntiJoinHasNullKeys();
      ASSERT_ANY_THROW(joinBridge->setAntiJoinHasNullKeys());
    } else {
      auto table = createFakeHashTable();
      rawTable = table.get();
      joinBridge->setHashTable(std::move(table), {});
      ASSERT_ANY_THROW(joinBridge->setHashTable(createFakeHashTable(), {}));
    }

    for (int32_t i = 0; i < numProbers_; ++i) {
      futures[i].wait();
    }

    // Check build results.
    futures = createEmptyFutures(numProbers_ * 2);
    for (int32_t i = 0; i < numProbers_ * 2; ++i) {
      auto tableOr = joinBridge->tableOrFuture(&futures[i]);
      ASSERT_TRUE(tableOr.has_value());
      ASSERT_FALSE(futures[i].valid());
      if (hasNullKeys) {
        ASSERT_TRUE(tableOr.value().antiJoinHasNullKeys);
        ASSERT_TRUE(tableOr.value().table == nullptr);
        ASSERT_FALSE(tableOr.value().restoredPartitionId.has_value());
        ASSERT_TRUE(tableOr.value().spillPartitionIds.empty());
      } else {
        ASSERT_FALSE(tableOr.value().antiJoinHasNullKeys);
        ASSERT_FALSE(tableOr.value().table == nullptr);
        ASSERT_EQ(tableOr.value().table.get(), rawTable);
        ASSERT_FALSE(tableOr.value().restoredPartitionId.has_value());
        ASSERT_TRUE(tableOr.value().spillPartitionIds.empty());
      }
    }

    // Verify builder will see no spill input.
    auto inputOr = joinBridge->spillInputOrFuture(&futures[0]);
    ASSERT_TRUE(inputOr.has_value());
    ASSERT_FALSE(futures[0].valid());
    ASSERT_TRUE(inputOr.value().spillPartition == nullptr);

    // Probe side completion.
    ASSERT_FALSE(joinBridge->probeFinished());
    ASSERT_ANY_THROW(joinBridge->probeFinished());
  }
}

TEST_P(HashJoinBridgeTest, withSpill) {
  struct {
    int32_t spillLevel;
    bool endWithNull;

    std::string debugString() const {
      return fmt::format(
          "spillLevel:{}, endWithNull:{}", spillLevel, endWithNull);
    }
  } testSettings[] = {
      {0, true}, {0, false}, {1, true}, {1, false}, {3, true}, {3, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto buildFutures = createEmptyFutures(numBuilders_);
    auto probeFutures = createEmptyFutures(numProbers_);

    auto joinBridge = createJoinBridge();

    for (int32_t i = 0; i < numBuilders_; ++i) {
      joinBridge->addBuilder();
    }
    joinBridge->start();

    std::optional<SpillPartitionId> restoringPartitionId;
    int32_t numSpilledPartitions = 0;
    int32_t numRestoredPartitions = 0;
    while (true) {
      // Wait for build table from probe side.
      for (int32_t i = 0; i < numProbers_; ++i) {
        ASSERT_FALSE(joinBridge->tableOrFuture(&probeFutures[i]).has_value());
      }
      // Finish build table.
      SpillPartitionSet spillPartitionSet;
      SpillPartitionIdSet spillPartitionIdSet;
      int32_t spillLevel = -1;
      if (restoringPartitionId.has_value()) {
        ++numRestoredPartitions;
        spillLevel =
            getSpillLevel(restoringPartitionId.value().partitionBitOffset());
        if (spillLevel < testData.spillLevel) {
          spillPartitionSet = makeFakeSpillPartitionSet(
              restoringPartitionId.value().partitionBitOffset() +
              numPartitionBits_);
        }
      } else {
        spillPartitionSet = makeFakeSpillPartitionSet(startPartitionBitOffset_);
      }
      bool hasMoreSpill;
      if (spillLevel >= testData.spillLevel && testData.endWithNull) {
        joinBridge->setAntiJoinHasNullKeys();
        hasMoreSpill = false;
      } else {
        numSpilledPartitions += spillPartitionSet.size();
        spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);
        hasMoreSpill = joinBridge->setHashTable(
            createFakeHashTable(), std::move(spillPartitionSet));
      }

      // Get built table from probe side.
      for (int32_t i = 0; i < numProbers_; ++i) {
        probeFutures[i].wait();
        auto tableOr = joinBridge->tableOrFuture(&probeFutures[i]);
        ASSERT_TRUE(tableOr.has_value());
        if (!hasMoreSpill && testData.endWithNull) {
          ASSERT_TRUE(tableOr.value().antiJoinHasNullKeys);
          ASSERT_TRUE(tableOr.value().table == nullptr);
        } else {
          ASSERT_FALSE(tableOr.value().antiJoinHasNullKeys);
          ASSERT_TRUE(tableOr.value().table != nullptr);
          ASSERT_EQ(tableOr.value().spillPartitionIds, spillPartitionIdSet);
        }
      }

      // Wait for probe to complete from build side.
      if (!hasMoreSpill) {
        for (int32_t i = 0; i < numBuilders_; ++i) {
          auto inputOr = joinBridge->spillInputOrFuture(&buildFutures[i]);
          ASSERT_TRUE(inputOr.has_value());
          ASSERT_TRUE(inputOr.value().spillPartition == nullptr);
        }
      } else {
        for (int32_t i = 0; i < numBuilders_; ++i) {
          ASSERT_FALSE(
              joinBridge->spillInputOrFuture(&buildFutures[i]).has_value());
        }
      }

      // Probe table.
      ASSERT_EQ(hasMoreSpill, joinBridge->probeFinished());
      if (!hasMoreSpill) {
        break;
      }

      // Resume build side for the next round.
      int32_t numSpilledFiles = 0;
      for (int32_t i = 0; i < numBuilders_; ++i) {
        auto inputOr = joinBridge->spillInputOrFuture(&buildFutures[i]);
        ASSERT_TRUE(inputOr.has_value());
        ASSERT_TRUE(inputOr.value().spillPartition != nullptr);
        restoringPartitionId = inputOr.value().spillPartition->id();
        numSpilledFiles += inputOr.value().spillPartition->numFiles();
      }
      ASSERT_EQ(numSpilledFiles, numSpillFilesPerPartition_);
      ASSERT_ANY_THROW(joinBridge->spillInputOrFuture(&buildFutures[0]));
    }
    if (testData.endWithNull) {
      ASSERT_GE(numSpilledPartitions, numRestoredPartitions);
    } else {
      ASSERT_EQ(numSpilledPartitions, numRestoredPartitions);
    }
  }
}

TEST_P(HashJoinBridgeTest, multiThreading) {
  for (int32_t iter = 0; iter < 10; ++iter) {
    std::vector<std::thread> builderThreads;
    builderThreads.reserve(numBuilders_);

    std::vector<std::thread> proberThreads;
    proberThreads.reserve(numProbers_);

    struct BarrierState {
      int32_t numRequested{0};
      std::vector<ContinuePromise> promises;
    };
    std::mutex barrierLock;
    std::unique_ptr<BarrierState> proberBarrier(new BarrierState());
    std::unique_ptr<BarrierState> builderBarrier(new BarrierState());

    auto joinBridge = createJoinBridge();
    for (size_t i = 0; i < numBuilders_; ++i) {
      joinBridge->addBuilder();
    }
    joinBridge->start();

    // Start one thread on behalf of a build operator execution.
    for (size_t i = 0; i < numBuilders_; ++i) {
      builderThreads.emplace_back([&]() {
        std::optional<SpillPartitionId> restoringPartitionId;
        while (true) {
          // Wait for peers to reach hash table build barrier.
          std::vector<ContinuePromise> promises;
          ContinueFuture future(ContinueFuture::makeEmpty());
          {
            std::lock_guard<std::mutex> l(barrierLock);
            if (++builderBarrier->numRequested < numBuilders_) {
              builderBarrier->promises.emplace_back(
                  "HashJoinBridgeTest::multiThreading");
              future = builderBarrier->promises.back().getSemiFuture();
            } else {
              promises = std::move(builderBarrier->promises);
            }
          }
          if (future.valid()) {
            future.wait();
          } else {
            builderBarrier.reset(new BarrierState());
            if (oneIn(10)) {
              joinBridge->setAntiJoinHasNullKeys();
            } else {
              auto partitionBitOffset = restoringPartitionId.has_value()
                  ? restoringPartitionId->partitionBitOffset() +
                      numPartitionBits_
                  : startPartitionBitOffset_;
              if (partitionBitOffset < 64 && oneIn(2)) {
                auto spillPartitionSet =
                    makeFakeSpillPartitionSet(partitionBitOffset);
                joinBridge->setHashTable(
                    createFakeHashTable(), std::move(spillPartitionSet));
              } else {
                joinBridge->setHashTable(createFakeHashTable(), {});
              }
            }
            for (auto& promise : promises) {
              promise.setValue();
            }
          }

          auto inputOr = joinBridge->spillInputOrFuture(&future);
          if (!inputOr.has_value()) {
            future.wait();
            inputOr = joinBridge->spillInputOrFuture(&future);
            ASSERT_TRUE(inputOr.has_value());
          }
          if (inputOr->spillPartition == nullptr) {
            break;
          }
          restoringPartitionId = inputOr->spillPartition->id();
        }
      });
    }

    // Start one thread on behalf of a probe operator execution.
    for (size_t i = 0; i < numProbers_; ++i) {
      proberThreads.emplace_back([&]() {
        SpillPartitionIdSet spillPartitionIdSet;
        while (true) {
          // Wait for build tables.
          ContinueFuture tableFuture(ContinueFuture::makeEmpty());

          auto tableOr = joinBridge->tableOrFuture(&tableFuture);
          if (!tableOr.has_value()) {
            tableFuture.wait();
            tableOr = joinBridge->tableOrFuture(&tableFuture);
            ASSERT_TRUE(tableOr.has_value());
          }
          if (tableOr.value().antiJoinHasNullKeys) {
            break;
          }
          ASSERT_TRUE(tableOr.value().table != nullptr);
          for (const auto& id : tableOr.value().spillPartitionIds) {
            ASSERT_FALSE(spillPartitionIdSet.contains(id));
            spillPartitionIdSet.insert(id);
          }
          if (tableOr.value().restoredPartitionId.has_value()) {
            ASSERT_TRUE(spillPartitionIdSet.contains(
                tableOr.value().restoredPartitionId.value()));
            spillPartitionIdSet.erase(
                tableOr.value().restoredPartitionId.value());
          }

          if (spillPartitionIdSet.empty()) {
            break;
          }

          // Wait for probe to finish.
          ContinueFuture probeFuture(ContinueFuture::makeEmpty());
          ASSERT_FALSE(probeFuture.valid());
          std::vector<ContinuePromise> promises;
          {
            std::lock_guard<std::mutex> l(barrierLock);
            if (++proberBarrier->numRequested < numProbers_) {
              proberBarrier->promises.emplace_back(
                  "HashJoinBridgeTest::multiThreading");
              probeFuture = proberBarrier->promises.back().getSemiFuture();
            } else {
              promises = std::move(proberBarrier->promises);
            }
          }
          if (probeFuture.valid()) {
            probeFuture.wait();
          } else {
            proberBarrier.reset(new BarrierState());
            ASSERT_TRUE(joinBridge->probeFinished());
            for (auto& promise : promises) {
              promise.setValue();
            }
          }
        }
      });
    }

    for (auto& th : builderThreads) {
      th.join();
    }
    for (auto& th : proberThreads) {
      th.join();
    }
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashJoinBridgeTest,
    HashJoinBridgeTest,
    testing::ValuesIn(HashJoinBridgeTest::getTestParams()));
