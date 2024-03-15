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

#include <gtest/gtest.h>
#include <algorithm>
#include <memory>

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::filesystems;
using facebook::velox::exec::test::TempDirectoryPath;

namespace {
static const int64_t kGB = 1'000'000'000;

// Class to write runtime stats in the tests to the stats container.
class TestRuntimeStatWriter : public BaseRuntimeStatWriter {
 public:
  explicit TestRuntimeStatWriter(
      std::unordered_map<std::string, RuntimeMetric>& stats)
      : stats_{stats} {}

  void addRuntimeStat(const std::string& name, const RuntimeCounter& value)
      override {
    addOperatorRuntimeStats(name, value, stats_);
  }

 private:
  std::unordered_map<std::string, RuntimeMetric>& stats_;
};
} // namespace

class SpillTest : public ::testing::TestWithParam<common::CompressionKind>,
                  public facebook::velox::test::VectorTestBase {
 public:
  explicit SpillTest()
      : statWriter_(std::make_unique<TestRuntimeStatWriter>(runtimeStats_)) {
    setThreadLocalRunTimeStatWriter(statWriter_.get());
    updateSpilledBytesCb_ = [&](uint64_t) {};
  }

  ~SpillTest() {
    setThreadLocalRunTimeStatWriter(nullptr);
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    allocator_ = memory::memoryManager()->allocator();
    tempDir_ = exec::test::TempDirectoryPath::create();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
    rng_.seed(1);
    compressionKind_ = GetParam();
  }

  uint8_t randPartitionBitOffset() {
    return folly::Random::rand32(rng_) % std::numeric_limits<uint8_t>::max();
  }
  uint32_t randPartitionNum() {
    return folly::Random::rand32(rng_) % std::numeric_limits<uint32_t>::max();
  }
  SpillPartitionId randPartitionId() {
    return SpillPartitionId(randPartitionBitOffset(), randPartitionNum());
  }

  void setupSpillState(
      int64_t targetFileSize,
      uint64_t writeBufferSize,
      int numPartitions,
      int numBatches,
      int numRowsPerBatch = 1000,
      int numDuplicates = 1,
      const std::vector<CompareFlags>& compareFlags = {}) {
    ASSERT_TRUE(compareFlags.empty() || compareFlags.size() == 1);
    ASSERT_EQ(numBatches % 2, 0);

    state_.reset();
    batchesByPartition_.clear();
    values_.clear();
    runtimeStats_.clear();
    stats_.wlock()->reset();

    fileNamePrefix_ = "test";
    values_.resize(numBatches * numRowsPerBatch);
    // Create a sequence of sorted 'values' in ascending order starting at -10.
    // Each distinct value occurs 'numDuplicates' times. The sequence total has
    // numBatches * kNumRowsPerBatch item. Each batch created in the test below,
    // contains a subsequence with index mod being equal to its batch number.
    const int kNumNulls = numBatches;
    for (int i = 0, value = -10; i < numRowsPerBatch * numBatches;) {
      while (i < kNumNulls) {
        values_[i++] = std::nullopt;
      }
      for (int j = 0; j < numDuplicates; ++j) {
        values_[i++] = value++;
      }
    }
    const bool nullsFirst =
        compareFlags.empty() ? true : compareFlags[0].nullsFirst;
    const bool ascending =
        compareFlags.empty() ? true : compareFlags[0].ascending;

    if (!ascending) {
      if (nullsFirst) {
        std::reverse(values_.begin() + kNumNulls, values_.end());
      } else {
        std::reverse(values_.begin(), values_.end());
        ASSERT_FALSE(values_.back().has_value());
      }
    } else {
      if (!nullsFirst) {
        for (int i = 0; i < values_.size(); ++i) {
          if (i < values_.size() - kNumNulls) {
            values_[i] = values_[i + kNumNulls];
          } else {
            values_[i] = std::nullopt;
          }
        }
      }
    }
    batchesByPartition_.resize(numPartitions);

    // Setup state that has 'numPartitions' partitions, each with its own
    // file list. We write 'numBatches' sorted vectors in each partition. The
    // vectors have the ith element = i * 'numBatches' + batch, where batch is
    // the batch number of the vector in the partition. When read back, both
    // partitions produce an ascending sequence of integers without gaps.
    stats_.wlock()->reset();
    state_ = std::make_unique<SpillState>(
        [&]() -> const std::string& { return tempDir_->path; },
        updateSpilledBytesCb_,
        fileNamePrefix_,
        numPartitions,
        1,
        compareFlags,
        targetFileSize,
        writeBufferSize,
        compressionKind_,
        pool(),
        &stats_);
    ASSERT_EQ(targetFileSize, state_->targetFileSize());
    ASSERT_EQ(numPartitions, state_->maxPartitions());
    ASSERT_EQ(stats_.rlock()->spilledPartitions, 0);
    ASSERT_EQ(stats_.rlock()->spilledPartitions, 0);
    ASSERT_TRUE(state_->spilledPartitionSet().empty());
    ASSERT_EQ(compressionKind_, state_->compressionKind());

    for (auto partition = 0; partition < state_->maxPartitions(); ++partition) {
      ASSERT_FALSE(state_->isPartitionSpilled(partition));
      // Expect an exception if partition is not set to spill.
      {
        RowVectorPtr dummyInput;
        VELOX_ASSERT_THROW(
            state_->appendToPartition(partition, dummyInput),
            fmt::format("Partition {} is not spilled", partition));
      }
      state_->setPartitionSpilled(partition);
      ASSERT_TRUE(state_->isPartitionSpilled(partition));
      ASSERT_FALSE(
          state_->testingNonEmptySpilledPartitionSet().contains(partition));
      for (auto iter = 0; iter < numBatches / 2; ++iter) {
        batchesByPartition_[partition].push_back(
            makeRowVector({makeFlatVector<int64_t>(
                numRowsPerBatch,
                [&](auto row) {
                  return values_[row * numBatches / 2 + iter].has_value()
                      ? values_[row * numBatches / 2 + iter].value()
                      : 0;
                },
                [&](auto row) {
                  return !values_[row * numBatches / 2 + iter].has_value();
                })}));
        state_->appendToPartition(
            partition, batchesByPartition_[partition].back());
        ASSERT_TRUE(
            state_->testingNonEmptySpilledPartitionSet().contains(partition));

        batchesByPartition_[partition].push_back(makeRowVector({makeFlatVector<
            int64_t>(
            numRowsPerBatch,
            [&](auto row) {
              return values_[(numRowsPerBatch + row) * numBatches / 2 + iter]
                         .has_value()
                  ? values_[(numRowsPerBatch + row) * numBatches / 2 + iter]
                        .value()
                  : 0;
            },
            [&](auto row) {
              return !values_[(numRowsPerBatch + row) * numBatches / 2 + iter]
                          .has_value();
            })}));
        state_->appendToPartition(
            partition, batchesByPartition_[partition].back());
        ASSERT_TRUE(
            state_->testingNonEmptySpilledPartitionSet().contains(partition));

        // Indicates that the next additions to 'partition' are not sorted
        // with respect to the values added so far.
        state_->finishFile(partition);
        ASSERT_TRUE(
            state_->testingNonEmptySpilledPartitionSet().contains(partition));
      }
    }
    ASSERT_EQ(stats_.rlock()->spilledPartitions, numPartitions);
    for (int i = 0; i < numPartitions; ++i) {
      ASSERT_TRUE(state_->spilledPartitionSet().contains(i));
    }
    // NOTE: we write numBatches for each partition. If the target file size is
    // 1, we will end up with 'numPartitions * numBatches' spilled files as each
    // batch will generate one spill file. If not, the target file size is set
    // to vary large and only finishWrite() generates a new spill file which is
    // called every two batches.
    auto expectedFiles = numPartitions * numBatches;
    if (targetFileSize > 1) {
      expectedFiles /= 2;
    }
    ASSERT_EQ(stats_.rlock()->spilledFiles, expectedFiles);
    ASSERT_GT(
        stats_.rlock()->spilledBytes,
        numPartitions * numBatches * sizeof(int64_t));
  }

  // 'numDuplicates' specifies the number of duplicates generated for each
  // distinct sort key value in test.
  void spillStateTest(
      int64_t targetFileSize,
      int numPartitions,
      int numBatches,
      int numDuplicates,
      const std::vector<CompareFlags>& compareFlags,
      uint64_t expectedNumSpilledFiles) {
    const int numRowsPerBatch = 1'000;
    SCOPED_TRACE(fmt::format(
        "targetFileSize: {}, numPartitions: {}, numBatches: {}, numDuplicates: {}, nullsFirst: {}, ascending: {}",
        targetFileSize,
        numPartitions,
        numBatches,
        numDuplicates,
        compareFlags.empty() ? true : compareFlags[0].nullsFirst,
        compareFlags.empty() ? true : compareFlags[0].ascending));

    const auto prevGStats = common::globalSpillStats();
    setupSpillState(
        targetFileSize,
        0,
        numPartitions,
        numBatches,
        numRowsPerBatch,
        numDuplicates,
        compareFlags);
    const auto stats = stats_.copy();
    ASSERT_EQ(stats.spilledPartitions, numPartitions);
    ASSERT_EQ(stats.spilledFiles, expectedNumSpilledFiles);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spillWrites, 0);
    ASSERT_GT(stats.spillWriteTimeUs, 0);
    ASSERT_GE(stats.spillFlushTimeUs, 0);
    ASSERT_GT(stats.spilledRows, 0);
    // NOTE: the following stats are not collected by spill state.
    ASSERT_EQ(stats.spillFillTimeUs, 0);
    ASSERT_EQ(stats.spillSortTimeUs, 0);
    const auto newGStats = common::globalSpillStats();
    ASSERT_EQ(
        prevGStats.spilledPartitions + stats.spilledPartitions,
        newGStats.spilledPartitions);
    ASSERT_EQ(
        prevGStats.spilledFiles + stats.spilledFiles, newGStats.spilledFiles);
    ASSERT_EQ(
        prevGStats.spilledBytes + stats.spilledBytes, newGStats.spilledBytes);
    ASSERT_EQ(
        prevGStats.spillWrites + stats.spillWrites, newGStats.spillWrites);
    ASSERT_EQ(
        prevGStats.spillWriteTimeUs + stats.spillWriteTimeUs,
        newGStats.spillWriteTimeUs);
    ASSERT_EQ(
        prevGStats.spillFlushTimeUs + stats.spillFlushTimeUs,
        newGStats.spillFlushTimeUs);
    ASSERT_EQ(
        prevGStats.spilledRows + stats.spilledRows, newGStats.spilledRows);
    ASSERT_EQ(
        prevGStats.spillFillTimeUs + stats.spillFillTimeUs,
        newGStats.spillFillTimeUs);
    ASSERT_EQ(
        prevGStats.spillSortTimeUs + stats.spillSortTimeUs,
        newGStats.spillSortTimeUs);

    // Verifies the spill file id
    for (auto& partitionNum : state_->spilledPartitionSet()) {
      const auto spilledFileIds = state_->testingSpilledFileIds(partitionNum);
      uint32_t expectedFileId{0};
      for (auto spilledFileId : spilledFileIds) {
        ASSERT_EQ(spilledFileId, expectedFileId++);
      }
    }
    std::vector<std::string> spilledFiles = state_->testingSpilledFilePaths();
    std::unordered_set<std::string> spilledFileSet(
        spilledFiles.begin(), spilledFiles.end());
    ASSERT_EQ(spilledFileSet.size(), spilledFiles.size());
    ASSERT_EQ(expectedNumSpilledFiles, spilledFileSet.size());
    // Verify the spilled file exist on file system.
    std::shared_ptr<FileSystem> fs =
        filesystems::getFileSystem(tempDir_->path, nullptr);
    uint64_t totalFileBytes{0};
    for (const auto& spilledFile : spilledFileSet) {
      auto readFile = fs->openFileForRead(spilledFile);
      ASSERT_NE(readFile.get(), nullptr);
      totalFileBytes += readFile->size();
    }
    ASSERT_EQ(stats.spilledBytes, totalFileBytes);
    ASSERT_EQ(prevGStats.spilledBytes + totalFileBytes, newGStats.spilledBytes);

    for (auto partition = 0; partition < state_->maxPartitions(); ++partition) {
      auto spillFiles = state_->finish(partition);
      auto spillPartition =
          SpillPartition(SpillPartitionId{0, partition}, std::move(spillFiles));
      auto merge = spillPartition.createOrderedReader(pool());
      int numReadBatches = 0;
      // We expect all the rows in dense increasing order.
      for (auto i = 0; i < numBatches * numRowsPerBatch; ++i) {
        auto stream = merge->next();
        ASSERT_NE(nullptr, stream);
        bool isLastBatch = false;
        if (values_[i].has_value()) {
          ASSERT_EQ(
              values_[i].value(),
              stream->current()
                  .childAt(0)
                  ->asUnchecked<FlatVector<int64_t>>()
                  ->valueAt(stream->currentIndex(&isLastBatch)))
              << i;
          ASSERT_EQ(
              values_[i].value(),
              stream->decoded(0).valueAt<int64_t>(stream->currentIndex()))
              << i;
        } else {
          ASSERT_TRUE(stream->current()
                          .childAt(0)
                          ->asUnchecked<FlatVector<int64_t>>()
                          ->isNullAt(stream->currentIndex(&isLastBatch)))
              << i;
          ASSERT_TRUE(stream->decoded(0).isNullAt(stream->currentIndex())) << i;
        }
        stream->pop();
        if (isLastBatch) {
          ++numReadBatches;
        }
      }
      ASSERT_EQ(nullptr, merge->next());
      // We do two append writes per each input batch.
      ASSERT_EQ(numBatches, numReadBatches);
    }

    const auto finalStats = stats_.copy();
    ASSERT_EQ(
        finalStats.toString(),
        fmt::format(
            "spillRuns[{}] spilledInputBytes[{}] spilledBytes[{}] spilledRows[{}] spilledPartitions[{}] spilledFiles[{}] spillFillTimeUs[{}] spillSortTime[{}] spillSerializationTime[{}] spillWrites[{}] spillFlushTime[{}] spillWriteTime[{}] maxSpillExceededLimitCount[0]",
            finalStats.spillRuns,
            succinctBytes(finalStats.spilledInputBytes),
            succinctBytes(finalStats.spilledBytes),
            finalStats.spilledRows,
            finalStats.spilledPartitions,
            finalStats.spilledFiles,
            succinctMicros(finalStats.spillFillTimeUs),
            succinctMicros(finalStats.spillSortTimeUs),
            succinctMicros(finalStats.spillSerializationTimeUs),
            finalStats.spillWrites,
            succinctMicros(finalStats.spillFlushTimeUs),
            succinctMicros(finalStats.spillWriteTimeUs)));

    // Verify the spilled files are still there after spill state destruction.
    for (const auto& spilledFile : spilledFileSet) {
      ASSERT_TRUE(fs->exists(spilledFile));
    }
    // Verify stats.
    ASSERT_EQ(runtimeStats_["spillFileSize"].count, spilledFiles.size());
  }

  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<TempDirectoryPath> tempDir_;
  memory::MemoryAllocator* allocator_;
  common::CompressionKind compressionKind_;
  std::vector<std::optional<int64_t>> values_;
  std::vector<std::vector<RowVectorPtr>> batchesByPartition_;
  std::string fileNamePrefix_;
  folly::Synchronized<common::SpillStats> stats_;
  std::unique_ptr<SpillState> state_;
  std::unordered_map<std::string, RuntimeMetric> runtimeStats_;
  std::unique_ptr<TestRuntimeStatWriter> statWriter_;
  common::UpdateAndCheckSpillLimitCB updateSpilledBytesCb_;
};

TEST_P(SpillTest, spillState) {
  // Set the target file size to a large value to avoid new file creation
  // triggered by batch write.

  // Test with distinct sort keys.
  spillStateTest(kGB, 2, 8, 1, {CompareFlags{true, true}}, 8);
  spillStateTest(kGB, 2, 8, 1, {CompareFlags{true, false}}, 8);
  spillStateTest(kGB, 2, 8, 1, {CompareFlags{false, true}}, 8);
  spillStateTest(kGB, 2, 8, 1, {CompareFlags{false, false}}, 8);
  spillStateTest(kGB, 2, 8, 1, {}, 8);

  // Test with duplicate sort keys.
  spillStateTest(kGB, 2, 8, 8, {CompareFlags{true, true}}, 8);
  spillStateTest(kGB, 2, 8, 8, {CompareFlags{true, false}}, 8);
  spillStateTest(kGB, 2, 8, 8, {CompareFlags{false, true}}, 8);
  spillStateTest(kGB, 2, 8, 8, {CompareFlags{false, false}}, 8);
  spillStateTest(kGB, 2, 8, 8, {}, 8);
}

TEST_P(SpillTest, spillTimestamp) {
  // Verify that timestamp type retains it nanosecond precision when spilled and
  // read back.
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  std::vector<CompareFlags> emptyCompareFlags;
  const std::string spillPath = tempDirectory->path + "/test";
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0},
      Timestamp{12, 0},
      Timestamp{0, 17'123'456},
      Timestamp{1, 17'123'456},
      Timestamp{-1, 17'123'456},
      Timestamp{Timestamp::kMaxSeconds, Timestamp::kMaxNanos},
      Timestamp{Timestamp::kMinSeconds, 0}};

  SpillState state(
      [&]() -> const std::string& { return tempDirectory->path; },
      updateSpilledBytesCb_,
      "test",
      1,
      1,
      emptyCompareFlags,
      1024,
      0,
      compressionKind_,
      pool(),
      &stats_);
  int partitionIndex = 0;
  state.setPartitionSpilled(partitionIndex);
  ASSERT_TRUE(state.isPartitionSpilled(partitionIndex));
  ASSERT_FALSE(
      state.testingNonEmptySpilledPartitionSet().contains(partitionIndex));
  state.appendToPartition(
      partitionIndex, makeRowVector({makeFlatVector<Timestamp>(timeValues)}));
  state.finishFile(partitionIndex);
  EXPECT_TRUE(
      state.testingNonEmptySpilledPartitionSet().contains(partitionIndex));

  SpillPartition spillPartition(SpillPartitionId{0, 0}, state.finish(0));
  auto merge = spillPartition.createOrderedReader(pool());
  ASSERT_TRUE(merge != nullptr);
  ASSERT_TRUE(spillPartition.createOrderedReader(pool()) == nullptr);
  for (auto i = 0; i < timeValues.size(); ++i) {
    auto* stream = merge->next();
    ASSERT_NE(stream, nullptr);
    ASSERT_EQ(
        timeValues[i],
        stream->decoded(0).valueAt<Timestamp>(stream->currentIndex()));
    stream->pop();
  }
  ASSERT_EQ(nullptr, merge->next());
}

TEST_P(SpillTest, spillStateWithSmallTargetFileSize) {
  // Set the target file size to a small value to open a new file on each batch
  // write.

  // Test with distinct sort keys.
  spillStateTest(1, 2, 8, 1, {CompareFlags{true, true}}, 8 * 2);
  spillStateTest(1, 2, 8, 1, {CompareFlags{true, false}}, 8 * 2);
  spillStateTest(1, 2, 8, 1, {CompareFlags{false, true}}, 8 * 2);
  spillStateTest(1, 2, 8, 1, {CompareFlags{false, false}}, 8 * 2);
  spillStateTest(1, 2, 8, 1, {}, 8 * 2);

  // Test with duplicated sort keys.
  spillStateTest(1, 2, 8, 8, {CompareFlags{true, false}}, 8 * 2);
  spillStateTest(1, 2, 8, 8, {CompareFlags{true, true}}, 8 * 2);
  spillStateTest(1, 2, 8, 8, {CompareFlags{false, false}}, 8 * 2);
  spillStateTest(1, 2, 8, 8, {CompareFlags{false, true}}, 8 * 2);
  spillStateTest(1, 2, 8, 8, {}, 8 * 2);
}

TEST_P(SpillTest, spillPartitionId) {
  SpillPartitionId partitionId1_2(1, 2);
  ASSERT_EQ(partitionId1_2.partitionBitOffset(), 1);
  ASSERT_EQ(partitionId1_2.partitionNumber(), 2);
  ASSERT_EQ(partitionId1_2.toString(), "[1,2]");

  SpillPartitionId partitionId1_2_dup(1, 2);
  ASSERT_EQ(partitionId1_2, partitionId1_2_dup);

  SpillPartitionId partitionId1_3(1, 3);
  ASSERT_NE(partitionId1_2, partitionId1_3);
  ASSERT_LT(partitionId1_2, partitionId1_3);

  for (int iter = 0; iter < 3; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    const int numIds = std::max<int>(1024, folly::Random::rand64(rng) % 4096);
    std::vector<SpillPartitionId> spillPartitionIds;
    spillPartitionIds.reserve(numIds);
    for (int i = 0; i < numIds; ++i) {
      spillPartitionIds.push_back(randPartitionId());
    }

    std::sort(spillPartitionIds.begin(), spillPartitionIds.end());
    std::vector<SpillPartitionId> distinctSpillPartitionIds;
    distinctSpillPartitionIds.reserve(numIds);
    distinctSpillPartitionIds.push_back(spillPartitionIds[0]);
    for (int i = 0; i < numIds - 1; ++i) {
      ASSERT_GE(
          spillPartitionIds[i].partitionBitOffset(),
          spillPartitionIds[i + 1].partitionBitOffset());
      if (spillPartitionIds[i].partitionBitOffset() ==
          spillPartitionIds[i + 1].partitionBitOffset()) {
        ASSERT_LE(
            spillPartitionIds[i].partitionNumber(),
            spillPartitionIds[i + 1].partitionNumber());
      }
      if (distinctSpillPartitionIds.back() != spillPartitionIds[i + 1]) {
        distinctSpillPartitionIds.push_back(spillPartitionIds[i + 1]);
      }
    }
    SpillPartitionIdSet partitionIdSet(
        spillPartitionIds.begin(), spillPartitionIds.end());
    ASSERT_EQ(partitionIdSet.size(), distinctSpillPartitionIds.size());
  }
}

TEST_P(SpillTest, spillPartitionSet) {
  for (int iter = 0; iter < 3; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    const int numPartitions =
        std::max<int>(128, folly::Random::rand64(rng) % 256);
    std::vector<std::unique_ptr<SpillPartition>> spillPartitions;
    spillPartitions.reserve(numPartitions);
    SpillPartitionIdSet partitionIdSet;
    partitionIdSet.reserve(numPartitions);
    RowVectorPtr output;
    while (spillPartitions.size() < numPartitions) {
      const SpillPartitionId id = randPartitionId();
      if (partitionIdSet.contains(id)) {
        continue;
      }
      partitionIdSet.insert(id);
      spillPartitions.push_back(std::make_unique<SpillPartition>(id));
      ASSERT_EQ(id, spillPartitions.back()->id());
      ASSERT_EQ(
          spillPartitions.back()->toString(),
          fmt::format(
              "SPILLED PARTITION[ID:{} FILES:0 SIZE:0B]", id.toString()));
      // Expect an empty reader.
      auto reader = spillPartitions.back()->createUnorderedReader(pool());
      ASSERT_FALSE(reader->nextBatch(output));
    }

    // Check if partition set is sorted as expected.
    SpillPartitionSet partitionSet;
    for (auto& partition : spillPartitions) {
      const auto id = partition->id();
      partitionSet.emplace(id, std::move(partition));
    }
    const SpillPartitionIdSet generatedPartitionIdSet =
        toSpillPartitionIdSet(partitionSet);
    ASSERT_EQ(partitionIdSet, generatedPartitionIdSet);

    std::unique_ptr<SpillPartitionId> prevId;
    for (auto& partitionEntry : partitionSet) {
      if (prevId != nullptr) {
        ASSERT_LT(*prevId, partitionEntry.first);
      }
      prevId = std::make_unique<SpillPartitionId>(
          partitionEntry.first.partitionBitOffset(),
          partitionEntry.first.partitionNumber());
    }
  }

  // Spill partition with files test.
  const int numSpillers = 10;
  const int numPartitions = 4;
  std::vector<std::vector<RowVectorPtr>> batchesByPartition(numPartitions);
  std::vector<std::unique_ptr<SpillPartition>> spillPartitions;
  int numBatchesPerPartition = 0;
  const int numRowsPerBatch = 50;
  std::vector<uint64_t> expectedPartitionSizes(numPartitions, 0);
  std::vector<uint64_t> expectedPartitionFiles(numPartitions, 0);
  for (int iter = 0; iter < numSpillers; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    int numBatches = 2 * (1 + folly::Random::rand32(rng) % 16);
    setupSpillState(
        iter % 2 ? 1 : kGB, 0, numPartitions, numBatches, numRowsPerBatch);
    numBatchesPerPartition += numBatches;
    for (int i = 0; i < numPartitions; ++i) {
      const SpillPartitionId id(0, i);
      auto spillFiles = state_->finish(i);
      for (const auto& fileInfo : spillFiles) {
        expectedPartitionSizes[i] += fileInfo.size;
        ++expectedPartitionFiles[i];
      }
      if (iter == 0) {
        spillPartitions.emplace_back(
            std::make_unique<SpillPartition>(id, std::move(spillFiles)));
      } else {
        spillPartitions[i]->addFiles(std::move(spillFiles));
      }
      std::copy(
          batchesByPartition_[i].begin(),
          batchesByPartition_[i].end(),
          std::back_inserter(batchesByPartition[i]));
    }
  }
  // Read verification.
  for (int i = 0; i < numPartitions; ++i) {
    RowVectorPtr output;
    {
      ASSERT_EQ(spillPartitions[i]->size(), expectedPartitionSizes[i]);
      ASSERT_EQ(
          spillPartitions[i]->toString(),
          fmt::format(
              "SPILLED PARTITION[ID:[0,{}] FILES:{} SIZE:{}]",
              i,
              expectedPartitionFiles[i],
              succinctBytes(expectedPartitionSizes[i])));
      auto reader = spillPartitions[i]->createUnorderedReader(pool());
      for (int j = 0; j < numBatchesPerPartition; ++j) {
        ASSERT_TRUE(reader->nextBatch(output));
        for (int row = 0; row < numRowsPerBatch; ++row) {
          ASSERT_EQ(
              0,
              output->compare(
                  batchesByPartition[i][j].get(), row, row, CompareFlags{}));
        }
      }
    }
    // Check spill partition state after creating the reader.
    ASSERT_EQ(0, spillPartitions[i]->numFiles());
    {
      auto reader = spillPartitions[i]->createUnorderedReader(pool());
      ASSERT_FALSE(reader->nextBatch(output));
    }
  }
}

TEST_P(SpillTest, spillPartitionSpilt) {
  for (int seed = 0; seed < 5; ++seed) {
    SCOPED_TRACE(fmt::format("seed: {}", seed));
    int numBatches = 50;
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);

    const int numRowsPerBatch = 50;
    setupSpillState(seed % 2 ? 1 : kGB, 0, 1, numBatches, numRowsPerBatch);
    const SpillPartitionId id(0, 0);

    auto spillPartition =
        std::make_unique<SpillPartition>(id, state_->finish(0));
    std::copy(
        batchesByPartition_[0].begin(),
        batchesByPartition_[0].end(),
        std::back_inserter(batches));

    folly::Random::DefaultGenerator rng;
    rng.seed(seed);
    const auto totalNumFiles = spillPartition->numFiles();
    const int32_t numShards = 1 + folly::Random::rand32(totalNumFiles * 2 / 3);
    auto spillPartitionShards = spillPartition->split(numShards);
    for (const auto& partitionShard : spillPartitionShards) {
      ASSERT_EQ(id, partitionShard->id());
    }

    // Even split distribution verification.
    int minNumFiles = std::numeric_limits<int>::max();
    int maxNumFiles = std::numeric_limits<int>::min();
    for (uint32_t i = 0; i < numShards; i++) {
      auto numFiles = spillPartitionShards[i]->numFiles();
      minNumFiles = std::min(minNumFiles, numFiles);
      maxNumFiles = std::max(maxNumFiles, numFiles);
    }
    ASSERT_LE(maxNumFiles - minNumFiles, 1);

    // Read verification.
    int batchIdx = 0;
    for (int32_t i = 0; i < numShards; ++i) {
      auto reader = spillPartitionShards[i]->createUnorderedReader(pool());
      RowVectorPtr output;
      while (reader->nextBatch(output)) {
        for (int row = 0; row < numRowsPerBatch; ++row) {
          ASSERT_EQ(
              0,
              output->compare(
                  batches[batchIdx].get(), row, row, CompareFlags{}));
        }
        ++batchIdx;
      }
    }
    ASSERT_EQ(batchIdx, numBatches);
  }
}

TEST_P(SpillTest, nonExistSpillFileOnDeletion) {
  const int32_t numRowsPerBatch = 50;
  std::vector<RowVectorPtr> batches;
  setupSpillState(kGB, 0, 1, 2, numRowsPerBatch);
  // Delete the tmp dir to verify the spill file deletion error won't fail the
  // test.
  tempDir_.reset();
  state_.reset();
}

namespace {
SpillFiles makeFakeSpillFiles(int32_t numFiles) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  static uint32_t fakeFileId{0};
  SpillFiles files;
  files.reserve(numFiles);
  const std::string filePathPrefix = tempDir->path + "/Spill";
  for (int32_t i = 0; i < numFiles; ++i) {
    const auto fileId = fakeFileId;
    files.push_back(
        {fileId,
         ROW({"k1", "k2"}, {BIGINT(), BIGINT()}),
         tempDir->path + "/Spill_" + std::to_string(fileId),
         1024,
         1,
         std::vector<CompareFlags>({}),
         common::CompressionKind_NONE});
  }
  return files;
}
} // namespace

TEST(SpillTest, removeEmptyPartitions) {
  SpillPartitionSet partitionSet;
  const int32_t partitionOffset = 8;
  const int32_t numPartitions = 8;

  for (int32_t partition = 0; partition < numPartitions; ++partition) {
    const SpillPartitionId id(partitionOffset, partition);
    if (partition & 0x01) {
      partitionSet.emplace(
          id,
          std::make_unique<SpillPartition>(
              id, makeFakeSpillFiles(1 + partition / 2)));
    } else {
      partitionSet.emplace(id, std::make_unique<SpillPartition>(id));
    }
  }
  ASSERT_EQ(partitionSet.size(), numPartitions);

  removeEmptyPartitions(partitionSet);
  ASSERT_EQ(partitionSet.size(), numPartitions / 2);

  for (int32_t partition = 0; partition < numPartitions / 2; ++partition) {
    const int32_t partitionNum = partition * 2 + 1;
    const SpillPartitionId id(partitionOffset, partitionNum);
    ASSERT_EQ(partitionSet.at(id)->id().partitionBitOffset(), partitionOffset);
    ASSERT_EQ(partitionSet.at(id)->id().partitionNumber(), partitionNum);
    ASSERT_EQ(partitionSet.at(id)->numFiles(), 1 + partitionNum / 2);
  }
}

INSTANTIATE_TEST_SUITE_P(
    SpillTestSuite,
    SpillTest,
    ::testing::Values(
        common::CompressionKind::CompressionKind_NONE,
        common::CompressionKind::CompressionKind_ZLIB,
        common::CompressionKind::CompressionKind_SNAPPY,
        common::CompressionKind::CompressionKind_ZSTD,
        common::CompressionKind::CompressionKind_LZ4,
        common::CompressionKind::CompressionKind_GZIP));
