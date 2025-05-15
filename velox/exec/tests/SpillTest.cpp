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

using namespace facebook;
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

struct TestParam {
  const common::CompressionKind compressionKind;
  const bool enablePrefixSort;

  TestParam(common::CompressionKind _compressionKind, bool _enablePrefixSort)
      : compressionKind(_compressionKind),
        enablePrefixSort(_enablePrefixSort) {}

  TestParam(uint32_t value)
      : compressionKind(static_cast<common::CompressionKind>(value >> 1)),
        enablePrefixSort(!!(value & 1)) {}

  uint32_t value() const {
    return static_cast<uint32_t>(compressionKind) << 1 | enablePrefixSort;
  }

  std::string toString() const {
    return fmt::format(
        "compressionKind: {}, enablePrefixSort: {}",
        compressionKind,
        enablePrefixSort);
  }
};

class SpillTest : public ::testing::TestWithParam<uint32_t>,
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

  static std::vector<uint32_t> getTestParams() {
    std::vector<uint32_t> testParams;
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_NONE, false}
            .value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_ZLIB, false}
            .value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_SNAPPY, false}
            .value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_ZSTD, false}
            .value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_LZ4, false}.value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_NONE, true}.value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_ZLIB, true}.value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_SNAPPY, true}
            .value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_ZSTD, true}.value());
    testParams.emplace_back(
        TestParam{common::CompressionKind::CompressionKind_LZ4, true}.value());
    return testParams;
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerNamedVectorSerde();
    }
  }

  void SetUp() override {
    allocator_ = memory::memoryManager()->allocator();
    tempDir_ = exec::test::TempDirectoryPath::create();
    filesystems::registerLocalFileSystem();
    rng_.seed(1);
    compressionKind_ = TestParam{GetParam()}.compressionKind;
    enablePrefixSort_ = TestParam{GetParam()}.enablePrefixSort;
  }

  uint8_t randSpillLevel() {
    return folly::Random::rand32(rng_) % (SpillPartitionId::kMaxSpillLevel + 1);
  }

  uint32_t randPartitionNum() {
    return folly::Random::rand32(rng_) %
        (1 << SpillPartitionId::kMaxPartitionBits);
  }

  SpillPartitionId genPartitionId(std::vector<uint32_t> partitionNums) {
    VELOX_CHECK(!partitionNums.empty());
    const auto spillLevel = partitionNums.size();
    SpillPartitionId id(partitionNums[0]);
    for (auto i = 1; i < spillLevel; ++i) {
      id = SpillPartitionId(id, partitionNums[i]);
    }
    return id;
  }

  SpillPartitionIdSet genPartitionIdSet(uint32_t maxPartitions) {
    SpillPartitionIdSet result;
    for (auto i = 0; i < maxPartitions; ++i) {
      result.emplace(SpillPartitionId(i));
    }
    return result;
  }

  // Randomly generate a partition set with common 'parent'. The set contains
  // 'numIds' of partition ids with randomly picked partition nums.
  SpillPartitionIdSet genRandomPartitionIdSet(
      std::optional<SpillPartitionId> parent,
      uint32_t numIds) {
    const auto maxPartitions = 1 << SpillPartitionId::kMaxPartitionBits;
    VELOX_CHECK_LE(numIds, maxPartitions);
    std::vector<uint32_t> partitionNums;
    partitionNums.resize(maxPartitions);
    std::iota(partitionNums.begin(), partitionNums.end(), 0);
    std::shuffle(partitionNums.begin(), partitionNums.end(), rng_);
    SpillPartitionIdSet result;
    for (auto i = 0; i < numIds; ++i) {
      auto id = parent.has_value()
          ? SpillPartitionId(parent.value(), partitionNums[i])
          : SpillPartitionId(partitionNums[i]);
      result.emplace(id);
    }
    return result;
  }

  SpillPartitionSet genSpillPartitionSet(
      std::optional<SpillPartitionId> parent,
      uint32_t numIds) {
    auto ids = genRandomPartitionIdSet(parent, numIds);
    SpillPartitionSet result;
    for (const auto& id : ids) {
      result.emplace(id, std::make_unique<SpillPartition>(id));
    }
    return result;
  }

  SpillPartitionId randPartitionId() {
    const auto spillLevel = randSpillLevel();
    SpillPartitionId partitionId(randPartitionNum());
    for (auto i = 0; i < spillLevel; ++i) {
      partitionId = SpillPartitionId(partitionId, randPartitionNum());
    }
    return partitionId;
  }

  SpillPartitionSet copySpillPartitionSet(
      const SpillPartitionSet& spillPartitionSet) {
    SpillPartitionSet result;
    for (const auto& [id, partition] : spillPartitionSet) {
      result.emplace(
          id, std::make_unique<SpillPartition>(id, partition->files()));
    }
    return result;
  }

  void setupSpillState(
      const SpillPartitionIdSet& partitionIds,
      int64_t targetFileSize,
      uint64_t writeBufferSize,
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
    spillStats_.wlock()->reset();

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

    // Setup state that has 'numPartitions' partitions, each with its own
    // file list. We write 'numBatches' sorted vectors in each partition. The
    // vectors have the ith element = i * 'numBatches' + batch, where batch is
    // the batch number of the vector in the partition. When read back, both
    // partitions produce an ascending sequence of integers without gaps.
    spillStats_.wlock()->reset();
    const std::optional<common::PrefixSortConfig> prefixSortConfig =
        enablePrefixSort_
        ? std::optional<common::PrefixSortConfig>(common::PrefixSortConfig())
        : std::nullopt;
    const int32_t numSortKeys = 1;
    const auto sortingKeys = SpillState::makeSortingKeys(
        compareFlags.empty() ? std::vector<CompareFlags>(1) : compareFlags);
    state_ = std::make_unique<SpillState>(
        [&]() -> const std::string& { return tempDir_->getPath(); },
        updateSpilledBytesCb_,
        fileNamePrefix_,
        sortingKeys,
        targetFileSize,
        writeBufferSize,
        compressionKind_,
        prefixSortConfig,
        pool(),
        &spillStats_);
    ASSERT_EQ(targetFileSize, state_->targetFileSize());
    ASSERT_EQ(spillStats_.rlock()->spilledPartitions, 0);
    ASSERT_EQ(spillStats_.rlock()->spilledPartitions, 0);
    ASSERT_TRUE(state_->spilledPartitionIdSet().empty());
    ASSERT_EQ(compressionKind_, state_->compressionKind());
    ASSERT_EQ(state_->sortingKeys().size(), numSortKeys);

    for (const auto& partitionId : partitionIds) {
      ASSERT_FALSE(state_->isPartitionSpilled(partitionId));
      ASSERT_EQ(state_->numFinishedFiles(partitionId), 0);
      // Expect an exception if partition is not set to spill.
      {
        RowVectorPtr dummyInput;
        VELOX_ASSERT_THROW(
            state_->appendToPartition(partitionId, dummyInput),
            fmt::format("Partition {} is not spilled", partitionId.toString()));
      }
      state_->setPartitionSpilled(partitionId);
      ASSERT_TRUE(state_->isPartitionSpilled(partitionId));
      ASSERT_FALSE(
          state_->testingNonEmptySpilledPartitionIdSet().contains(partitionId));
      for (auto iter = 0; iter < numBatches / 2; ++iter) {
        batchesByPartition_[partitionId].push_back(
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
            partitionId, batchesByPartition_[partitionId].back());
        ASSERT_TRUE(state_->testingNonEmptySpilledPartitionIdSet().contains(
            partitionId));

        batchesByPartition_[partitionId].push_back(
            makeRowVector({makeFlatVector<int64_t>(
                numRowsPerBatch,
                [&](auto row) {
                  return values_
                             [(numRowsPerBatch + row) * numBatches / 2 + iter]
                                 .has_value()
                      ? values_[(numRowsPerBatch + row) * numBatches / 2 + iter]
                            .value()
                      : 0;
                },
                [&](auto row) {
                  return !values_
                              [(numRowsPerBatch + row) * numBatches / 2 + iter]
                                  .has_value();
                })}));
        state_->appendToPartition(
            partitionId, batchesByPartition_[partitionId].back());
        ASSERT_TRUE(state_->testingNonEmptySpilledPartitionIdSet().contains(
            partitionId));

        ASSERT_GE(state_->numFinishedFiles(partitionId), 0);
        // Indicates that the next additions to 'partition' are not sorted
        // with respect to the values added so far.
        state_->finishFile(partitionId);
        ASSERT_GE(state_->numFinishedFiles(partitionId), 1);
        ASSERT_TRUE(state_->testingNonEmptySpilledPartitionIdSet().contains(
            partitionId));
      }
    }
    ASSERT_EQ(spillStats_.rlock()->spilledPartitions, partitionIds.size());
    for (const auto& partitionId : partitionIds) {
      ASSERT_TRUE(state_->spilledPartitionIdSet().contains(partitionId));
    }
    // NOTE: we write numBatches for each partition. If the target file size is
    // 1, we will end up with 'numPartitions * numBatches' spilled files as each
    // batch will generate one spill file. If not, the target file size is set
    // to vary large and only finishWrite() generates a new spill file which is
    // called every two batches.
    const auto numPartitions = partitionIds.size();
    auto expectedFiles = numPartitions * numBatches;
    if (targetFileSize > 1) {
      expectedFiles /= 2;
    }
    ASSERT_EQ(spillStats_.rlock()->spilledFiles, expectedFiles);
    ASSERT_GT(
        spillStats_.rlock()->spilledBytes,
        numPartitions * numBatches * sizeof(int64_t));
    int numFinishedFiles{0};
    for (const auto& partitionId : partitionIds) {
      numFinishedFiles += state_->numFinishedFiles(partitionId);
    }
    ASSERT_EQ(numFinishedFiles, expectedFiles);
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

    SpillPartitionIdSet partitionIds = genPartitionIdSet(numPartitions);

    setupSpillState(
        partitionIds,
        targetFileSize,
        0,
        numBatches,
        numRowsPerBatch,
        numDuplicates,
        compareFlags);
    const auto stats = spillStats_.copy();
    ASSERT_EQ(stats.spilledPartitions, numPartitions);
    ASSERT_EQ(stats.spilledFiles, expectedNumSpilledFiles);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spillWrites, 0);
    // NOTE: On fast machines we might have sub-microsecond in each write,
    // resulting in 0us total write time.
    ASSERT_GE(stats.spillWriteTimeNanos, 0);
    ASSERT_GE(stats.spillFlushTimeNanos, 0);
    ASSERT_GT(stats.spilledRows, 0);
    // NOTE: the following stats are not collected by spill state.
    ASSERT_EQ(stats.spillFillTimeNanos, 0);
    ASSERT_EQ(stats.spillSortTimeNanos, 0);
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
        prevGStats.spillWriteTimeNanos + stats.spillWriteTimeNanos,
        newGStats.spillWriteTimeNanos);
    ASSERT_EQ(
        prevGStats.spillFlushTimeNanos + stats.spillFlushTimeNanos,
        newGStats.spillFlushTimeNanos);
    ASSERT_EQ(
        prevGStats.spilledRows + stats.spilledRows, newGStats.spilledRows);
    ASSERT_EQ(
        prevGStats.spillFillTimeNanos + stats.spillFillTimeNanos,
        newGStats.spillFillTimeNanos);
    ASSERT_EQ(
        prevGStats.spillSortTimeNanos + stats.spillSortTimeNanos,
        newGStats.spillSortTimeNanos);

    // Verifies the spill file id
    for (auto& partitionId : state_->spilledPartitionIdSet()) {
      const auto spilledFileIds = state_->testingSpilledFileIds(partitionId);
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
        filesystems::getFileSystem(tempDir_->getPath(), nullptr);
    uint64_t totalFileBytes{0};
    for (const auto& spilledFile : spilledFileSet) {
      auto readFile = fs->openFileForRead(spilledFile);
      ASSERT_NE(readFile.get(), nullptr);
      totalFileBytes += readFile->size();
    }
    ASSERT_EQ(stats.spilledBytes, totalFileBytes);
    ASSERT_EQ(prevGStats.spilledBytes + totalFileBytes, newGStats.spilledBytes);

    for (const auto& partitionId : partitionIds) {
      auto spillFiles = state_->finish(partitionId);
      ASSERT_EQ(state_->numFinishedFiles(partitionId), 0);
      auto spillPartition =
          SpillPartition(SpillPartitionId(partitionId), std::move(spillFiles));
      auto merge =
          spillPartition.createOrderedReader(1 << 20, pool(), &spillStats_);
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

    const auto finalStats = spillStats_.copy();
    ASSERT_EQ(finalStats.spillReadBytes, finalStats.spilledBytes);
    ASSERT_GT(finalStats.spillReads, 0);
    ASSERT_GT(finalStats.spillReadTimeNanos, 0);
    ASSERT_GT(finalStats.spillDeserializationTimeNanos, 0);
    ASSERT_EQ(
        finalStats.toString(),
        fmt::format(
            "spillRuns[{}] spilledInputBytes[{}] spilledBytes[{}] spilledRows[{}] "
            "spilledPartitions[{}] spilledFiles[{}] spillFillTimeNanos[{}] "
            "spillSortTimeNanos[{}] spillExtractVectorTime[{}] spillSerializationTimeNanos[{}] spillWrites[{}] "
            "spillFlushTimeNanos[{}] spillWriteTimeNanos[{}] maxSpillExceededLimitCount[0] "
            "spillReadBytes[{}] spillReads[{}] spillReadTimeNanos[{}] "
            "spillReadDeserializationTimeNanos[{}]",
            finalStats.spillRuns,
            succinctBytes(finalStats.spilledInputBytes),
            succinctBytes(finalStats.spilledBytes),
            finalStats.spilledRows,
            finalStats.spilledPartitions,
            finalStats.spilledFiles,
            succinctNanos(finalStats.spillFillTimeNanos),
            succinctNanos(finalStats.spillSortTimeNanos),
            succinctNanos(finalStats.spillExtractVectorTimeNanos),
            succinctNanos(finalStats.spillSerializationTimeNanos),
            finalStats.spillWrites,
            succinctNanos(finalStats.spillFlushTimeNanos),
            succinctNanos(finalStats.spillWriteTimeNanos),
            succinctBytes(finalStats.spillReadBytes),
            finalStats.spillReads,
            succinctNanos(finalStats.spillReadTimeNanos),
            succinctNanos(finalStats.spillDeserializationTimeNanos)));
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
  bool enablePrefixSort_;
  std::vector<std::optional<int64_t>> values_;
  folly::F14FastMap<SpillPartitionId, std::vector<RowVectorPtr>>
      batchesByPartition_;
  std::string fileNamePrefix_;
  folly::Synchronized<common::SpillStats> spillStats_;
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
  const std::string spillPath = tempDirectory->getPath() + "/test";
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0},
      Timestamp{12, 0},
      Timestamp{0, 17'123'456},
      Timestamp{1, 17'123'456},
      Timestamp{-1, 17'123'456},
      Timestamp{Timestamp::kMaxSeconds, Timestamp::kMaxNanos},
      Timestamp{Timestamp::kMinSeconds, 0}};
  const std::optional<common::PrefixSortConfig> prefixSortConfig =
      enablePrefixSort_
      ? std::optional<common::PrefixSortConfig>(common::PrefixSortConfig())
      : std::nullopt;
  SpillState state(
      [&]() -> const std::string& { return tempDirectory->getPath(); },
      updateSpilledBytesCb_,
      "test",
      SpillState::makeSortingKeys(std::vector<CompareFlags>(1)),
      1024,
      0,
      compressionKind_,
      prefixSortConfig,
      pool(),
      &spillStats_);
  SpillPartitionId partitionId{0};
  state.setPartitionSpilled(partitionId);
  ASSERT_TRUE(state.isPartitionSpilled(partitionId));
  ASSERT_FALSE(
      state.testingNonEmptySpilledPartitionIdSet().contains(partitionId));
  state.appendToPartition(
      partitionId, makeRowVector({makeFlatVector<Timestamp>(timeValues)}));
  state.finishFile(partitionId);
  EXPECT_TRUE(
      state.testingNonEmptySpilledPartitionIdSet().contains(partitionId));

  SpillPartition spillPartition(SpillPartitionId{0}, state.finish(partitionId));
  auto merge =
      spillPartition.createOrderedReader(1 << 20, pool(), &spillStats_);
  ASSERT_TRUE(merge != nullptr);
  ASSERT_TRUE(
      spillPartition.createOrderedReader(1 << 20, pool(), &spillStats_) ==
      nullptr);
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
  SpillPartitionId partitionId1_2(2);
  ASSERT_EQ(partitionBitOffset(partitionId1_2, 0, 3), 0);
  ASSERT_EQ(partitionId1_2.partitionNumber(), 2);
  ASSERT_EQ(partitionId1_2.toString(), "[levels: 1, partitions: [2]]");

  SpillPartitionId partitionId1_2_dup(2);
  ASSERT_EQ(partitionId1_2, partitionId1_2_dup);

  SpillPartitionId partitionId1_3(3);
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
      const auto curLevel = spillPartitionIds[i].spillLevel();
      const auto nextLevel = spillPartitionIds[i + 1].spillLevel();
      int32_t commonAncestorLevel{-1};
      for (auto level = 0; level <= std::min(curLevel, nextLevel); ++level) {
        const auto curPartitionNum =
            spillPartitionIds[i].partitionNumber(level);
        const auto nextPartitionNum =
            spillPartitionIds[i + 1].partitionNumber(level);
        if (curPartitionNum != nextPartitionNum) {
          commonAncestorLevel = level - 1;
          break;
        }
      }

      if (commonAncestorLevel >= 0) {
        ASSERT_EQ(
            spillPartitionIds[i].partitionNumber(commonAncestorLevel),
            spillPartitionIds[i + 1].partitionNumber(commonAncestorLevel));
      }
      ASSERT_LE(
          spillPartitionIds[i].partitionNumber(commonAncestorLevel + 1),
          spillPartitionIds[i + 1].partitionNumber(commonAncestorLevel + 1));
      if (distinctSpillPartitionIds.back() != spillPartitionIds[i + 1]) {
        distinctSpillPartitionIds.push_back(spillPartitionIds[i + 1]);
      }
    }
    SpillPartitionIdSet partitionIdSet(
        spillPartitionIds.begin(), spillPartitionIds.end());
    ASSERT_EQ(partitionIdSet.size(), distinctSpillPartitionIds.size());
  }
}

TEST(SpillTest, spillPartitionIdInvalid) {
  // Exceeds max spill level
  SpillPartitionId id(0);
  for (auto i = 0; i < SpillPartitionId::kMaxSpillLevel; ++i) {
    id = SpillPartitionId(id, 0);
  }
  VELOX_ASSERT_THROW(SpillPartitionId(id, 0), "exceeds max spill level");

  // Exceeds max partition number
  ASSERT_NO_THROW(
      SpillPartitionId((1 << SpillPartitionId::kMaxPartitionBits) - 1));
  VELOX_ASSERT_THROW(
      SpillPartitionId(1 << SpillPartitionId::kMaxPartitionBits),
      "exceeds max partition number");
}

TEST_P(SpillTest, spillPartitionIdHierarchy) {
  std::vector<SpillPartitionId> ids{
      genPartitionId({6}),
      genPartitionId({5, 6}),
      genPartitionId({5, 5}),
      genPartitionId({5})};
  std::sort(ids.begin(), ids.end());
  std::vector<SpillPartitionId> expectedIds{
      genPartitionId({5}),
      genPartitionId({5, 5}),
      genPartitionId({5, 6}),
      genPartitionId({6})};
  ASSERT_TRUE(ids == expectedIds);

  const auto id1 = genPartitionId({3, 3, 3});
  const auto id2 = genPartitionId({3, 3, 2});
  const auto id3 = genPartitionId({3, 2, 3});
  ASSERT_NE(id1, id2);
  ASSERT_NE(id1, id3);

  const uint8_t kStartPartitionBitOffset = 0;
  const uint8_t kNumPartitionBits = 3;
  SpillPartitionId id = genPartitionId({3, 3});
  ASSERT_EQ(
      partitionBitOffset(id, kStartPartitionBitOffset, kNumPartitionBits), 3);
  ASSERT_EQ(id.partitionNumber(), 3);
  ASSERT_EQ(id.partitionNumber(0), 3);
  ASSERT_EQ(id.partitionNumber(1), 3);
  ASSERT_THROW(id.partitionNumber(2), VeloxException);
  ASSERT_EQ(id.toString(), "[levels: 2, partitions: [3,3]]");

  auto wholeId = genPartitionId({3, 3, 2});
  ASSERT_EQ(
      partitionBitOffset(wholeId, kStartPartitionBitOffset, kNumPartitionBits),
      6);
  ASSERT_EQ(wholeId.partitionNumber(), 2);
  ASSERT_EQ(wholeId.partitionNumber(0), 3);
  ASSERT_EQ(wholeId.partitionNumber(1), 3);
  ASSERT_EQ(wholeId.partitionNumber(2), 2);
  ASSERT_EQ(wholeId.toString(), "[levels: 3, partitions: [3,3,2]]");
}

TEST_P(SpillTest, spillPartitionIdLookupBasic) {
  {
    // Bit representation of leaf partition: 0
    const auto partitionId = genPartitionId({0});
    SpillPartitionIdLookup lookup(
        {partitionId}, /*startPartitionBit=*/0, /*numPartitionBits=*/1);
    VELOX_CHECK_EQ(lookup.partition(0), partitionId);
    VELOX_CHECK(!lookup.partition(1).valid());
    VELOX_CHECK_EQ(lookup.partition(2), partitionId);
    VELOX_CHECK(!lookup.partition(3).valid());
  }

  {
    // Bit representation of leaf partitions: 0100 (0x4), 0001 (0x1)
    const auto partitionId_0_1 = genPartitionId({0, 1});
    const auto partitionId_1_0 = genPartitionId({1, 0});
    SpillPartitionIdSet partitionIds{partitionId_0_1, partitionId_1_0};
    SpillPartitionIdLookup lookup(
        partitionIds, /*startPartitionBit=*/4, /*numPartitionBits=*/2);
    VELOX_CHECK_EQ(lookup.partition(0x00000040), partitionId_0_1);
    VELOX_CHECK_EQ(lookup.partition(0x77777747), partitionId_0_1);
    VELOX_CHECK_EQ(lookup.partition(0xB7B7B74B), partitionId_0_1);

    VELOX_CHECK_EQ(lookup.partition(0x00000010), partitionId_1_0);
    VELOX_CHECK_EQ(lookup.partition(0x77777717), partitionId_1_0);
    VELOX_CHECK_EQ(lookup.partition(0x7A7A7A17), partitionId_1_0);

    VELOX_CHECK(!lookup.partition(0x00000030).valid());
    VELOX_CHECK(!lookup.partition(0x77777737).valid());
    VELOX_CHECK(!lookup.partition(0x00000000).valid());
    VELOX_CHECK(!lookup.partition(0x00000020).valid());
    VELOX_CHECK(!lookup.partition(0x00000050).valid());
    VELOX_CHECK(!lookup.partition(0x00000060).valid());
    VELOX_CHECK(!lookup.partition(0x000000F0).valid());
  }
}

TEST_P(SpillTest, spillPartitionIdLookupInvalid) {
  // Partition number exceeds lookup ctor partition bits.
  VELOX_ASSERT_THROW(
      SpillPartitionIdLookup(
          {genPartitionId({0, 4, 6})},
          /*startPartitionBit=*/0,
          /*numPartitionBits=*/2),
      "exceeds max partition number");

  VELOX_ASSERT_THROW(
      SpillPartitionIdLookup(
          {genPartitionId({0, 1, 2})},
          /*startPartitionBit=*/58,
          /*numPartitionBits=*/3),
      "Insufficient lookup bits.");

  VELOX_ASSERT_THROW(
      SpillPartitionIdLookup(
          {genPartitionId({0, 1, 2})},
          /*startPartitionBit=*/62,
          /*numPartitionBits=*/3),
      "Insufficient lookup bits.");

  VELOX_ASSERT_THROW(
      SpillPartitionIdLookup(
          {genPartitionId({0, 1}), genPartitionId({0, 1, 2})},
          /*startPartitionBit=*/0,
          /*numPartitionBits=*/3),
      "Duplicated lookup key");

  VELOX_ASSERT_THROW(
      SpillPartitionIdLookup(
          {genPartitionId({0}), genPartitionId({0, 1, 2})},
          /*startPartitionBit=*/0,
          /*numPartitionBits=*/3),
      "Duplicated lookup key");
}

TEST_P(SpillTest, spillPartitionIdLookup) {
  const uint32_t startPartitionBitOffset = 19;
  const uint32_t numPartitionBits = 3;

  struct TestData {
    std::unordered_map<SpillPartitionId, uint64_t> idMatchCountMap;

    std::string debugString() const {
      std::stringstream ss;
      ss << "[";
      for (const auto& [id, count] : idMatchCountMap) {
        ss << " {" << id.toString() << " : " << count << "} \n";
      }
      ss << "]";
      return ss.str();
    }
  };

  const auto kInvalidId = SpillPartitionId();

  std::vector<TestData> testData{
      /* TestData */
      {/* idMatchCountMap */
       {{genPartitionId({0, 0}), 1 << 6},
        {genPartitionId({0, 1}), 1 << 6},
        {genPartitionId({1, 0}), 1 << 6},
        {genPartitionId({1, 1}), 1 << 6},
        {SpillPartitionId(), (1 << 12) - (4 << 6)}}},

      /* TestData */
      {/* idMatchCountMap */
       {{genPartitionId({0}), 1 << 9},
        {genPartitionId({1, 1}), 1 << 6},
        {genPartitionId({2, 1, 2}), 1 << 3},
        {genPartitionId({7, 6, 5, 4}), 1},
        {kInvalidId, (1 << 12) - (1 << 9) - (1 << 6) - (1 << 3) - 1}}},

      /* TestData */
      {/* idMatchCountMap */
       {{genPartitionId({0, 1, 0}), 1 << 3},
        {genPartitionId({0, 1, 1}), 1 << 3},
        {genPartitionId({0, 1, 2}), 1 << 3},
        {genPartitionId({0, 1, 3}), 1 << 3},
        {genPartitionId({1, 1, 0}), 1 << 3},
        {genPartitionId({1, 1, 1}), 1 << 3},
        {genPartitionId({1, 1, 2}), 1 << 3},
        {genPartitionId({1, 1, 3}), 1 << 3},
        {kInvalidId, (1 << 12) - (8 << 3)}}},
  };

  for (const auto& data : testData) {
    SCOPED_TRACE(data.debugString());
    SpillPartitionIdSet idSet;
    for (const auto& [id, count] : data.idMatchCountMap) {
      if (id.valid()) {
        idSet.emplace(id);
      }
    }
    SpillPartitionIdLookup lookup(
        idSet, startPartitionBitOffset, numPartitionBits);
    std::unordered_map<SpillPartitionId, uint64_t> actualIdMatchCountMap;
    for (uint64_t hashBase = 0; hashBase < (1 << 12); ++hashBase) {
      const auto id = lookup.partition(hashBase << startPartitionBitOffset);
      if (actualIdMatchCountMap.count(id) == 0) {
        actualIdMatchCountMap[id] = 0;
      }
      ++actualIdMatchCountMap[id];
    }
    ASSERT_EQ(data.idMatchCountMap, actualIdMatchCountMap);
  }
}

TEST_P(SpillTest, spillPartitionFunctionBasic) {
  std::vector<RowVectorPtr> inputVectors;
  const auto rowType =
      ROW({"key1", "key2", "key3", "value"},
          {BIGINT(), VARCHAR(), VARCHAR(), VARCHAR()});
  {
    // Simple input vector for basic testing.
    const uint64_t numRows = 100;
    std::vector<VectorPtr> columns;
    columns.push_back(
        makeFlatVector<int64_t>(numRows, [](auto row) { return row; }));
    columns.push_back(makeFlatVector<std::string>(
        numRows, [](auto row) { return fmt::format("key_{}", row); }));
    columns.push_back(makeFlatVector<std::string>(
        numRows, [](auto row) { return fmt::format("key_{}_{}", row, row); }));
    columns.push_back(makeFlatVector<std::string>(
        numRows, [](auto row) { return fmt::format("val_{}", row); }));
    inputVectors.push_back(makeRowVector(columns));
  }

  // Additional 2 fuzzed vector for wider range testing.
  {
    VectorFuzzer::Options options;
    options.vectorSize = 100;
    options.nullRatio = 0.3;
    options.allowDictionaryVector = true;
    inputVectors.push_back(VectorFuzzer(options, pool()).fuzzRow(rowType));
  }

  {
    VectorFuzzer::Options options;
    options.vectorSize = 1000;
    options.nullRatio = 0.2;
    options.stringVariableLength = true;
    options.allowDictionaryVector = true;
    inputVectors.push_back(VectorFuzzer(options, pool()).fuzzRow(rowType));
  }

  struct TestData {
    std::string name;
    SpillPartitionIdSet partitionIds;
    uint32_t startPartitionBit;
    uint32_t numPartitionBits;
    std::vector<column_index_t> keyChannels;
    size_t numRows;
  };

  std::vector<TestData> testCases = {
      // Basic test with multiple partitions
      {"basic",
       {genPartitionId({0}),
        genPartitionId({1}),
        genPartitionId({2, 1}),
        genPartitionId({3, 2})},
       0,
       2,
       {0, 1},
       100},

      // Test with empty key channels
      {"emptyKeys", {genPartitionId({0}), genPartitionId({1})}, 0, 2, {}, 50},

      // Test with nulls
      {"withNulls",
       {genPartitionId({0}), genPartitionId({1}), genPartitionId({2})},
       0,
       2,
       {0, 1},
       80},

      // Test with higher start partition bits
      {"highStartBits",
       {genPartitionId({0}),
        genPartitionId({1}),
        genPartitionId({2}),
        genPartitionId({3})},
       8,
       2,
       {0, 1, 2},
       120}};

  for (const auto& data : testCases) {
    for (auto i = 0; i < inputVectors.size(); ++i) {
      SCOPED_TRACE(fmt::format("Test case: {}, Input vector {}", data.name, i));

      SpillPartitionIdLookup lookup(
          data.partitionIds, data.startPartitionBit, data.numPartitionBits);
      if (data.keyChannels.empty()) {
        VELOX_ASSERT_THROW(
            SpillPartitionFunction(lookup, rowType, data.keyChannels),
            "Key channels must not be empty");
        continue;
      }
      SpillPartitionFunction partitionFunction(
          lookup, rowType, data.keyChannels);

      auto& input = inputVectors[i];
      std::vector<SpillPartitionId> resultPartitionIds;
      partitionFunction.partition(*input, resultPartitionIds);

      ASSERT_EQ(resultPartitionIds.size(), input->size());
      for (const auto& id : resultPartitionIds) {
        ASSERT_TRUE((!id.valid()) || data.partitionIds.contains(id));
      }

      std::unordered_set<SpillPartitionId> uniquePartitions(
          resultPartitionIds.begin(), resultPartitionIds.end());
      ASSERT_GT(uniquePartitions.size(), 1);
    }
  }
}

TEST_P(SpillTest, iterableSpillPartitionSetBasic) {
  // Create first level spill partitions
  SpillPartitionSet firstLevelPartitions;
  SpillPartitionId id0(0);
  SpillPartitionId id1(1);
  SpillPartitionId id2(2);
  firstLevelPartitions.emplace(id0, std::make_unique<SpillPartition>(id0));
  firstLevelPartitions.emplace(id1, std::make_unique<SpillPartition>(id1));
  firstLevelPartitions.emplace(id2, std::make_unique<SpillPartition>(id2));

  SpillPartitionSet secondLevelPartitions;
  SpillPartitionId id1_0(id1, 0);
  SpillPartitionId id1_1(id1, 1);
  SpillPartitionId id1_2(id1, 2);
  secondLevelPartitions.emplace(id1_0, std::make_unique<SpillPartition>(id1_0));
  secondLevelPartitions.emplace(id1_1, std::make_unique<SpillPartition>(id1_1));
  secondLevelPartitions.emplace(id1_2, std::make_unique<SpillPartition>(id1_2));

  IterableSpillPartitionSet iterableSet;
  VELOX_ASSERT_THROW(
      iterableSet.insert({}), "Inserted spill partition set must not be empty");
  ASSERT_FALSE(iterableSet.hasNext());
  VELOX_ASSERT_THROW(iterableSet.next(), "No more spill partitions to read");
  iterableSet.insert(std::move(firstLevelPartitions));
  ASSERT_TRUE(iterableSet.hasNext());

  ASSERT_EQ(iterableSet.next().id(), id0);
  SpillPartitionSet secondLevelPartitionsCopy =
      copySpillPartitionSet(secondLevelPartitions);

  VELOX_ASSERT_THROW(
      iterableSet.insert(std::move(secondLevelPartitionsCopy)),
      "Partition set does not have the same parent");
  ASSERT_EQ(iterableSet.next().id(), id1);

  iterableSet.insert(std::move(secondLevelPartitions));
  ASSERT_EQ(iterableSet.next().id(), id1_0);
  ASSERT_EQ(iterableSet.next().id(), id1_1);
  ASSERT_EQ(iterableSet.next().id(), id1_2);

  VELOX_ASSERT_THROW(
      iterableSet.spillPartitions(),
      "Spill partitions can only be extracted out after entire set is read");
  ASSERT_EQ(iterableSet.next().id(), id2);
  ASSERT_FALSE(iterableSet.hasNext());

  const auto& extractedPartitions = iterableSet.spillPartitions();
  ASSERT_EQ(extractedPartitions.size(), 5);
  ASSERT_TRUE(extractedPartitions.find(id0) != extractedPartitions.end());
  ASSERT_TRUE(extractedPartitions.find(id2) != extractedPartitions.end());
  ASSERT_TRUE(extractedPartitions.find(id1_0) != extractedPartitions.end());
  ASSERT_TRUE(extractedPartitions.find(id1_1) != extractedPartitions.end());
  ASSERT_TRUE(extractedPartitions.find(id1_2) != extractedPartitions.end());

  ASSERT_FALSE(iterableSet.hasNext());
  ASSERT_EQ(iterableSet.spillPartitions().size(), 5);
  iterableSet.reset();
  for (auto i = 0; i < 5; ++i) {
    ASSERT_TRUE(iterableSet.hasNext());
    iterableSet.next();
  }
  ASSERT_FALSE(iterableSet.hasNext());
}

TEST_P(SpillTest, iterableSpillPartitionSet) {
  struct TestData {
    uint32_t maxPartitions;
    uint32_t maxNumInsertions;

    std::string debugString() {
      return fmt::format(
          "maxPartitions: {}, maxNumInsertions: {}",
          maxPartitions,
          maxNumInsertions);
    }
  };

  std::vector<TestData> testData{{1, 1}, {4, 4}, {8, 2}, {8, 20}};
  for (const auto& data : testData) {
    SpillPartitionSet expectedPartitions;
    IterableSpillPartitionSet iterableSet;

    for (auto i = data.maxNumInsertions; i > 0; --i) {
      if (!iterableSet.hasNext()) {
        if (i != data.maxNumInsertions) {
          break;
        }
        iterableSet.insert(genSpillPartitionSet(
            std::nullopt,
            folly::Random::rand32(rng_) % data.maxPartitions + 1));
        continue;
      }
      ASSERT_TRUE(iterableSet.hasNext());
      auto partition = iterableSet.next();
      expectedPartitions.emplace(
          partition.id(),
          std::make_unique<SpillPartition>(partition.id(), partition.files()));
      if (fuzzer::coinToss(rng_, 1.0 / data.maxPartitions)) {
        iterableSet.insert(genSpillPartitionSet(
            partition.id(),
            folly::Random::rand32(rng_) % data.maxPartitions + 1));
        auto iter = expectedPartitions.find(partition.id());
        ASSERT_TRUE(iter != expectedPartitions.end());
        expectedPartitions.erase(iter);
      }
    }

    while (iterableSet.hasNext()) {
      const auto partition = iterableSet.next();
      expectedPartitions.emplace(
          partition.id(),
          std::make_unique<SpillPartition>(partition.id(), partition.files()));
    }

    const auto& actualPartitions = iterableSet.spillPartitions();

    auto iterExpected = expectedPartitions.begin();
    auto iterActual = actualPartitions.begin();
    while (iterExpected != expectedPartitions.end()) {
      ASSERT_EQ(iterActual->first, iterExpected->first);
      ASSERT_EQ(iterActual->second->id(), iterExpected->second->id());
      ASSERT_EQ(
          iterActual->second->numFiles(), iterExpected->second->numFiles());
      ++iterExpected;
      ++iterActual;
    }
    ASSERT_TRUE(iterActual == actualPartitions.end());
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
      auto reader = spillPartitions.back()->createUnorderedReader(
          1 << 20, pool(), &spillStats_);
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

    std::optional<SpillPartitionId> prevId;
    for (auto& partitionEntry : partitionSet) {
      if (prevId.has_value()) {
        ASSERT_LT(prevId.value(), partitionEntry.first);
      }
      prevId = partitionEntry.first;
    }
  }

  // Spill partition with files test.
  const int numSpillers = 10;
  const int numPartitions = 4;
  SpillPartitionIdSet partitionIds = genPartitionIdSet(numPartitions);

  folly::F14FastMap<SpillPartitionId, std::vector<RowVectorPtr>>
      batchesByPartition;
  folly::F14FastMap<SpillPartitionId, std::unique_ptr<SpillPartition>>
      spillPartitions;
  int numBatchesPerPartition = 0;
  const int numRowsPerBatch = 50;
  folly::F14FastMap<SpillPartitionId, uint64_t> expectedPartitionSizes;
  folly::F14FastMap<SpillPartitionId, uint64_t> expectedPartitionFiles;
  for (int iter = 0; iter < numSpillers; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    int numBatches = 2 * (1 + folly::Random::rand32(rng) % 16);

    setupSpillState(
        partitionIds, iter % 2 ? 1 : kGB, 0, numBatches, numRowsPerBatch);
    numBatchesPerPartition += numBatches;
    for (const auto& partitionId : partitionIds) {
      expectedPartitionSizes.try_emplace(partitionId, 0);
      expectedPartitionFiles.try_emplace(partitionId, 0);

      auto spillFiles = state_->finish(partitionId);
      for (const auto& fileInfo : spillFiles) {
        expectedPartitionSizes[partitionId] += fileInfo.size;
        ++expectedPartitionFiles[partitionId];
      }
      if (iter == 0) {
        spillPartitions.emplace(
            partitionId,
            std::make_unique<SpillPartition>(
                partitionId, std::move(spillFiles)));
      } else {
        spillPartitions[partitionId]->addFiles(std::move(spillFiles));
      }
      std::copy(
          batchesByPartition_[partitionId].begin(),
          batchesByPartition_[partitionId].end(),
          std::back_inserter(batchesByPartition[partitionId]));
    }
  }
  // Read verification.
  for (const auto& partitionId : partitionIds) {
    RowVectorPtr output;
    {
      ASSERT_EQ(
          spillPartitions[partitionId]->size(),
          expectedPartitionSizes[partitionId]);
      ASSERT_EQ(
          spillPartitions[partitionId]->toString(),
          fmt::format(
              "SPILLED PARTITION[ID:{} FILES:{} SIZE:{}]",
              partitionId.toString(),
              expectedPartitionFiles[partitionId],
              succinctBytes(expectedPartitionSizes[partitionId])));
      auto reader = spillPartitions[partitionId]->createUnorderedReader(
          1 << 20, pool(), &spillStats_);
      for (int j = 0; j < numBatchesPerPartition; ++j) {
        ASSERT_TRUE(reader->nextBatch(output));
        for (int row = 0; row < numRowsPerBatch; ++row) {
          ASSERT_EQ(
              0,
              output->compare(
                  batchesByPartition[partitionId][j].get(),
                  row,
                  row,
                  CompareFlags{}));
        }
      }
    }
    // Check spill partition state after creating the reader.
    ASSERT_EQ(0, spillPartitions[partitionId]->numFiles());
    {
      auto reader = spillPartitions[partitionId]->createUnorderedReader(
          1 << 20, pool(), &spillStats_);
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
    setupSpillState(
        genPartitionIdSet(1),
        seed % 2 ? 1 : kGB,
        0,
        numBatches,
        numRowsPerBatch);
    const SpillPartitionId partitionId(0);

    auto spillPartition = std::make_unique<SpillPartition>(
        partitionId, state_->finish(partitionId));
    std::copy(
        batchesByPartition_[partitionId].begin(),
        batchesByPartition_[partitionId].end(),
        std::back_inserter(batches));

    folly::Random::DefaultGenerator rng;
    rng.seed(seed);
    const auto totalNumFiles = spillPartition->numFiles();
    const int32_t numShards = 1 + folly::Random::rand32(totalNumFiles * 2 / 3);
    auto spillPartitionShards = spillPartition->split(numShards);
    for (const auto& partitionShard : spillPartitionShards) {
      ASSERT_EQ(partitionId, partitionShard->id());
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
      auto reader = spillPartitionShards[i]->createUnorderedReader(
          1 << 20, pool(), &spillStats_);
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
  setupSpillState(genPartitionIdSet(1), kGB, 0, 2, numRowsPerBatch);
  // Delete the tmp dir to verify the spill file deletion error won't fail the
  // test.
  tempDir_.reset();
  state_.reset();
}

TEST_P(SpillTest, validatePerSpillWriteSize) {
  struct TestRowVector : RowVector {
    explicit TestRowVector(std::shared_ptr<const Type> type)
        : RowVector(nullptr, type, nullptr, 0, {}) {}

    uint64_t estimateFlatSize() const override {
      // Return 10GB
      constexpr uint64_t tenGB = 10 * static_cast<uint64_t>(1024 * 1024 * 1024);
      return tenGB;
    }
  };

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  SpillState state(
      [&]() -> const std::string& { return tempDirectory->getPath(); },
      updateSpilledBytesCb_,
      "test",
      SpillState::makeSortingKeys(std::vector<CompareFlags>(1)),
      1024,
      0,
      compressionKind_,
      std::nullopt,
      pool(),
      &spillStats_,
      "");
  SpillPartitionId partitionId{0};
  state.setPartitionSpilled(partitionId);
  ASSERT_TRUE(state.isPartitionSpilled(partitionId));
  VELOX_ASSERT_THROW(
      state.appendToPartition(
          partitionId, std::make_shared<TestRowVector>(HUGEINT())),
      "Spill bytes will overflow");
}

namespace {
SpillFiles makeFakeSpillFiles(int32_t numFiles) {
  auto tempDir = exec::test::TempDirectoryPath::create();
  static uint32_t fakeFileId{0};
  SpillFiles files;
  files.reserve(numFiles);
  const std::string filePathPrefix = tempDir->getPath() + "/Spill";
  for (int32_t i = 0; i < numFiles; ++i) {
    const auto fileId = fakeFileId;
    files.push_back(
        {fileId,
         ROW({"k1", "k2"}, {BIGINT(), BIGINT()}),
         tempDir->getPath() + "/Spill_" + std::to_string(fileId),
         1024,
         SpillState::makeSortingKeys(std::vector<CompareFlags>(1)),
         common::CompressionKind_NONE});
  }
  return files;
}
} // namespace

TEST(SpillTest, removeEmptyPartitions) {
  SpillPartitionSet partitionSet;
  const uint32_t partitionOffset = 8;
  const uint32_t numPartitionBits = 3;
  const uint32_t numPartitions = 8;

  for (int32_t partition = 0; partition < numPartitions; ++partition) {
    const SpillPartitionId id(partition);
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
    const SpillPartitionId id(partitionNum);
    ASSERT_EQ(
        partitionBitOffset(
            partitionSet.at(id)->id(), partitionOffset, numPartitionBits),
        partitionOffset);
    ASSERT_EQ(partitionSet.at(id)->id().partitionNumber(), partitionNum);
    ASSERT_EQ(partitionSet.at(id)->numFiles(), 1 + partitionNum / 2);
  }
}

TEST(SpillTest, scopedSpillInjectionRegex) {
  {
    TestScopedSpillInjection scopedSpillInjection(100, ".*?(TableWrite).*");
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite.hive-xyz"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite-hive-xyz"));
    ASSERT_FALSE(testingTriggerSpill("op.1.0.0.RowNumber"));
    ASSERT_TRUE(testingTriggerSpill(""));
  }

  {
    TestScopedSpillInjection scopedSpillInjection(
        100, ".*?(RowNumber|TableWrite|HashBuild).*");
    ASSERT_TRUE(testingTriggerSpill("op.1.0.RowNumber"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite.hive-xyz"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite-hive-xyz"));
    ASSERT_TRUE(testingTriggerSpill("op.1..0.HashBuild"));
    ASSERT_FALSE(testingTriggerSpill("op.1.0.0.Aggregation"));
    ASSERT_TRUE(testingTriggerSpill(""));
  }

  {
    TestScopedSpillInjection scopedSpillInjection(
        100, R"(.*?(RowNumber|TableWrite|HashBuild)(?:\..*)?)");
    ASSERT_TRUE(testingTriggerSpill("op.1.RowNumber"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite.hive-xyz"));
    ASSERT_FALSE(testingTriggerSpill("op.1.0.0.TableWrite-hive-xyz"));
    ASSERT_FALSE(testingTriggerSpill("op.1.0.0.Aggregation"));
    ASSERT_TRUE(testingTriggerSpill(""));
  }

  {
    TestScopedSpillInjection scopedSpillInjection(100);
    ASSERT_TRUE(testingTriggerSpill("op.1.RowNumber"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite.hive-xyz"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.TableWrite-hive-xyz"));
    ASSERT_TRUE(testingTriggerSpill("op.1.0.0.Aggregation"));
    ASSERT_TRUE(testingTriggerSpill());
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillTestSuite,
    SpillTest,
    ::testing::ValuesIn(SpillTest::getTestParams()));
