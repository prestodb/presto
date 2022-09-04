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
#include "velox/exec/Spill.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::filesystems;
using facebook::velox::exec::test::TempDirectoryPath;

class SpillTest : public testing::Test,
                  public facebook::velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    mappedMemory_ = memory::MappedMemory::getInstance();
    tempDir_ = exec::test::TempDirectoryPath::create();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
    rng_.seed(1);
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

    spillPath_ = tempDir_->path + "/test";
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
        assert(!values_.back().has_value());
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
    state_ = std::make_unique<SpillState>(
        spillPath_,
        numPartitions,
        1,
        compareFlags,
        targetFileSize,
        *pool(),
        *mappedMemory_);
    EXPECT_EQ(targetFileSize, state_->targetFileSize());
    EXPECT_EQ(numPartitions, state_->maxPartitions());
    EXPECT_EQ(0, state_->spilledPartitions());
    EXPECT_TRUE(state_->spilledPartitionSet().empty());

    for (auto partition = 0; partition < state_->maxPartitions(); ++partition) {
      EXPECT_FALSE(state_->isPartitionSpilled(partition));
      // Expect an exception if partition is not set to spill.
      {
        RowVectorPtr dummyInput;
        EXPECT_ANY_THROW(state_->appendToPartition(partition, dummyInput));
      }
      state_->setPartitionSpilled(partition);
      EXPECT_TRUE(state_->isPartitionSpilled(partition));
      EXPECT_FALSE(state_->hasFiles(partition));
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
        EXPECT_TRUE(state_->hasFiles(partition));

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
        EXPECT_TRUE(state_->hasFiles(partition));

        // Indicates that the next additions to 'partition' are not sorted
        // with respect to the values added so far.
        state_->finishWrite(partition);
        EXPECT_TRUE(state_->hasFiles(partition));
      }
    }
    EXPECT_EQ(numPartitions, state_->spilledPartitions());
    for (int i = 0; i < numPartitions; ++i) {
      EXPECT_TRUE(state_->spilledPartitionSet().contains(i));
    }
    EXPECT_LT(
        numPartitions * numBatches * sizeof(int64_t), state_->spilledBytes());
  }

  // 'numDuplicates' specifies the number of duplicates generated for each
  // distinct sort key value in test.
  void spillStateTest(
      int64_t targetFileSize,
      int numPartitions,
      int numBatches,
      int numDuplicates,
      const std::vector<CompareFlags>& compareFlags,
      int64_t expectedNumSpilledFiles) {
    const int numRowsPerBatch = 20'000;
    SCOPED_TRACE(fmt::format(
        "targetFileSize: {}, numPartitions: {}, numBatches: {}, numDuplicates: {}, nullsFirst: {}, ascending: {}",
        targetFileSize,
        numPartitions,
        numBatches,
        numDuplicates,
        compareFlags.empty() ? true : compareFlags[0].nullsFirst,
        compareFlags.empty() ? true : compareFlags[0].ascending));
    setupSpillState(
        targetFileSize,
        numPartitions,
        numBatches,
        numRowsPerBatch,
        numDuplicates,
        compareFlags);

    ASSERT_EQ(expectedNumSpilledFiles, state_->spilledFiles());
    std::vector<std::string> spilledFiles = state_->testingSpilledFilePaths();
    std::unordered_set<std::string> spilledFileSet(
        spilledFiles.begin(), spilledFiles.end());
    EXPECT_EQ(spilledFileSet.size(), spilledFiles.size());
    EXPECT_EQ(expectedNumSpilledFiles, spilledFileSet.size());
    // Verify the spilled file exist on file system.
    std::shared_ptr<FileSystem> fs =
        filesystems::getFileSystem(tempDir_->path, nullptr);
    for (const auto& spilledFile : spilledFileSet) {
      auto readFile = fs->openFileForRead(spilledFile);
      EXPECT_NE(readFile.get(), nullptr);
    }

    for (auto partition = 0; partition < state_->maxPartitions(); ++partition) {
      int numReadBatches = 0;
      auto merge = state_->startMerge(partition, nullptr);
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
    // Both spilled bytes and files stats are cleared after merge read.
    EXPECT_EQ(0, state_->spilledBytes());
    EXPECT_EQ(0, state_->spilledFiles());
    // Verify the spilled file has been removed from file system after spill
    // state destruction.
    for (const auto& spilledFile : spilledFiles) {
      EXPECT_ANY_THROW(fs->openFileForRead(spilledFile));
    }
  }

  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<TempDirectoryPath> tempDir_;
  memory::MappedMemory* mappedMemory_;
  std::vector<std::optional<int64_t>> values_;
  std::vector<std::vector<RowVectorPtr>> batchesByPartition_;
  std::string spillPath_;
  std::unique_ptr<SpillState> state_;
};

TEST_F(SpillTest, spillState) {
  // Set the target file size to a large value to avoid new file creation
  // triggered by batch write.

  // Test with distinct sort keys.
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{true, true}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{true, false}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{false, true}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{false, false}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {}, 10);

  // Test with duplicate sort keys.
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{true, true}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{true, false}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{false, true}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{false, false}}, 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {}, 10);
}

TEST_F(SpillTest, spillTimestamp) {
  // Verify that timestamp type retains it nanosecond precision when spilled and
  // read back.
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  std::vector<CompareFlags> emptyCompareFlags;
  const std::string kSpillPath = tempDirectory->path + "/test";
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0},
      Timestamp{12, 0},
      Timestamp{0, 17'123'456},
      Timestamp{1, 17'123'456},
      Timestamp{-1, 17'123'456}};
  SpillState state(
      kSpillPath, 1, 1, emptyCompareFlags, 1024, *pool(), *mappedMemory_);
  int partitionIndex = 0;
  state.setPartitionSpilled(partitionIndex);
  EXPECT_TRUE(state.isPartitionSpilled(partitionIndex));
  EXPECT_FALSE(state.hasFiles(partitionIndex));
  state.appendToPartition(
      partitionIndex, makeRowVector({makeFlatVector<Timestamp>(timeValues)}));
  state.finishWrite(partitionIndex);
  EXPECT_TRUE(state.hasFiles(partitionIndex));

  auto merge = state.startMerge(partitionIndex, nullptr);
  for (auto i = 0; i < timeValues.size(); ++i) {
    auto stream = merge->next();
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(
        timeValues[i],
        stream->decoded(0).valueAt<Timestamp>(stream->currentIndex()));
    stream->pop();
  }
  ASSERT_EQ(nullptr, merge->next());
}

TEST_F(SpillTest, spillStateWithSmallTargetFileSize) {
  // Set the target file size to a small value to open a new file on each batch
  // write.

  // Test with distinct sort keys.
  spillStateTest(1, 2, 10, 1, {CompareFlags{true, true}}, 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{true, false}}, 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{false, true}}, 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{false, false}}, 10 * 2);
  spillStateTest(1, 2, 10, 1, {}, 10 * 2);

  // Test with duplicated sort keys.
  spillStateTest(1, 2, 10, 10, {CompareFlags{true, false}}, 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{true, true}}, 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{false, false}}, 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{false, true}}, 10 * 2);
  spillStateTest(1, 2, 10, 10, {}, 10 * 2);
}

TEST_F(SpillTest, spillPartitionId) {
  SpillPartitionId invalidPartitionId;
  ASSERT_FALSE(invalidPartitionId.valid());

  SpillPartitionId partitionId1_2(1, 2);
  ASSERT_TRUE(partitionId1_2.valid());
  ASSERT_EQ(partitionId1_2.partitionBitOffset, 1);
  ASSERT_EQ(partitionId1_2.partitionNumber, 2);
  ASSERT_EQ(partitionId1_2.toString(), "[1,2]");

  SpillPartitionId partitionId1_2_dup(1, 2);
  ASSERT_EQ(partitionId1_2, partitionId1_2_dup);

  SpillPartitionId partitionId1_3(1, 3);
  ASSERT_NE(partitionId1_2, partitionId1_3);
  ASSERT_LT(partitionId1_2, partitionId1_3);

  for (int iter = 0; iter < 5; ++iter) {
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
          spillPartitionIds[i].partitionBitOffset,
          spillPartitionIds[i + 1].partitionBitOffset);
      if (spillPartitionIds[i].partitionBitOffset ==
          spillPartitionIds[i + 1].partitionBitOffset) {
        ASSERT_LE(
            spillPartitionIds[i].partitionNumber,
            spillPartitionIds[i + 1].partitionNumber);
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

TEST_F(SpillTest, spillPartitionSet) {
  for (int iter = 0; iter < 5; ++iter) {
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
      spillPartitions.push_back(std::make_unique<SpillPartition>(id));
      ASSERT_EQ(id, spillPartitions.back()->id());
      // Expect an empty reader.
      auto reader = spillPartitions.back()->createReader();
      ASSERT_FALSE(reader->nextBatch(output));
    }

    // Check if partition set is sorted as expected.
    SpillPartitionSet partitionSet;
    for (auto& partition : spillPartitions) {
      const auto id = partition->id();
      partitionSet.emplace(id, std::move(partition));
    }
    SpillPartitionId prevId;
    for (auto& partitionEntry : partitionSet) {
      if (prevId.valid()) {
        ASSERT_LT(prevId, partitionEntry.first);
      }
      prevId = partitionEntry.first;
    }
  }

  // Spill partition with files test.
  const int numSpillers = 10;
  const int numPartitions = 4;
  std::vector<std::vector<RowVectorPtr>> batchesByPartition(numPartitions);
  std::vector<std::unique_ptr<SpillPartition>> spillPartitions;
  int numBatchesPerPartition = 0;
  const int numRowsPerPartition = 100;
  for (int iter = 0; iter < numSpillers; ++iter) {
    folly::Random::DefaultGenerator rng;
    rng.seed(iter);
    int numBatches = 2 * (1 + folly::Random::rand32(rng) % 16);
    setupSpillState(
        iter % 2 ? 1 : 1'000'000'000,
        numPartitions,
        numBatches,
        numRowsPerPartition);
    numBatchesPerPartition += numBatches;
    for (int i = 0; i < numPartitions; ++i) {
      const SpillPartitionId id(0, i);
      if (iter == 0) {
        spillPartitions.emplace_back(
            std::make_unique<SpillPartition>(id, state_->files(i)));
      } else {
        spillPartitions[i]->addFiles(state_->files(i));
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
      auto reader = spillPartitions[i]->createReader();
      for (int j = 0; j < numBatchesPerPartition; ++j) {
        ASSERT_TRUE(reader->nextBatch(output));
        for (int row = 0; row < numRowsPerPartition; ++row) {
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
      auto reader = spillPartitions[i]->createReader();
      ASSERT_FALSE(reader->nextBatch(output));
    }
  }
}
