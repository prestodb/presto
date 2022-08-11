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

class SpillTest : public testing::Test,
                  public facebook::velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    mappedMemory_ = memory::MappedMemory::getInstance();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
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
    SCOPED_TRACE(fmt::format(
        "targetFileSize: {}, numPartitions: {}, numBatches: {}, numDuplicates: {}, nullsFirst: {}, ascending: {}",
        targetFileSize,
        numPartitions,
        numBatches,
        numDuplicates,
        compareFlags.empty() ? true : compareFlags[0].nullsFirst,
        compareFlags.empty() ? true : compareFlags[0].ascending));
    ASSERT_TRUE(compareFlags.empty() || compareFlags.size() == 1);
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    const std::string kSpillPath = tempDirectory->path + "/test";
    std::shared_ptr<FileSystem> fs;
    std::vector<std::string> spilledFiles;
    const int kNumRowsPerBatch = 200'000;
    ASSERT_EQ(kNumRowsPerBatch % 2, 0);
    std::vector<std::optional<int64_t>> values(numBatches * kNumRowsPerBatch);
    // Create a sequence of sorted 'values' in ascending order starting at -10.
    // Each distinct value occurs 'numDuplicates' times. The sequence total has
    // numBatches * kNumRowsPerBatch item. Each batch created in the test below,
    // contains a subsequence with index mod being equal to its batch number.
    const int kNumNulls = numBatches;
    for (int i = 0, value = -10; i < kNumRowsPerBatch * numBatches; ++value) {
      while (i < kNumNulls) {
        values[i++] = std::nullopt;
      }
      for (int j = 0; j < numDuplicates; ++j, ++i) {
        values[i] = value;
      }
    }
    const bool nullsFirst =
        compareFlags.empty() ? true : compareFlags[0].nullsFirst;
    const bool ascending =
        compareFlags.empty() ? true : compareFlags[0].ascending;

    if (!ascending) {
      if (nullsFirst) {
        std::reverse(values.begin() + kNumNulls, values.end());
      } else {
        std::reverse(values.begin(), values.end());
        assert(!values.back().has_value());
      }
    } else {
      if (!nullsFirst) {
        for (int i = 0; i < values.size(); ++i) {
          if (i < values.size() - kNumNulls) {
            values[i] = values[i + kNumNulls];
          } else {
            values[i] = std::nullopt;
          }
        }
      }
    }
    {
      // We make a state that has 'numPartitions' partitions, each with its own
      // file list. We write 'numBatches' sorted vectors in each partition. The
      // vectors have the ith element = i * 'numBatches' + batch, where batch is
      // the batch number of the vector in the partition. When read back, both
      // partitions produce an ascending sequence of integers without gaps.
      SpillState state(
          kSpillPath,
          numPartitions,
          1,
          compareFlags,
          targetFileSize,
          *pool(),
          *mappedMemory_);
      EXPECT_EQ(targetFileSize, state.targetFileSize());
      EXPECT_EQ(numPartitions, state.maxPartitions());
      EXPECT_EQ(0, state.spilledPartitions());

      for (auto partition = 0; partition < state.maxPartitions(); ++partition) {
        EXPECT_FALSE(state.isPartitionSpilled(partition));
        // Expect an exception if partition is not set to spill.
        {
          RowVectorPtr dummyInput;
          EXPECT_ANY_THROW(state.appendToPartition(partition, dummyInput));
        }
        state.setPartitionSpilled(partition);
        EXPECT_TRUE(state.isPartitionSpilled(partition));
        EXPECT_FALSE(state.hasFiles(partition));
        for (auto batch = 0; batch < numBatches; ++batch) {
          state.appendToPartition(
              partition,
              makeRowVector({makeFlatVector<int64_t>(
                  kNumRowsPerBatch / 2,
                  [&](auto row) {
                    return values[row * numBatches + batch].has_value()
                        ? values[row * numBatches + batch].value()
                        : 0;
                  },
                  [&](auto row) {
                    return !values[row * numBatches + batch].has_value();
                  })}));
          EXPECT_TRUE(state.hasFiles(partition));

          state.appendToPartition(
              partition,
              makeRowVector({makeFlatVector<int64_t>(
                  kNumRowsPerBatch / 2,
                  [&](auto row) {
                    return values
                               [(kNumRowsPerBatch / 2 + row) * numBatches +
                                batch]
                                   .has_value()
                        ? values
                              [(kNumRowsPerBatch / 2 + row) * numBatches +
                               batch]
                                  .value()
                        : 0;
                  },
                  [&](auto row) {
                    return !values
                                [(kNumRowsPerBatch / 2 + row) * numBatches +
                                 batch]
                                    .has_value();
                  })}));
          EXPECT_TRUE(state.hasFiles(partition));

          // Indicates that the next additions to 'partition' are not sorted
          // with respect to the values added so far.
          state.finishWrite(partition);
          EXPECT_TRUE(state.hasFiles(partition));
        }
      }
      EXPECT_EQ(numPartitions, state.spilledPartitions());
      EXPECT_LT(
          2 * numPartitions * numBatches * sizeof(int64_t),
          state.spilledBytes());
      EXPECT_EQ(expectedNumSpilledFiles, state.spilledFiles());
      spilledFiles = state.testingSpilledFilePaths();
      std::unordered_set<std::string> spilledFileSet(
          spilledFiles.begin(), spilledFiles.end());
      EXPECT_EQ(spilledFileSet.size(), spilledFiles.size());
      EXPECT_EQ(expectedNumSpilledFiles, spilledFileSet.size());
      // Verify the spilled file exist on file system.
      fs = filesystems::getFileSystem(tempDirectory->path, nullptr);
      for (const auto& spilledFile : spilledFileSet) {
        auto readFile = fs->openFileForRead(spilledFile);
        EXPECT_NE(readFile.get(), nullptr);
      }

      for (auto partition = 0; partition < state.maxPartitions(); ++partition) {
        int numReadBatches = 0;
        auto merge = state.startMerge(partition, nullptr);
        // We expect all the rows in dense increasing order.
        for (auto i = 0; i < numBatches * kNumRowsPerBatch; ++i) {
          auto stream = merge->next();
          ASSERT_NE(nullptr, stream);
          bool isLastBatch = false;
          if (values[i].has_value()) {
            EXPECT_EQ(
                values[i].value(),
                stream->current()
                    .childAt(0)
                    ->asUnchecked<FlatVector<int64_t>>()
                    ->valueAt(stream->currentIndex(&isLastBatch)))
                << i;
            EXPECT_EQ(
                values[i].value(),
                stream->decoded(0).valueAt<int64_t>(stream->currentIndex()))
                << i;
          } else {
            EXPECT_TRUE(stream->current()
                            .childAt(0)
                            ->asUnchecked<FlatVector<int64_t>>()
                            ->isNullAt(stream->currentIndex(&isLastBatch)))
                << i;
            EXPECT_TRUE(stream->decoded(0).isNullAt(stream->currentIndex()))
                << i;
          }
          stream->pop();
          if (isLastBatch) {
            ++numReadBatches;
          }
        }
        ASSERT_EQ(nullptr, merge->next());
        // We do two append writes per each input batch.
        ASSERT_EQ(2 * numBatches, numReadBatches);
      }
      // Both spilled bytes and files stats are cleared after merge read.
      EXPECT_EQ(0, state.spilledBytes());
      EXPECT_EQ(0, state.spilledFiles());
    }
    // Verify the spilled file has been removed from file system after spill
    // state destruction.
    for (const auto& spilledFile : spilledFiles) {
      EXPECT_ANY_THROW(fs->openFileForRead(spilledFile));
    }
  }

  memory::MappedMemory* mappedMemory_;
};

TEST_F(SpillTest, spillState) {
  // Set the target file size to a large value to avoid new file creation
  // triggered by batch write.

  // Test with distinct sort keys.
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{true, true}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{true, false}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{false, true}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {CompareFlags{false, false}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 1, {}, 2 * 10);

  // Test with duplicate sort keys.
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{true, true}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{true, false}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {CompareFlags{false, true}}, 2 * 10);
  spillStateTest(
      1'000'000'000, 2, 10, 10, {CompareFlags{false, false}}, 2 * 10);
  spillStateTest(1'000'000'000, 2, 10, 10, {}, 2 * 10);
}

TEST_F(SpillTest, spillStateWithSmallTargetFileSize) {
  // Set the target file size to a small value to open a new file on each batch
  // write.

  // Test with distinct sort keys.
  spillStateTest(1, 2, 10, 1, {CompareFlags{true, true}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{true, false}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{false, true}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 1, {CompareFlags{false, false}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 1, {}, 2 * 10 * 2);

  // Test with duplicated sort keys.
  spillStateTest(1, 2, 10, 10, {CompareFlags{true, false}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{true, true}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{false, false}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 10, {CompareFlags{false, true}}, 2 * 10 * 2);
  spillStateTest(1, 2, 10, 10, {}, 2 * 10 * 2);
}
