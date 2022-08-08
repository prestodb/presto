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

#include "velox/exec/Spiller.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/RowContainerTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;
using facebook::velox::filesystems::FileSystem;

namespace {
struct TestParam {
  Spiller::Type type;
  // Specifies the spill executor pool size. If the size is zero, then spill
  // write path is executed inline with spiller control code path.
  int poolSize;

  TestParam(Spiller::Type _type, int _poolSize)
      : type(_type), poolSize(_poolSize) {}
};

std::vector<TestParam> getTestParams() {
  std::vector<TestParam> params;
  for (int i = 0; i < Spiller::kNumTypes; ++i) {
    const auto type = static_cast<Spiller::Type>(i);
    // TODO: add to test kHashJoin after it is supported.
    if (type == Spiller::Type::kHashJoin) {
      continue;
    }
    for (int poolSize : {0, 8}) {
      params.emplace_back(type, poolSize);
    }
  }
  return params;
}

// Set sequential value in a given child vector. 'value' is the starting value.
void setSequentialValue(
    RowVectorPtr rowVector,
    column_index_t childIndex,
    int64_t value = 0) {
  auto valueVector = rowVector->childAt(childIndex)->as<FlatVector<int64_t>>();
  for (auto i = 0; i < valueVector->size(); ++i) {
    valueVector->set(i, value + i);
  }
}
} // namespace

class SpillerTest : public exec::test::RowContainerTestBase,
                    public testing::WithParamInterface<TestParam> {
 public:
  static void SetUpTestCase() {
    TestValue::enable();
  }

  SpillerTest()
      : type_(GetParam().type),
        executorPoolSize_(GetParam().poolSize),
        hashBits_(0, type_ == Spiller::Type::kOrderBy ? 0 : 2),
        numPartitions_(hashBits_.numPartitions()) {}

  void SetUp() override {
    RowContainerTestBase::SetUp();
    tempDirPath_ = exec::test::TempDirectoryPath::create();
    fs_ = filesystems::getFileSystem(tempDirPath_->path, nullptr);
  }

 protected:
  void testSpill(int32_t spillPct, int numDuplicates, bool makeError = false) {
    constexpr int32_t kNumRows = 100'000;
    std::shared_ptr<const RowType> rowType = ROW({
        {"bool_val", BOOLEAN()},
        {"tiny_val", TINYINT()},
        {"small_val", SMALLINT()},
        {"int_val", INTEGER()},
        {"long_val", BIGINT()},
        {"ordinal", BIGINT()},
        {"float_val", REAL()},
        {"double_val", DOUBLE()},
        {"string_val", VARCHAR()},
        {"array_val", ARRAY(VARCHAR())},
        {"struct_val", ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
        {"map_val",
         MAP(VARCHAR(),
             MAP(BIGINT(),
                 ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))},
    });
    setupSpillData(rowType, 6, kNumRows, numDuplicates, [&](RowVectorPtr rows) {
      // Set ordinal so that the sorted order is unambiguous.
      setSequentialValue(rows, 5);
    });
    sortSpillData();

    setupSpiller(2'000'000, makeError);

    // We spill spillPct% of the data in 10% increments.
    runSpill(spillPct, 10, makeError);
    if (makeError) {
      return;
    }
    // Verify the spilled file exist on file system.
    const int numSpilledFiles = spiller_->spilledFiles();
    EXPECT_GT(numSpilledFiles, 0);
    const auto spilledFileSet = spiller_->state().TEST_spilledFiles();
    EXPECT_EQ(numSpilledFiles, spilledFileSet.size());

    for (auto spilledFile : spilledFileSet) {
      auto readFile = fs_->openFileForRead(spilledFile);
      EXPECT_NE(readFile.get(), nullptr);
    }

    Spiller::SpillRows unspilledPartitionRows = spiller_->finishSpill();
    if (spillPct == 100) {
      EXPECT_TRUE(unspilledPartitionRows.empty());
      EXPECT_EQ(0, rowContainer_->numRows());
    }

    verifySpillData();

    spiller_.reset();
    // Verify the spilled file has been removed from file system after spiller
    // destruction.
    for (auto spilledFile : spilledFileSet) {
      EXPECT_ANY_THROW(fs_->openFileForRead(spilledFile));
    }
  }

  // 'numDuplicates' specifies the number of duplicate rows generated for each
  // distinct sorting key in test.
  void setupSpillData(
      std::shared_ptr<const RowType> rowType,
      int32_t numKeys,
      int32_t numRows,
      int32_t numDuplicates,
      std::function<void(RowVectorPtr)> customizeData = {},
      std::vector<int> numRowsPerPartition = {}) {
    if (!numRowsPerPartition.empty()) {
      int32_t totalCount = 0;
      for (auto count : numRowsPerPartition) {
        totalCount += count;
      }
      VELOX_CHECK_EQ(totalCount, numRows);
    }

    rowVector_ = BaseVector::create<RowVector>(rowType, numRows, pool_.get());

    const SelectivityVector allRows(numRows);

    const auto& childTypes = rowType->children();
    std::vector<TypePtr> keys(childTypes.begin(), childTypes.begin() + numKeys);
    std::vector<TypePtr> dependents;
    if (numKeys < childTypes.size()) {
      dependents.insert(
          dependents.end(), childTypes.begin() + numKeys, childTypes.end());
    }
    // Make non-join build container so that spill runs are sorted. Note
    // that a distinct or group by hash table can have dependents if
    // some keys are known to be unique by themselves. Aggregation
    // spilling will be tested separately.
    rowContainer_ = makeRowContainer(keys, dependents, false);

    // Setup temporary row to check spilling partition number.
    char* testRow = rowContainer_->newRow();
    std::vector<char*> testRows(1, testRow);
    const auto testRowSet = folly::Range<char**>(testRows.data(), 1);
    std::vector<uint64_t> hashes(1);

    int numFilledRows = 0;
    do {
      RowVectorPtr batch = makeDataset(rowType, numRows, customizeData);
      if (!numRowsPerPartition.empty()) {
        for (int index = 0; index < numRows; ++index) {
          for (int i = 0; i < keys.size(); ++i) {
            DecodedVector decodedVector(*batch->childAt(i), allRows);
            rowContainer_->store(decodedVector, index, testRow, i);
            // Calculate hashes for this batch of spill candidates.
            rowContainer_->hash(i, testRowSet, i > 0, hashes.data());
          }
          const int partitionNum =
              hashBits_.partition(hashes[0], numPartitions_);
          // Copy 'index'th row from 'batch' to 'rowVector_' with
          // 'numDuplicates' times. 'numDuplicates' is the number of duplicates
          // per each distinct row key.
          for (int i = 0;
               i < numDuplicates && --numRowsPerPartition[partitionNum] >= 0;
               ++i) {
            rowVector_->copy(batch.get(), numFilledRows++, index, 1);
          }
        }
      } else {
        rowVector_ = batch;
        numFilledRows += numRows;
      }
    } while (numFilledRows < numRows);
    rowContainer_->clear();

    rows_.resize(numRows);
    for (int i = 0; i < numRows; ++i) {
      rows_[i] = rowContainer_->newRow();
    }

    for (auto column = 0; column < rowVector_->childrenSize(); ++column) {
      DecodedVector decoded(*rowVector_->childAt(column), allRows);
      for (auto index = 0; index < numRows; ++index) {
        rowContainer_->store(decoded, index, rows_[index], column);
      }
    }
  }

  void sortSpillData() {
    partitions_.clear();
    const auto numRows = rows_.size();
    ASSERT_EQ(numRows, rowContainer_->numRows());
    std::vector<uint64_t> hashes(numRows);
    const auto& keys = rowContainer_->keyTypes();
    // Calculate a hash for every key in 'rows'.
    for (auto i = 0; i < keys.size(); ++i) {
      rowContainer_->hash(
          i, folly::Range<char**>(rows_.data(), numRows), i > 0, hashes.data());
    }

    partitions_.resize(numPartitions_);
    for (auto i = 0; i < rowContainer_->numRows(); ++i) {
      partitions_[hashBits_.partition(hashes[i], numPartitions_)].push_back(i);
    }

    // We sort the rows in each partition in key order.
    for (auto& partition : partitions_) {
      std::sort(
          partition.begin(),
          partition.end(),
          [&](int32_t leftIndex, int32_t rightIndex) {
            return rowContainer_->compareRows(
                       rows_[leftIndex], rows_[rightIndex]) < 0;
          });
    }
  }

  void setupSpiller(int64_t targetFileSize, bool makeError) {
    // We spill 'data' in one partition in type of kOrderBy, otherwise in 4
    // partitions.
    spiller_ = std::make_unique<Spiller>(
        type_,
        *rowContainer_,
        [&](folly::Range<char**> rows) { rowContainer_->eraseRows(rows); },
        asRowType(rowVector_->type()),
        hashBits_,
        rowContainer_->keyTypes().size(),
        makeError ? "/bad/path" : tempDirPath_->path,
        targetFileSize,
        *pool_,
        executor());
    EXPECT_EQ(numPartitions_, spiller_->state().maxPartitions());
  }

  void runSpill(int32_t spillPct, int32_t incPct, bool expectedError) {
    // We spill spillPct% of the data in 10% increments.
    auto initialBytes = rowContainer_->allocatedBytes();
    auto initialRows = rowContainer_->numRows();
    for (int32_t pct = incPct; pct <= spillPct; pct += incPct) {
      try {
        spiller_->spill(
            initialRows - (initialRows * pct / 100),
            initialBytes - (initialBytes * pct / 100));
        EXPECT_FALSE(expectedError);
      } catch (const std::exception& e) {
        if (!expectedError) {
          throw;
        }
        return;
      }
    }
  }

  void verifySpillData() {
    // We read back the spilled and not spilled data in each of the
    // partitions. We check that the data comes back in key order.
    for (auto partitionIndex = 0; partitionIndex < numPartitions_;
         ++partitionIndex) {
      if (!spiller_->isSpilled(partitionIndex)) {
        continue;
      }
      // We make a merge reader that merges the spill files and the rows that
      // are still in the RowContainer.
      auto merge = spiller_->startMerge(partitionIndex);

      // We read the spilled data back and check that it matches the sorted
      // order of the partition.
      auto& indices = partitions_[partitionIndex];
      for (auto i = 0; i < indices.size(); ++i) {
        auto stream = merge->next();
        if (!stream) {
          FAIL() << "Stream ends after " << i << " entries";
          break;
        }
        EXPECT_TRUE(rowVector_->equalValueAt(
            &stream->current(), indices[i], stream->currentIndex()));
        stream->pop();
      }
    }
  }

  folly::IOThreadPoolExecutor* executor() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> l(mutex);
    if (executorPoolSize_ == 0) {
      return nullptr;
    }
    if (executor_ == nullptr) {
      executor_ =
          std::make_unique<folly::IOThreadPoolExecutor>(executorPoolSize_);
    }
    return executor_.get();
  }

  void reset() {
    spiller_.reset();
    rowContainer_.reset();
    rowVector_.reset();
    rows_.clear();
    partitions_.clear();
  }

  const Spiller::Type type_;
  const int executorPoolSize_;
  const HashBitRange hashBits_;
  const int numPartitions_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<TempDirectoryPath> tempDirPath_;
  std::shared_ptr<FileSystem> fs_;
  std::unique_ptr<RowContainer> rowContainer_;
  RowVectorPtr rowVector_;
  std::vector<char*> rows_;
  std::vector<std::vector<int32_t>> partitions_;
  std::unique_ptr<Spiller> spiller_;
};

TEST_P(SpillerTest, spilFew) {
  // Test with distinct sort keys.
  testSpill(10, 1);
  // Test with duplicate sort keys.
  testSpill(10, 10);
}

TEST_P(SpillerTest, spilMost) {
  // Test with distinct sort keys.
  testSpill(60, 1);
  // Test with duplicate sort keys.
  testSpill(60, 10);
}

TEST_P(SpillerTest, spillAll) {
  // Test with distinct sort keys.
  testSpill(100, 1);
  testSpill(100, 10);
}

TEST_P(SpillerTest, error) {
  testSpill(100, 1, true);
}

TEST_P(SpillerTest, spillWithEmptyPartitions) {
  if (type_ == Spiller::Type::kOrderBy) {
    LOG(INFO) << "kOrderBy type which has only one partition, "
                 "is not relevant for this test.";
    return;
  }
  auto rowType = ROW({{"long_val", BIGINT()}, {"string_val", VARCHAR()}});
  struct {
    std::vector<int> rowsPerPartition;
    int numDuplicates;

    std::string debugString() const {
      return fmt::format(
          "rowsPerPartition: [{}], numDuplicates: {}",
          folly::join(':', rowsPerPartition),
          numDuplicates);
    }
  } testSettings[] = {// Test with distinct sort keys.
                      {{5000, 0, 0, 0}, 1},
                      {{5'000, 5'000, 0, 1'000}, 1},
                      {{5'000, 0, 5'000, 1'000}, 1},
                      {{5'000, 1'000, 5'000, 0}, 1},
                      // Test with duplicate sort keys.
                      {{5000, 0, 0, 0}, 10},
                      {{5'000, 5'000, 0, 1'000}, 10},
                      {{5'000, 0, 5'000, 1'000}, 10},
                      {{5'000, 1'000, 5'000, 0}, 10}};
  for (auto testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    reset();
    int32_t numRows = 0;
    for (const auto partitionRows : testData.rowsPerPartition) {
      numRows += partitionRows;
    }
    int64_t outputIndex = 0;
    setupSpillData(
        rowType,
        1,
        numRows,
        testData.numDuplicates,
        [&](RowVectorPtr rowVector) {
          // Set ordinal so that the sorted order is unambiguous.
          setSequentialValue(rowVector, 0, outputIndex);
          outputIndex += rowVector->size();
        },
        testData.rowsPerPartition);
    sortSpillData();
    // Setup a large target file size and spill only once to ensure the number
    // of spilled files matches the number of spilled partitions.
    setupSpiller(2'000'000'000, false);
    // We spill spillPct% of the data all at once.
    runSpill(100, 100, false);

    int32_t numNonEmptyPartitions = 0;
    for (int partition = 0; partition < numPartitions_; ++partition) {
      if (testData.rowsPerPartition[partition] != 0) {
        ASSERT_TRUE(spiller_->state().isPartitionSpilled(partition));
        ++numNonEmptyPartitions;
      } else {
        ASSERT_FALSE(spiller_->state().isPartitionSpilled(partition))
            << partition;
      }
    }
    const int numSpilledFiles = spiller_->spilledFiles();
    ASSERT_EQ(numNonEmptyPartitions, numSpilledFiles);
    // Expect no non-spilling partitions.
    EXPECT_TRUE(spiller_->finishSpill().empty());
    verifySpillData();
    EXPECT_EQ(numRows, spiller_->spilledBytesAndRows().second);
  }
}

TEST_P(SpillerTest, spillWithNonSpillingPartitions) {
  if (type_ == Spiller::Type::kOrderBy) {
    // kOrderBy type which has only one partition, is un-relevant for this test.
    GTEST_SKIP();
  }
  std::shared_ptr<const RowType> rowType =
      ROW({{"long_val", BIGINT()}, {"string_val", VARCHAR()}});
  struct {
    std::vector<int> rowsPerPartition;
    int numDuplicates;
    int expectedSpillPartitionIndex;

    std::string debugString() const {
      return fmt::format(
          "rowsPerPartition: [{}], numDuplicates: {}, expectedSpillPartitionIndex: {}",
          folly::join(':', rowsPerPartition),
          numDuplicates,
          expectedSpillPartitionIndex);
    }
  } testSettings[] = {// Test with distinct sort keys.
                      {{5'000, 1, 0, 0}, 1, 0},
                      {{1, 1, 1, 5000}, 1, 3},
                      // Test with duplicate sort keys.
                      {{5'000, 1, 0, 0}, 10, 0},
                      {{1, 1, 1, 5000}, 10, 3}};
  for (auto testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    reset();
    int32_t numRows = 0;
    for (const auto partitionRows : testData.rowsPerPartition) {
      numRows += partitionRows;
    }
    int64_t outputIndex = 0;
    setupSpillData(
        rowType,
        1,
        numRows,
        testData.numDuplicates,
        [&](RowVectorPtr rowVector) {
          // Set ordinal so that the sorted order is unambiguous.
          setSequentialValue(rowVector, 0, outputIndex);
          outputIndex += rowVector->size();
        },
        testData.rowsPerPartition);
    sortSpillData();
    // Setup a large target file size and spill only once to ensure the number
    // of spilled files matches the number of spilled partitions.
    setupSpiller(2'000'000'000, false);
    // We spill spillPct% of the data all at once.
    runSpill(20, 20, false);

    for (int partition = 0; partition < numPartitions_; ++partition) {
      EXPECT_EQ(
          testData.expectedSpillPartitionIndex == partition,
          spiller_->state().isPartitionSpilled(partition));
    }
    ASSERT_EQ(1, spiller_->spilledFiles());
    // Expect non-spilling partition.
    EXPECT_FALSE(spiller_->finishSpill().empty());
    verifySpillData();
    EXPECT_LT(0, spiller_->spilledBytesAndRows().second);
    EXPECT_GT(numRows, spiller_->spilledBytesAndRows().second);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    SpillerTest,
    testing::ValuesIn(getTestParams()));
