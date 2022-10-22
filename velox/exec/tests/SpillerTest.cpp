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
#include <unordered_set>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/OperatorUtils.h"
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

  std::string toString() const {
    return fmt::format("{}|{}", Spiller::typeName(type), poolSize);
  }
};

struct TestParamsBuilder {
  std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    for (int i = 0; i < Spiller::kNumTypes; ++i) {
      const auto type = static_cast<Spiller::Type>(i);
      if (typesToExclude.find(type) == typesToExclude.end()) {
        for (int poolSize : {0, 8}) {
          params.emplace_back(type, poolSize);
        }
      }
    }
    return params;
  }

  std::unordered_set<Spiller::Type> typesToExclude{};
};

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

void resizeVector(RowVector& vector, vector_size_t size) {
  vector.prepareForReuse();
  vector.resize(size);
  for (int32_t childIdx = 0; childIdx < vector.childrenSize(); ++childIdx) {
    vector.childAt(childIdx)->resize(size);
  }
}
} // namespace

class SpillerTest : public exec::test::RowContainerTestBase {
 public:
  static void SetUpTestCase() {
    TestValue::enable();
  }

  explicit SpillerTest(const TestParam& param)
      : param_(param),
        type_(param.type),
        executorPoolSize_(param.poolSize),
        hashBits_(0, type_ == Spiller::Type::kOrderBy ? 0 : 2),
        numPartitions_(hashBits_.numPartitions()) {}

  void SetUp() override {
    RowContainerTestBase::SetUp();
    rng_.seed(1);
    tempDirPath_ = exec::test::TempDirectoryPath::create();
    fs_ = filesystems::getFileSystem(tempDirPath_->path, nullptr);
    rowType_ = ROW({
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
    numKeys_ = 6;
    keyChannels_.resize(numKeys_);
    std::iota(std::begin(keyChannels_), std::end(keyChannels_), 0);
    spillIndicesBuffers_.resize(numPartitions_);
    numPartitionInputs_.resize(numPartitions_, 0);
  }

 protected:
  void testSortedSpill(
      int32_t spillPct,
      int numDuplicates,
      int32_t outputBatchSize = 0,
      bool ascending = true,
      bool makeError = false) {
    SCOPED_TRACE(fmt::format(
        "spillPct:{} numDuplicates:{} outputBatchSize:{} ascending:{} makeError:{}",
        spillPct,
        numDuplicates,
        outputBatchSize,
        ascending,
        makeError));
    constexpr int32_t kNumRows = 5'000;

    setupSpillData(
        rowType_, numKeys_, kNumRows, numDuplicates, [&](RowVectorPtr rows) {
          // Set ordinal so that the sorted order is unambiguous.
          setSequentialValue(rows, 5);
        });
    sortSpillData(ascending);

    setupSpiller(2'000'000, makeError, ascending);

    // We spill spillPct% of the data in 10% increments.
    runSpill(spillPct, 10, makeError);
    if (makeError) {
      return;
    }
    // Verify the spilled file exist on file system.
    const int numSpilledFiles = spiller_->spilledFiles();
    EXPECT_GT(numSpilledFiles, 0);
    const auto spilledFileSet = spiller_->state().testingSpilledFilePaths();
    EXPECT_EQ(numSpilledFiles, spilledFileSet.size());

    for (auto spilledFile : spilledFileSet) {
      auto readFile = fs_->openFileForRead(spilledFile);
      EXPECT_NE(readFile.get(), nullptr);
    }
    if (spillPct == 100) {
      ASSERT_TRUE(spiller_->isAnySpilled());
      ASSERT_TRUE(spiller_->isAnySpilled());
    }

    Spiller::SpillRows unspilledPartitionRows = spiller_->finishSpill();
    SpillPartitionNumSet expectedSpillPartitions;
    if (spillPct == 100) {
      EXPECT_TRUE(unspilledPartitionRows.empty());
      EXPECT_EQ(0, rowContainer_->numRows());
      EXPECT_EQ(numPartitions_, spiller_->stats().spilledPartitions);
      for (int i = 0; i < numPartitions_; ++i) {
        expectedSpillPartitions.insert(i);
      }
      EXPECT_EQ(
          expectedSpillPartitions, spiller_->state().spilledPartitionSet());
    } else {
      EXPECT_GE(numPartitions_, spiller_->stats().spilledPartitions);
      EXPECT_GE(numPartitions_, spiller_->state().spilledPartitionSet().size());
    }
    // Assert we can't call any spill function after the spiller has been
    // finalized.
    ASSERT_ANY_THROW(spiller_->spill({0}));
    ASSERT_ANY_THROW(spiller_->spill(0, nullptr));
    ASSERT_ANY_THROW(spiller_->spill(100, 100));

    verifySortedSpillData(outputBatchSize);

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
      RowTypePtr rowType,
      int32_t numKeys,
      int32_t numRows,
      int32_t numDuplicates,
      std::function<void(RowVectorPtr)> customizeData = {},
      std::vector<int> numRowsPerPartition = {}) {
    rowVector_.reset();
    rowContainer_.reset();

    if (!numRowsPerPartition.empty()) {
      int32_t totalCount = 0;
      for (auto count : numRowsPerPartition) {
        totalCount += count;
      }
      VELOX_CHECK_EQ(totalCount, numRows);
    }

    rowVector_ = BaseVector::create<RowVector>(rowType, numRows, pool_.get());
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

    if (numRows == 0 || type_ == Spiller::Type::kHashJoinProbe) {
      return;
    }
    const SelectivityVector allRows(numRows);
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

  void sortSpillData(bool ascending = true) {
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
    compareFlags_.clear();
    if (!ascending) {
      for (int i = 0; i < rowContainer_->keyTypes().size(); ++i) {
        compareFlags_.push_back({true, false});
      }
    }
    for (auto& partition : partitions_) {
      std::sort(
          partition.begin(),
          partition.end(),
          [&](int32_t leftIndex, int32_t rightIndex) {
            return rowContainer_->compareRows(
                       rows_[leftIndex], rows_[rightIndex], compareFlags_) < 0;
          });
    }
  }

  void
  setupSpiller(int64_t targetFileSize, bool makeError, bool ascending = true) {
    if (type_ == Spiller::Type::kHashJoinProbe) {
      // kHashJoinProbe doesn't have associated row container.
      spiller_ = std::make_unique<Spiller>(
          type_,
          asRowType(rowVector_->type()),
          hashBits_,
          makeError ? "/bad/path" : tempDirPath_->path,
          targetFileSize,
          *pool_,
          executor());
    } else if (type_ == Spiller::Type::kOrderBy) {
      // We spill 'data' in one partition in type of kOrderBy, otherwise in 4
      // partitions.
      spiller_ = std::make_unique<Spiller>(
          type_,
          rowContainer_.get(),
          [&](folly::Range<char**> rows) { rowContainer_->eraseRows(rows); },
          asRowType(rowVector_->type()),
          rowContainer_->keyTypes().size(),
          compareFlags_,
          makeError ? "/bad/path" : tempDirPath_->path,
          targetFileSize,
          *pool_,
          executor());
    } else {
      spiller_ = std::make_unique<Spiller>(
          type_,
          rowContainer_.get(),
          [&](folly::Range<char**> rows) { rowContainer_->eraseRows(rows); },
          asRowType(rowVector_->type()),
          hashBits_,
          rowContainer_->keyTypes().size(),
          compareFlags_,
          makeError ? "/bad/path" : tempDirPath_->path,
          targetFileSize,
          *pool_,
          executor());
    }
    if (type_ == Spiller::Type::kOrderBy) {
      ASSERT_EQ(spiller_->state().maxPartitions(), 1);
    } else {
      ASSERT_EQ(spiller_->state().maxPartitions(), numPartitions_);
    }
    ASSERT_FALSE(spiller_->isAllSpilled());
    ASSERT_FALSE(spiller_->isAnySpilled());
    ASSERT_EQ(spiller_->hashBits(), hashBits_);
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

  void verifySortedSpillData(int32_t outputBatchSize = 0) {
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
      if (outputBatchSize == 0) {
        for (auto i = 0; i < indices.size(); ++i) {
          auto stream = merge->next();
          if (!stream) {
            FAIL() << "Stream ends after " << i << " entries";
            break;
          }
          ASSERT_TRUE(rowVector_->equalValueAt(
              &stream->current(), indices[i], stream->currentIndex()));
          stream->pop();
        }
      } else {
        int nextBatchSize = std::min<int>(indices.size(), outputBatchSize);
        auto outputVector = BaseVector::create<RowVector>(
            rowVector_->type(), nextBatchSize, pool_.get());
        resizeVector(*outputVector, nextBatchSize);

        int i = 0;
        int outputRow = 0;
        int outputSize = 0;
        std::vector<const RowVector*> sourceVectors(outputBatchSize);
        std::vector<vector_size_t> sourceIndices(outputBatchSize);
        for (;;) {
          auto stream = merge->next();
          if (stream == nullptr) {
            for (int j = 0; j < outputVector->size(); ++j, ++i) {
              ASSERT_TRUE(
                  rowVector_->equalValueAt(outputVector.get(), indices[i], j))
                  << j << ", " << i;
            }
            ASSERT_EQ(i, indices.size());
            break;
          }
          sourceVectors[outputSize] = &stream->current();
          bool isEndOfBatch = false;
          sourceIndices[outputSize] = stream->currentIndex(&isEndOfBatch);
          ++outputSize;
          if (isEndOfBatch) {
            // The stream is at end of input batch. Need to copy out the rows
            // before fetching next batch in 'pop'.
            gatherCopy(
                outputVector.get(),
                outputRow,
                outputSize,
                sourceVectors,
                sourceIndices);
            outputRow += outputSize;
            outputSize = 0;
          }

          // Advance the stream.
          stream->pop();

          // The output buffer is full. Need to copy out the rows.
          if (outputRow + outputSize == nextBatchSize) {
            gatherCopy(
                outputVector.get(),
                outputRow,
                outputSize,
                sourceVectors,
                sourceIndices);
            for (int j = 0; j < outputVector->size(); ++j, ++i) {
              ASSERT_TRUE(
                  rowVector_->equalValueAt(outputVector.get(), indices[i], j))
                  << outputVector->toString(0, nextBatchSize - 1) << i << ", "
                  << j;
            }
            outputRow = 0;
            outputSize = 0;
            nextBatchSize = std::min<int>(indices.size() - i, outputBatchSize);
            resizeVector(*outputVector, nextBatchSize);
          }
        }
      }
    }
  }

  void splitByPartition(
      const RowVectorPtr& input,
      HashPartitionFunction& partitionFn,
      std::vector<std::vector<RowVectorPtr>>& inputsByPartition) {
    spillPartitions_.resize(input->size());
    for (int partition = 0; partition < numPartitions_; ++partition) {
      if (spillIndicesBuffers_[partition] == nullptr) {
        spillIndicesBuffers_[partition] =
            allocateIndices(input->size(), pool_.get());
      } else {
        AlignedBuffer::reallocate<vector_size_t>(
            &spillIndicesBuffers_[partition], input->size());
      }
    }
    ::memset(
        numPartitionInputs_.data(),
        0,
        numPartitionInputs_.size() * sizeof(vector_size_t));

    partitionFn.partition(*input, spillPartitions_);

    for (auto i = 0; i < input->size(); ++i) {
      auto partition = spillPartitions_[i];
      spillIndicesBuffers_[partition]
          ->asMutable<vector_size_t>()[numPartitionInputs_[partition]++] = i;
    }
    for (int partition = 0; partition < numPartitions_; ++partition) {
      if (numPartitionInputs_[partition] == 0) {
        inputsByPartition[partition].push_back(nullptr);
      } else {
        inputsByPartition[partition].push_back(wrap(
            numPartitionInputs_[partition],
            spillIndicesBuffers_[partition],
            input));
      }
    }
  }

  void testNonSortedSpill(
      int numSpillers,
      int numBatchRows,
      int numAppendBatches,
      int targetFileSize) {
    ASSERT_TRUE(
        type_ == Spiller::Type::kHashJoinBuild ||
        type_ == Spiller::Type::kHashJoinProbe);

    const int numSpillPartitions =
        1 + folly::Random().rand32() % numPartitions_;
    SpillPartitionNumSet spillPartitionNumSet;
    while (spillPartitionNumSet.size() < numSpillPartitions) {
      spillPartitionNumSet.insert(folly::Random().rand32() % numPartitions_);
    }
    SCOPED_TRACE(fmt::format(
        "Param: {}, numSpillers: {}, numBatchRows: {}, numAppendBatches: {}, targetFileSize: {}, spillPartitionNumSet: {}",
        param_.toString(),
        numSpillers,
        numBatchRows,
        numAppendBatches,
        targetFileSize,
        folly::join(",", spillPartitionNumSet)));

    std::vector<std::vector<RowVectorPtr>> inputsByPartition(numPartitions_);

    std::vector<column_index_t> keyChannels(numKeys_);
    HashPartitionFunction spillHashFunction(hashBits_, rowType_, keyChannels_);
    // Setup a number of spillers to spill data and then accumulate results from
    // them by partition.
    std::vector<std::unique_ptr<Spiller>> spillers;
    for (int iter = 0; iter < numSpillers; ++iter) {
      setupSpillData(
          rowType_,
          numKeys_,
          type_ == Spiller::Type::kHashJoinBuild ? numBatchRows * 10 : 0,
          1,
          nullptr,
          {});
      setupSpiller(targetFileSize, false);
      // Can't append without marking a partition as spilling.
      ASSERT_ANY_THROW(spiller_->spill(0, rowVector_));
      // Can't create sorted stream reader from non-sorted spiller.
      ASSERT_ANY_THROW(spiller_->startMerge(0));

      splitByPartition(rowVector_, spillHashFunction, inputsByPartition);

      std::vector<Spiller::SpillableStats> statsList;
      if (type_ == Spiller::Type::kHashJoinProbe) {
        spiller_->setPartitionsSpilled(spillPartitionNumSet);
#ifndef NDEBUG
        ASSERT_ANY_THROW(spiller_->setPartitionsSpilled(spillPartitionNumSet));
#endif
        ASSERT_ANY_THROW(spiller_->fillSpillRuns(statsList););
      } else {
        spiller_->fillSpillRuns(statsList);
        spiller_->spill(spillPartitionNumSet);
      }
      // Spill data.
      for (int i = 0; i < numAppendBatches; ++i) {
        RowVectorPtr batch = makeDataset(rowType_, numBatchRows, nullptr);
        splitByPartition(batch, spillHashFunction, inputsByPartition);
        for (const auto& partition : spillPartitionNumSet) {
          spiller_->spill(partition, inputsByPartition[partition].back());
        }
        if (type_ == Spiller::Type::kHashJoinBuild) {
          statsList.clear();
          spiller_->fillSpillRuns(statsList);
          for (int partition = 0; partition < numPartitions_; ++partition) {
            if (spillPartitionNumSet.count(partition) != 0) {
              ASSERT_EQ(statsList[partition].numBytes, 0);
              ASSERT_EQ(statsList[partition].numRows, 0);
            }
          }
        } else {
          ASSERT_ANY_THROW(spiller_->fillSpillRuns(statsList););
        }
      }
      // Assert that non-sorted spiller type doesn't support incremental
      // spilling.
      ASSERT_ANY_THROW(spiller_->spill(100, 100));
      spillers.push_back(std::move(spiller_));
    }

    // Read back data from all the spilled partitions and verify.
    verifyNonSortedSpillData(
        std::move(spillers), spillPartitionNumSet, inputsByPartition);
  }

  void verifyNonSortedSpillData(
      std::vector<std::unique_ptr<Spiller>> spillers,
      const SpillPartitionNumSet& spillPartitionNumSet,
      const std::vector<std::vector<RowVectorPtr>>& inputsByPartition) {
    ASSERT_TRUE(
        type_ == Spiller::Type::kHashJoinBuild ||
        type_ == Spiller::Type::kHashJoinProbe);

    SpillPartitionSet spillPartitionSet;
    for (auto& spiller : spillers) {
      spiller->finishSpill(spillPartitionSet);
      ASSERT_ANY_THROW(spiller->spill(0, nullptr));
      ASSERT_ANY_THROW(spiller->spill({0}));
    }
    ASSERT_EQ(spillPartitionSet.size(), spillPartitionNumSet.size());

    for (auto& spillPartitionEntry : spillPartitionSet) {
      const int partition = spillPartitionEntry.first.partitionNumber();
      ASSERT_EQ(
          hashBits_.begin(), spillPartitionEntry.first.partitionBitOffset());
      auto reader = spillPartitionEntry.second->createReader();
      if (type_ == Spiller::Type::kHashJoinProbe) {
        // For hash probe type, we append each input vector as one batch in
        // spill file so that we can do one-to-one comparison.
        for (int i = 0; i < inputsByPartition[partition].size(); ++i) {
          const auto& expectedVector = inputsByPartition[partition][i];
          if (expectedVector == nullptr) {
            continue;
          }
          RowVectorPtr outputVector;
          ASSERT_TRUE(reader->nextBatch(outputVector));
          for (int row = 0; row < expectedVector->size(); ++row) {
            ASSERT_EQ(
                expectedVector->compare(
                    outputVector.get(), row, row, CompareFlags{}),
                0);
          }
        }
      } else {
        // For hash build type, spill partition operation might generate
        // different number of batches in spill file, then we have to do row by
        // row comparison.
        RowVectorPtr outputVector;
        int outputRow = 0;
        for (int i = 0; i < inputsByPartition[partition].size(); ++i) {
          const auto& expectedVector = inputsByPartition[partition][i];
          if (expectedVector == nullptr) {
            continue;
          }
          for (int row = 0; row < expectedVector->size(); ++row) {
            if (outputVector == nullptr || outputRow >= outputVector->size()) {
              ASSERT_TRUE(reader->nextBatch(outputVector))
                  << "input row: " << row << " input size "
                  << expectedVector->size() << " output row " << outputRow
                  << " output size " << outputVector->size() << " batch " << i;
              outputRow = 0;
            }
            ASSERT_EQ(
                expectedVector->compare(
                    outputVector.get(), row, outputRow++, CompareFlags{}),
                0)
                << "input row: " << row << " input size "
                << expectedVector->size() << " output row " << outputRow - 1
                << " output size " << outputVector->size() << " batch " << i;
          }
        }
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

  const TestParam param_;
  const Spiller::Type type_;
  const int executorPoolSize_;
  const HashBitRange hashBits_;
  const int numPartitions_;
  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<TempDirectoryPath> tempDirPath_;
  std::shared_ptr<FileSystem> fs_;
  RowTypePtr rowType_;
  int numKeys_;
  std::vector<column_index_t> keyChannels_;
  std::vector<uint32_t> spillPartitions_;
  std::vector<BufferPtr> spillIndicesBuffers_;
  std::vector<vector_size_t> numPartitionInputs_;
  std::unique_ptr<RowContainer> rowContainer_;
  RowVectorPtr rowVector_;
  std::vector<char*> rows_;
  std::vector<std::vector<int32_t>> partitions_;
  std::vector<CompareFlags> compareFlags_;
  std::unique_ptr<Spiller> spiller_;
};

class AllTypes : public SpillerTest,
                 public testing::WithParamInterface<TestParam> {
 public:
  AllTypes() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder().getTestParams();
  }
};

class NoHashJoin : public SpillerTest,
                   public testing::WithParamInterface<TestParam> {
 public:
  NoHashJoin() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kHashJoinProbe, Spiller::Type::kHashJoinBuild}}
        .getTestParams();
  }
};

class NoHashJoinNoOrderBy : public SpillerTest,
                            public testing::WithParamInterface<TestParam> {
 public:
  NoHashJoinNoOrderBy() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kHashJoinProbe,
             Spiller::Type::kHashJoinBuild,
             Spiller::Type::kOrderBy}}
        .getTestParams();
  }
};

TEST_P(NoHashJoin, spilFew) {
  // Test with distinct sort keys.
  testSortedSpill(10, 1);
  testSortedSpill(10, 1, 0, false);
  testSortedSpill(10, 1, 32);
  testSortedSpill(10, 1, 32, false);
  // Test with duplicate sort keys.
  testSortedSpill(10, 10);
  testSortedSpill(10, 10, 0, false);
  testSortedSpill(10, 10, 32);
  testSortedSpill(10, 10, 32, false);
}

TEST_P(NoHashJoin, spilMost) {
  // Test with distinct sort keys.
  testSortedSpill(60, 1);
  testSortedSpill(60, 1, 0, false);
  testSortedSpill(60, 1, 32);
  testSortedSpill(60, 1, 32, false);
  // Test with duplicate sort keys.
  testSortedSpill(60, 10);
  testSortedSpill(60, 10, 0, false);
  testSortedSpill(60, 10, 32);
  testSortedSpill(60, 10, 32, false);
}

TEST_P(NoHashJoin, spillAll) {
  // Test with distinct sort keys.
  testSortedSpill(100, 1);
  testSortedSpill(100, 1, 0, false);
  testSortedSpill(100, 1, 32);
  testSortedSpill(100, 1, 32, false);
  // Test with duplicate sort keys.
  testSortedSpill(100, 10);
  testSortedSpill(100, 10, 0, false);
  testSortedSpill(100, 10, 32);
  testSortedSpill(100, 10, 32, false);
}

TEST_P(NoHashJoin, error) {
  testSortedSpill(100, 1, 0, true);
}

TEST_P(NoHashJoinNoOrderBy, spillWithEmptyPartitions) {
  // kOrderBy type which has only one partition which is not relevant for this
  // test.
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
    ASSERT_TRUE(spiller_->isAnySpilled());
    ASSERT_FALSE(spiller_->isAllSpilled());

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
    verifySortedSpillData();
    EXPECT_EQ(numRows, spiller_->stats().spilledRows);
  }
}

TEST_P(NoHashJoinNoOrderBy, spillWithNonSpillingPartitions) {
  // kOrderBy type which has only one partition, is irrelevant for this test.
  RowTypePtr rowType = ROW({{"long_val", BIGINT()}, {"string_val", VARCHAR()}});
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
    ASSERT_TRUE(spiller_->isAnySpilled());
    ASSERT_FALSE(spiller_->isAllSpilled());
    ASSERT_EQ(1, spiller_->spilledFiles());
    // Expect non-spilling partition.
    EXPECT_FALSE(spiller_->finishSpill().empty());
    verifySortedSpillData();
    EXPECT_LT(0, spiller_->stats().spilledRows);
    EXPECT_GT(numRows, spiller_->stats().spilledRows);
  }
}

TEST_P(AllTypes, nonSortedSpillFunctions) {
  if (type_ == Spiller::Type::kOrderBy || type_ == Spiller::Type::kAggregate) {
    setupSpillData(rowType_, numKeys_, 1'000, 1, nullptr, {});
    sortSpillData();
    setupSpiller(100'000, false);
    {
      RowVectorPtr dummyVector;
      EXPECT_ANY_THROW(spiller_->spill(0, dummyVector));
    }
    std::vector<Spiller::SpillableStats> statsList;
    spiller_->fillSpillRuns(statsList);
    std::vector<uint32_t> spillPartitionNums(numPartitions_);
    std::iota(spillPartitionNums.begin(), spillPartitionNums.end(), 0);
    spiller_->spill(SpillPartitionNumSet(
        spillPartitionNums.begin(), spillPartitionNums.end()));
    spiller_->finishSpill();
    verifySortedSpillData();
    return;
  }
  testNonSortedSpill(1, 1000, 1, 1);
  testNonSortedSpill(1, 1000, 10, 1);
  testNonSortedSpill(1, 1000, 1, 1'000'000'000);
  testNonSortedSpill(1, 1000, 10, 1'000'000'000);
  testNonSortedSpill(4, 1000, 1, 1);
  testNonSortedSpill(4, 1000, 10, 1);
  testNonSortedSpill(4, 1000, 1, 1'000'000'000);
  testNonSortedSpill(4, 1000, 10, 1'000'000'000);
  // Empty case.
  testNonSortedSpill(1, 1000, 0, 1);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    AllTypes,
    testing::ValuesIn(AllTypes::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    NoHashJoin,
    testing::ValuesIn(NoHashJoin::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    NoHashJoinNoOrderBy,
    testing::ValuesIn(NoHashJoinNoOrderBy::getTestParams()));

TEST(SpillerTest, stats) {
  Spiller::Stats sumStats;
  EXPECT_EQ(0, sumStats.spilledRows);
  EXPECT_EQ(0, sumStats.spilledBytes);
  EXPECT_EQ(0, sumStats.spilledPartitions);

  Spiller::Stats stats;
  stats.spilledRows = 10;
  stats.spilledBytes = 100;
  stats.spilledPartitions = 2;

  sumStats += stats;
  EXPECT_EQ(stats.spilledRows, sumStats.spilledRows);
  EXPECT_EQ(stats.spilledBytes, sumStats.spilledBytes);
  EXPECT_EQ(stats.spilledPartitions, sumStats.spilledPartitions);

  sumStats += stats;
  EXPECT_EQ(2 * stats.spilledRows, sumStats.spilledRows);
  EXPECT_EQ(2 * stats.spilledBytes, sumStats.spilledBytes);
  EXPECT_EQ(2 * stats.spilledPartitions, sumStats.spilledPartitions);

  sumStats += stats;
  EXPECT_EQ(3 * stats.spilledRows, sumStats.spilledRows);
  EXPECT_EQ(3 * stats.spilledBytes, sumStats.spilledBytes);
  EXPECT_EQ(3 * stats.spilledPartitions, sumStats.spilledPartitions);
}

TEST(SpillerTest, spillLevel) {
  const uint8_t kInitialBitOffset = 16;
  const uint8_t kNumPartitionsBits = 3;
  const HashBitRange partitionBits(
      kInitialBitOffset, kInitialBitOffset + kNumPartitionsBits);
  const Spiller::Config config(
      "fakeSpillPath", 0.0, nullptr, 0, partitionBits, 0, 0);
  struct {
    uint8_t bitOffset;
    // Indicates an invalid if 'expectedLevel' is negative.
    int32_t expectedLevel;

    std::string debugString() const {
      return fmt::format(
          "bitOffset:{}, expectedLevel:{}", bitOffset, expectedLevel);
    }
  } testSettings[] = {
      {0, -1},
      {kInitialBitOffset - 1, -1},
      {kInitialBitOffset - kNumPartitionsBits, -1},
      {kInitialBitOffset, 0},
      {kInitialBitOffset + 1, -1},
      {kInitialBitOffset + kNumPartitionsBits, 1},
      {kInitialBitOffset + 3 * kNumPartitionsBits, 3},
      {kInitialBitOffset + 15 * kNumPartitionsBits, 15},
      {kInitialBitOffset + 16 * kNumPartitionsBits, -1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedLevel == -1) {
      ASSERT_ANY_THROW(config.spillLevel(testData.bitOffset));
    } else {
      ASSERT_EQ(config.spillLevel(testData.bitOffset), testData.expectedLevel);
    }
  }
}

TEST(SpillerTest, spillLevelLimit) {
  struct {
    uint8_t startBitOffset;
    int32_t numBits;
    uint8_t bitOffset;
    int32_t maxSpillLevel;
    int32_t expectedExceeds;

    std::string debugString() const {
      return fmt::format(
          "startBitOffset:{}, numBits:{}, bitOffset:{}, maxSpillLevel:{}, expectedExceeds:{}",
          startBitOffset,
          numBits,
          bitOffset,
          maxSpillLevel,
          expectedExceeds);
    }
  } testSettings[] = {
      {0, 2, 2, 0, true},
      {0, 2, 2, 1, false},
      {0, 2, 4, 0, true},
      {0, 2, 0, -1, false},
      {0, 2, 62, -1, false},
      {0, 2, 63, -1, true},
      {0, 2, 64, -1, true},
      {0, 2, 65, -1, true},
      {30, 3, 30, 0, false},
      {30, 3, 33, 0, true},
      {30, 3, 30, 1, false},
      {30, 3, 33, 1, false},
      {30, 3, 36, 1, true},
      {30, 3, 0, -1, false},
      {30, 3, 60, -1, false},
      {30, 3, 63, -1, true},
      {30, 3, 66, -1, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    const HashBitRange partitionBits(
        testData.startBitOffset, testData.startBitOffset + testData.numBits);
    const Spiller::Config config(
        "fakeSpillPath",
        0.0,
        nullptr,
        0,
        partitionBits,
        testData.maxSpillLevel,
        0);

    ASSERT_EQ(
        testData.expectedExceeds,
        config.exceedSpillLevelLimit(testData.bitOffset));
  }
}
