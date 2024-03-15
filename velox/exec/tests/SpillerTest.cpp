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
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/tests/utils/RowContainerTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;
using facebook::velox::filesystems::FileSystem;

namespace {
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

struct TestParam {
  Spiller::Type type;
  // Specifies the spill executor pool size. If the size is zero, then spill
  // write path is executed inline with spiller control code path.
  int poolSize;
  common::CompressionKind compressionKind;
  core::JoinType joinType;

  TestParam(
      Spiller::Type _type,
      int _poolSize,
      common::CompressionKind _compressionKind,
      core::JoinType _joinType)
      : type(_type),
        poolSize(_poolSize),
        compressionKind(_compressionKind),
        joinType(_joinType) {}

  std::string toString() const {
    return fmt::format(
        "{}|{}|{}|{}",
        Spiller::typeName(type),
        poolSize,
        compressionKindToString(compressionKind),
        joinTypeName(joinType));
  }
};

struct TestParamsBuilder {
  std::vector<TestParam> getTestParams() {
    std::vector<TestParam> params;
    const auto numSpillerTypes = static_cast<int8_t>(Spiller::Type::kNumTypes);
    for (int i = 0; i < numSpillerTypes; ++i) {
      const auto type = static_cast<Spiller::Type>(i);
      if (typesToExclude.find(type) == typesToExclude.end()) {
        common::CompressionKind compressionKind =
            static_cast<common::CompressionKind>(numSpillerTypes % 6);
        for (int poolSize : {0, 8}) {
          params.emplace_back(
              type, poolSize, compressionKind, core::JoinType::kRight);
          if (type == Spiller::Type::kHashJoinBuild) {
            params.emplace_back(
                type, poolSize, compressionKind, core::JoinType::kLeft);
          }
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
    memory::MemoryManager::testingSetInstance({});
  }

  explicit SpillerTest(const TestParam& param)
      : param_(param),
        type_(param.type),
        executorPoolSize_(param.poolSize),
        compressionKind_(param.compressionKind),
        joinType_(param.joinType),
        spillProbedFlag_(
            type_ == Spiller::Type::kHashJoinBuild &&
            needRightSideJoin(joinType_)),
        hashBits_(
            0,
            (type_ == Spiller::Type::kOrderByInput ||
             type_ == Spiller::Type::kOrderByOutput ||
             type_ == Spiller::Type::kAggregateOutput ||
             type_ == Spiller::Type::kAggregateInput)
                ? 0
                : 2),
        numPartitions_(hashBits_.numPartitions()),
        statWriter_(std::make_unique<TestRuntimeStatWriter>(stats_)) {
    setThreadLocalRunTimeStatWriter(statWriter_.get());
  }

  ~SpillerTest() {
    setThreadLocalRunTimeStatWriter(nullptr);
  }

  void SetUp() override {
    RowContainerTestBase::SetUp();
    rng_.seed(1);
    tempDirPath_ = exec::test::TempDirectoryPath::create();
    fs_ = filesystems::getFileSystem(tempDirPath_->path, nullptr);
    containerType_ = ROW({
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
    if (type_ == Spiller::Type::kHashJoinBuild) {
      rowType_ = hashJoinTableSpillType(containerType_, joinType_);
    } else {
      rowType_ = containerType_;
    }
    numKeys_ = 6;
    keyChannels_.resize(numKeys_);
    std::iota(std::begin(keyChannels_), std::end(keyChannels_), 0);
    spillIndicesBuffers_.resize(numPartitions_);
    numPartitionInputs_.resize(numPartitions_, 0);
    // Check spiller memory pool properties.
    ASSERT_EQ(memory::spillMemoryPool()->name(), "__sys_spilling__");
    ASSERT_EQ(
        memory::spillMemoryPool()->kind(), memory::MemoryPool::Kind::kLeaf);
    ASSERT_TRUE(memory::spillMemoryPool()->threadSafe());
  }

 protected:
  SpillPartitionNumSet allPartitionNumSet() const {
    std::vector<uint32_t> spillPartitionNums(numPartitions_);
    std::iota(spillPartitionNums.begin(), spillPartitionNums.end(), 0);
    return SpillPartitionNumSet(
        spillPartitionNums.begin(), spillPartitionNums.end());
  }

  void testSortedSpill(
      int numDuplicates,
      int32_t outputBatchSize = 0,
      bool ascending = true,
      bool makeError = false) {
    SCOPED_TRACE(fmt::format(
        "spillType: {} numDuplicates: {} outputBatchSize: {} ascending: {} makeError: {}",
        Spiller::typeName(type_),
        numDuplicates,
        outputBatchSize,
        ascending,
        makeError));
    constexpr int32_t kNumRows = 5'000;
    const auto prevGStats = common::globalSpillStats();

    setupSpillData(numKeys_, kNumRows, numDuplicates, [&](RowVectorPtr rows) {
      // Set ordinal so that the sorted order is unambiguous.
      setSequentialValue(rows, 5);
    });
    sortSpillData(ascending);

    setupSpiller(2'000'000, 0, makeError);

    // We spill spillPct% of the data in 10% increments.
    runSpill(makeError);
    if (makeError) {
      return;
    }
    // Verify the spilled file exist on file system.
    auto stats = spiller_->stats();
    const auto numSpilledFiles = stats.spilledFiles;
    if (type_ == Spiller::Type::kAggregateOutput) {
      ASSERT_EQ(numSpilledFiles, 1);
    } else {
      ASSERT_GT(numSpilledFiles, 0);
    }
    const auto spilledFileSet = spiller_->state().testingSpilledFilePaths();
    ASSERT_EQ(spilledFileSet.size(), numSpilledFiles);

    uint64_t totalSpilledBytes{0};
    for (auto spilledFile : spilledFileSet) {
      auto readFile = fs_->openFileForRead(spilledFile);
      ASSERT_NE(readFile.get(), nullptr);
      totalSpilledBytes += readFile->size();
    }
    ASSERT_TRUE(spiller_->isAnySpilled());
    ASSERT_TRUE(spiller_->isAllSpilled());
    ASSERT_FALSE(spiller_->finalized());
    VELOX_ASSERT_THROW(spiller_->spill(0, nullptr), "Unexpected spiller type");
    VELOX_ASSERT_THROW(
        spiller_->setPartitionsSpilled({}), "Unexpected spiller type");
    auto spillPartition = spiller_->finishSpill();
    ASSERT_TRUE(spiller_->finalized());
    ASSERT_EQ(rowContainer_->numRows(), 0);
    ASSERT_EQ(numPartitions_, spiller_->stats().spilledPartitions);
    ASSERT_EQ(numPartitions_, spiller_->state().spilledPartitionSet().size());
    ASSERT_EQ(numSpilledFiles, spiller_->stats().spilledFiles);

    // Assert we can't call any spill function after the spiller has been
    // finalized.
    VELOX_ASSERT_THROW(spiller_->spill(), "Spiller has been finalize");
    VELOX_ASSERT_THROW(
        spiller_->spill(0, nullptr), "Spiller has been finalize");
    VELOX_ASSERT_THROW(spiller_->spill(RowContainerIterator{}), "");

    verifySortedSpillData(&spillPartition, outputBatchSize);

    stats = spiller_->stats();
    ASSERT_EQ(stats.spilledFiles, spilledFileSet.size());
    ASSERT_EQ(stats.spilledPartitions, numPartitions_);
    ASSERT_EQ(stats.spilledRows, kNumRows);
    ASSERT_EQ(stats.spilledBytes, totalSpilledBytes);
    ASSERT_GT(stats.spillWriteTimeUs, 0);
    if (type_ == Spiller::Type::kAggregateOutput) {
      ASSERT_EQ(stats.spillSortTimeUs, 0);
    } else {
      ASSERT_GT(stats.spillSortTimeUs, 0);
    }
    ASSERT_GT(stats.spillFlushTimeUs, 0);
    ASSERT_GT(stats.spillFillTimeUs, 0);
    ASSERT_GT(stats.spillSerializationTimeUs, 0);
    ASSERT_GT(stats.spillWrites, 0);

    const auto newGStats = common::globalSpillStats();
    ASSERT_EQ(
        prevGStats.spilledFiles + stats.spilledFiles, newGStats.spilledFiles);
    ASSERT_EQ(
        prevGStats.spilledRows + stats.spilledRows, newGStats.spilledRows);
    ASSERT_EQ(
        prevGStats.spilledPartitions + stats.spilledPartitions,
        newGStats.spilledPartitions);
    ASSERT_EQ(
        prevGStats.spilledBytes + stats.spilledBytes, newGStats.spilledBytes);
    ASSERT_EQ(
        prevGStats.spillWriteTimeUs + stats.spillWriteTimeUs,
        newGStats.spillWriteTimeUs);
    ASSERT_EQ(
        prevGStats.spillSortTimeUs + stats.spillSortTimeUs,
        newGStats.spillSortTimeUs);
    ASSERT_EQ(
        prevGStats.spillFlushTimeUs + stats.spillFlushTimeUs,
        newGStats.spillFlushTimeUs)
        << prevGStats.spillFlushTimeUs << " " << stats.spillFlushTimeUs << " "
        << newGStats.spillFlushTimeUs;
    ASSERT_EQ(
        prevGStats.spillFillTimeUs + stats.spillFillTimeUs,
        newGStats.spillFillTimeUs);
    ASSERT_EQ(
        prevGStats.spillSerializationTimeUs + stats.spillSerializationTimeUs,
        newGStats.spillSerializationTimeUs);
    ASSERT_EQ(
        prevGStats.spillWrites + stats.spillWrites, newGStats.spillWrites);

    spiller_.reset();
    // Verify the spilled files are still there after spiller destruction.
    for (const auto& spilledFile : spilledFileSet) {
      ASSERT_NO_THROW(fs_->exists(spilledFile));
    }
  }

  // 'numDuplicates' specifies the number of duplicate rows generated for each
  // distinct sorting key in test.
  void setupSpillData(
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

    rowVector_ = BaseVector::create<RowVector>(rowType_, numRows, pool_.get());
    const auto& childTypes = containerType_->children();
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
      RowVectorPtr batch = makeDataset(rowType_, numRows, customizeData);
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

    for (auto column = 0; column < containerType_->size(); ++column) {
      DecodedVector decoded(*rowVector_->childAt(column), allRows);
      for (auto index = 0; index < numRows; ++index) {
        rowContainer_->store(decoded, index, rows_[index], column);
      }
    }

    if (spillProbedFlag_) {
      auto* probedFlagVector =
          rowVector_->childAt(containerType_->size())->asFlatVector<bool>();
      // The probed flag vector used by hash build spilling has no nulls so
      // clear them in test.
      probedFlagVector->clearAllNulls();
      for (auto index = 0; index < numRows; ++index) {
        if (probedFlagVector->valueAt(index)) {
          rowContainer_->setProbedFlag(&rows_[index], 1);
        }
      }
    }
  }

  void setupSpillContainer(const RowTypePtr& rowType, int32_t numKeys) {
    const auto& childTypes = rowType->children();
    std::vector<TypePtr> keys(childTypes.begin(), childTypes.begin() + numKeys);
    std::vector<TypePtr> dependents;
    if (numKeys < childTypes.size()) {
      dependents.insert(
          dependents.end(), childTypes.begin() + numKeys, childTypes.end());
    }
    rowContainer_ = makeRowContainer(keys, dependents, false);
    rowType_ = rowType;
  }

  void writeSpillData(const std::vector<RowVectorPtr>& batches) {
    vector_size_t numRows = 0;
    for (const auto& batch : batches) {
      numRows += batch->size();
    }
    if (rowVector_ == nullptr) {
      rowVector_ =
          BaseVector::create<RowVector>(rowType_, numRows, pool_.get());
    }
    rows_.resize(numRows);
    for (int i = 0; i < numRows; ++i) {
      rows_[i] = rowContainer_->newRow();
    }

    vector_size_t nextRow = 0;
    for (const auto& batch : batches) {
      rowVector_->append(batch.get());
      const SelectivityVector allRows(batch->size());
      for (int index = 0; index < batch->size(); ++index, ++nextRow) {
        for (int i = 0; i < rowType_->size(); ++i) {
          DecodedVector decodedVector(*batch->childAt(i), allRows);
          rowContainer_->store(decodedVector, index, rows_[nextRow], i);
        }
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
    // NOTE: for aggregation output type, we expect the merge read to produce
    // the output rows in the same order of the row insertion. So do need the
    // sort for testing.
    if (type_ == Spiller::Type::kAggregateOutput ||
        type_ == Spiller::Type::kOrderByOutput) {
      return;
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

  void setupSpiller(
      uint64_t targetFileSize,
      uint64_t writeBufferSize,
      bool makeError,
      uint64_t maxSpillRunRows = 0) {
    static const std::string kBadSpillDirPath = "/bad/path";
    common::GetSpillDirectoryPathCB badSpillDirCb =
        [&]() -> const std::string& { return kBadSpillDirPath; };
    common::GetSpillDirectoryPathCB tempSpillDirCb =
        [&]() -> const std::string& { return tempDirPath_->path; };
    stats_.clear();

    common::SpillConfig spillConfig;
    spillConfig.getSpillDirPathCb = makeError ? badSpillDirCb : tempSpillDirCb;
    spillConfig.updateAndCheckSpillLimitCb = [&](uint64_t) {};
    spillConfig.fileNamePrefix = "prefix";
    spillConfig.writeBufferSize = writeBufferSize;
    spillConfig.executor = executor();
    spillConfig.compressionKind = compressionKind_;
    spillConfig.maxSpillRunRows = maxSpillRunRows;
    spillConfig.maxFileSize = targetFileSize;
    spillConfig.fileCreateConfig = {};

    if (type_ == Spiller::Type::kHashJoinProbe) {
      // kHashJoinProbe doesn't have associated row container.
      spiller_ =
          std::make_unique<Spiller>(type_, rowType_, hashBits_, &spillConfig);
    } else if (
        type_ == Spiller::Type::kOrderByInput ||
        type_ == Spiller::Type::kAggregateInput) {
      // We spill 'data' in one partition in type of kOrderBy, otherwise in 4
      // partitions.
      spiller_ = std::make_unique<Spiller>(
          type_,
          rowContainer_.get(),
          rowType_,
          rowContainer_->keyTypes().size(),
          compareFlags_,
          &spillConfig);
    } else if (
        type_ == Spiller::Type::kAggregateOutput ||
        type_ == Spiller::Type::kOrderByOutput) {
      spiller_ = std::make_unique<Spiller>(
          type_, rowContainer_.get(), rowType_, &spillConfig);
    } else if (type_ == Spiller::Type::kRowNumber) {
      spiller_ = std::make_unique<Spiller>(
          type_, rowContainer_.get(), rowType_, hashBits_, &spillConfig);
    } else {
      VELOX_CHECK_EQ(type_, Spiller::Type::kHashJoinBuild);
      spiller_ = std::make_unique<Spiller>(
          type_,
          joinType_,
          rowContainer_.get(),
          rowType_,
          hashBits_,
          &spillConfig);
    }
    ASSERT_EQ(spiller_->state().maxPartitions(), numPartitions_);
    ASSERT_FALSE(spiller_->isAllSpilled());
    ASSERT_FALSE(spiller_->isAnySpilled());
    ASSERT_EQ(spiller_->hashBits(), hashBits_);
  }

  void runSpill(bool expectedError) {
    try {
      if (type_ != Spiller::Type::kAggregateOutput) {
        spiller_->spill();
      } else {
        RowContainerIterator iter;
        spiller_->spill(iter);
      }
      rowContainer_->clear();
      ASSERT_FALSE(expectedError);
    } catch (const std::exception& e) {
      ASSERT_TRUE(expectedError);
    }
  }

  void verifySortedSpillData(
      SpillPartition* spillPartition,
      int32_t outputBatchSize = 0) {
    ASSERT_EQ(numPartitions_, 1);
    ASSERT_TRUE(spiller_->isSpilled(0));

    // We make a merge reader that merges the spill files and the rows that
    // are still in the RowContainer.
    auto merge = spillPartition->createOrderedReader(pool());
    ASSERT_TRUE(merge != nullptr);
    ASSERT_TRUE(spillPartition->createOrderedReader(pool()) == nullptr);

    // We read the spilled data back and check that it matches the sorted
    // order of the partition.
    auto& indices = partitions_[0];
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

    const auto singlePartition =
        partitionFn.partition(*input, spillPartitions_);

    for (auto i = 0; i < input->size(); ++i) {
      const auto partition = singlePartition.has_value()
          ? singlePartition.value()
          : spillPartitions_[i];
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
      int targetFileSize,
      uint64_t maxSpillRunRows) {
    ASSERT_TRUE(
        type_ == Spiller::Type::kHashJoinBuild ||
        type_ == Spiller::Type::kHashJoinProbe ||
        type_ == Spiller::Type::kRowNumber);

    const int numSpillPartitions = type_ != Spiller::Type::kHashJoinProbe
        ? numPartitions_
        : 1 + folly::Random().rand32() % numPartitions_;
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
      const auto prevGStats = common::globalSpillStats();
      setupSpillData(
          numKeys_,
          type_ != Spiller::Type::kHashJoinProbe ? numBatchRows * 10 : 0,
          1,
          nullptr,
          {});
      setupSpiller(targetFileSize, 0, false, maxSpillRunRows);
      // Can't append without marking a partition as spilling.
      VELOX_ASSERT_THROW(spiller_->spill(0, rowVector_), "");

      splitByPartition(rowVector_, spillHashFunction, inputsByPartition);
      if (type_ == Spiller::Type::kHashJoinProbe) {
        spiller_->setPartitionsSpilled(spillPartitionNumSet);
#ifndef NDEBUG
        VELOX_ASSERT_THROW(
            spiller_->setPartitionsSpilled(spillPartitionNumSet), "");
#endif
      } else {
        VELOX_ASSERT_THROW(
            spiller_->setPartitionsSpilled(spillPartitionNumSet), "");
        spiller_->spill();
        rowContainer_->clear();
        ASSERT_TRUE(spiller_->isAllSpilled());
      }
      // Spill data.
      for (int i = 0; i < numAppendBatches; ++i) {
        RowVectorPtr batch = makeDataset(rowType_, numBatchRows, nullptr);
        splitByPartition(batch, spillHashFunction, inputsByPartition);
        for (const auto& partition : spillPartitionNumSet) {
          spiller_->spill(partition, inputsByPartition[partition].back());
        }
      }
      // Assert that hash probe type of spiller type doesn't support incremental
      // spilling.
      if (type_ == Spiller::Type::kHashJoinProbe) {
        VELOX_ASSERT_THROW(spiller_->spill(), "");
      } else {
        spiller_->spill();
        ASSERT_TRUE(spiller_->isAllSpilled());
      }

      const auto stats = spiller_->stats();
      ASSERT_GE(stats.spilledFiles, 0);
      if (type_ == Spiller::Type::kHashJoinProbe) {
        if (numAppendBatches == 0) {
          ASSERT_EQ(stats.spilledRows, 0);
          ASSERT_EQ(stats.spilledBytes, 0);
          ASSERT_EQ(stats.spillWriteTimeUs, 0);
          ASSERT_EQ(stats.spillFlushTimeUs, 0);
          ASSERT_EQ(stats.spillSerializationTimeUs, 0);
          ASSERT_EQ(stats.spillWrites, 0);
        } else {
          ASSERT_GT(stats.spilledRows, 0);
          ASSERT_GT(stats.spilledBytes, 0);
          ASSERT_GT(stats.spillWriteTimeUs, 0);
          ASSERT_GT(stats.spillFlushTimeUs, 0);
          ASSERT_GT(stats.spillSerializationTimeUs, 0);
          ASSERT_GT(stats.spillWrites, 0);
        }
      } else {
        ASSERT_GT(stats.spilledRows, 0);
        ASSERT_GT(stats.spilledBytes, 0);
        ASSERT_GT(stats.spillWriteTimeUs, 0);
        ASSERT_GT(stats.spillFlushTimeUs, 0);
        ASSERT_GT(stats.spillSerializationTimeUs, 0);
        ASSERT_GT(stats.spillWrites, 0);
      }
      ASSERT_GT(stats.spilledPartitions, 0);
      ASSERT_EQ(stats.spillSortTimeUs, 0);
      if (type_ == Spiller::Type::kHashJoinBuild ||
          type_ == Spiller::Type::kRowNumber) {
        ASSERT_GT(stats.spillFillTimeUs, 0);
      } else {
        ASSERT_EQ(stats.spillFillTimeUs, 0);
      }

      const auto newGStats = common::globalSpillStats();
      ASSERT_EQ(
          prevGStats.spilledFiles + stats.spilledFiles, newGStats.spilledFiles);
      ASSERT_EQ(
          prevGStats.spilledRows + stats.spilledRows, newGStats.spilledRows);
      ASSERT_EQ(
          prevGStats.spilledPartitions + stats.spilledPartitions,
          newGStats.spilledPartitions);
      ASSERT_EQ(
          prevGStats.spilledBytes + stats.spilledBytes, newGStats.spilledBytes);
      ASSERT_EQ(
          prevGStats.spillWriteTimeUs + stats.spillWriteTimeUs,
          newGStats.spillWriteTimeUs);
      ASSERT_EQ(
          prevGStats.spillSortTimeUs + stats.spillSortTimeUs,
          newGStats.spillSortTimeUs);
      ASSERT_EQ(
          prevGStats.spillFlushTimeUs + stats.spillFlushTimeUs,
          newGStats.spillFlushTimeUs)
          << prevGStats.spillFlushTimeUs << " " << stats.spillFlushTimeUs << " "
          << newGStats.spillFlushTimeUs;
      ASSERT_EQ(
          prevGStats.spillFillTimeUs + stats.spillFillTimeUs,
          newGStats.spillFillTimeUs);
      ASSERT_EQ(
          prevGStats.spillSerializationTimeUs + stats.spillSerializationTimeUs,
          newGStats.spillSerializationTimeUs);
      ASSERT_EQ(
          prevGStats.spillWrites + stats.spillWrites, newGStats.spillWrites);

      spillers.push_back(std::move(spiller_));
    }

    // Read back data from all the spilled partitions and verify.
    verifyNonSortedSpillData(
        std::move(spillers), spillPartitionNumSet, inputsByPartition);
    // Spilled file stats should be updated after finalizing spiller.
    ASSERT_GT(common::globalSpillStats().spilledFiles, 0);
  }

  void verifyNonSortedSpillData(
      std::vector<std::unique_ptr<Spiller>> spillers,
      const SpillPartitionNumSet& spillPartitionNumSet,
      const std::vector<std::vector<RowVectorPtr>>& inputsByPartition) {
    ASSERT_TRUE(
        type_ == Spiller::Type::kHashJoinBuild ||
        type_ == Spiller::Type::kRowNumber ||
        type_ == Spiller::Type::kHashJoinProbe);

    SpillPartitionSet spillPartitionSet;
    for (auto& spiller : spillers) {
      spiller->finishSpill(spillPartitionSet);
      VELOX_ASSERT_THROW(
          spiller->spill(0, nullptr), "Spiller has been finalized");
      VELOX_ASSERT_THROW(spiller->spill(), "Spiller has been finalized");
    }
    ASSERT_EQ(spillPartitionSet.size(), spillPartitionNumSet.size());

    for (auto& spillPartitionEntry : spillPartitionSet) {
      const int partition = spillPartitionEntry.first.partitionNumber();
      ASSERT_EQ(
          hashBits_.begin(), spillPartitionEntry.first.partitionBitOffset());
      auto reader = spillPartitionEntry.second->createUnorderedReader(pool());
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

  void verifyNonSortedSpillData(
      const SpillPartitionNumSet& spillPartitionNumSet,
      const std::vector<std::vector<RowVectorPtr>>& inputsByPartition) {
    ASSERT_TRUE(
        type_ == Spiller::Type::kHashJoinBuild ||
        type_ == Spiller::Type::kRowNumber ||
        type_ == Spiller::Type::kHashJoinProbe);

    if (numPartitions_ > 0) {
      VELOX_ASSERT_THROW(spiller_->finishSpill(), "");
    }
    SpillPartitionSet spillPartitionSet;
    spiller_->finishSpill(spillPartitionSet);
    VELOX_ASSERT_THROW(
        spiller_->spill(0, nullptr), "Spiller has been finalized");
    VELOX_ASSERT_THROW(spiller_->spill(), "Spiller has been finalized");
    VELOX_ASSERT_THROW(spiller_->spill(RowContainerIterator{}), "");
    ASSERT_EQ(spillPartitionSet.size(), spillPartitionNumSet.size());

    for (auto& spillPartitionEntry : spillPartitionSet) {
      const int partition = spillPartitionEntry.first.partitionNumber();
      ASSERT_EQ(
          hashBits_.begin(), spillPartitionEntry.first.partitionBitOffset());
      auto reader = spillPartitionEntry.second->createUnorderedReader(pool());
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
  const int32_t executorPoolSize_;
  const common::CompressionKind compressionKind_;
  const core::JoinType joinType_;
  const bool spillProbedFlag_;
  const HashBitRange hashBits_;
  const int32_t numPartitions_;
  std::unordered_map<std::string, RuntimeMetric> stats_;
  std::unique_ptr<TestRuntimeStatWriter> statWriter_;
  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<TempDirectoryPath> tempDirPath_;
  std::shared_ptr<FileSystem> fs_;
  RowTypePtr containerType_;
  RowTypePtr rowType_;
  int32_t numKeys_;
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

struct AllTypesTestParam {
  TestParam param;
  uint64_t maxSpillRunRows;
};

class AllTypes : public SpillerTest,
                 public testing::WithParamInterface<AllTypesTestParam> {
 public:
  AllTypes()
      : SpillerTest(GetParam().param),
        maxSpillRunRows_(GetParam().maxSpillRunRows) {}

  static std::vector<AllTypesTestParam> getTestParams() {
    auto testParams = TestParamsBuilder().getTestParams();

    std::vector<AllTypesTestParam> allTypesTestParams;
    for (const auto& testParam : testParams) {
      for (const auto& maxSpillRunRows :
           std::vector<uint64_t>{0, 101, 1'000'000}) {
        allTypesTestParams.push_back({testParam, maxSpillRunRows});
      }
    }

    return allTypesTestParams;
  }

 protected:
  uint64_t maxSpillRunRows_;
};

TEST_P(AllTypes, nonSortedSpillFunctions) {
  if (type_ == Spiller::Type::kOrderByInput ||
      type_ == Spiller::Type::kOrderByOutput ||
      type_ == Spiller::Type::kAggregateInput ||
      type_ == Spiller::Type::kAggregateOutput) {
    setupSpillData(numKeys_, 5'000, 1, nullptr, {});
    sortSpillData();
    setupSpiller(100'000, 0, false, maxSpillRunRows_);
    {
      RowVectorPtr dummyVector;
      VELOX_ASSERT_THROW(
          spiller_->spill(0, dummyVector), "Unexpected spiller type");
    }

    if (type_ == Spiller::Type::kOrderByOutput) {
      RowContainerIterator rowIter;
      std::vector<char*> rows(5'000);
      int numListedRows{0};
      numListedRows = rowContainer_->listRows(&rowIter, 5000, rows.data());
      ASSERT_EQ(numListedRows, 5000);
      spiller_->spill(rows);
    } else {
      spiller_->spill();
    }

    ASSERT_FALSE(spiller_->finalized());
    SpillPartitionSet spillPartitionSet;
    spiller_->finishSpill(spillPartitionSet);
    ASSERT_TRUE(spiller_->finalized());
    ASSERT_EQ(spillPartitionSet.size(), 1);
    verifySortedSpillData(spillPartitionSet.begin()->second.get());
    return;
  }
  testNonSortedSpill(2, 5'000, 3, 1, maxSpillRunRows_);
  testNonSortedSpill(2, 5'000, 3, 1'000'000'000, maxSpillRunRows_);
  // Empty case.
  testNonSortedSpill(1, 5'000, 0, 1, maxSpillRunRows_);
}

class NoHashJoin : public SpillerTest,
                   public testing::WithParamInterface<TestParam> {
 public:
  NoHashJoin() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kHashJoinProbe,
             Spiller::Type::kHashJoinBuild,
             Spiller::Type::kRowNumber,
             Spiller::Type::kOrderByOutput}}
        .getTestParams();
  }
};

TEST_P(NoHashJoin, spilFew) {
  // Test with distinct sort keys.
  testSortedSpill(10, 1);
  testSortedSpill(10, 1, false, false);
  testSortedSpill(10, 1, true);
  testSortedSpill(10, 1, true, false);
  // Test with duplicate sort keys.
  testSortedSpill(10, 10);
  testSortedSpill(10, 10, false, false);
  testSortedSpill(10, 10, true);
  testSortedSpill(10, 10, true, false);
}

TEST_P(NoHashJoin, spilMost) {
  // Test with distinct sort keys.
  testSortedSpill(60, 1);
  testSortedSpill(60, 1, false, false);
  testSortedSpill(60, 1, true);
  testSortedSpill(60, 1, true, false);
  // Test with duplicate sort keys.
  testSortedSpill(60, 10);
  testSortedSpill(60, 10, false, false);
  testSortedSpill(60, 10, true);
  testSortedSpill(60, 10, true, false);
}

TEST_P(NoHashJoin, spillAll) {
  // Test with distinct sort keys.
  testSortedSpill(100, 1);
  testSortedSpill(100, 1, false, false);
  testSortedSpill(100, 1, true);
  testSortedSpill(100, 1, true, false);
  // Test with duplicate sort keys.
  testSortedSpill(100, 10);
  testSortedSpill(100, 10, false, false);
  testSortedSpill(100, 10, true);
  testSortedSpill(100, 10, true, false);
}

TEST_P(NoHashJoin, error) {
  testSortedSpill(100, 1, false, true);
}

class HashJoinBuildOnly : public SpillerTest,
                          public testing::WithParamInterface<TestParam> {
 public:
  HashJoinBuildOnly() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kAggregateInput,
             Spiller::Type::kAggregateOutput,
             Spiller::Type::kHashJoinProbe,
             Spiller::Type::kOrderByInput,
             Spiller::Type::kOrderByOutput}}
        .getTestParams();
  }
};

TEST_P(HashJoinBuildOnly, spillPartition) {
  setupSpillData(numKeys_, 1'000, 1, nullptr, {});
  std::vector<std::vector<RowVectorPtr>> vectorsByPartition(numPartitions_);
  HashPartitionFunction spillHashFunction(hashBits_, rowType_, keyChannels_);
  splitByPartition(rowVector_, spillHashFunction, vectorsByPartition);
  setupSpiller(100'000, 0, false);
  spiller_->spill();
  rowContainer_->clear();
  spiller_->spill();
  verifyNonSortedSpillData(allPartitionNumSet(), vectorsByPartition);
  VELOX_ASSERT_THROW(spiller_->spill(), "Spiller has been finalized");
  VELOX_ASSERT_THROW(spiller_->spill(RowContainerIterator{}), "");
}

TEST_P(HashJoinBuildOnly, writeBufferSize) {
  std::vector<uint64_t> writeBufferSizes = {0, 4'000'000'000};
  for (const auto writeBufferSize : writeBufferSizes) {
    SCOPED_TRACE(
        fmt::format("writeBufferSize {}", succinctBytes(writeBufferSize)));
    setupSpillData(numKeys_, 1'000, 1, nullptr, {});
    setupSpiller(4'000'000'000, writeBufferSize, false);
    spiller_->spill();
    ASSERT_TRUE(spiller_->isAllSpilled());
    const int numDiskWrites = spiller_->stats().spillWrites;
    if (writeBufferSize != 0) {
      ASSERT_EQ(numDiskWrites, 0);
    }

    HashPartitionFunction spillHashFunction(hashBits_, rowType_, keyChannels_);

    VectorFuzzer::Options options;
    options.vectorSize = 100;
    VectorFuzzer fuzzer(options, pool_.get());

    // Tracks the partition has split spill vector input.
    int spillInputVectorCount{0};
    const int numBatches = 20;
    for (int i = 0; i < numBatches; ++i) {
      const auto inputVector = fuzzer.fuzzRow(rowType_);
      std::vector<std::vector<RowVectorPtr>> splitVectors(numPartitions_);
      splitByPartition(inputVector, spillHashFunction, splitVectors);
      for (int partition = 0; partition < numPartitions_; ++partition) {
        const auto& splitVector = splitVectors[partition];
        if (!splitVector.empty()) {
          spiller_->spill(partition, splitVector.back());
          ++spillInputVectorCount;
        }
      }
      // Accumulate all the spilled vectors together.
      rowVector_->append(inputVector.get());
    }
    const int numNonEmptySpilledPartitions =
        spiller_->state().testingNonEmptySpilledPartitionSet().size();

    std::vector<std::vector<RowVectorPtr>> expectedVectorByPartition(
        numPartitions_);
    splitByPartition(rowVector_, spillHashFunction, expectedVectorByPartition);
    std::vector<uint32_t> spillPartitionNums(numPartitions_);
    std::iota(spillPartitionNums.begin(), spillPartitionNums.end(), 0);
    SpillPartitionNumSet spillPartitionSet(
        spillPartitionNums.begin(), spillPartitionNums.end());
    verifyNonSortedSpillData(spillPartitionSet, expectedVectorByPartition);

    const auto stats = spiller_->stats();
    if (writeBufferSize > 0) {
      // With disk write buffering, all the input merged into one disk write per
      // partition.
      ASSERT_EQ(stats.spillWrites, numNonEmptySpilledPartitions);
    } else {
      // By disable write buffering, then each spill input causes a disk write.
      ASSERT_EQ(stats.spillWrites, numDiskWrites + spillInputVectorCount);
    }
  }
}

class AggregationOutputOnly : public SpillerTest,
                              public testing::WithParamInterface<TestParam> {
 public:
  AggregationOutputOnly() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kAggregateInput,
             Spiller::Type::kHashJoinBuild,
             Spiller::Type::kRowNumber,
             Spiller::Type::kHashJoinProbe,
             Spiller::Type::kOrderByInput,
             Spiller::Type::kOrderByOutput}}
        .getTestParams();
  }
};

TEST_P(AggregationOutputOnly, basic) {
  const int numRows = 5'000;
  struct {
    int spillRowOffset;
    uint64_t maxSpillRunRows;
    std::string debugString() const {
      return fmt::format(
          "spillRowOffset {}, maxSpillRunRows {}",
          spillRowOffset,
          maxSpillRunRows);
    }
  } testSettings[] = {
      {0, 101},
      {1, 101},
      {5'000 / 20, 101},
      {5'000 - 1, 101},
      {5'000 + 1, 101},
      {5'000 * 2, 101},
      {0, 100'000},
      {1, 100'000},
      {5'000 / 20, 0},
      {5'000 - 1, 0},
      {5'000 + 1, 0},
      {5'000 * 2, 0}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    setupSpillData(numKeys_, numRows, 0);
    sortSpillData();
    // NOTE: target file size is ignored by aggregation output spiller type.
    setupSpiller(0, 1'000'000, false, testData.maxSpillRunRows);
    RowContainerIterator rowIter;
    std::vector<char*> rows(numRows);
    int numListedRows{0};
    if (testData.spillRowOffset != 0) {
      numListedRows = rowContainer_->listRows(
          &rowIter, testData.spillRowOffset, rows.data());
    }
    ASSERT_EQ(numListedRows, std::min(numRows, testData.spillRowOffset));
    {
      RowVectorPtr dummy;
      VELOX_ASSERT_THROW(
          spiller_->spill(0, dummy),
          "Unexpected spiller type: AGGREGATE_OUTPUT");
    }
    spiller_->spill(rowIter);
    ASSERT_EQ(rowContainer_->numRows(), numRows);
    rowContainer_->clear();

    auto spillPartition = spiller_->finishSpill();
    ASSERT_TRUE(spiller_->finalized());

    const int expectedNumSpilledRows = numRows - numListedRows;
    auto merge = spillPartition.createOrderedReader(pool());
    if (expectedNumSpilledRows == 0) {
      ASSERT_TRUE(merge == nullptr);
    } else {
      for (auto i = 0; i < expectedNumSpilledRows; ++i) {
        auto* stream = merge->next();
        ASSERT_TRUE(stream != nullptr);
        ASSERT_TRUE(rowVector_->equalValueAt(
            &stream->current(),
            partitions_[0][numListedRows + i],
            stream->currentIndex()));
        stream->pop();
      }
    }

    const auto stats = spiller_->stats();
    if (expectedNumSpilledRows == 0) {
      ASSERT_EQ(stats.spilledFiles, 0) << stats.toString();
      ASSERT_EQ(stats.spilledRows, 0) << stats.toString();
    } else {
      ASSERT_EQ(stats.spilledFiles, 1) << stats.toString();
      ASSERT_EQ(stats.spilledRows, expectedNumSpilledRows) << stats.toString();
    }
    ASSERT_EQ(stats.spillSortTimeUs, 0);
  }
}

class OrderByOutputOnly : public SpillerTest,
                          public testing::WithParamInterface<TestParam> {
 public:
  OrderByOutputOnly() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kAggregateInput,
             Spiller::Type::kAggregateOutput,
             Spiller::Type::kHashJoinBuild,
             Spiller::Type::kHashJoinProbe,
             Spiller::Type::kRowNumber,
             Spiller::Type::kOrderByInput}}
        .getTestParams();
  }
};

TEST_P(OrderByOutputOnly, basic) {
  const int numRows = 5'000;
  struct {
    int numSpillRows;

    std::string debugString() const {
      return fmt::format("numSpillRows {}", numSpillRows);
    }
  } testSettings[] = {{0}, {1000}, {5000}, {5000 - 1}, {5000 + 1}, {50000 * 2}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    setupSpillData(numKeys_, numRows, 0);
    sortSpillData();
    // NOTE: target file size is ignored by aggregation output spiller type.
    setupSpiller(0, 1'000'000, false);
    RowContainerIterator rowIter;
    std::vector<char*> rows(numRows);
    int numListedRows{0};
    numListedRows =
        rowContainer_->listRows(&rowIter, testData.numSpillRows, rows.data());
    ASSERT_LE(numListedRows, numRows);
    {
      RowVectorPtr dummy;
      VELOX_ASSERT_THROW(
          spiller_->spill(0, dummy),
          "Unexpected spiller type: ORDER_BY_OUTPUT");
    }
    {
      std::vector<char*> emptyRows;
      VELOX_ASSERT_THROW(spiller_->spill(emptyRows), "");
    }
    auto spillRows =
        std::vector<char*>(rows.begin(), rows.begin() + numListedRows);
    spiller_->spill(spillRows);
    ASSERT_EQ(rowContainer_->numRows(), numRows);
    rowContainer_->clear();

    rowContainer_->clear();
    auto spillPartition = spiller_->finishSpill();
    ASSERT_TRUE(spiller_->finalized());

    const int expectedNumSpilledRows = numListedRows;
    auto merge = spillPartition.createOrderedReader(pool());
    if (expectedNumSpilledRows == 0) {
      ASSERT_TRUE(merge == nullptr);
    } else {
      for (auto i = 0; i < expectedNumSpilledRows; ++i) {
        auto* stream = merge->next();
        ASSERT_TRUE(stream != nullptr);
        ASSERT_TRUE(rowVector_->equalValueAt(
            &stream->current(), partitions_[0][i], stream->currentIndex()));
        stream->pop();
      }
    }

    const auto stats = spiller_->stats();
    if (expectedNumSpilledRows == 0) {
      ASSERT_EQ(stats.spilledFiles, 0) << stats.toString();
      ASSERT_EQ(stats.spilledRows, 0) << stats.toString();
    } else {
      ASSERT_EQ(stats.spilledFiles, 1) << stats.toString();
      ASSERT_EQ(stats.spilledRows, expectedNumSpilledRows) << stats.toString();
    }
    ASSERT_EQ(stats.spillSortTimeUs, 0);
  }
}

class MaxSpillRunTest : public SpillerTest,
                        public testing::WithParamInterface<TestParam> {
 public:
  MaxSpillRunTest() : SpillerTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return TestParamsBuilder{
        .typesToExclude =
            {Spiller::Type::kHashJoinProbe, Spiller::Type::kOrderByOutput}}
        .getTestParams();
  }
};

TEST_P(MaxSpillRunTest, basic) {
  struct {
    uint64_t maxSpillRunRows;
    uint8_t expectedNumFiles;
    std::string debugString() const {
      return fmt::format(
          "maxSpillRunRows {}, expectedNumFiles {}",
          maxSpillRunRows,
          expectedNumFiles);
    }
  } testSettings[] = {{0, 1}, {101, 3}, {4095, 3}, {4096, 3}, {4097, 2}};

  auto numRows = 10'000;
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupSpillData(numKeys_, numRows, 1, nullptr, {});
    sortSpillData();
    setupSpiller(
        std::numeric_limits<uint64_t>::max(),
        0,
        false,
        testData.maxSpillRunRows);
    if (type_ == Spiller::Type::kOrderByOutput) {
      RowContainerIterator rowIter;
      std::vector<char*> rows(numRows);
      int numListedRows{0};
      numListedRows = rowContainer_->listRows(&rowIter, numRows, rows.data());
      ASSERT_EQ(numListedRows, numRows);
      spiller_->spill(rows);
    } else {
      spiller_->spill();
    }
    ASSERT_FALSE(spiller_->finalized());
    SpillPartitionSet spillPartitionSet;
    spiller_->finishSpill(spillPartitionSet);
    ASSERT_TRUE(spiller_->finalized());

    auto numFiles{0};
    auto totalSize{0};
    std::vector<std::string_view> spilledFiles;
    for (const auto& [_, sp] : spillPartitionSet) {
      numFiles += sp->numFiles();
      totalSize += sp->size();
    }

    const auto& stats = spiller_->stats();
    ASSERT_EQ(totalSize, stats.spilledBytes);
    if (type_ == Spiller::Type::kAggregateOutput ||
        type_ == Spiller::Type::kOrderByOutput) {
      ASSERT_EQ(numFiles, 1);
      ASSERT_EQ(spillPartitionSet.size(), 1);
    } else if (
        type_ == Spiller::Type::kAggregateInput ||
        type_ == Spiller::Type::kOrderByInput) {
      // Need sort.
      ASSERT_EQ(numFiles, testData.expectedNumFiles);
      ASSERT_EQ(spillPartitionSet.size(), 1);
    } else {
      ASSERT_EQ(numFiles, hashBits_.numPartitions());
      ASSERT_EQ(spillPartitionSet.size(), numFiles);
    }
  }
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
    HashJoinBuildOnly,
    testing::ValuesIn(HashJoinBuildOnly::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    AggregationOutputOnly,
    testing::ValuesIn(AggregationOutputOnly::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    OrderByOutputOnly,
    testing::ValuesIn(OrderByOutputOnly::getTestParams()));

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillerTest,
    MaxSpillRunTest,
    testing::ValuesIn(MaxSpillRunTest::getTestParams()));
