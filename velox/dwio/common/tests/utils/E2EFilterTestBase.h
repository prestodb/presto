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

#pragma once

#include "velox/common/time/Timer.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/dwio/common/tests/utils/FilterGenerator.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"

#include <gtest/gtest.h>

namespace facebook::velox::dwio::common {

template <typename T>
class TestingHook : public ValueHook {
 public:
  explicit TestingHook(FlatVector<T>* result) : result_(result) {}

  void addValue(vector_size_t row, const void* value) override {
    result_->set(row, *reinterpret_cast<const T*>(value));
  }

 private:
  FlatVector<T>* result_;
};

template <>
inline void TestingHook<StringView>::addValue(
    vector_size_t row,
    const void* value) {
  result_->set(
      row, StringView(*reinterpret_cast<const folly::StringPiece*>(value)));
}

// Utility for checking that a subsequent batch of output does not
// overwrite internals of a possibly retained previous batch.
class OwnershipChecker {
 public:
  // Receives consecutive batches during a test and checks that
  // subsequent ones do not overwrite retained content from previous
  // ones.
  void check(const VectorPtr& batch);

 private:
  // Sequence number of batch for check().
  int32_t batchCounter_{0};
  // References the previous batch of results for check().
  VectorPtr previousBatch_;
  // Copy of 'previousBatch_' used for validating no overwrite from fetching the
  // next batch.
  VectorPtr previousBatchCopy_;
};

class E2EFilterTestBase : public testing::Test {
 protected:
  static constexpr int32_t kRowsInGroup = 10'000;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  static bool useRandomSeed() {
    // Check environment variable because `buck test` does not allow pass in
    // command line arguments.
    const char* env = getenv("VELOX_TEST_USE_RANDOM_SEED");
    return !env ? false : folly::to<bool>(env);
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("E2EFilterTestBase");
    leafPool_ = rootPool_->addLeafChild("E2EFilterTestBase");
    if (useRandomSeed()) {
      seed_ = folly::Random::secureRand32();
      LOG(INFO) << "Random seed: " << seed_;
    }
  }

  static bool typeKindSupportsValueHook(TypeKind kind) {
    return kind != TypeKind::TIMESTAMP && kind != TypeKind::ARRAY &&
        kind != TypeKind::ROW && kind != TypeKind::MAP;
  }

  std::vector<RowVectorPtr> makeDataset(
      std::function<void()> customize,
      bool forRowGroupSkip,
      bool withRecursiveNulls);

  void makeAllNulls(const std::string& fieldName);

  template <typename T>
  void useSuppliedValues(
      const std::string& fieldName,
      int32_t batchIndex,
      const std::vector<T>& values) {
    dataSetBuilder_->withSuppliedValuesForField(
        Subfield(fieldName), batchIndex, values);
  }

  template <typename T>
  void makeIntDistribution(
      const std::string& fieldName,
      int64_t min,
      int64_t max,
      int32_t repeats,
      int32_t rareFrequency,
      int64_t rareMin,
      int64_t rareMax,
      bool keepNulls) {
    dataSetBuilder_->withIntDistributionForField<T>(
        Subfield(fieldName),
        min,
        max,
        repeats,
        rareFrequency,
        rareMin,
        rareMax,
        keepNulls);
  }

  template <typename T>
  void makeIntRle(const std::string& fieldName) {
    dataSetBuilder_->withIntRleForField<T>(Subfield(fieldName));
  }

  template <typename T>
  void makeIntMainlyConstant(const std::string& fieldName) {
    dataSetBuilder_->withIntMainlyConstantForField<T>(Subfield(fieldName));
  }

  template <typename T>
  void makeQuantizedFloat(
      const std::string& fieldName,
      int64_t buckets,
      bool keepNulls) {
    dataSetBuilder_->withQuantizedFloatForField<T>(
        Subfield(fieldName), buckets, keepNulls);
  }

  // Makes strings with an ascending sequence of S<n>, followed by
  // random values with the given cardinality, then repeated
  // values. This is intended to hit different RLE encodings,
  // e.g. repeat, repeat with delta and random values within a
  // range. These patterns repeat every 100 values so as to trigger
  // dictionary encoding.
  void makeStringDistribution(
      const std::string& fieldName,
      int cardinality,
      bool keepNulls,
      bool addOneOffs);

  // Makes non-null strings unique by appending a row number.
  void makeStringUnique(const std::string& fieldName);

  // Makes all data in 'batches_' non-null. This finds a sampling of
  // non-null values from each column and replaces nulls in the column
  // in question with one of these. A column where only nulls are
  // found in sampling is not changed.
  void makeNotNull(int32_t firstRow = 0);

  void makeNotNullRecursive(int32_t firstRow, RowVectorPtr batch);

  template <typename T>
  void makeReapeatingValues(
      const std::string& fieldName,
      int32_t batchIndex,
      int32_t firstRow,
      int32_t lastRow,
      T value) {
    dataSetBuilder_->withReapeatingValuesForField<T>(
        Subfield(fieldName), batchIndex, firstRow, lastRow, value);
  }

  virtual void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip) = 0;

  virtual std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) = 0;

  virtual void setUpRowReaderOptions(
      dwio::common::RowReaderOptions& opts,
      const std::shared_ptr<ScanSpec>& spec) {
    opts.setScanSpec(spec);
    opts.setTimestampPrecision(TimestampPrecision::kNanoseconds);
  }

  void readWithoutFilter(
      std::shared_ptr<common::ScanSpec> spec,
      const std::vector<RowVectorPtr>& batches,
      uint64_t& time);

  void readWithFilter(
      std::shared_ptr<common::ScanSpec> spec,
      const MutationSpec&,
      const std::vector<RowVectorPtr>& batches,
      const std::vector<uint64_t>& hitRows,
      uint64_t& time,
      bool useValueHook,
      bool skipCheck = false);

  template <TypeKind Kind>
  bool checkLoadWithHook(
      const std::vector<RowVectorPtr>& batches,
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint64_t>& hitRows,
      int32_t rowIndex) {
    using T = typename TypeTraits<Kind>::NativeType;
    std::vector<vector_size_t> rows;
    // The 5 first values are densely read.
    for (int32_t i = 0; i < 5 && i < batch->size(); ++i) {
      rows.push_back(i);
    }
    for (int32_t i = 5; i < 5 && i < batch->size(); i += 2) {
      rows.push_back(i);
    }
    auto result = std::static_pointer_cast<FlatVector<T>>(
        BaseVector::create(child->type(), batch->size(), leafPool_.get()));
    TestingHook<T> hook(result.get());
    child->as<LazyVector>()->load(rows, &hook);
    for (auto i = 0; i < rows.size(); ++i) {
      auto row = rows[i] + rowIndex;
      auto reference = batches[common::batchNumber(hitRows[row])]
                           ->childAt(columnIndex)
                           ->as<FlatVector<T>>();
      auto referenceIndex = common::batchRow(hitRows[row]);
      if (reference->isNullAt(referenceIndex)) {
        continue; // The hook is not called on nulls.
      }
      if (reference->valueAt(referenceIndex) != result->valueAt(i)) {
        return false;
      }
    }
    return true;
  }

  bool loadWithHook(
      const std::vector<RowVectorPtr>& batches,
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint64_t>& hitRows,
      int32_t rowIndex);

  void testFilterSpecs(
      const std::vector<RowVectorPtr>& batches,
      const std::vector<FilterSpec>& filterSpecs);

  void testNoRowGroupSkip(
      const std::vector<RowVectorPtr>& batches,
      const std::vector<std::string>& filterable,
      int32_t numCombinations);

  void testRowGroupSkip(
      const std::vector<RowVectorPtr>& batches,
      const std::vector<std::string>& filterable);

 private:
  void testReadWithFilterLazy(
      const std::shared_ptr<common::ScanSpec>& spec,
      const MutationSpec&,
      const std::vector<RowVectorPtr>& batches,
      const std::vector<uint64_t>& hitRows);

  void testPruningWithFilter(
      std::vector<RowVectorPtr>& batches,
      const std::vector<std::string>& filterable);

 protected:
  void testScenario(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool withRecursiveNulls = true);

 private:
  void testMetadataFilterImpl(
      const std::vector<RowVectorPtr>& batches,
      common::Subfield filterField,
      std::unique_ptr<common::Filter> filter,
      core::ExpressionEvaluator*,
      const std::string& remainingFilter,
      std::function<bool(int64_t a, int64_t c)> validationFilter);

 protected:
  void testMetadataFilter();

  void testSubfieldsPruning();

  void testMutationCornerCases();

  // Allows testing reading with different batch sizes.
  void resetReadBatchSizes() {
    nextReadSizeIndex_ = 0;
  }

  int32_t nextReadBatchSize() {
    if (nextReadSizeIndex_ >= readSizes_.size()) {
      return 1000;
    }
    return readSizes_[nextReadSizeIndex_++];
  }

  const size_t kBatchCount = 4;
  // kBatchSize must be greater than 10000 for RowGroup skipping test
  const size_t kBatchSize = 25'000;

  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::unique_ptr<common::FilterGenerator> filterGenerator_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<const RowType> rowType_;
  std::string sinkData_;
  bool useVInts_ = true;
  dwio::common::RuntimeStatistics runtimeStats_;
  // Number of calls to flush policy between starting new stripes.
  int32_t flushEveryNBatches_{10};
  int32_t nextReadSizeIndex_{0};
  std::vector<int32_t> readSizes_;
  int32_t batchCount_ = kBatchCount;
  int32_t batchSize_ = kBatchSize;
  bool testRowGroupSkip_ = true;
  uint32_t seed_ = 1;
};

} // namespace facebook::velox::dwio::common
