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

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <memory>

#include "velox/common/time/Timer.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/vector/FlatVector.h"

#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/FilterGenerator.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

namespace facebook::velox::dwio::dwrf {

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

  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
  }

  static bool typeKindSupportsValueHook(TypeKind kind) {
    return kind != TypeKind::TIMESTAMP && kind != TypeKind::ARRAY &&
        kind != TypeKind::ROW && kind != TypeKind::MAP;
  }

  void makeRowType(const std::string& columns, bool wrapInStruct);

  void makeDataset(
      std::function<void()> customizeData,
      bool forRowGroupSkip = false);

  // Adds high values to 'batches_' so that these values occur only in some row
  // groups. Tests skipping row groups based on row group stats.
  void addRowGroupSpecificData();

  // Adds 'marker' to random places in selectable  row groups for 'i'th child in
  // 'batches' If 'marker' occurs in skippable row groups, sets the element to
  // T(). Row group numbers that are multiples of 3 are skippable.
  template <typename T>
  void setRowGroupMarkers(
      const std::vector<RowVectorPtr>& batches,
      int32_t child,
      T marker) {
    int32_t row = 0;
    for (auto& batch : batches) {
      auto values = batch->childAt(child)->as<FlatVector<T>>();
      for (auto i = 0; i < values->size(); ++i) {
        auto rowGroup = row++ / kRowsInGroup;
        bool isIn = (rowGroup % 3) != 0;
        if (isIn) {
          if (folly::Random::rand32(filterGenerator->rng()) % 100 == 0) {
            values->set(i, marker);
          }
        } else {
          if (!values->isNullAt(i) && values->valueAt(i) == marker) {
            values->set(i, T());
          }
        }
      }
    }
  }

  void makeAllNulls(const std::string& name);

  template <typename T>
  void makeIntDistribution(
      const common::Subfield& field,
      int64_t min,
      int64_t max,
      int32_t repeats,
      int32_t rareFrequency,
      int64_t rareMin,
      int64_t rareMax,
      bool keepNulls) {
    int counter = 0;
    for (RowVectorPtr batch : batches_) {
      auto numbers =
          common::getChildBySubfield(batch.get(), field)->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (keepNulls && numbers->isNullAt(row)) {
          continue;
        }
        int64_t value;
        if (counter % 100 < repeats) {
          value = counter % repeats;
          numbers->set(row, value);
        } else if (counter % 100 > 90 && row > 0) {
          numbers->copy(numbers, row - 1, row, 1);
        } else {
          if (rareFrequency && counter % rareFrequency == 0) {
            value = rareMin +
                (folly::Random::rand32(filterGenerator->rng()) %
                 (rareMax - rareMin));
          } else {
            value = min +
                (folly::Random::rand32(filterGenerator->rng()) % (max - min));
          }
          numbers->set(row, value);
        }
        ++counter;
      }
    }
  }

  // Makes strings with an ascending sequence of S<n>, followed by
  // random values with the given cardinality, then repeated
  // values. This is intended to hit different RLE encodings,
  // e.g. repeat, repeat with delta and random values within a
  // range. These patterns repeat every 100 values so as to trigger
  // dictionary encoding.
  void makeStringDistribution(
      const common::Subfield& field,
      int cardinality,
      bool keepNulls,
      bool addOneOffs);

  // Makes non-null strings unique by appending a row number.
  void makeStringUnique(const common::Subfield& field);

  // Makes all data in 'batches_' non-null. This finds a sampling of
  // non-null values from each column and replaces nulls in the column
  // in question with one of these. A column where only nulls are
  // found in sampling is not changed.
  void makeNotNull(int32_t firstRow = 0);

  void makeNotNullRecursive(int32_t firstRow, RowVectorPtr batch);

  virtual void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip) = 0;

  virtual std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::InputStream> input) = 0;

  void readWithoutFilter(
      std::shared_ptr<common::ScanSpec> spec,
      const std::vector<RowVectorPtr>& batches,
      uint64_t& time);

  void readWithFilter(
      std::shared_ptr<common::ScanSpec> spec,
      const std::vector<RowVectorPtr>& batches,
      const std::vector<uint32_t>& hitRows,
      uint64_t& time,
      bool useValueHook,
      bool skipCheck = false);

  template <TypeKind Kind>
  bool checkLoadWithHook(
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint32_t>& hitRows,
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
        BaseVector::create(child->type(), batch->size(), pool_.get()));
    TestingHook<T> hook(result.get());
    child->as<LazyVector>()->load(rows, &hook);
    for (auto i = 0; i < rows.size(); ++i) {
      auto row = rows[i] + rowIndex;
      auto reference = batches_[common::batchNumber(hitRows[row])]
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
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint32_t>& hitRows,
      int32_t rowIndex);

  void testFilterSpecs(const std::vector<common::FilterSpec>& filterSpecs);

  void testRowGroupSkip(const std::vector<std::string>& filterable);

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool tryNoNulls = false,
      bool tryNoVInts = false);

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

  std::unique_ptr<common::FilterGenerator> filterGenerator;
  std::unique_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<const RowType> rowType_;
  dwio::common::MemorySink* sinkPtr_;
  std::vector<RowVectorPtr> batches_;
  bool useVInts_ = true;
  dwio::common::RuntimeStatistics runtimeStats_;
  // Number of calls to flush policy between starting new stripes.
  int32_t flushEveryNBatches_{10};
  int32_t nextReadSizeIndex_{0};
  std::vector<int32_t> readSizes_;
};

} // namespace facebook::velox::dwio::dwrf
