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

#include "velox/dwio/common/tests/utils/FilterGenerator.h"
#include "velox/type/Subfield.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {

class DataSetBuilder {
 public:
  explicit DataSetBuilder(
      memory::MemoryPool& pool,
      folly::Random::DefaultGenerator::result_type seed =
          folly::Random::DefaultGenerator::default_seed)
      : pool_(pool), rng_(seed), batches_(nullptr) {}

  static RowTypePtr makeRowType(const std::string& columns, bool wrapInStruct);

  static RowTypePtr makeRowType(std::vector<TypePtr>&& types) {
    return velox::test::VectorMaker::rowType(
        std::forward<std::vector<TypePtr>&&>(types));
  }

  // Create batchCount number of batches, each with numRows number of rows. The
  // data was randomly created.
  DataSetBuilder& makeDataset(
      RowTypePtr rowType,
      const size_t batchCount,
      const size_t numRows,
      const bool withRecursiveNulls = true);

  // Adds high values to 'batches_' so that these values occur only in some row
  // groups. Tests skipping row groups based on row group stats.
  DataSetBuilder& withRowGroupSpecificData(int32_t numRowsPerGroup);

  // Makes all data in 'batches_' after firstRow non-null. This finds a sampling
  // of non-null values from each column and replaces nulls in the column in
  // question with one of these. A column where only nulls are found in sampling
  // is not changed.
  DataSetBuilder& withNoNullsAfter(int32_t firstRow = 0);

  // Make all rows for the specific Subfield field null
  DataSetBuilder& withAllNullsForField(const common::Subfield& field);

  // Make the data for the specific Subfield field with nulls at
  // nullsPercentX100 %
  DataSetBuilder& withNullsForField(
      const common::Subfield& field,
      uint8_t nullsPercentX100);

  DataSetBuilder& withStringDistributionForField(
      const common::Subfield& field,
      int cardinality,
      bool keepNulls,
      bool addOneOffs);

  // Makes non-null strings unique by appending a row number for the input
  // Subfield.
  DataSetBuilder& withUniqueStringsForField(
      const facebook::velox::common::Subfield& field);

  template <typename T>
  DataSetBuilder& withIntDistributionForField(
      const common::Subfield& field,
      int64_t min,
      int64_t max,
      int32_t repeats,
      int32_t rareFrequency,
      int64_t rareMin,
      int64_t rareMax,
      bool keepNulls) {
    int counter = 0;
    auto vec = *batches_;
    for (RowVectorPtr batch : vec) {
      auto numbers = dwio::common::getChildBySubfield(batch.get(), field)
                         ->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (keepNulls && numbers->isNullAt(row)) {
          continue;
        }
        if (counter % 100 < repeats) {
          numbers->set(row, T(counter % repeats));
        } else if (counter % 100 > 90 && row > 0) {
          numbers->copy(numbers, row - 1, row, 1);
        } else {
          int64_t value;
          if (rareFrequency && counter % rareFrequency == 0) {
            value =
                rareMin + (folly::Random::rand32(rng_) % (rareMax - rareMin));
          } else {
            value = min + (folly::Random::rand32(rng_) % (max - min));
          }
          numbers->set(row, T(value));
        }
        ++counter;
      }
    }

    return *this;
  }

  template <typename T>
  DataSetBuilder& withIntRleForField(const common::Subfield& field) {
    constexpr int kMinRun = 5;
    constexpr int kMaxRun = 101;
    int remaining = 0;
    T value;
    auto vec = *batches_;
    for (auto& batch : vec) {
      auto numbers = dwio::common::getChildBySubfield(batch.get(), field)
                         ->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (numbers->isNullAt(row)) {
          continue;
        }
        if (remaining == 0) {
          value = numbers->valueAt(row);
          remaining =
              kMinRun + folly::Random::rand32(rng_) % (kMaxRun - kMinRun);
        }
        numbers->set(row, value);
        --remaining;
      }
    }
    return *this;
  }

  template <typename T>
  DataSetBuilder& withIntMainlyConstantForField(const common::Subfield& field) {
    for (auto& batch : *batches_) {
      std::optional<T> value;
      auto* numbers = dwio::common::getChildBySubfield(batch.get(), field)
                          ->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (numbers->isNullAt(row)) {
          continue;
        }
        if (folly::Random::randDouble01(rng_) < 0.95) {
          if (!value.has_value()) {
            value = numbers->valueAt(row);
          } else {
            numbers->set(row, *value);
          }
        }
      }
    }
    return *this;
  }

  template <typename T>
  DataSetBuilder& withQuantizedFloatForField(
      const common::Subfield& field,
      int64_t buckets,
      bool keepNulls) {
    for (RowVectorPtr batch : *batches_) {
      auto numbers = dwio::common::getChildBySubfield(batch.get(), field)
                         ->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (keepNulls && numbers->isNullAt(row)) {
          continue;
        }
        T value = numbers->valueAt(row);
        numbers->set(row, ceil(value * buckets) / buckets);
      }
    }

    return *this;
  }

  template <typename T>
  DataSetBuilder& withReapeatingValuesForField(
      const common::Subfield& field,
      int32_t batchIndex,
      int32_t firstRow,
      int32_t lastRow,
      T value) {
    VELOX_CHECK_LT(batchIndex, batches_->size());

    auto batch = (*batches_)[batchIndex];
    auto fieldValues = dwio::common::getChildBySubfield(batch.get(), field)
                           ->as<FlatVector<T>>();

    auto numRows = batch->size();
    VELOX_CHECK_LT(firstRow, numRows);
    VELOX_CHECK_LT(firstRow, lastRow);
    VELOX_CHECK_LE(lastRow, numRows);

    for (auto row = firstRow; row < lastRow; ++row) {
      fieldValues->set(row, value);
    }

    return *this;
  }

  template <typename T>
  void withSuppliedValuesForField(
      const common::Subfield& field,
      int32_t batchIndex,
      const std::vector<T>& values) {
    VELOX_CHECK_LT(batchIndex, batches_->size());

    auto numbers =
        dwio::common::getChildBySubfield((*batches_)[batchIndex].get(), field)
            ->as<FlatVector<T>>();
    numbers->resize(values.size());
    (*batches_)[batchIndex]->resize(values.size());

    for (auto row = 0; row < values.size(); ++row) {
      numbers->set(row, values[row]);
    }
  }

  DataSetBuilder& makeUniformMapKeys(const common::Subfield& field);

  // Ensures that there are non-inlined various string sizes in map keys/values
  // if either key or value is a string.
  DataSetBuilder& makeMapStringValues(const common::Subfield& field);

  std::unique_ptr<std::vector<RowVectorPtr>> build();

 private:
  // Adds 'marker' to random places in selectable  row groups for 'i'th
  // columnIndex in 'batches' If 'marker' occurs in skippable row groups, sets
  // the element to T(). Row group numbers that are multiples of 3 are
  // skippable.
  template <typename T>
  void
  setRowGroupMarkers(int32_t columnIndex, int32_t numRowsInGroup, T marker) {
    int32_t row = 0;
    for (auto& batch : *batches_) {
      auto values = batch->childAt(columnIndex)->as<FlatVector<T>>();
      for (auto i = 0; i < values->size(); ++i) {
        auto rowGroup = row++ / numRowsInGroup;
        bool isIn = (rowGroup % 3) != 0;
        if (isIn) {
          if (folly::Random::rand32(rng_) % 100 == 0) {
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

  static std::vector<vector_size_t> getSomeNonNullRowNumbers(
      VectorPtr batch,
      uint32_t numRows);

  memory::MemoryPool& pool_;
  std::mt19937 rng_;
  std::unique_ptr<std::vector<RowVectorPtr>> batches_;
};
} // namespace facebook::velox::test
