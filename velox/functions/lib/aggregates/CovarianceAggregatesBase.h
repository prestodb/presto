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

#include "velox/exec/Aggregate.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate {
template <typename T>
SimpleVector<T>* asSimpleVector(
    const RowVector* rowVector,
    int32_t childIndex) {
  return rowVector->childAt(childIndex)->as<SimpleVector<T>>();
}

template <typename T>
T* mutableRawValues(const RowVector* rowVector, int32_t childIndex) {
  return rowVector->childAt(childIndex)
      ->as<FlatVector<T>>()
      ->mutableRawValues();
}

// Indices into RowType representing intermediate results of covar_samp and
// covar_pop. Columns appear in alphabetical order.
struct CovarIndices {
  int32_t count;
  int32_t meanX;
  int32_t meanY;
  int32_t c2;
};
constexpr CovarIndices kCovarIndices{1, 2, 3, 0};

// Indices into RowType representing intermediate results of corr. Columns
// appear in alphabetical order.
struct CorrIndices : public CovarIndices {
  int32_t m2X;
  int32_t m2Y;
};
constexpr CorrIndices kCorrIndices{{1, 4, 5, 0}, 2, 3};

struct CovarAccumulator {
  int64_t count() const;

  double meanX() const;

  double meanY() const;

  double c2() const;

  void update(double x, double y);

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other);

 private:
  int64_t count_{0};
  double meanX_{0};
  double meanY_{0};
  double c2_{0};
};

struct RegrAccumulator : public CovarAccumulator {
  double m2X() const;

  void update(double x, double y);

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther);

 protected:
  double m2X_{0};
};

struct ExtendedRegrAccumulator : public RegrAccumulator {
  double m2Y() const;

  void update(double x, double y);

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther,
      double m2YOther);

 private:
  double m2Y_{0};
};

class CovarIntermediateInput {
 public:
  explicit CovarIntermediateInput(
      const RowVector* rowVector,
      const CovarIndices& indices = kCovarIndices)
      : count_{asSimpleVector<int64_t>(rowVector, indices.count)},
        meanX_{asSimpleVector<double>(rowVector, indices.meanX)},
        meanY_{asSimpleVector<double>(rowVector, indices.meanY)},
        c2_{asSimpleVector<double>(rowVector, indices.c2)} {}

  void mergeInto(CovarAccumulator& accumulator, vector_size_t row);

 protected:
  SimpleVector<int64_t>* count_;
  SimpleVector<double>* meanX_;
  SimpleVector<double>* meanY_;
  SimpleVector<double>* c2_;
};

class CovarIntermediateResult {
 public:
  explicit CovarIntermediateResult(
      const RowVector* rowVector,
      const CovarIndices& indices = kCovarIndices)
      : count_{mutableRawValues<int64_t>(rowVector, indices.count)},
        meanX_{mutableRawValues<double>(rowVector, indices.meanX)},
        meanY_{mutableRawValues<double>(rowVector, indices.meanY)},
        c2_{mutableRawValues<double>(rowVector, indices.c2)} {}

  static std::string type() {
    return "row(double,bigint,double,double)";
  }

  void set(vector_size_t row, const CovarAccumulator& accumulator);

 private:
  int64_t* count_;
  double* meanX_;
  double* meanY_;
  double* c2_;
};

struct CorrAccumulator : public CovarAccumulator {
  double m2X() const {
    return m2X_;
  }

  double m2Y() const {
    return m2Y_;
  }

  void update(double x, double y);

  void merge(
      int64_t countOther,
      double meanXOther,
      double meanYOther,
      double c2Other,
      double m2XOther,
      double m2YOther);

 private:
  double m2X_{0};
  double m2Y_{0};
};

class CorrIntermediateInput : public CovarIntermediateInput {
 public:
  explicit CorrIntermediateInput(const RowVector* rowVector)
      : CovarIntermediateInput(rowVector, kCorrIndices),
        m2X_{asSimpleVector<double>(rowVector, kCorrIndices.m2X)},
        m2Y_{asSimpleVector<double>(rowVector, kCorrIndices.m2Y)} {}

  void mergeInto(CorrAccumulator& accumulator, vector_size_t row);

 private:
  SimpleVector<double>* m2X_;
  SimpleVector<double>* m2Y_;
};

class CorrIntermediateResult : public CovarIntermediateResult {
 public:
  explicit CorrIntermediateResult(const RowVector* rowVector)
      : CovarIntermediateResult(rowVector, kCorrIndices),
        m2X_{mutableRawValues<double>(rowVector, kCorrIndices.m2X)},
        m2Y_{mutableRawValues<double>(rowVector, kCorrIndices.m2Y)} {}

  static std::string type() {
    return "row(double,bigint,double,double,double,double)";
  }

  void set(vector_size_t row, const CorrAccumulator& accumulator);

 private:
  double* m2X_;
  double* m2Y_;
};

// @tparam T Type of the raw input and final result. Can be double or float.
template <
    typename T,
    typename TAccumulator,
    typename TIntermediateInput,
    typename TIntermediateResult,
    typename TResultAccessor>
class CovarianceAggregate : public exec::Aggregate {
 public:
  explicit CovarianceAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    if constexpr (
        std::is_same_v<TAccumulator, RegrAccumulator> ||
        std::is_same_v<TAccumulator, ExtendedRegrAccumulator>) {
      // The args order of linear regression function is (y, x), so we need to
      // swap the order
      decodedX_.decode(*args[1], rows);
      decodedY_.decode(*args[0], rows);
    } else {
      decodedX_.decode(*args[0], rows);
      decodedY_.decode(*args[1], rows);
    }

    rows.applyToSelected([&](auto row) {
      if (decodedX_.isNullAt(row) || decodedY_.isNullAt(row)) {
        return;
      }
      auto* group = groups[row];
      exec::Aggregate::clearNull(group);
      accumulator(group)->update(
          decodedX_.valueAt<T>(row), decodedY_.valueAt<T>(row));
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);

    auto baseRowVector = static_cast<const RowVector*>(decodedPartial_.base());
    TIntermediateInput input{baseRowVector};

    rows.applyToSelected([&](auto row) {
      if (decodedPartial_.isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPartial_.index(row);
      auto* group = groups[row];
      exec::Aggregate::clearNull(group);
      input.mergeInto(*accumulator(group), decodedIndex);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    if constexpr (
        std::is_same_v<TAccumulator, RegrAccumulator> ||
        std::is_same_v<TAccumulator, ExtendedRegrAccumulator>) {
      // The args order of linear regression function is (y, x), so we need to
      // swap the order
      decodedX_.decode(*args[1], rows);
      decodedY_.decode(*args[0], rows);
    } else {
      decodedX_.decode(*args[0], rows);
      decodedY_.decode(*args[1], rows);
    }

    exec::Aggregate::clearNull(group);
    auto* accumulator = this->accumulator(group);

    rows.applyToSelected([&](auto row) {
      if (decodedX_.isNullAt(row) || decodedY_.isNullAt(row)) {
        return;
      }
      accumulator->update(decodedX_.valueAt<T>(row), decodedY_.valueAt<T>(row));
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);

    exec::Aggregate::clearNull(group);
    auto* accumulator = this->accumulator(group);

    auto baseRowVector = static_cast<const RowVector*>(decodedPartial_.base());
    TIntermediateInput input{baseRowVector};

    rows.applyToSelected([&](auto row) {
      if (decodedPartial_.isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPartial_.index(row);
      input.mergeInto(*accumulator, decodedIndex);
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<T>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    T* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        auto* accumulator = this->accumulator(group);
        if (TResultAccessor::hasResult(*accumulator)) {
          clearNull(rawNulls, i);
          rawValues[i] = static_cast<T>(TResultAccessor::result(*accumulator));
        } else {
          vector->setNull(i, true);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    rowVector->resize(numGroups);
    for (auto& child : rowVector->children()) {
      child->resize(numGroups);
    }

    uint64_t* rawNulls = getRawNulls(rowVector);

    TIntermediateResult covarResult{rowVector};

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rowVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        covarResult.set(i, *accumulator(group));
      }
    }
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) TAccumulator();
    }
  }

 private:
  inline TAccumulator* accumulator(char* group) {
    return exec::Aggregate::value<TAccumulator>(group);
  }

  DecodedVector decodedX_;
  DecodedVector decodedY_;
  DecodedVector decodedPartial_;
};

} // namespace facebook::velox::functions::aggregate
