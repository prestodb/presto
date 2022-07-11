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

#include "velox/functions/sparksql/aggregates/LastAggregate.h"

#include <fmt/format.h>

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql::aggregates {

namespace {

/// LastAggregateNumeric returns the last value of |expr| for a group of rows.
/// If |ignoreNull| is true, returns only non-null values.
/// Usage: last(expr [,ignoreNull])
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.  This can be
/// made deterministic by providing explicit ordering by adding order by or sort
/// by in query.
///
/// Supported types: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE
template <typename TDataType>
class LastAggregateNumeric : public exec::Aggregate {
 public:
  explicit LastAggregateNumeric(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TDataType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    setIgnoreNull(rows, args);

    if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          updateValue(groups[i], decoded.valueAt<TDataType>(i));
        } else if (!ignoreNull_.value()) {
          setNull(groups[i]);
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValue(groups[i], decoded.valueAt<TDataType>(i));
      });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    setIgnoreNull(rows, decoded);
    auto valueVector = rowVector->childAt(0)->as<SimpleVector<TDataType>>();

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      auto decodedIndex = decoded.index(i);
      if (!valueVector->isNullAt(decodedIndex)) {
        updateValue(groups[i], valueVector->valueAt(decodedIndex));
      } else if (!ignoreNull_.value()) {
        setNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    setIgnoreNull(rows, args);

    auto i = rows.end() - 1;
    while (i >= rows.begin()) {
      if (!rows.isValid(i)) {
        --i;
        continue;
      }
      if (!decoded.isNullAt(i)) {
        updateValue(group, decoded.valueAt<TDataType>(i));
        return;
      }
      if (!ignoreNull_.value()) {
        setNull(group);
        return;
      }
      --i;
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    setIgnoreNull(rows, decoded);
    auto valueVector = rowVector->childAt(0)->as<SimpleVector<TDataType>>();

    auto i = rows.end() - 1;
    while (i >= rows.begin()) {
      if (!rows.isValid(i) || decoded.isNullAt(i)) {
        --i;
        continue;
      }
      auto decodedIndex = decoded.index(i);
      if (!valueVector->isNullAt(decodedIndex)) {
        updateValue(group, valueVector->valueAt(decodedIndex));
        return;
      }
      if (!ignoreNull_.value()) {
        setNull(group);
        return;
      }
      --i;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TDataType>>();
    VELOX_CHECK(
        vector,
        "Unexpected type of the result vector: {}",
        (*result)->type()->toString());
    VELOX_CHECK_EQ(vector->elementSize(), sizeof(TDataType));

    vector->resize(numGroups);
    TDataType* rawValues = vector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(vector);
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = *value<TDataType>(group);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    auto valueVector = rowVector->childAt(0)->asFlatVector<TDataType>();
    auto ignoreNullVector = rowVector->childAt(1)->asFlatVector<bool>();

    rowVector->resize(numGroups);
    valueVector->resize(numGroups);
    ignoreNullVector->resize(numGroups);

    uint64_t* rawNulls = getRawNulls(rowVector);
    TDataType* rawValues = valueVector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (!ignoreNull_.has_value()) {
        // ignoreNull_ will remain unset if extractAccumulators is called
        // without calling addRawInput or addIntermediateResults.  We mark
        // the row as null in this case.
        rowVector->setNull(i, true);
        continue;
      }
      clearNull(rawNulls, i);
      ignoreNullVector->set(i, ignoreNull_.value());
      if (isNull(group)) {
        valueVector->setNull(i, true);
      } else {
        valueVector->setNull(i, false);
        rawValues[i] = *value<TDataType>(group);
      }
    }
  }

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}

 private:
  /// For raw input, if second argument is present read ignoreNull from this
  /// constant mapping.
  inline void setIgnoreNull(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    if (args.size() == 1) {
      // By default do not ignore null values.
      ignoreNull_ = false;
      return;
    }
    DecodedVector ignoreNullVector(*args[1], rows);
    VELOX_CHECK(
        ignoreNullVector.isConstantMapping(),
        "ignoreNull argument must be constant for all input rows");
    ignoreNull_ = ignoreNullVector.valueAt<bool>(0);
  }

  /// For intermediate results, read ignoreNull from any selected row. This flag
  /// will have same value for all non null rows in input.
  inline void setIgnoreNull(
      const SelectivityVector& rows,
      const DecodedVector& decoded) {
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    auto ignoreNullVector = rowVector->childAt(1)->as<SimpleVector<bool>>();

    rows.testSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return true;
      }
      ignoreNull_ = ignoreNullVector->valueAt(decoded.index(i));
      return false;
    });
  }

  inline void updateValue(char* group, TDataType value) {
    clearNull(group);
    *Aggregate::value<TDataType>(group) = value;
  }

  std::optional<bool> ignoreNull_;
};

/// LastAggregateNonNumeric returns the last value of |expr| for a group of
/// rows. If |ignoreNull| is true, returns only non-null values. Usage:
/// last(expr [,ignoreNull])
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.  This can be
/// made deterministic by providing explicit ordering by adding order by or sort
/// by in query.
//
/// Supported types: VARCHAR, ARRAY, MAP
class LastAggregateNonNumeric : public exec::Aggregate {
 public:
  explicit LastAggregateNonNumeric(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  /// We use singleValueAccumulator to save the results for each group. This
  /// struct will allow us to save variable-width value.
  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(aggregate::SingleValueAccumulator);
  }

  /// Initialize each group.
  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) aggregate::SingleValueAccumulator();
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    setIgnoreNull(rows, args);

    const auto* indices = decoded.indices();
    const auto* baseVector = decoded.base();
    if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          updateValue(groups[i], baseVector, indices[i]);
        } else if (!ignoreNull_.value()) {
          setNull(groups[i]);
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateValue(groups[i], baseVector, indices[i]);
      });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    setIgnoreNull(rows, decoded);
    auto baseVector = rowVector->childAt(0).get();

    const auto* indices = decoded.indices();
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      if (!baseVector->isNullAt(indices[i])) {
        updateValue(groups[i], baseVector, indices[i]);
      } else if (!ignoreNull_.value()) {
        setNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows, true);
    setIgnoreNull(rows, args);

    const auto* indices = decoded.indices();
    const auto* baseVector = decoded.base();

    auto i = rows.end() - 1;
    while (i >= rows.begin()) {
      if (!rows.isValid(i)) {
        --i;
        continue;
      }
      if (!decoded.isNullAt(i)) {
        updateValue(group, baseVector, indices[i]);
        return;
      }
      if (!ignoreNull_.value()) {
        setNull(group);
        return;
      }
      --i;
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    setIgnoreNull(rows, decoded);
    auto baseVector = rowVector->childAt(0).get();

    const auto* indices = decoded.indices();
    auto i = rows.end() - 1;
    while (i >= rows.begin()) {
      if (!rows.isValid(i) || decoded.isNullAt(i)) {
        --i;
        continue;
      }
      if (!baseVector->isNullAt(indices[i])) {
        updateValue(group, baseVector, indices[i]);
        return;
      }
      if (!ignoreNull_.value()) {
        setNull(group);
        return;
      }
      --i;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);

    auto* rawNulls = getRawNulls(result->get());

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        (*result)->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto accumulator = value<aggregate::SingleValueAccumulator>(group);
        accumulator->read(*result, i);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");

    auto baseVector = rowVector->childAt(0);
    auto ignoreNullVector = rowVector->childAt(1)->asFlatVector<bool>();

    rowVector->resize(numGroups);
    baseVector->resize(numGroups);
    ignoreNullVector->resize(numGroups);

    uint64_t* rawNulls = getRawNulls(rowVector);
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (!ignoreNull_.has_value()) {
        // ignoreNull_ will remain unset if extractAccumulators is called
        // without calling addRawInput or addIntermediateResults.  We mark
        // the row as null in this case.
        rowVector->setNull(i, true);
        continue;
      }
      clearNull(rawNulls, i);
      ignoreNullVector->set(i, ignoreNull_.value());
      if (isNull(group)) {
        (*result)->setNull(i, true);
      } else {
        (*result)->setNull(i, false);
        auto accumulator = value<aggregate::SingleValueAccumulator>(group);
        accumulator->read(baseVector, i);
      }
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<aggregate::SingleValueAccumulator>(group)->destroy(allocator_);
    }
  }

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}

 private:
  /// For raw input, if second argument is present read ignoreNull from this
  /// constant mapping.
  inline void setIgnoreNull(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    if (args.size() == 1) {
      // By default do not ignore null values.
      ignoreNull_ = false;
      return;
    }
    DecodedVector ignoreNullVector(*args[1], rows);
    VELOX_CHECK(
        ignoreNullVector.isConstantMapping(),
        "ignoreNull argument must be constant for all input rows");
    ignoreNull_ = ignoreNullVector.valueAt<bool>(0);
  }

  /// For intermediate results, read ignoreNull from any selected row. This flag
  /// will have same value for all non null rows in input.
  inline void setIgnoreNull(
      const SelectivityVector& rows,
      const DecodedVector& decoded) {
    auto rowVector = dynamic_cast<const RowVector*>(decoded.base());
    VELOX_CHECK_NOT_NULL(rowVector);
    auto ignoreNullVector = rowVector->childAt(1)->as<SimpleVector<bool>>();

    rows.testSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return true;
      }
      ignoreNull_ = ignoreNullVector->valueAt(decoded.index(i));
      return false;
    });
  }

  inline void
  updateValue(char* group, const BaseVector* baseVector, vector_size_t index) {
    clearNull(group);
    auto* accumulator = value<aggregate::SingleValueAccumulator>(group);
    accumulator->write(baseVector, index, allocator_);
  }

  std::optional<bool> ignoreNull_;
};

} // namespace

bool registerLastAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          .intermediateType("row(T, boolean)")
          .returnType("T")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          .argumentType("boolean")
          .intermediateType("row(T, boolean)")
          .returnType("T")
          .build(),
  };

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 2, "{} takes at most 2 arguments", name);
        const auto& inputType = argTypes[0];
        TypeKind dataKind = exec::isRawInput(step)
            ? inputType->kind()
            : inputType->childAt(0)->kind();
        switch (dataKind) {
          case TypeKind::TINYINT:
            return std::make_unique<LastAggregateNumeric<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<LastAggregateNumeric<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<LastAggregateNumeric<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<LastAggregateNumeric<int64_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<LastAggregateNumeric<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<LastAggregateNumeric<double>>(resultType);
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
            return std::make_unique<LastAggregateNonNumeric>(resultType);
          default:
            VELOX_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      });
}

} // namespace facebook::velox::functions::sparksql::aggregates
