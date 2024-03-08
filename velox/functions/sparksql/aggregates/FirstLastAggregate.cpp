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

#include <string>

#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

/// FirstLastAggregate returns the first or last value of |expr| for a group of
/// rows. If |ignoreNull| is true, returns only non-null values.
///
/// The function is non-deterministic because its results depends on the order
/// of the rows which may be non-deterministic after a shuffle.
template <bool numeric, typename TData>
class FirstLastAggregateBase
    : public SimpleNumericAggregate<TData, TData, TData> {
  using BaseAggregate = SimpleNumericAggregate<TData, TData, TData>;

 protected:
  using TAccumulator = std::conditional_t<
      numeric,
      std::optional<TData>,
      std::optional<SingleValueAccumulator>>;

 public:
  explicit FirstLastAggregateBase(TypePtr resultType)
      : BaseAggregate(std::move(resultType)) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return 1;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    Aggregate::setAllNulls(groups, indices);

    for (auto i : indices) {
      new (groups[i] + Aggregate::offset_) TAccumulator();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    if constexpr (numeric) {
      BaseAggregate::doExtractValues(
          groups, numGroups, result, [&](char* group) {
            auto accumulator = Aggregate::value<TAccumulator>(group);
            return accumulator->value();
          });
    } else {
      VELOX_CHECK(result);
      (*result)->resize(numGroups);

      auto* rawNulls = Aggregate::getRawNulls(result->get());

      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (Aggregate::isNull(group)) {
          (*result)->setNull(i, true);
        } else {
          Aggregate::clearNull(rawNulls, i);
          auto accumulator = Aggregate::value<TAccumulator>(group);
          accumulator->value().read(*result, i);
        }
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

    auto ignoreNullVector = rowVector->childAt(1)->asFlatVector<bool>();
    rowVector->resize(numGroups);
    ignoreNullVector->resize(numGroups);

    extractValues(groups, numGroups, &(rowVector->childAt(0)));
    for (auto i = 0; i < numGroups; i++) {
      if (Aggregate::isNull(groups[i])) {
        rowVector->setNull(i, true);
      }
    }
  }

  void destroy(folly::Range<char**> groups) override {
    if constexpr (!numeric) {
      for (auto group : groups) {
        auto accumulator = Aggregate::value<TAccumulator>(group);
        // If ignoreNull is true and groups are all null, accumulator will not
        // set.
        if (accumulator->has_value()) {
          accumulator->value().destroy(Aggregate::allocator_);
        }
      }
    }
  }

 protected:
  void decodeIntermediateRows(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    this->decodedIntermediates_.decode(*args[0], rows);
    auto rowVector =
        this->decodedIntermediates_.base()->template as<RowVector>();
    VELOX_CHECK_NOT_NULL(rowVector);
    VELOX_CHECK_EQ(
        rowVector->childrenSize(),
        2,
        "intermediate results must have 2 children");
    this->decodedValue_.decode(*rowVector->childAt(0), rows);
  }

  DecodedVector decodedValue_;
  DecodedVector decodedIntermediates_;
};

template <>
inline int32_t
FirstLastAggregateBase<true, int128_t>::accumulatorAlignmentSize() const {
  return static_cast<int32_t>(sizeof(int128_t));
}

template <bool ignoreNull, typename TData, bool numeric>
class FirstAggregate : public FirstLastAggregateBase<numeric, TData> {
 public:
  explicit FirstAggregate(TypePtr resultType)
      : FirstLastAggregateBase<numeric, TData>(std::move(resultType)) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      updateValue(i, groups[i], this->decodedValue_);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodeIntermediateRows(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue(
            this->decodedIntermediates_.index(i),
            groups[i],
            this->decodedValue_);
      } else {
        updateNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.testSelected([&](vector_size_t i) {
      return updateValue(i, group, this->decodedValue_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodeIntermediateRows(rows, args);

    rows.testSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        return updateValue(
            this->decodedIntermediates_.index(i), group, this->decodedValue_);
      } else {
        return updateNull(group);
      }
    });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<numeric, TData>::TAccumulator;

  bool updateNull(char* group) {
    auto accumulator = Aggregate::value<TAccumulator>(group);
    if (accumulator->has_value()) {
      return false;
    }

    if constexpr (ignoreNull) {
      return true;
    } else {
      if constexpr (numeric) {
        *accumulator = TData();
      } else {
        *accumulator = SingleValueAccumulator();
      }
      return false;
    }
  }

  // If we found a valid value, set to accumulator, then skip remaining rows in
  // group.
  bool updateValue(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);
    if (accumulator->has_value()) {
      return false;
    }

    if constexpr (!numeric) {
      return updateNonNumeric(index, group, decodedVector);
    } else {
      if (!decodedVector.isNullAt(index)) {
        Aggregate::clearNull(group);
        auto value = decodedVector.valueAt<TData>(index);
        *accumulator = value;
        return false;
      }

      if constexpr (ignoreNull) {
        return true;
      } else {
        *accumulator = TData();
        return false;
      }
    }
  }

  bool updateNonNumeric(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if (!decodedVector.isNullAt(index)) {
      Aggregate::clearNull(group);
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decodedVector.base(),
          decodedVector.index(index),
          Aggregate::allocator_);
      return false;
    }

    if constexpr (ignoreNull) {
      return true;
    } else {
      *accumulator = SingleValueAccumulator();
      return false;
    }
  }
};

template <bool ignoreNull, typename TData, bool numeric>
class LastAggregate : public FirstLastAggregateBase<numeric, TData> {
 public:
  explicit LastAggregate(TypePtr resultType)
      : FirstLastAggregateBase<numeric, TData>(std::move(resultType)) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      updateValue(i, groups[i], this->decodedValue_);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodeIntermediateRows(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue(
            this->decodedIntermediates_.index(i),
            groups[i],
            this->decodedValue_);
      } else {
        updateNull(groups[i]);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodedValue_.decode(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { updateValue(i, group, this->decodedValue_); });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    this->decodeIntermediateRows(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (!this->decodedIntermediates_.isNullAt(i)) {
        updateValue(
            this->decodedIntermediates_.index(i), group, this->decodedValue_);
      } else {
        updateNull(group);
      }
    });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<numeric, TData>::TAccumulator;

  void updateNull(char* group) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if constexpr (!ignoreNull) {
      Aggregate::setNull(group);
      if constexpr (numeric) {
        *accumulator = TData();
      } else {
        *accumulator = SingleValueAccumulator();
      }
    }
  }

  void updateValue(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector) {
    if constexpr (!numeric) {
      return updateNonNumeric(index, group, decodedVector);
    } else {
      auto accumulator = Aggregate::value<TAccumulator>(group);

      if (!decodedVector.isNullAt(index)) {
        Aggregate::clearNull(group);
        *accumulator = decodedVector.valueAt<TData>(index);
        return;
      }

      if constexpr (!ignoreNull) {
        Aggregate::setNull(group);
        *accumulator = TData();
      }
    }
  }

  void updateNonNumeric(
      vector_size_t index,
      char* group,
      const DecodedVector& decodedVector) {
    auto accumulator = Aggregate::value<TAccumulator>(group);

    if (!decodedVector.isNullAt(index)) {
      Aggregate::clearNull(group);
      if (accumulator->has_value()) {
        accumulator->value().destroy(Aggregate::allocator_);
      }
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decodedVector.base(),
          decodedVector.index(index),
          Aggregate::allocator_);
      return;
    }

    if constexpr (!ignoreNull) {
      if (accumulator->has_value()) {
        accumulator->value().destroy(Aggregate::allocator_);
      }
      Aggregate::setNull(group);
      *accumulator = SingleValueAccumulator();
    }
  }
};

} // namespace

template <template <bool B1, typename T, bool B2> class TClass, bool ignoreNull>
AggregateRegistrationResult registerFirstLast(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures = {
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          // Second column is a placeholder.
          .intermediateType("row(T, boolean)")
          .returnType("T")
          .build()};

  signatures.push_back(AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .intermediateType("DECIMAL(a_precision, a_scale)")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .build());

  return registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/) -> std::unique_ptr<Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only 1 arguments", name);
        const auto& inputType = argTypes[0];
        TypeKind dataKind = isRawInput(step) ? inputType->kind()
                                             : inputType->childAt(0)->kind();
        switch (dataKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<TClass<ignoreNull, bool, true>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<TClass<ignoreNull, int8_t, true>>(
                resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<TClass<ignoreNull, int16_t, true>>(
                resultType);
          case TypeKind::INTEGER:
            return std::make_unique<TClass<ignoreNull, int32_t, true>>(
                resultType);
          case TypeKind::BIGINT:
            return std::make_unique<TClass<ignoreNull, int64_t, true>>(
                resultType);
          case TypeKind::REAL:
            return std::make_unique<TClass<ignoreNull, float, true>>(
                resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<TClass<ignoreNull, double, true>>(
                resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<TClass<ignoreNull, Timestamp, true>>(
                resultType);
          case TypeKind::HUGEINT:
            return std::make_unique<TClass<ignoreNull, int128_t, true>>(
                resultType);
          case TypeKind::VARBINARY:
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<TClass<ignoreNull, ComplexType, false>>(
                resultType);
          default:
            VELOX_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      },
      withCompanionFunctions,
      overwrite);
}

void registerFirstLastAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerFirstLast<FirstAggregate, false>(
      prefix + "first", withCompanionFunctions, overwrite);
  registerFirstLast<FirstAggregate, true>(
      prefix + "first_ignore_null", withCompanionFunctions, overwrite);
  registerFirstLast<LastAggregate, false>(
      prefix + "last", withCompanionFunctions, overwrite);
  registerFirstLast<LastAggregate, true>(
      prefix + "last_ignore_null", withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
