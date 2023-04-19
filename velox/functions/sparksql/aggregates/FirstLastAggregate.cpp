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
#include "velox/functions/lib/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"

using namespace facebook::velox::aggregate;

namespace facebook::velox::functions::sparksql::aggregates {

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

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);

    for (auto i : indices) {
      new (groups[i] + exec::Aggregate::offset_) TAccumulator();
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    if constexpr (numeric) {
      BaseAggregate::doExtractValues(
          groups, numGroups, result, [&](char* group) {
            auto accumulator = exec::Aggregate::value<TAccumulator>(group);
            return accumulator->value();
          });
    } else {
      VELOX_CHECK(result);
      (*result)->resize(numGroups);

      auto* rawNulls = exec::Aggregate::getRawNulls(result->get());

      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (exec::Aggregate::isNull(group)) {
          (*result)->setNull(i, true);
        } else {
          exec::Aggregate::clearNull(rawNulls, i);
          auto accumulator = exec::Aggregate::value<TAccumulator>(group);
          accumulator->value().read(*result, i);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    if constexpr (!numeric) {
      for (auto group : groups) {
        auto accumulator = exec::Aggregate::value<TAccumulator>(group);
        accumulator->value().destroy(exec::Aggregate::allocator_);
      }
    }
  }
};

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
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { updateValue(i, groups[i], decoded); });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    DecodedVector decoded(*args[0], rows);

    rows.testSelected(
        [&](vector_size_t i) { return updateValue(i, group, decoded); });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<numeric, TData>::TAccumulator;

  // If we found a valid value, set to accumulator, then skip remaining rows in
  // group.
  bool updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
    auto accumulator = exec::Aggregate::value<TAccumulator>(group);
    if (accumulator->has_value()) {
      return false;
    }

    if constexpr (!numeric) {
      return updateNonNumeric(i, group, decoded);
    } else {
      if (!decoded.isNullAt(i)) {
        exec::Aggregate::clearNull(group);
        auto value = decoded.valueAt<TData>(i);
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

  bool updateNonNumeric(vector_size_t i, char* group, DecodedVector& decoded) {
    auto accumulator = exec::Aggregate::value<TAccumulator>(group);

    if (!decoded.isNullAt(i)) {
      exec::Aggregate::clearNull(group);
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decoded.base(), decoded.index(i), exec::Aggregate::allocator_);
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
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { updateValue(i, groups[i], decoded); });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected(
        [&](vector_size_t i) { updateValue(i, group, decoded); });
  }

 private:
  using TAccumulator =
      typename FirstLastAggregateBase<numeric, TData>::TAccumulator;

  void updateValue(vector_size_t i, char* group, DecodedVector& decoded) {
    if constexpr (!numeric) {
      return updateNonNumeric(i, group, decoded);
    } else {
      auto accumulator = exec::Aggregate::value<TAccumulator>(group);

      if (!decoded.isNullAt(i)) {
        exec::Aggregate::clearNull(group);
        *accumulator = decoded.valueAt<TData>(i);
        return;
      }

      if constexpr (!ignoreNull) {
        exec::Aggregate::setNull(group);
        *accumulator = TData();
      }
    }
  }

  void updateNonNumeric(vector_size_t i, char* group, DecodedVector& decoded) {
    auto accumulator = exec::Aggregate::value<TAccumulator>(group);

    if (!decoded.isNullAt(i)) {
      exec::Aggregate::clearNull(group);
      *accumulator = SingleValueAccumulator();
      accumulator->value().write(
          decoded.base(), decoded.index(i), exec::Aggregate::allocator_);
      return;
    }

    if constexpr (!ignoreNull) {
      exec::Aggregate::setNull(group);
      *accumulator = SingleValueAccumulator();
    }
  }
};

} // namespace

template <template <bool B1, typename T, bool B2> class TClass, bool ignoreNull>
bool registerFirstLast(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("T")
          .intermediateType("T")
          .returnType("T")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only 1 arguments", name);
        const auto& inputType = argTypes[0];
        TypeKind dataKind = inputType->kind();
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
          case TypeKind::DATE:
            return std::make_unique<TClass<ignoreNull, Date, true>>(resultType);
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
      });
}

void registerFirstLastAggregates(const std::string& prefix) {
  registerFirstLast<FirstAggregate, false>(prefix + "first");
  registerFirstLast<FirstAggregate, true>(prefix + "first_ignore_null");
  registerFirstLast<LastAggregate, false>(prefix + "last");
  registerFirstLast<LastAggregate, true>(prefix + "last_ignore_null");
}

} // namespace facebook::velox::functions::sparksql::aggregates
