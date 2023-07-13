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
#include "velox/exec/Aggregate.h"
#include "velox/exec/SetAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
class SetBaseAggregate : public exec::Aggregate {
 public:
  explicit SetBaseAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = SetAccumulator<T>;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) AccumulatorType(type, allocator_);
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto arrayVector = (*result)->as<ArrayVector>();
    arrayVector->resize(numGroups);

    auto* rawOffsets = arrayVector->offsets()->asMutable<vector_size_t>();
    auto* rawSizes = arrayVector->sizes()->asMutable<vector_size_t>();

    vector_size_t numValues = 0;
    uint64_t* rawNulls = getRawNulls(arrayVector);
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      if (isNull(group)) {
        arrayVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);

        const auto size = value(group)->size();

        rawOffsets[i] = numValues;
        rawSizes[i] = size;

        numValues += size;
      }
    }

    if constexpr (std::is_same_v<T, ComplexType>) {
      auto values = arrayVector->elements();
      values->resize(numValues);

      vector_size_t offset = 0;
      for (auto i = 0; i < numGroups; ++i) {
        auto* group = groups[i];
        if (!isNull(group)) {
          offset += value(group)->extractValues(*values, offset);
        }
      }
    } else {
      auto values = arrayVector->elements()->as<FlatVector<T>>();
      values->resize(numValues);

      vector_size_t offset = 0;
      for (auto i = 0; i < numGroups; ++i) {
        auto* group = groups[i];
        if (!isNull(group)) {
          offset += value(group)->extractValues(*values, offset);
        }
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    return extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();
    decodedElements_.decode(*baseArray->elements());

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];
      clearNull(group);

      auto tracker = trackRowSize(group);

      auto decodedIndex = decoded_.index(i);
      value(group)->addValues(
          *baseArray, decodedIndex, decodedElements_, allocator_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();

    decodedElements_.decode(*baseArray->elements());

    auto* accumulator = value(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        return;
      }

      clearNull(group);

      auto decodedIndex = decoded_.index(i);
      accumulator->addValues(
          *baseArray, decodedIndex, decodedElements_, allocator_);
    });
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      if (!isNull(group)) {
        value(group)->free(*allocator_);
      }
    }
  }

 protected:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  DecodedVector decoded_;
  DecodedVector decodedElements_;
};

template <typename T>
class SetAggAggregate : public SetBaseAggregate<T> {
 public:
  explicit SetAggAggregate(const TypePtr& resultType)
      : SetBaseAggregate<T>(resultType) {}

  using Base = SetBaseAggregate<T>;

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decoded_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* group = groups[i];
      Base::clearNull(group);

      auto tracker = Base::trackRowSize(group);
      Base::value(group)->addValue(Base::decoded_, i, Base::allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decoded_.decode(*args[0], rows);

    Base::clearNull(group);
    auto* accumulator = Base::value(group);

    auto tracker = Base::trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      accumulator->addValue(Base::decoded_, i, Base::allocator_);
    });
  }
};

template <typename T>
class SetUnionAggregate : public SetBaseAggregate<T> {
 public:
  explicit SetUnionAggregate(const TypePtr& resultType)
      : SetBaseAggregate<T>(resultType) {}

  using Base = SetBaseAggregate<T>;

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    Base::addIntermediateResults(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    Base::addSingleGroupIntermediateResults(group, rows, args, mayPushdown);
  }
};

template <template <typename T> class Aggregate>
std::unique_ptr<exec::Aggregate> create(
    TypeKind typeKind,
    const TypePtr& resultType) {
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return std::make_unique<Aggregate<bool>>(resultType);
    case TypeKind::TINYINT:
      return std::make_unique<Aggregate<int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<Aggregate<int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<Aggregate<int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<Aggregate<int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<Aggregate<float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<Aggregate<double>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<Aggregate<Timestamp>>(resultType);
    case TypeKind::VARCHAR:
      return std::make_unique<Aggregate<StringView>>(resultType);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return std::make_unique<Aggregate<ComplexType>>(resultType);
    default:
      VELOX_UNREACHABLE("Unexpected type {}", mapTypeKindToName(typeKind));
  }
}

exec::AggregateRegistrationResult registerSetAgg(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("array(T)")
          .argumentType("T")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);

        const TypeKind typeKind = exec::isRawInput(step)
            ? argTypes[0]->kind()
            : argTypes[0]->childAt(0)->kind();

        return create<SetAggAggregate>(typeKind, resultType);
      });
}

exec::AggregateRegistrationResult registerSetUnion(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("array(T)")
          .argumentType("array(T)")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);

        const TypeKind typeKind = argTypes[0]->childAt(0)->kind();

        return create<SetUnionAggregate>(typeKind, resultType);
      });
}

} // namespace

void registerSetAggAggregate(const std::string& prefix) {
  registerSetAgg(prefix + kSetAgg);
}

void registerSetUnionAggregate(const std::string& prefix) {
  registerSetUnion(prefix + kSetUnion);
}

} // namespace facebook::velox::aggregate::prestosql
