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
#include "velox/exec/DistinctAggregations.h"
#include "velox/exec/SetAccumulator.h"

namespace facebook::velox::exec {

namespace {

template <
    typename T,
    typename AccumulatorType = aggregate::prestosql::SetAccumulator<T>>
class TypedDistinctAggregations : public DistinctAggregations {
 public:
  TypedDistinctAggregations(
      std::vector<AggregateInfo*> aggregates,
      const RowTypePtr& inputType,
      memory::MemoryPool* pool)
      : pool_{pool},
        aggregates_{std::move(aggregates)},
        inputs_{aggregates_[0]->inputs},
        inputType_(TypedDistinctAggregations::makeInputTypeForAccumulator(
            inputType,
            inputs_)) {}

  /// Returns metadata about the accumulator used to store unique inputs.
  Accumulator accumulator() const override {
    return {
        false, // isFixedSize
        sizeof(AccumulatorType),
        false, // usesExternalMemory
        1, // alignment
        nullptr,
        [](folly::Range<char**> /*groups*/, VectorPtr& /*result*/) {
          VELOX_UNREACHABLE();
        },
        [this](folly::Range<char**> groups) {
          for (auto* group : groups) {
            auto* accumulator =
                reinterpret_cast<AccumulatorType*>(group + offset_);
            accumulator->free(*allocator_);
          }
        }};
  }

  void addInput(
      char** groups,
      const RowVectorPtr& input,
      const SelectivityVector& rows) override {
    decodeInput(input, rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* group = groups[i];
      auto* accumulator = reinterpret_cast<AccumulatorType*>(group + offset_);

      RowSizeTracker<char, uint32_t> tracker(
          group[rowSizeOffset_], *allocator_);
      accumulator->addValue(decodedInput_, i, allocator_);
    });

    inputForAccumulator_.reset();
  }

  void addSingleGroupInput(
      char* group,
      const RowVectorPtr& input,
      const SelectivityVector& rows) override {
    decodeInput(input, rows);

    auto* accumulator = reinterpret_cast<AccumulatorType*>(group + offset_);
    RowSizeTracker<char, uint32_t> tracker(group[rowSizeOffset_], *allocator_);
    rows.applyToSelected([&](vector_size_t i) {
      accumulator->addValue(decodedInput_, i, allocator_);
    });

    inputForAccumulator_.reset();
  }

  void extractValues(folly::Range<char**> groups, const RowVectorPtr& result)
      override {
    SelectivityVector rows;
    for (auto i = 0; i < aggregates_.size(); ++i) {
      const auto& aggregate = *aggregates_[i];

      // For each group, add distinct inputs to aggregate.
      for (auto* group : groups) {
        auto* accumulator = reinterpret_cast<AccumulatorType*>(group + offset_);

        // TODO Process group rows in batches to avoid creating very large input
        // vectors.
        auto data = BaseVector::create(inputType_, accumulator->size(), pool_);
        if constexpr (std::is_same_v<T, ComplexType>) {
          accumulator->extractValues(*data, 0);
        } else {
          accumulator->extractValues(*(data->template as<FlatVector<T>>()), 0);
        }

        if (data->size() > 0) {
          rows.resize(data->size());
          std::vector<VectorPtr> inputForAggregation =
              makeInputForAggregation(data);
          aggregate.function->addSingleGroupRawInput(
              group, rows, inputForAggregation, false);
        }
      }

      aggregate.function->extractValues(
          groups.data(), groups.size(), &result->childAt(aggregate.output));

      // Release memory back to HashStringAllocator to allow next
      // aggregate to re-use it.
      aggregate.function->destroy(groups);

      // Overwrite empty groups over the destructed groups to keep the container
      // in a well formed state.
      raw_vector<int32_t> temp;
      aggregate.function->initializeNewGroups(
          groups.data(),
          folly::Range<const int32_t*>(
              iota(groups.size(), temp), groups.size()));
    }
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      groups[i][nullByte_] |= nullMask_;
      new (groups[i] + offset_) AccumulatorType(inputType_, allocator_);
    }

    for (auto i = 0; i < aggregates_.size(); ++i) {
      const auto& aggregate = *aggregates_[i];
      aggregate.function->initializeNewGroups(groups, indices);
    }
  }

 private:
  bool isSingleInputAggregate() const {
    return aggregates_[0]->inputs.size() == 1;
  }

  void decodeInput(const RowVectorPtr& input, const SelectivityVector& rows) {
    inputForAccumulator_ = makeInputForAccumulator(input);
    decodedInput_.decode(*inputForAccumulator_, rows);
  }

  static TypePtr makeInputTypeForAccumulator(
      const RowTypePtr& rowType,
      const std::vector<column_index_t>& inputs) {
    if (inputs.size() == 1) {
      return rowType->childAt(inputs[0]);
    }

    // Otherwise, synthesize a ROW(distinct_channels[0..N])
    std::vector<TypePtr> types;
    std::vector<std::string> names;
    for (column_index_t channelIndex : inputs) {
      names.emplace_back(rowType->nameOf(channelIndex));
      types.emplace_back(rowType->childAt(channelIndex));
    }
    return ROW(std::move(names), std::move(types));
  }

  VectorPtr makeInputForAccumulator(const RowVectorPtr& input) const {
    if (isSingleInputAggregate()) {
      return input->childAt(inputs_[0]);
    }

    std::vector<VectorPtr> newChildren(inputs_.size());
    for (int i = 0; i < inputs_.size(); ++i) {
      newChildren[i] = input->childAt(inputs_[i]);
    }
    return std::make_shared<RowVector>(
        pool_, inputType_, nullptr, input->size(), newChildren);
  }

  std::vector<VectorPtr> makeInputForAggregation(const VectorPtr& input) const {
    if (isSingleInputAggregate()) {
      return {std::move(input)};
    }
    return input->template asUnchecked<RowVector>()->children();
  }

  memory::MemoryPool* const pool_;
  const std::vector<AggregateInfo*> aggregates_;
  const std::vector<column_index_t> inputs_;
  const TypePtr inputType_;

  DecodedVector decodedInput_;
  VectorPtr inputForAccumulator_;
};

template <TypeKind Kind>
std::unique_ptr<DistinctAggregations>
createDistinctAggregationsWithCustomCompare(
    std::vector<AggregateInfo*> aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  return std::make_unique<TypedDistinctAggregations<
      typename TypeTraits<Kind>::NativeType,
      aggregate::prestosql::CustomComparisonSetAccumulator<Kind>>>(
      aggregates, inputType, pool);
}
} // namespace

// static
std::unique_ptr<DistinctAggregations> DistinctAggregations::create(
    std::vector<AggregateInfo*> aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  VELOX_CHECK_EQ(aggregates.size(), 1);
  VELOX_CHECK(!aggregates[0]->inputs.empty());

  const bool isSingleInput = aggregates[0]->inputs.size() == 1;
  if (!isSingleInput) {
    return std::make_unique<TypedDistinctAggregations<ComplexType>>(
        aggregates, inputType, pool);
  }

  const auto type = inputType->childAt(aggregates[0]->inputs[0]);

  if (type->providesCustomComparison()) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        createDistinctAggregationsWithCustomCompare,
        type->kind(),
        aggregates,
        inputType,
        pool);
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<TypedDistinctAggregations<bool>>(
          aggregates, inputType, pool);
    case TypeKind::TINYINT:
      return std::make_unique<TypedDistinctAggregations<int8_t>>(
          aggregates, inputType, pool);
    case TypeKind::SMALLINT:
      return std::make_unique<TypedDistinctAggregations<int16_t>>(
          aggregates, inputType, pool);
    case TypeKind::INTEGER:
      return std::make_unique<TypedDistinctAggregations<int32_t>>(
          aggregates, inputType, pool);
    case TypeKind::BIGINT:
      return std::make_unique<TypedDistinctAggregations<int64_t>>(
          aggregates, inputType, pool);
    case TypeKind::HUGEINT:
      return std::make_unique<TypedDistinctAggregations<int128_t>>(
          aggregates, inputType, pool);
    case TypeKind::REAL:
      return std::make_unique<TypedDistinctAggregations<float>>(
          aggregates, inputType, pool);
    case TypeKind::DOUBLE:
      return std::make_unique<TypedDistinctAggregations<double>>(
          aggregates, inputType, pool);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TypedDistinctAggregations<Timestamp>>(
          aggregates, inputType, pool);
    case TypeKind::VARBINARY:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      return std::make_unique<TypedDistinctAggregations<StringView>>(
          aggregates, inputType, pool);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return std::make_unique<TypedDistinctAggregations<ComplexType>>(
          aggregates, inputType, pool);
    default:
      VELOX_UNREACHABLE("Unexpected type {}", type->toString());
  }
}

} // namespace facebook::velox::exec
