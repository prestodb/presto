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

template <typename T>
class TypedDistinctAggregations : public DistinctAggregations {
 public:
  TypedDistinctAggregations(
      std::vector<AggregateInfo*> aggregates,
      const RowTypePtr& inputType,
      memory::MemoryPool* pool)
      : aggregates_{std::move(aggregates)},
        input_{aggregates_[0]->inputs[0]},
        inputType_{inputType->childAt(input_)},
        pool_{pool} {}

  using AccumulatorType = aggregate::prestosql::SetAccumulator<T>;

  /// Returns metadata about the accumulator used to store unique inputs.
  Accumulator accumulator() const override {
    return {
        false, // isFixedSize
        sizeof(AccumulatorType),
        false, // usesExternalMemory
        1, // alignment
        [this](folly::Range<char**> groups) {
          for (auto* group : groups) {
            auto* accumulator =
                reinterpret_cast<AccumulatorType*>(group + offset_);
            accumulator->free(*allocator_);
          }
        }};
  }

  void initializeNewGroups(
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

  void addInput(
      char** groups,
      const RowVectorPtr& input,
      const SelectivityVector& rows) override {
    decodedInput_.decode(*input->childAt(input_), rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* group = groups[i];
      auto* accumulator = reinterpret_cast<AccumulatorType*>(group + offset_);

      RowSizeTracker<char, uint32_t> tracker(
          group[rowSizeOffset_], *allocator_);
      accumulator->addValue(decodedInput_, i, allocator_);
    });
  }

  void addSingleGroupInput(
      char* group,
      const RowVectorPtr& input,
      const SelectivityVector& rows) override {
    decodedInput_.decode(*input->childAt(input_), rows);

    auto* accumulator = reinterpret_cast<AccumulatorType*>(group + offset_);
    RowSizeTracker<char, uint32_t> tracker(group[rowSizeOffset_], *allocator_);
    rows.applyToSelected([&](vector_size_t i) {
      accumulator->addValue(decodedInput_, i, allocator_);
    });
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

        rows.resize(data->size());
        aggregate.function->addSingleGroupRawInput(group, rows, {data}, false);
      }

      aggregate.function->extractValues(
          groups.data(), groups.size(), &result->childAt(aggregate.output));

      // Release memory back to HashStringAllocator to allow next
      // aggregate to re-use it.
      aggregate.function->destroy(groups);
    }
  }

 private:
  const std::vector<AggregateInfo*> aggregates_;
  const column_index_t input_;
  const TypePtr inputType_;
  memory::MemoryPool* const pool_;

  DecodedVector decodedInput_;
};

} // namespace

// static
std::unique_ptr<DistinctAggregations> DistinctAggregations::create(
    std::vector<AggregateInfo*> aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  column_index_t input;

  VELOX_CHECK(!aggregates.empty());
  for (auto i = 0; i < aggregates.size(); ++i) {
    auto* aggregate = aggregates[i];
    VELOX_USER_CHECK_EQ(aggregate->inputs.size(), 1);
    if (i == 0) {
      input = aggregate->inputs[0];
    } else {
      VELOX_CHECK_EQ(input, aggregate->inputs[0]);
    }
  }

  const auto type = inputType->childAt(input);

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
    case TypeKind::REAL:
      return std::make_unique<TypedDistinctAggregations<float>>(
          aggregates, inputType, pool);
    case TypeKind::DOUBLE:
      return std::make_unique<TypedDistinctAggregations<double>>(
          aggregates, inputType, pool);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TypedDistinctAggregations<Timestamp>>(
          aggregates, inputType, pool);
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
