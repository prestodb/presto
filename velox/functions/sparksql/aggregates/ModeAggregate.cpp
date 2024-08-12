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

#include "folly/container/F14Map.h"

#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/exec/Strings.h"

using namespace facebook::velox::aggregate::prestosql;
using namespace facebook::velox::exec;

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

// Mode aggregate function for scalar types.
template <
    typename T,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
class ModeAggregate {
 public:
  using InputType = Row<T>;

  using IntermediateType = Map<T, int64_t>;

  using OutputType = T;

  struct AccumulatorType {
    using ValueMap = folly::F14FastMap<
        T,
        int64_t,
        Hash,
        EqualTo,
        AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;

    // A map of T -> count.
    ValueMap values;

    explicit AccumulatorType(HashStringAllocator* allocator)
        : values{AlignedStlAllocator<std::pair<const T, int64_t>, 16>(
              allocator)} {}

    static constexpr bool is_fixed_size_ = false;

    void addInput(HashStringAllocator* /*allocator*/, arg_type<T> data) {
      values[data]++;
    }

    void combine(
        HashStringAllocator* /*allocator*/,
        arg_type<IntermediateType> other) {
      for (const auto& [key, count] : other) {
        if (count.has_value()) {
          values[key] += count.value();
        }
      }
    }

    bool writeIntermediateResult(out_type<IntermediateType>& out) {
      out.reserve(values.size());
      for (const auto& [value, count] : values) {
        out.emplace(value, count);
      }
      return true;
    }

    bool writeFinalResult(out_type<OutputType>& out) {
      auto iterator = std::max_element(
          std::begin(values),
          std::end(values),
          [](const std::pair<T, int64_t>& p1, const std::pair<T, int64_t>& p2) {
            return p1.second < p2.second;
          });
      out = iterator->first;
      return true;
    }

    void destroy(HashStringAllocator* /*allocator*/) {
      using TValues = decltype(values);
      values.~TValues();
    }
  };
};

// Mode aggregate function for VARCHAR type.
class StringModeAggregate {
 public:
  using InputType = Row<Varchar>;

  using IntermediateType = Map<Varchar, int64_t>;

  using OutputType = Varchar;

  struct AccumulatorType {
    using ValueMap = folly::F14FastMap<
        StringView,
        int64_t,
        std::hash<StringView>,
        std::equal_to<StringView>,
        AlignedStlAllocator<std::pair<const StringView, int64_t>, 16>>;

    // A map of unique StringViews pointing to storage managed by 'strings'.
    ValueMap values;

    // Stores unique non-null non-inline strings.
    Strings strings;

    explicit AccumulatorType(HashStringAllocator* allocator)
        : values{AlignedStlAllocator<std::pair<const StringView, int64_t>, 16>(
              allocator)} {}

    static constexpr bool is_fixed_size_ = false;

    void addInput(HashStringAllocator* allocator, arg_type<Varchar> data) {
      values[store(data, allocator)]++;
    }

    void combine(
        HashStringAllocator* allocator,
        arg_type<IntermediateType> other) {
      for (const auto& [key, count] : other) {
        if (count.has_value()) {
          values[store(key, allocator)] += count.value();
        }
      }
    }

    bool writeIntermediateResult(out_type<IntermediateType>& out) {
      out.reserve(values.size());
      for (const auto& [value, count] : values) {
        out.add_item() = std::make_tuple(value, count);
      }
      return true;
    }

    bool writeFinalResult(out_type<OutputType>& out) {
      auto iterator = std::max_element(
          std::begin(values),
          std::end(values),
          [](const std::pair<StringView, int64_t>& p1,
             const std::pair<StringView, int64_t>& p2) {
            return p1.second < p2.second;
          });
      out = iterator->first;
      return true;
    }

    void destroy(HashStringAllocator* allocator) {
      strings.free(*allocator);
      using TValues = decltype(values);
      values.~TValues();
    }

   private:
    // Stores the non-inlined string in memory blocks managed by
    // HashStringAllocator, if it's not exist in the values map.
    StringView store(StringView value, HashStringAllocator* allocator) {
      if (!value.isInline()) {
        auto it = values.find(value);
        if (it == values.end()) {
          value = strings.append(value, *allocator);
        }
      }
      return value;
    }
  };
};

struct ComplexTypeAccumulator {
  using ValueMap = folly::F14FastMap<
      AddressableNonNullValueList::Entry,
      int64_t,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo,
      AlignedStlAllocator<
          std::pair<const AddressableNonNullValueList::Entry, int64_t>,
          16>>;

  ValueMap values;

  AddressableNonNullValueList serializedValues;

  ComplexTypeAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : values{
            0,
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            AlignedStlAllocator<
                std::pair<const AddressableNonNullValueList::Entry, int64_t>,
                16>(allocator)} {}

  size_t size() const {
    return values.size();
  }

  void addValue(
      DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    addValueWithCount(*decoded.base(), decoded.index(index), 1, allocator);
  }

  void addValueWithCount(
      const BaseVector& keys,
      vector_size_t index,
      int64_t count,
      HashStringAllocator* allocator) {
    auto entry = serializedValues.append(keys, index, allocator);

    auto it = values.find(entry);
    if (it == values.end()) {
      // New entry.
      values[entry] += count;
    } else {
      // Existing entry.
      serializedValues.removeLast(entry);

      it->second += count;
    }
  }

  void extractValues(
      BaseVector& keys,
      FlatVector<int64_t>& counts,
      vector_size_t offset) {
    auto index = offset;
    for (const auto& [position, count] : values) {
      AddressableNonNullValueList::read(position, keys, index);
      counts.set(index, count);
      ++index;
    }
  }

  void extractValues(const VectorPtr& results, vector_size_t offset) {
    auto iterator = std::max_element(
        std::begin(values),
        std::end(values),
        [](const std::pair<AddressableNonNullValueList::Entry, int64_t>& p1,
           const std::pair<AddressableNonNullValueList::Entry, int64_t>& p2) {
          return p1.second < p2.second;
        });
    AddressableNonNullValueList::read(iterator->first, *results, offset);
  }

  void free(HashStringAllocator& allocator) {
    serializedValues.free(allocator);
    using TValues = decltype(values);
    values.~TValues();
  }
};

// Mode aggregate function for complex types.
template <typename T>
class ComplexTypeModeAggregate : public Aggregate {
 public:
  explicit ComplexTypeModeAggregate(TypePtr resultType, TypePtr inputType)
      : Aggregate(std::move(resultType)), inputType_(std::move(inputType)) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ComplexTypeAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(*result);
    (*result)->resize(numGroups);
    uint64_t* rawNulls = nullptr;
    if ((*result)->mayHaveNulls()) {
      BufferPtr& nulls = (*result)->mutableNulls((*result)->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto* accumulator = value<ComplexTypeAccumulator>(group);
      if (accumulator->size() == 0) {
        (*result)->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        accumulator->extractValues(*result, i);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    vector_size_t numElements = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      numElements += value<ComplexTypeAccumulator>(groups[i])->size();
    }
    mapKeys->resize(numElements);
    flatValues->resize(numElements);

    auto* rawNulls = mapVector->mutableRawNulls();
    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto* accumulator = value<ComplexTypeAccumulator>(group);

      const auto mapSize = accumulator->size();
      mapVector->setOffsetAndSize(i, offset, mapSize);
      if (mapSize == 0) {
        bits::setNull(rawNulls, i, true);
      } else {
        clearNull(rawNulls, i);
        accumulator->extractValues(*mapKeys, *flatValues, offset);
        offset += mapSize;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedKeys_.decode(*args[0], rows);

    rows.applyToSelected([&](auto row) {
      if (decodedKeys_.isNullAt(row)) {
        // Nulls among the values being aggregated are ignored.
        return;
      }
      auto group = groups[row];
      auto* accumulator = value<ComplexTypeAccumulator>(group);

      auto tracker = trackRowSize(group);
      accumulator->addValue(decodedKeys_, row, allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedKeys_.decode(*args[0], rows);
    auto* accumulator = value<ComplexTypeAccumulator>(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      // Nulls among the values being aggregated are ignored.
      if (!decodedKeys_.isNullAt(row)) {
        accumulator->addValue(decodedKeys_, row, allocator_);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto* indices = decodedIntermediate_.indices();
    auto* mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    auto rawSizes = mapVector->rawSizes();
    auto rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        auto group = groups[row];
        auto* accumulator = value<ComplexTypeAccumulator>(group);

        auto tracker = trackRowSize(group);
        addToFinalAggregation(
            mapKeys.get(),
            flatValues,
            indices[row],
            rawSizes,
            rawOffsets,
            accumulator,
            allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto* indices = decodedIntermediate_.indices();
    auto* mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    auto* accumulator = value<ComplexTypeAccumulator>(group);

    auto tracker = trackRowSize(group);

    auto* rawSizes = mapVector->rawSizes();
    auto* rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        addToFinalAggregation(
            mapKeys.get(),
            flatValues,
            indices[row],
            rawSizes,
            rawOffsets,
            accumulator,
            allocator_);
      }
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_)
          ComplexTypeAccumulator{inputType_, allocator_};
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      if (isInitialized(group) && !isNull(group)) {
        value<ComplexTypeAccumulator>(group)->free(*allocator_);
      }
    }
  }

 private:
  // Combines a partial aggregation represented by the key-value pair at row in
  // mapKeys and mapValues into groupMap.
  FOLLY_ALWAYS_INLINE void addToFinalAggregation(
      const BaseVector* mapKeys,
      const FlatVector<int64_t>* flatValues,
      vector_size_t index,
      const vector_size_t* rawSizes,
      const vector_size_t* rawOffsets,
      ComplexTypeAccumulator* accumulator,
      HashStringAllocator* allocator) {
    auto size = rawSizes[index];
    auto offset = rawOffsets[index];

    for (int i = 0; i < size; ++i) {
      const auto count = flatValues->valueAt(offset + i);
      accumulator->addValueWithCount(*mapKeys, offset + i, count, allocator);
    }
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedIntermediate_;
  TypePtr inputType_;
};
} // namespace

void registerModeAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures = {
      AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .intermediateType("map(T,bigint)")
          .argumentType("T")
          .build(),
  };

  auto name = prefix + "mode";
  registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/) -> std::unique_ptr<Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{}: unexpected number of arguments", name);

        auto inputType =
            isRawInput(step) ? argTypes[0] : argTypes[0]->childAt(0);
        switch (inputType->kind()) {
          case TypeKind::BOOLEAN:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<bool>>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<int8_t>>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<int16_t>>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<int32_t>>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<int64_t>>>(resultType);
          case TypeKind::HUGEINT:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<int128_t>>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<SimpleAggregateAdapter<ModeAggregate<
                float,
                util::floating_point::NaNAwareHash<float>,
                util::floating_point::NaNAwareEquals<float>>>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<SimpleAggregateAdapter<ModeAggregate<
                double,
                util::floating_point::NaNAwareHash<double>,
                util::floating_point::NaNAwareEquals<double>>>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<
                SimpleAggregateAdapter<ModeAggregate<Timestamp>>>(resultType);
          case TypeKind::UNKNOWN:
            // Regitsers Mode function with unknown type, this needs hasher for
            // UnknownValue, we use folly::hasher for it.
            return std::make_unique<SimpleAggregateAdapter<ModeAggregate<
                UnknownValue,
                folly::hasher<UnknownValue>,
                std::equal_to<UnknownValue>>>>(resultType);
          case TypeKind::VARCHAR:
          case TypeKind::VARBINARY:
            return std::make_unique<
                SimpleAggregateAdapter<StringModeAggregate>>(resultType);
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<ComplexTypeModeAggregate<ComplexType>>(
                resultType, inputType);
          default:
            VELOX_NYI(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      },
      {false /*orderSensitive*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
