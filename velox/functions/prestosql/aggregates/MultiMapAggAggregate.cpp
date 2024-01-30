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
#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Strings.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ValueList.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {
namespace {

/// Maintains a key-to-<list-of-values> map. Keys must be non-null.
template <
    typename K,
    typename Hash = std::hash<K>,
    typename EqualTo = std::equal_to<K>>
struct MultiMapAccumulator {
  folly::F14FastMap<
      K,
      ValueList,
      Hash,
      EqualTo,
      AlignedStlAllocator<std::pair<const K, ValueList>, 16>>
      keys;

  Strings stringKeys;

  MultiMapAccumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : keys{AlignedStlAllocator<std::pair<const K, ValueList>, 16>(
            allocator)} {}

  MultiMapAccumulator(
      Hash hash,
      EqualTo equalTo,
      HashStringAllocator* allocator)
      : keys{
            0, // initialCapacity
            hash,
            equalTo,
            AlignedStlAllocator<std::pair<const K, ValueList>, 16>(
                allocator)} {}

  size_t size() const {
    return keys.size();
  }

  size_t numValues() const {
    size_t cnt = 0;
    for (const auto& entry : keys) {
      cnt += entry.second.size();
    }
    return cnt;
  }

  /// Adds key-value pair.
  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    auto& values = insertKey(decodedKeys, index, allocator);
    values.appendValue(decodedValues, index, &allocator);
  }

  /// Adds a key with a list of values.
  void insertMultiple(
      const DecodedVector& decodedKeys,
      vector_size_t keyIndex,
      const DecodedVector& decodedValues,
      vector_size_t valueIndex,
      vector_size_t numValues,
      HashStringAllocator& allocator) {
    auto& values = insertKey(decodedKeys, keyIndex, allocator);
    for (auto i = 0; i < numValues; ++i) {
      values.appendValue(decodedValues, valueIndex + i, &allocator);
    }
  }

  ValueList& insertKey(
      const DecodedVector& decodedKeys,
      vector_size_t index,
      HashStringAllocator& allocator) {
    auto key = decodedKeys.valueAt<K>(index);
    if constexpr (std::is_same_v<K, StringView>) {
      if (!key.isInline() && !keys.contains(key)) {
        key = stringKeys.append(key, allocator);
      }
    }

    return keys.insert({key, ValueList()}).first->second;
  }

  void extract(
      VectorPtr& mapKeys,
      ArrayVector& mapValueArrays,
      vector_size_t& keyOffset,
      vector_size_t& valueOffset) {
    auto flatKeys = mapKeys->asFlatVector<K>();
    auto mapValues = mapValueArrays.elements();

    for (auto& entry : keys) {
      flatKeys->set(keyOffset, entry.first);

      const auto numValues = entry.second.size();
      mapValueArrays.setOffsetAndSize(keyOffset, valueOffset, numValues);

      aggregate::ValueListReader reader(entry.second);
      for (auto i = 0; i < numValues; i++) {
        reader.next(*mapValues, valueOffset++);
      }

      ++keyOffset;
    }
  }

  void free(HashStringAllocator& allocator) {
    for (auto& entry : keys) {
      entry.second.free(&allocator);
    }

    stringKeys.free(allocator);
  }
};

struct ComplexTypeMultiMapAccumulator {
  MultiMapAccumulator<
      AddressableNonNullValueList::Entry,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  /// Stores unique non-null keys.
  AddressableNonNullValueList serializedKeys;

  ComplexTypeMultiMapAccumulator(
      const TypePtr& type,
      HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  size_t size() const {
    return base.size();
  }

  size_t numValues() const {
    return base.numValues();
  }

  /// Adds key-value pair.
  void insert(
      const DecodedVector& decodedKeys,
      const DecodedVector& decodedValues,
      vector_size_t index,
      HashStringAllocator& allocator) {
    const auto entry = serializedKeys.append(decodedKeys, index, &allocator);

    auto& values = insertKey(entry);
    values.appendValue(decodedValues, index, &allocator);
  }

  /// Adds a key with a list of values.
  void insertMultiple(
      const DecodedVector& decodedKeys,
      vector_size_t keyIndex,
      const DecodedVector& decodedValues,
      vector_size_t valueIndex,
      vector_size_t numValues,
      HashStringAllocator& allocator) {
    const auto entry = serializedKeys.append(decodedKeys, keyIndex, &allocator);

    auto& values = insertKey(entry);
    for (auto i = 0; i < numValues; ++i) {
      values.appendValue(decodedValues, valueIndex + i, &allocator);
    }
  }

  ValueList& insertKey(const AddressableNonNullValueList::Entry& key) {
    auto result = base.keys.insert({key, ValueList()});
    if (!result.second) {
      serializedKeys.removeLast(key);
    }

    return result.first->second;
  }

  void extract(
      VectorPtr& mapKeys,
      ArrayVector& mapValueArrays,
      vector_size_t& keyOffset,
      vector_size_t& valueOffset) {
    auto& mapValues = mapValueArrays.elements();

    for (auto& entry : base.keys) {
      AddressableNonNullValueList::read(entry.first, *mapKeys, keyOffset);

      const auto numValues = entry.second.size();
      mapValueArrays.setOffsetAndSize(keyOffset, valueOffset, numValues);

      aggregate::ValueListReader reader(entry.second);
      for (auto i = 0; i < numValues; i++) {
        reader.next(*mapValues, valueOffset++);
      }

      ++keyOffset;
    }
  }

  void free(HashStringAllocator& allocator) {
    base.free(allocator);
    serializedKeys.free(allocator);
  }
};

template <typename T>
struct MultiMapAccumulatorTypeTraits {
  using AccumulatorType = MultiMapAccumulator<T>;
};

template <>
struct MultiMapAccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeMultiMapAccumulator;
};

template <typename K>
class MultiMapAggAggregate : public exec::Aggregate {
 public:
  explicit MultiMapAggAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType =
      typename MultiMapAccumulatorTypeTraits<K>::AccumulatorType;

  bool isFixedSize() const override {
    return false;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    for (auto index : indices) {
      new (groups[index] + offset_) AccumulatorType(type, allocator_);
    }
    setAllNulls(groups, indices);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedKeys_.decode(*args[0], rows);
    decodedValues_.decode(*args[1], rows);

    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys.
      if (!decodedKeys_.isNullAt(row)) {
        auto group = groups[row];
        clearNull(group);

        auto* accumulator = value<AccumulatorType>(group);
        auto tracker = trackRowSize(group);
        accumulator->insert(decodedKeys_, decodedValues_, row, *allocator_);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();

    decodedKeys_.decode(*mapVector->mapKeys());
    decodedValueArrays_.decode(*mapVector->mapValues());

    auto* valueArrays_ = decodedValueArrays_.base()->template as<ArrayVector>();

    decodedValues_.decode(*valueArrays_->elements());

    VELOX_CHECK_NOT_NULL(mapVector);
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedMaps_.isNullAt(row)) {
        auto* group = groups[row];
        auto accumulator = value<AccumulatorType>(group);
        clearNull(group);
        auto decodedRow = decodedMaps_.index(row);
        auto offset = mapVector->offsetAt(decodedRow);
        auto size = mapVector->sizeAt(decodedRow);
        checkNullKeys(decodedKeys_, offset, size);
        for (auto i = offset; i < offset + size; ++i) {
          auto numValues = valueArrays_->sizeAt(decodedValueArrays_.index(i));
          auto valueOffset =
              valueArrays_->offsetAt(decodedValueArrays_.index(i));
          accumulator->insertMultiple(
              decodedKeys_,
              i,
              decodedValues_,
              valueOffset,
              numValues,
              *allocator_);
        }
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedKeys_.decode(*args[0], rows);
    decodedValues_.decode(*args[1], rows);

    auto* accumulator = value<AccumulatorType>(group);
    auto tracker = trackRowSize(group);

    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys.
      if (!decodedKeys_.isNullAt(row)) {
        clearNull(group);
        accumulator->insert(decodedKeys_, decodedValues_, row, *allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();

    decodedKeys_.decode(*mapVector->mapKeys());
    decodedValueArrays_.decode(*mapVector->mapValues());

    auto* valueArrays_ = decodedValueArrays_.base()->template as<ArrayVector>();

    decodedValues_.decode(*valueArrays_->elements());

    auto accumulator = value<AccumulatorType>(group);

    VELOX_CHECK_NOT_NULL(mapVector);
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(group);
        auto decodedRow = decodedMaps_.index(row);
        auto offset = mapVector->offsetAt(decodedRow);
        auto size = mapVector->sizeAt(decodedRow);
        checkNullKeys(decodedKeys_, offset, size);
        for (auto i = offset; i < offset + size; ++i) {
          auto numValues = valueArrays_->sizeAt(decodedValueArrays_.index(i));
          auto valueOffset =
              valueArrays_->offsetAt(decodedValueArrays_.index(i));
          accumulator->insertMultiple(
              decodedKeys_,
              i,
              decodedValues_,
              valueOffset,
              numValues,
              *allocator_);
        }
      }
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);
    auto mapKeys = mapVector->mapKeys();
    auto mapValueArrays = mapVector->mapValues()->as<ArrayVector>();
    auto numKeys = countKeys(groups, numGroups);
    mapKeys->resize(numKeys);
    mapValueArrays->resize(numKeys);

    auto mapValues = mapValueArrays->elements();
    mapValues->resize(countValues(groups, numGroups));

    auto* rawNulls = getRawNulls(mapVector);
    vector_size_t keyOffset = 0;
    vector_size_t valueOffset = 0;

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];

      if (isNull(group)) {
        mapVector->setNull(i, true);
        mapVector->setOffsetAndSize(i, 0, 0);
        continue;
      }

      clearNull(rawNulls, i);

      auto accumulator = value<AccumulatorType>(group);
      auto mapSize = accumulator->size();
      if (mapSize) {
        mapVector->setOffsetAndSize(i, keyOffset, mapSize);
        accumulator->extract(mapKeys, *mapValueArrays, keyOffset, valueOffset);
      } else {
        mapVector->setOffsetAndSize(i, 0, 0);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      auto accumulator = value<AccumulatorType>(group);
      accumulator->free(*allocator_);
      destroyAccumulator<AccumulatorType>(group);
    }
  }

 private:
  static void checkNullKeys(
      const DecodedVector& keys,
      vector_size_t offset,
      vector_size_t size) {
    static const char* kNullKey = "map key cannot be null";
    if (keys.mayHaveNulls()) {
      for (auto i = offset; i < offset + size; ++i) {
        VELOX_USER_CHECK(!keys.isNullAt(i), kNullKey);
      }
    }
  }

  vector_size_t countKeys(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<AccumulatorType>(groups[i])->size();
    }
    return size;
  }

  vector_size_t countValues(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<AccumulatorType>(groups[i])->numValues();
    }
    return size;
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  DecodedVector decodedMaps_;
  DecodedVector decodedValueArrays_;
};

} // namespace

void registerMultiMapAggAggregate(const std::string& prefix) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("K")
          .typeVariable("V")
          .returnType("map(K,array(V))")
          .intermediateType("map(K,array(V))")
          .argumentType("K")
          .argumentType("V")
          .build()};

  auto name = prefix + kMultiMapAgg;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        auto typeKind = resultType->childAt(0)->kind();
        switch (typeKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<MultiMapAggAggregate<bool>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<MultiMapAggAggregate<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<MultiMapAggAggregate<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<MultiMapAggAggregate<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<MultiMapAggAggregate<int64_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<MultiMapAggAggregate<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<MultiMapAggAggregate<double>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<MultiMapAggAggregate<Timestamp>>(
                resultType);
          case TypeKind::VARBINARY:
            [[fallthrough]];
          case TypeKind::VARCHAR:
            return std::make_unique<MultiMapAggAggregate<StringView>>(
                resultType);
          case TypeKind::ARRAY:
            [[fallthrough]];
          case TypeKind::MAP:
            [[fallthrough]];
          case TypeKind::ROW:
            return std::make_unique<MultiMapAggAggregate<ComplexType>>(
                resultType);
          case TypeKind::UNKNOWN:
            return std::make_unique<MultiMapAggAggregate<int32_t>>(resultType);
          default:
            VELOX_UNREACHABLE(
                "Unexpected type {}", mapTypeKindToName(typeKind));
        }
      });
}

} // namespace facebook::velox::aggregate::prestosql
