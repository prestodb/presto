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
#include "velox/exec/Strings.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/ApproxMostFrequentStreamSummary.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
struct Accumulator {
  functions::ApproxMostFrequentStreamSummary<T, AlignedStlAllocator<T, 16>>
      summary;

  explicit Accumulator(HashStringAllocator* allocator)
      : summary(AlignedStlAllocator<T, 16>(allocator)) {}

  void insert(T value, int64_t count = 1) {
    summary.insert(value, count);
  }
};

template <>
struct Accumulator<StringView> {
  functions::ApproxMostFrequentStreamSummary<
      StringView,
      AlignedStlAllocator<StringView, 16>>
      summary;
  HashStringAllocator* allocator;
  Strings strings;

  explicit Accumulator(HashStringAllocator* allocator)
      : summary(AlignedStlAllocator<StringView, 16>(allocator)),
        allocator(allocator) {}

  ~Accumulator() {
    strings.free(*allocator);
  }

  void insert(StringView value, int64_t count = 1) {
    if (!value.isInline() && !summary.contains(value)) {
      value = strings.append(value, *allocator);
    }
    summary.insert(value, count);
  }
};

template <typename T>
struct ApproxMostFrequentAggregate : exec::Aggregate {
  explicit ApproxMostFrequentAggregate(const TypePtr& resultType)
      : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(Accumulator<T>);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) Accumulator<T>(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    destroyAccumulators<Accumulator<T>>(groups);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool) override {
    decodeArguments(rows, args);
    rows.applyToSelected([&](auto row) {
      if (!decodedValues_.isNullAt(row)) {
        auto* accumulator = initAccumulator(groups[row]);
        accumulator->insert(decodedValues_.valueAt<T>(row));
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool) override {
    addIntermediate<false>(groups, rows, args);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool) override {
    decodeArguments(rows, args);
    auto* accumulator = initAccumulator(group);
    rows.applyToSelected([&](auto row) {
      if (!decodedValues_.isNullAt(row)) {
        accumulator->insert(decodedValues_.valueAt<T>(row));
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool) override {
    addIntermediate<true>(group, rows, args);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    (*result)->resize(numGroups);
    if (buckets_ == kMissingArgument) {
      // No data has been added.
      for (int i = 0; i < numGroups; ++i) {
        VELOX_DCHECK_EQ(value<Accumulator<T>>(groups[i])->summary.size(), 0);
        (*result)->setNull(i, true);
      }
      return;
    }
    VELOX_USER_CHECK_LE(buckets_, std::numeric_limits<int>::max());
    auto mapVector = (*result)->as<MapVector>();
    auto [keys, values] = prepareFinalResult(groups, numGroups, mapVector);
    vector_size_t entryCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto* summary = &value<Accumulator<T>>(groups[i])->summary;
      int size = std::min<int>(buckets_, summary->size());
      if (size == 0) {
        mapVector->setNull(i, true);
      } else {
        summary->topK(
            buckets_,
            keys->mutableRawValues() + entryCount,
            values->mutableRawValues() + entryCount);
        if constexpr (std::is_same_v<T, StringView>) {
          // Populate the string buffers.
          for (int j = 0; j < size; ++j) {
            keys->set(entryCount + j, keys->valueAtFast(entryCount + j));
          }
        }
        entryCount += size;
      }
      mapVector->setOffsetAndSize(i, entryCount - size, size);
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVec = (*result)->as<RowVector>();
    VELOX_CHECK(rowVec);
    rowVec->childAt(0) = std::make_shared<ConstantVector<int64_t>>(
        rowVec->pool(), numGroups, false, BIGINT(), int64_t(buckets_));
    rowVec->childAt(1) = std::make_shared<ConstantVector<int64_t>>(
        rowVec->pool(), numGroups, false, BIGINT(), int64_t(capacity_));
    auto values = rowVec->childAt(2)->as<ArrayVector>();
    auto counts = rowVec->childAt(3)->as<ArrayVector>();
    rowVec->resize(numGroups);
    values->resize(numGroups);
    counts->resize(numGroups);

    auto v = values->elements()->template asFlatVector<T>();
    auto c = counts->elements()->template asFlatVector<int64_t>();
    vector_size_t entryCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto* accumulator = value<const Accumulator<T>>(groups[i]);
      entryCount += accumulator->summary.size();
    }
    v->resize(entryCount);
    c->resize(entryCount);
    v->resetNulls();
    c->resetNulls();

    entryCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto* summary = &value<const Accumulator<T>>(groups[i])->summary;
      if (summary->size() == 0) {
        rowVec->setNull(i, true);
      } else {
        if constexpr (std::is_same_v<T, StringView>) {
          for (int j = 0; j < summary->size(); ++j) {
            v->set(entryCount + j, summary->values()[j]);
          }
        } else {
          memcpy(
              v->mutableRawValues() + entryCount,
              summary->values(),
              sizeof(T) * summary->size());
        }
        memcpy(
            c->mutableRawValues() + entryCount,
            summary->counts(),
            sizeof(int64_t) * summary->size());
        values->setOffsetAndSize(i, entryCount, summary->size());
        counts->setOffsetAndSize(i, entryCount, summary->size());
        entryCount += summary->size();
      }
    }
  }

 private:
  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_CHECK_EQ(args.size(), 3);
    DecodedVector decodedBuckets(*args[0], rows);
    decodedValues_.decode(*args[1], rows);
    DecodedVector decodedCapacity(*args[2], rows);
    setConstantArgument("Buckets", buckets_, decodedBuckets);
    setConstantArgument("Capacity", capacity_, decodedCapacity);
  }

  static void
  setConstantArgument(const char* name, int64_t& val, int64_t newVal) {
    VELOX_USER_CHECK_GT(newVal, 0, "{} must be positive", name);
    if (val == kMissingArgument) {
      val = newVal;
    } else {
      VELOX_USER_CHECK_EQ(
          newVal, val, "{} argument must be constant for all input rows", name);
    }
  }

  static void setConstantArgument(
      const char* name,
      int64_t& val,
      const DecodedVector& vec) {
    VELOX_USER_CHECK(
        vec.isConstantMapping(),
        "{} argument must be constant for all input rows",
        name);
    setConstantArgument(name, val, vec.valueAt<int64_t>(0));
  }

  Accumulator<T>* initAccumulator(char* group) {
    auto accumulator = value<Accumulator<T>>(group);
    VELOX_USER_CHECK_LE(capacity_, std::numeric_limits<int>::max());
    accumulator->summary.setCapacity(capacity_);
    return accumulator;
  }

  template <bool kSingleGroup>
  void addIntermediate(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto rowVec = static_cast<const RowVector*>(decoded.base());
    auto buckets = rowVec->childAt(0)->as<SimpleVector<int64_t>>();
    auto capacity = rowVec->childAt(1)->as<SimpleVector<int64_t>>();
    auto values = rowVec->childAt(2)->as<ArrayVector>();
    auto counts = rowVec->childAt(3)->as<ArrayVector>();
    VELOX_CHECK(buckets);
    VELOX_CHECK(capacity);
    VELOX_CHECK(values);
    VELOX_CHECK(counts);

    auto v = values->elements()->template asFlatVector<T>();
    auto c = counts->elements()->template asFlatVector<int64_t>();
    VELOX_CHECK(v);
    VELOX_CHECK(c);

    Accumulator<T>* accumulator = nullptr;
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      setConstantArgument("Buckets", buckets_, buckets->valueAt(i));
      setConstantArgument("Capacity", capacity_, capacity->valueAt(i));
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initAccumulator(group);
        }
      } else {
        accumulator = initAccumulator(group[row]);
      }
      auto size = values->sizeAt(i);
      VELOX_DCHECK_EQ(counts->sizeAt(i), size);
      auto vo = values->offsetAt(i);
      auto co = counts->offsetAt(i);
      for (int j = 0; j < size; ++j) {
        accumulator->insert(v->valueAt(vo + j), c->valueAt(co + j));
      }
    });
  }

  std::pair<FlatVector<T>*, FlatVector<int64_t>*>
  prepareFinalResult(char** groups, int32_t numGroups, MapVector* result) {
    VELOX_CHECK(result);
    auto keys = result->mapKeys()->asUnchecked<FlatVector<T>>();
    auto values = result->mapValues()->asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK(keys);
    VELOX_CHECK(values);
    vector_size_t entryCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto* summary = &value<const Accumulator<T>>(groups[i])->summary;
      entryCount += std::min<int>(buckets_, summary->size());
    }
    keys->resize(entryCount);
    values->resize(entryCount);
    return std::make_pair(keys, values);
  }

  static constexpr int64_t kMissingArgument = -1;
  DecodedVector decodedValues_;
  int64_t buckets_ = kMissingArgument;
  int64_t capacity_ = kMissingArgument;
};

template <TypeKind kKind>
std::unique_ptr<exec::Aggregate> makeApproxMostFrequentAggregate(
    const TypePtr& resultType,
    const std::string& name,
    const TypePtr& valueType) {
  if constexpr (
      kKind == TypeKind::TINYINT || kKind == TypeKind::SMALLINT ||
      kKind == TypeKind::INTEGER || kKind == TypeKind::BIGINT ||
      kKind == TypeKind::VARCHAR) {
    return std::make_unique<
        ApproxMostFrequentAggregate<typename TypeTraits<kKind>::NativeType>>(
        resultType);
  } else {
    VELOX_USER_FAIL(
        "Unsupported value type for {} aggregation {}",
        name,
        valueType->toString());
  }
}

} // namespace

void registerApproxMostFrequentAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& valueType :
       {"tinyint", "smallint", "integer", "bigint", "varchar"}) {
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType(fmt::format("map({},bigint)", valueType))
            .intermediateType(fmt::format(
                "row(bigint, bigint, array({}), array(bigint))", valueType))
            .argumentType("bigint")
            .argumentType(valueType)
            .argumentType("bigint")
            .build());
  }
  auto name = prefix + kApproxMostFrequent;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>&,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        auto& valueType = exec::isPartialOutput(step)
            ? resultType->childAt(2)->childAt(0)
            : resultType->childAt(0);
        return VELOX_DYNAMIC_TYPE_DISPATCH(
            makeApproxMostFrequentAggregate,
            valueType->kind(),
            resultType,
            name,
            valueType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
