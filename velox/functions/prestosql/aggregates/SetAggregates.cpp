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
#include "velox/functions/lib/CheckNestedNulls.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T, bool ignoreNulls = false>
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
    addIntermediateResultsInt(groups, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupIntermediateResultsInt(group, rows, args, false);
  }

 protected:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  void addIntermediateResultsInt(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool clearNullForAllInputs) {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();
    decodedElements_.decode(*baseArray->elements());

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        if (clearNullForAllInputs) {
          clearNull(groups[i]);
        }
        return;
      }

      auto* group = groups[i];
      clearNull(group);

      auto tracker = trackRowSize(group);

      auto decodedIndex = decoded_.index(i);
      if constexpr (ignoreNulls) {
        value(group)->addNonNullValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      } else {
        value(group)->addValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResultsInt(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool clearNullForAllInputs) {
    decoded_.decode(*args[0], rows);

    auto baseArray = decoded_.base()->template as<ArrayVector>();

    decodedElements_.decode(*baseArray->elements());

    auto* accumulator = value(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded_.isNullAt(i)) {
        if (clearNullForAllInputs) {
          clearNull(group);
        }
        return;
      }

      clearNull(group);

      auto decodedIndex = decoded_.index(i);
      if constexpr (ignoreNulls) {
        accumulator->addNonNullValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      } else {
        accumulator->addValues(
            *baseArray, decodedIndex, decodedElements_, allocator_);
      }
    });
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) AccumulatorType(type, allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      if (isInitialized(group) && !isNull(group)) {
        value(group)->free(*allocator_);
      }
    }
  }

  DecodedVector decoded_;
  DecodedVector decodedElements_;
};

template <typename T, bool ignoreNulls = false>
class SetAggAggregate : public SetBaseAggregate<T, ignoreNulls> {
 public:
  explicit SetAggAggregate(
      const TypePtr& resultType,
      const bool throwOnNestedNulls = false)
      : SetBaseAggregate<T, ignoreNulls>(resultType),
        throwOnNestedNulls_(throwOnNestedNulls) {}

  using Base = SetBaseAggregate<T, ignoreNulls>;

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& elements = args[0];

    if (throwOnNestedNulls_) {
      DecodedVector decodedElements(*elements, rows);
      auto indices = decodedElements.indices();
      rows.applyToSelected([&](vector_size_t i) {
        velox::functions::checkNestedNulls(
            decodedElements, indices, i, throwOnNestedNulls_);
      });
    }

    const auto numRows = rows.size();

    // Convert input to a single-entry array.

    // Set nulls for rows not present in 'rows'.
    auto* pool = Base::allocator_->pool();
    BufferPtr nulls = allocateNulls(numRows, pool);
    memcpy(
        nulls->asMutable<uint64_t>(),
        rows.asRange().bits(),
        bits::nbytes(numRows));

    // Set offsets to 0, 1, 2, 3...
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::iota(rawOffsets, rawOffsets + numRows, 0);

    // Set sizes to 1.
    BufferPtr sizes = allocateSizes(numRows, pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::fill(rawSizes, rawSizes + numRows, 1);

    result = std::make_shared<ArrayVector>(
        pool,
        ARRAY(elements->type()),
        nulls,
        numRows,
        offsets,
        sizes,
        BaseVector::loadedVectorShared(elements));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decoded_.decode(*args[0], rows);
    auto indices = Base::decoded_.indices();
    rows.applyToSelected([&](vector_size_t i) {
      auto* group = groups[i];
      Base::clearNull(group);

      if (throwOnNestedNulls_) {
        velox::functions::checkNestedNulls(
            Base::decoded_, indices, i, throwOnNestedNulls_);
      }

      auto tracker = Base::trackRowSize(group);
      if constexpr (ignoreNulls) {
        Base::value(group)->addNonNullValue(
            Base::decoded_, i, Base::allocator_);
      } else {
        Base::value(group)->addValue(Base::decoded_, i, Base::allocator_);
      }
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
    auto indices = Base::decoded_.indices();
    rows.applyToSelected([&](vector_size_t i) {
      if (throwOnNestedNulls_) {
        velox::functions::checkNestedNulls(
            Base::decoded_, indices, i, throwOnNestedNulls_);
      }

      if constexpr (ignoreNulls) {
        accumulator->addNonNullValue(Base::decoded_, i, Base::allocator_);
      } else {
        accumulator->addValue(Base::decoded_, i, Base::allocator_);
      }
    });
  }

 private:
  const bool throwOnNestedNulls_;
};

template <typename T>
class SetUnionAggregate : public SetBaseAggregate<T> {
 public:
  explicit SetUnionAggregate(const TypePtr& resultType)
      : SetBaseAggregate<T>(resultType) {}

  using Base = SetBaseAggregate<T>;

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    auto arrayInput = args[0];

    if (arrayInput->mayHaveNulls()) {
      // Convert null arrays into empty arrays. set_union(<all null>) returns
      // empty array, not null.

      auto copy = BaseVector::create<ArrayVector>(
          arrayInput->type(), rows.size(), arrayInput->pool());
      copy->copy(arrayInput.get(), rows, nullptr);

      rows.applyToSelected([&](auto row) {
        if (copy->isNullAt(row)) {
          copy->setOffsetAndSize(row, 0, 0);
          copy->setNull(row, false);
        }
      });

      arrayInput = copy;
    }

    if (rows.isAllSelected()) {
      result = arrayInput;
    } else {
      auto* pool = SetBaseAggregate<T>::allocator_->pool();
      const auto numRows = rows.size();

      // Set nulls for rows not present in 'rows'.
      BufferPtr nulls = allocateNulls(numRows, pool);
      memcpy(
          nulls->asMutable<uint64_t>(),
          rows.asRange().bits(),
          bits::nbytes(numRows));

      BufferPtr indices = allocateIndices(numRows, pool);
      auto* rawIndices = indices->asMutable<vector_size_t>();
      std::iota(rawIndices, rawIndices + numRows, 0);
      result =
          BaseVector::wrapInDictionary(nulls, indices, rows.size(), arrayInput);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    // Make sure to clear null flag for the accumulators even if all inputs are
    // null. set_union(<all nulls>) returns empty array, not null.
    Base::addIntermediateResultsInt(groups, rows, args, true);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    // Make sure to clear null flag for the accumulators even if all inputs are
    // null. set_union(<all nulls>) returns empty array, not null.
    Base::addSingleGroupIntermediateResultsInt(group, rows, args, true);
  }
};

/// Returns the number of distinct non-null values in a group. This is an
/// internal function only used for testing.
template <typename T>
class CountDistinctAggregate : public SetAggAggregate<T, true> {
 public:
  explicit CountDistinctAggregate(
      const TypePtr& resultType,
      const TypePtr& inputType)
      : SetAggAggregate<T, true>(resultType, false), inputType_{inputType} {}

  using Base = SetAggAggregate<T, true>;

  bool supportsToIntermediate() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + Base::offset_)
          SetAccumulator<T>(inputType_, Base::allocator_);
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    return Base::extractValues(groups, numGroups, result);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult = (*result)->as<FlatVector<int64_t>>();
    flatResult->resize(numGroups);

    uint64_t* rawNulls = exec::Aggregate::getRawNulls(flatResult);
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      if (Base::isNull(group)) {
        Base::clearNull(rawNulls, i);
        flatResult->set(i, 0);
      } else {
        Base::clearNull(rawNulls, i);

        const auto size = Base::value(group)->size();
        flatResult->set(i, size);
      }
    }
  }

 private:
  TypePtr inputType_;
};

template <template <typename T> class Aggregate>
std::unique_ptr<exec::Aggregate> create(
    const TypePtr& inputType,
    const TypePtr& resultType) {
  switch (inputType->kind()) {
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
    case TypeKind::HUGEINT:
      VELOX_CHECK(
          inputType->isLongDecimal(),
          "Non-decimal use of HUGEINT is not supported");
      return std::make_unique<Aggregate<int128_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<Aggregate<float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<Aggregate<double>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<Aggregate<Timestamp>>(resultType);
    case TypeKind::VARBINARY:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      return std::make_unique<Aggregate<StringView>>(resultType);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return std::make_unique<Aggregate<ComplexType>>(resultType);
    default:
      VELOX_UNREACHABLE(
          "Unexpected type {}", mapTypeKindToName(inputType->kind()));
  }
}

} // namespace

void registerSetAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("array(T)")
          .argumentType("T")
          .build()};

  auto name = prefix + kSetAgg;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);

        const bool isRawInput = exec::isRawInput(step);
        const TypePtr& inputType =
            isRawInput ? argTypes[0] : argTypes[0]->childAt(0);
        const TypeKind typeKind = inputType->kind();
        const bool throwOnNestedNulls = isRawInput;

        switch (typeKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<SetAggAggregate<bool>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<SetAggAggregate<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<SetAggAggregate<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<SetAggAggregate<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<SetAggAggregate<int64_t>>(resultType);
          case TypeKind::HUGEINT:
            VELOX_CHECK(
                inputType->isLongDecimal(),
                "Non-decimal use of HUGEINT is not supported");
            return std::make_unique<SetAggAggregate<int128_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<SetAggAggregate<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<SetAggAggregate<double>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<SetAggAggregate<Timestamp>>(resultType);
          case TypeKind::VARBINARY:
            [[fallthrough]];
          case TypeKind::VARCHAR:
            return std::make_unique<SetAggAggregate<StringView>>(resultType);
          case TypeKind::ARRAY:
            [[fallthrough]];
          case TypeKind::MAP:
            [[fallthrough]];
          case TypeKind::ROW:
            return std::make_unique<SetAggAggregate<ComplexType>>(
                resultType, throwOnNestedNulls);
          default:
            VELOX_UNREACHABLE(
                "Unexpected type {}", mapTypeKindToName(typeKind));
        }
      },
      withCompanionFunctions,
      overwrite);
}

void registerSetUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .intermediateType("array(T)")
          .argumentType("array(T)")
          .build()};

  auto name = prefix + kSetUnion;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);

        return create<SetUnionAggregate>(argTypes[0]->childAt(0), resultType);
      },
      withCompanionFunctions,
      overwrite);
}

void registerCountDistinctAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("array(T)")
          .argumentType("T")
          .build()};

  auto name = prefix + "$internal$count_distinct";
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [](core::AggregationNode::Step step,
         const std::vector<TypePtr>& argTypes,
         const TypePtr& resultType,
         const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);

        const bool isRawInput = exec::isRawInput(step);
        const TypeKind typeKind =
            isRawInput ? argTypes[0]->kind() : argTypes[0]->childAt(0)->kind();

        switch (typeKind) {
          case TypeKind::BOOLEAN:
            return std::make_unique<CountDistinctAggregate<bool>>(
                resultType, argTypes[0]);
          case TypeKind::TINYINT:
            return std::make_unique<CountDistinctAggregate<int8_t>>(
                resultType, argTypes[0]);
          case TypeKind::SMALLINT:
            return std::make_unique<CountDistinctAggregate<int16_t>>(
                resultType, argTypes[0]);
          case TypeKind::INTEGER:
            return std::make_unique<CountDistinctAggregate<int32_t>>(
                resultType, argTypes[0]);
          case TypeKind::BIGINT:
            return std::make_unique<CountDistinctAggregate<int64_t>>(
                resultType, argTypes[0]);
          case TypeKind::HUGEINT:
            return std::make_unique<CountDistinctAggregate<int128_t>>(
                resultType, argTypes[0]);
          case TypeKind::REAL:
            return std::make_unique<CountDistinctAggregate<float>>(
                resultType, argTypes[0]);
          case TypeKind::DOUBLE:
            return std::make_unique<CountDistinctAggregate<double>>(
                resultType, argTypes[0]);
          case TypeKind::TIMESTAMP:
            return std::make_unique<CountDistinctAggregate<Timestamp>>(
                resultType, argTypes[0]);
          case TypeKind::VARBINARY:
            [[fallthrough]];
          case TypeKind::VARCHAR:
            return std::make_unique<CountDistinctAggregate<StringView>>(
                resultType, argTypes[0]);
          case TypeKind::ARRAY:
            [[fallthrough]];
          case TypeKind::MAP:
            [[fallthrough]];
          case TypeKind::ROW:
            return std::make_unique<CountDistinctAggregate<ComplexType>>(
                resultType, argTypes[0]);
          default:
            VELOX_UNREACHABLE(
                "Unexpected type {}", mapTypeKindToName(typeKind));
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
