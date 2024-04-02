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
#include "velox/exec/ContainerRowSerde.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/ValueList.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

namespace facebook::velox::aggregate::prestosql {
namespace {

struct ArrayAccumulator {
  ValueList elements;
};

class ArrayAggAggregate : public exec::Aggregate {
 public:
  explicit ArrayAggAggregate(TypePtr resultType, bool ignoreNulls)
      : Aggregate(resultType), ignoreNulls_(ignoreNulls) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ArrayAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& elements = args[0];

    const auto numRows = rows.size();

    // Convert input to a single-entry array.

    // Set nulls for rows not present in 'rows'.
    auto* pool = allocator_->pool();
    BufferPtr nulls = allocateNulls(numRows, pool);
    auto mutableNulls = nulls->asMutable<uint64_t>();
    memcpy(
        nulls->asMutable<uint64_t>(),
        rows.asRange().bits(),
        bits::nbytes(numRows));

    auto loadedElements = BaseVector::loadedVectorShared(elements);

    if (ignoreNulls_ && loadedElements->mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t row) {
        if (loadedElements->isNullAt(row)) {
          bits::setNull(mutableNulls, row);
        }
      });
    }

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
        loadedElements);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<ArrayVector>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    auto elements = vector->elements();
    elements->resize(countElements(groups, numGroups));

    uint64_t* rawNulls = getRawNulls(vector);
    vector_size_t offset = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      auto& values = value<ArrayAccumulator>(groups[i])->elements;
      auto arraySize = values.size();
      if (arraySize) {
        clearNull(rawNulls, i);

        ValueListReader reader(values);
        for (auto index = 0; index < arraySize; ++index) {
          reader.next(*elements, offset + index);
        }
        vector->setOffsetAndSize(i, offset, arraySize);
        offset += arraySize;
      } else {
        vector->setNull(i, true);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedElements_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t row) {
      if (ignoreNulls_ && decodedElements_.isNullAt(row)) {
        return;
      }
      auto group = groups[row];
      auto tracker = trackRowSize(group);
      value<ArrayAccumulator>(group)->elements.appendValue(
          decodedElements_, row, allocator_);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);

    auto arrayVector = decodedIntermediate_.base()->as<ArrayVector>();
    auto& elements = arrayVector->elements();
    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      auto decodedRow = decodedIntermediate_.index(row);
      auto tracker = trackRowSize(group);
      if (!decodedIntermediate_.isNullAt(row)) {
        value<ArrayAccumulator>(group)->elements.appendRange(
            elements,
            arrayVector->offsetAt(decodedRow),
            arrayVector->sizeAt(decodedRow),
            allocator_);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto& values = value<ArrayAccumulator>(group)->elements;

    decodedElements_.decode(*args[0], rows);
    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      if (ignoreNulls_ && decodedElements_.isNullAt(row)) {
        return;
      }
      values.appendValue(decodedElements_, row, allocator_);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto arrayVector = decodedIntermediate_.base()->as<ArrayVector>();

    auto& values = value<ArrayAccumulator>(group)->elements;
    auto elements = arrayVector->elements();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        auto decodedRow = decodedIntermediate_.index(row);
        values.appendRange(
            elements,
            arrayVector->offsetAt(decodedRow),
            arrayVector->sizeAt(decodedRow),
            allocator_);
      }
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) ArrayAccumulator();
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if (isInitialized(group)) {
        value<ArrayAccumulator>(group)->elements.free(allocator_);
      }
    }
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<ArrayAccumulator>(groups[i])->elements.size();
    }
    return size;
  }

  // A boolean representing whether to ignore nulls when aggregating inputs.
  const bool ignoreNulls_;
  // Reusable instance of DecodedVector for decoding input vectors.
  DecodedVector decodedElements_;
  DecodedVector decodedIntermediate_;
};

} // namespace

void registerArrayAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("E")
          .returnType("array(E)")
          .intermediateType("array(E)")
          .argumentType("E")
          .build()};

  auto name = prefix + kArrayAgg;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<ArrayAggAggregate>(
            resultType, config.prestoArrayAggIgnoreNulls());
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
