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
#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/ValueList.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::aggregate {
namespace {

struct ArrayAccumulator {
  ValueList elements;
};

class ArrayAggAggregate : public exec::Aggregate {
 public:
  explicit ArrayAggAggregate(
      core::AggregationNode::Step step,
      TypePtr resultType)
      : Aggregate(step, resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ArrayAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) ArrayAccumulator();
    }
  }

  void finalize(char** groups, int32_t numGroups) override {
    for (auto i = 0; i < numGroups; i++) {
      value<ArrayAccumulator>(groups[i])->elements.finalize(allocator_);
    }
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
      clearNull(rawNulls, i);

      auto& values = value<ArrayAccumulator>(groups[i])->elements;
      auto arraySize = values.size();
      ValueListReader reader(values);
      for (auto index = 0; index < arraySize; ++index) {
        reader.next(*elements, offset + index);
      }
      vector->setOffsetAndSize(i, offset, arraySize);
      offset += arraySize;
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedElements_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      value<ArrayAccumulator>(group)->elements.appendValue(
          decodedElements_, row, allocator_);
    });
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::ARRAY);
    auto arrayVector = args[0]->as<ArrayVector>();
    auto& elements = arrayVector->elements();
    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      value<ArrayAccumulator>(group)->elements.appendRange(
          elements,
          arrayVector->offsetAt(row),
          arrayVector->sizeAt(row),
          allocator_);
    });
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto& values = value<ArrayAccumulator>(group)->elements;

    decodedElements_.decode(*args[0], rows);
    rows.applyToSelected([&](vector_size_t row) {
      values.appendValue(decodedElements_, row, allocator_);
    });
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto& values = value<ArrayAccumulator>(group)->elements;

    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::ARRAY);
    auto arrayVector = args[0]->as<ArrayVector>();
    auto elements = arrayVector->elements();
    rows.applyToSelected([&](vector_size_t row) {
      values.appendRange(
          elements,
          arrayVector->offsetAt(row),
          arrayVector->sizeAt(row),
          allocator_);
    });
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<ArrayAccumulator>(group)->elements.free(allocator_);
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

  // Reusable instance of DecodedVector for decoding input vectors.
  DecodedVector decodedElements_;
};

bool registerArrayAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto rawInput = exec::isRawInput(step);
        TypePtr returnType = rawInput ? ARRAY(argTypes[0]) : argTypes[0];
        return std::make_unique<ArrayAggAggregate>(step, returnType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerArrayAggregate(kArrayAgg);

} // namespace
} // namespace facebook::velox::aggregate
