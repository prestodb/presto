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
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/prestosql/aggregates/ValueList.h"

using namespace facebook::velox::exec;

namespace facebook::velox::aggregate {

namespace {

// Write ValueList accumulators to Array-typed intermediate or final result
// vectors.
// TODO: This API only works if it is the only logic writing to `writer`.
template <typename T>
void copyValueListToArrayWriter(ArrayWriter<T>& writer, ValueList& elements) {
  writer.resetLength();
  auto size = elements.size();
  if (size == 0) {
    return;
  }
  writer.reserve(size);

  ValueListReader reader(elements);
  for (vector_size_t i = 0; i < size; ++i) {
    reader.next(*writer.elementsVector(), writer.valuesOffset() + i);
  }
  writer.resize(size);
}

class ArrayAggAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<Generic<T1>>;

  // Type of intermediate result vector.
  using IntermediateType = Array<Generic<T1>>;

  // Type of output vector.
  using OutputType = Array<Generic<T1>>;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      exec::out_type<Array<Generic<T1>>>& out,
      exec::optional_arg_type<Generic<T1>> in) {
    if (in.has_value()) {
      out.add_item().copy_from(in.value());
    } else {
      out.add_null();
    }
    return true;
  }

  struct AccumulatorType {
    ValueList elements_;

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(HashStringAllocator* /*allocator*/)
        : elements_{} {}

    static constexpr bool is_fixed_size_ = false;

    // addInput expects one parameter of exec::optional_arg_type<T> for each
    // child-type T wrapped in InputType.
    bool addInput(
        HashStringAllocator* allocator,
        exec::optional_arg_type<Generic<T1>> data) {
      elements_.appendValue(data, allocator);
      return true;
    }

    // combine expects one parameter of
    // exec::optional_arg_type<IntermediateType>.
    bool combine(
        HashStringAllocator* allocator,
        exec::optional_arg_type<Array<Generic<T1>>> other) {
      if (!other.has_value()) {
        return false;
      }
      for (auto element : other.value()) {
        elements_.appendValue(element, allocator);
      }
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        exec::out_type<Array<Generic<T1>>>& out) {
      if (!nonNullGroup) {
        return false;
      }
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        exec::out_type<Array<Generic<T1>>>& out) {
      // If the group's accumulator is null, the corresponding intermediate
      // result is null too.
      if (!nonNullGroup) {
        return false;
      }
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    void destroy(HashStringAllocator* allocator) {
      elements_.free(allocator);
    }
  };
};

} // namespace

exec::AggregateRegistrationResult registerSimpleArrayAggAggregate(
    const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("E")
          .returnType("array(E)")
          .intermediateType("array(E)")
          .argumentType("E")
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
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<SimpleAggregateAdapter<ArrayAggAggregate>>(
            resultType);
      },
      true /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

} // namespace facebook::velox::aggregate
