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

#include "velox/functions/sparksql/aggregates/CollectListAggregate.h"

#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/lib/aggregates/ValueList.h"

using namespace facebook::velox::aggregate;
using namespace facebook::velox::exec;

namespace facebook::velox::functions::aggregate::sparksql {
namespace {
class CollectListAggregate {
 public:
  using InputType = Row<Generic<T1>>;

  using IntermediateType = Array<Generic<T1>>;

  using OutputType = Array<Generic<T1>>;

  /// In Spark, when all inputs are null, the output is an empty array instead
  /// of null. Therefore, in the writeIntermediateResult and writeFinalResult,
  /// we still need to output the empty element_ when the group is null. This
  /// behavior can only be achieved when the default-null behavior is disabled.
  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      exec::out_type<Array<Generic<T1>>>& out,
      exec::optional_arg_type<Generic<T1>> in) {
    if (in.has_value()) {
      out.add_item().copy_from(in.value());
      return true;
    }
    return false;
  }

  struct AccumulatorType {
    ValueList elements_;

    explicit AccumulatorType(HashStringAllocator* /*allocator*/)
        : elements_{} {}

    static constexpr bool is_fixed_size_ = false;

    bool addInput(
        HashStringAllocator* allocator,
        exec::optional_arg_type<Generic<T1>> data) {
      if (data.has_value()) {
        elements_.appendValue(data, allocator);
        return true;
      }
      return false;
    }

    bool combine(
        HashStringAllocator* allocator,
        exec::optional_arg_type<IntermediateType> other) {
      if (!other.has_value()) {
        return false;
      }
      for (auto element : other.value()) {
        elements_.appendValue(element, allocator);
      }
      return true;
    }

    bool writeIntermediateResult(
        bool /*nonNullGroup*/,
        exec::out_type<IntermediateType>& out) {
      // If the group's accumulator is null, the corresponding intermediate
      // result is an empty array.
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    bool writeFinalResult(
        bool /*nonNullGroup*/,
        exec::out_type<OutputType>& out) {
      // If the group's accumulator is null, the corresponding result is an
      // empty array.
      copyValueListToArrayWriter(out, elements_);
      return true;
    }

    void destroy(HashStringAllocator* allocator) {
      elements_.free(allocator);
    }
  };
};

AggregateRegistrationResult registerCollectList(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
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
        return std::make_unique<SimpleAggregateAdapter<CollectListAggregate>>(
            resultType);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerCollectListAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCollectList(
      prefix + "collect_list", withCompanionFunctions, overwrite);
}
} // namespace facebook::velox::functions::aggregate::sparksql
