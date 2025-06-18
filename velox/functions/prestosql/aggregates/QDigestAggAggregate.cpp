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

#include "velox/functions/prestosql/aggregates/QDigestAggAggregate.h"

#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/QuantileDigest.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::qdigest;

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T, typename Func, bool DefaultMaxError>
struct QDigestAccumulator {
  static constexpr bool is_fixed_size_ = false;
  static constexpr bool is_aligned_ = true;

  QuantileDigest<T> digest;

  explicit QDigestAccumulator(HashStringAllocator* allocator, Func* /*fn*/)
      : digest(
            StlAllocator<T>(allocator),
            DefaultMaxError ? 0.01 : kUninitializedMaxError) {}

  void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> value) {
    digest.add(value, 1);
  }

  void addInput(
      HashStringAllocator* /*allocator*/,
      exec::arg_type<T> value,
      int64_t weight) {
    digest.add(value, static_cast<double>(weight));
  }

  void addInput(
      HashStringAllocator* /*allocator*/,
      exec::arg_type<T> value,
      int64_t weight,
      double accuracy) {
    checkSetAccuracy(accuracy);
    digest.add(value, static_cast<double>(weight));
  }

  void combine(
      HashStringAllocator* /*allocator*/,
      exec::arg_type<Varbinary> other) {
    digest.mergeSerialized(other.data());
  }

  bool writeFinalResult(exec::out_type<Varbinary>& out) {
    out.resize(digest.serializedByteSize());
    digest.serialize(out.data());
    return true;
  }

  bool writeIntermediateResult(exec::out_type<Varbinary>& out) {
    out.resize(digest.serializedByteSize());
    digest.serialize(out.data());
    return true;
  }

 private:
  void checkSetAccuracy(double accuracy) {
    VELOX_USER_CHECK(
        accuracy > 0.0 && accuracy < 1.0,
        "{}: accuracy must be in range (0.0, 1.0), got {}",
        kQDigestAgg,
        accuracy);
    if (digest.getMaxError() == kUninitializedMaxError) {
      digest.setMaxError(accuracy);
    } else if (digest.getMaxError() != accuracy) {
      VELOX_USER_FAIL(
          "{}: accuracy must be the same for all rows in the group, got {} and {}",
          kQDigestAgg,
          digest.getMaxError(),
          accuracy);
    }
  }
};

template <typename T, int NumArgs>
class QDigestAggAggregate {};

template <typename T>
class QDigestAggAggregate<T, 1> {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result vector.
  using IntermediateType = Varbinary;

  // Type of output vector.
  using OutputType = Varbinary;

  using AccumulatorType =
      QDigestAccumulator<T, QDigestAggAggregate<T, 1>, true>;
};

template <typename T>
class QDigestAggAggregate<T, 2> {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T, int64_t>;

  // Type of intermediate result vector.
  using IntermediateType = Varbinary;

  // Type of output vector.
  using OutputType = Varbinary;

  using AccumulatorType =
      QDigestAccumulator<T, QDigestAggAggregate<T, 2>, true>;
};

template <typename T>
class QDigestAggAggregate<T, 3> {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T, int64_t, double>;

  // Type of intermediate result vector.
  using IntermediateType = Varbinary;

  // Type of output vector.
  using OutputType = Varbinary;

  using AccumulatorType =
      QDigestAccumulator<T, QDigestAggAggregate<T, 3>, false>;
};

template <int NumArgs>
std::unique_ptr<exec::Aggregate> createQDigestAggAggregate(
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType) {
  VELOX_CHECK_GE(argTypes.size(), 1);
  if (argTypes[0]->kind() == TypeKind::BIGINT) {
    return std::make_unique<
        SimpleAggregateAdapter<QDigestAggAggregate<int64_t, NumArgs>>>(
        step, argTypes, resultType);
  } else if (argTypes[0]->kind() == TypeKind::REAL) {
    return std::make_unique<
        SimpleAggregateAdapter<QDigestAggAggregate<float, NumArgs>>>(
        step, argTypes, resultType);
  } else if (argTypes[0]->kind() == TypeKind::DOUBLE) {
    return std::make_unique<
        SimpleAggregateAdapter<QDigestAggAggregate<double, NumArgs>>>(
        step, argTypes, resultType);
  }
  VELOX_UNREACHABLE("Unsupported argument type: {}", argTypes.size());
}

} // namespace

void registerQDigestAggAggregate(const std::string& prefix, bool overwrite) {
  registerQDigestType();

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& type : {"bigint", "real", "double"}) {
    const auto digestType = fmt::format("qdigest({})", type);
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(digestType)
                             .intermediateType(digestType)
                             .argumentType(type)
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(digestType)
                             .intermediateType(digestType)
                             .argumentType(type)
                             .argumentType("bigint")
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(digestType)
                             .intermediateType(digestType)
                             .argumentType(type)
                             .argumentType("bigint")
                             .argumentType("double")
                             .build());
  }

  auto name = prefix + kQDigestAgg;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (argTypes.size() == 1) {
          return createQDigestAggAggregate<1>(step, argTypes, resultType);
        } else if (argTypes.size() == 2) {
          return createQDigestAggAggregate<2>(step, argTypes, resultType);
        } else if (argTypes.size() == 3) {
          return createQDigestAggAggregate<3>(step, argTypes, resultType);
        } else {
          VELOX_UNREACHABLE(
              "Unexpected number of arguments: {}", argTypes.size());
        }
      },
      false /*registerCompanionFunctions*/,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
