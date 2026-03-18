/*
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

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"

#include "DataSketches/kll_sketch.hpp"

#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/type/HugeInt.h"

namespace facebook::presto::functions::aggregate {

namespace {

const char* const kKllSketch = "sketch_kll";
const char* const kKllSketchWithK = "sketch_kll_with_k";
const int DEFAULT_K = 200;
const int MAX_K = 65535;

template <typename T>
class KllSketchAggregate {
 public:
  using InputType = velox::Row<T>;
  using IntermediateType = velox::Varbinary;
  using OutputType = velox::Varbinary;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      velox::exec::out_type<IntermediateType>& out,
      velox::exec::optional_arg_type<T> in) {
    if (in.has_value()) {
      auto sketch = datasketches::kll_sketch<T>(DEFAULT_K);
      sketch.update(in.value());
      auto serialized = sketch.serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
    }
    return true;
  }

  struct AccumulatorType {
    datasketches::kll_sketch<T> sketch;

    AccumulatorType() = delete;

    explicit AccumulatorType(
        velox::HashStringAllocator* /*allocator*/,
        KllSketchAggregate* /*fn*/)
        : sketch(DEFAULT_K) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data) {
      if (data.has_value()) {
        sketch.update(data.value());
      }
      return true;
    }

    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value()) {
        auto otherSketch = datasketches::kll_sketch<T>::deserialize(
            other->data(), other->size());
        sketch.merge(otherSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      auto serialized = sketch.serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }
  };
};

// Specialization for VARCHAR (std::string)
template <>
struct KllSketchAggregate<std::string>::AccumulatorType {
  datasketches::kll_sketch<std::string> sketch;

  AccumulatorType() = delete;

  explicit AccumulatorType(
      velox::HashStringAllocator* /*allocator*/,
      KllSketchAggregate* /*fn*/)
      : sketch(DEFAULT_K) {}

  bool addInput(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<std::string> data) {
    if (data.has_value()) {
      const auto& strView = data.value();
      sketch.update(std::string(strView.data(), strView.size()));
    }
    return true;
  }

  bool combine(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::Varbinary> other) {
    if (other.has_value()) {
      auto otherSketch = datasketches::kll_sketch<std::string>::deserialize(
          other->data(), other->size());
      sketch.merge(otherSketch);
    }
    return true;
  }

  bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
    auto serialized = sketch.serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
    return true;
  }

  bool writeFinalResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    return getResult(out);
  }

  bool writeIntermediateResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    return getResult(out);
  }
};

template <>
bool KllSketchAggregate<std::string>::toIntermediate(
    velox::exec::out_type<IntermediateType>& out,
    velox::exec::optional_arg_type<std::string> in) {
  if (in.has_value()) {
    auto sketch = datasketches::kll_sketch<std::string>(DEFAULT_K);
    const auto& strView = in.value();
    sketch.update(std::string(strView.data(), strView.size()));
    auto serialized = sketch.serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
  }
  return true;
}

template <typename T>
class KllSketchWithKAggregate {
 public:
  using InputType = velox::Row<T, int64_t>;
  using IntermediateType = velox::Varbinary;
  using OutputType = velox::Varbinary;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      velox::exec::out_type<IntermediateType>& out,
      velox::exec::optional_arg_type<T> in,
      velox::exec::optional_arg_type<int64_t> k) {
    if (in.has_value() && k.has_value()) {
      int kValue = static_cast<int>(k.value());
      VELOX_CHECK_GE(kValue, 8, "k value must be at least 8");
      VELOX_CHECK_LE(kValue, MAX_K, "k value must be at most {}", MAX_K);

      auto sketch = datasketches::kll_sketch<T>(kValue);
      sketch.update(in.value());
      auto serialized = sketch.serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
    }
    return true;
  }

  struct AccumulatorType {
    std::unique_ptr<datasketches::kll_sketch<T>> sketch;
    int k;

    AccumulatorType() = delete;

    explicit AccumulatorType(
        velox::HashStringAllocator* /*allocator*/,
        KllSketchWithKAggregate* /*fn*/)
        : sketch(nullptr), k(DEFAULT_K) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data,
        velox::exec::optional_arg_type<int64_t> kValue) {
      if (kValue.has_value()) {
        int kInt = static_cast<int>(kValue.value());
        VELOX_CHECK_GE(kInt, 8, "k value must be at least 8");
        VELOX_CHECK_LE(kInt, MAX_K, "k value must be at most {}", MAX_K);

        if (!sketch) {
          k = kInt;
          sketch = std::make_unique<datasketches::kll_sketch<T>>(k);
        }
      }

      if (data.has_value() && sketch) {
        sketch->update(data.value());
      }
      return true;
    }

    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value()) {
        auto otherSketch = datasketches::kll_sketch<T>::deserialize(
            other->data(), other->size());
        if (!sketch) {
          sketch = std::make_unique<datasketches::kll_sketch<T>>(
              otherSketch.get_k());
        }
        sketch->merge(otherSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      if (sketch) {
        auto serialized = sketch->serialize();
        out.resize(serialized.size());
        std::memcpy(out.data(), serialized.data(), serialized.size());
      }
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }
  };
};

// Specialization for VARCHAR with K
template <>
struct KllSketchWithKAggregate<std::string>::AccumulatorType {
  std::unique_ptr<datasketches::kll_sketch<std::string>> sketch;
  int k;

  AccumulatorType() = delete;

  explicit AccumulatorType(
      velox::HashStringAllocator* /*allocator*/,
      KllSketchWithKAggregate* /*fn*/)
      : sketch(nullptr), k(DEFAULT_K) {}

  bool addInput(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<std::string> data,
      velox::exec::optional_arg_type<int64_t> kValue) {
    if (kValue.has_value()) {
      int kInt = static_cast<int>(kValue.value());
      VELOX_CHECK_GE(kInt, 8, "k value must be at least 8");
      VELOX_CHECK_LE(kInt, MAX_K, "k value must be at most {}", MAX_K);

      if (!sketch) {
        k = kInt;
        sketch = std::make_unique<datasketches::kll_sketch<std::string>>(k);
      }
    }

    if (data.has_value() && sketch) {
      const auto& strView = data.value();
      sketch->update(std::string(strView.data(), strView.size()));
    }
    return true;
  }

  bool combine(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::Varbinary> other) {
    if (other.has_value()) {
      auto otherSketch = datasketches::kll_sketch<std::string>::deserialize(
          other->data(), other->size());
      if (!sketch) {
        sketch = std::make_unique<datasketches::kll_sketch<std::string>>(
            otherSketch.get_k());
      }
      sketch->merge(otherSketch);
    }
    return true;
  }

  bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
    if (sketch) {
      auto serialized = sketch->serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
    }
    return true;
  }

  bool writeFinalResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    return getResult(out);
  }

  bool writeIntermediateResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    return getResult(out);
  }
};

template <>
bool KllSketchWithKAggregate<std::string>::toIntermediate(
    velox::exec::out_type<IntermediateType>& out,
    velox::exec::optional_arg_type<std::string> in,
    velox::exec::optional_arg_type<int64_t> k) {
  if (in.has_value() && k.has_value()) {
    int kValue = static_cast<int>(k.value());
    VELOX_CHECK_GE(kValue, 8, "k value must be at least 8");
    VELOX_CHECK_LE(kValue, MAX_K, "k value must be at most {}", MAX_K);

    auto sketch = datasketches::kll_sketch<std::string>(kValue);
    const auto& strView = in.value();
    sketch.update(std::string(strView.data(), strView.size()));
    auto serialized = sketch.serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
  }
  return true;
}

} // namespace

velox::exec::AggregateRegistrationResult registerKllSketchAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures;
  std::string returnType = "varbinary";
  std::string intermediateType = "varbinary";

  for (const auto& inputType : {"bigint", "double", "varchar", "boolean"}) {
    signatures.push_back(
        velox::exec::AggregateFunctionSignatureBuilder()
            .returnType(returnType)
            .intermediateType(intermediateType)
            .argumentType(inputType)
            .build());
  }

  auto name = prefix + kKllSketch;

  return velox::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          velox::core::AggregationNode::Step step,
          const std::vector<velox::TypePtr>& argTypes,
          const velox::TypePtr& resultType,
          const velox::core::QueryConfig& /*config*/)
          -> std::unique_ptr<velox::exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        if (velox::exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case velox::TypeKind::BIGINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<int64_t>>>(step, argTypes, resultType);
            case velox::TypeKind::DOUBLE:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<double>>>(step, argTypes, resultType);
            case velox::TypeKind::VARCHAR:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<std::string>>>(step, argTypes, resultType);
            case velox::TypeKind::BOOLEAN:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<bool>>>(step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          // For intermediate steps, we always deserialize from varbinary
          // The type doesn't matter since we're just passing serialized data
          return std::make_unique<
              velox::exec::SimpleAggregateAdapter<KllSketchAggregate<double>>>(
              step, argTypes, resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

velox::exec::AggregateRegistrationResult registerKllSketchWithKAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures;
  std::string returnType = "varbinary";
  std::string intermediateType = "varbinary";

  for (const auto& inputType : {"bigint", "double", "varchar", "boolean"}) {
    signatures.push_back(
        velox::exec::AggregateFunctionSignatureBuilder()
            .returnType(returnType)
            .intermediateType(intermediateType)
            .argumentType(inputType)
            .argumentType("bigint")
            .build());
  }

  auto name = prefix + kKllSketchWithK;

  return velox::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          velox::core::AggregationNode::Step step,
          const std::vector<velox::TypePtr>& argTypes,
          const velox::TypePtr& resultType,
          const velox::core::QueryConfig& /*config*/)
          -> std::unique_ptr<velox::exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 2, "{} takes exactly two arguments", name);
        auto inputType = argTypes[0];
        if (velox::exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case velox::TypeKind::BIGINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<int64_t>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::DOUBLE:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<double>>>(step, argTypes, resultType);
            case velox::TypeKind::VARCHAR:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<std::string>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::BOOLEAN:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<bool>>>(step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          return std::make_unique<velox::exec::SimpleAggregateAdapter<
              KllSketchWithKAggregate<double>>>(step, argTypes, resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::presto::functions::aggregate
