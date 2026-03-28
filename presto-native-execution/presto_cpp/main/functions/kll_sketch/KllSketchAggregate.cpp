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

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/type/HugeInt.h"

namespace facebook::presto::functions::aggregate {

namespace {

const char* const kKllSketch = "sketch_kll";
const char* const kKllSketchWithK = "sketch_kll_with_k";
const int kDefaultK = 200;
const int64_t kMaxK = 65535;

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
      auto sketch = datasketches::kll_sketch<T>(kDefaultK);
      sketch.update(in.value());
      auto serialized = sketch.serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
      return true;
    }
    // Return false (NULL) if no input
    return false;
  }

  struct AccumulatorType {
    std::unique_ptr<datasketches::kll_sketch<T>> sketch;

    AccumulatorType() = delete;

    explicit AccumulatorType(
        velox::HashStringAllocator* /*allocator*/,
        KllSketchAggregate* /*fn*/)
        : sketch(nullptr) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data) {
      if (data.has_value()) {
        // Lazy initialization - only create sketch when we have data
        if (!sketch) {
          sketch = std::make_unique<datasketches::kll_sketch<T>>(kDefaultK);
        }
        sketch->update(data.value());
      }
      return true;
    }

    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value() && other->size() > 0) {
        auto otherSketch = datasketches::kll_sketch<T>::deserialize(
            other->data(), other->size());
        if (!sketch) {
          sketch = std::make_unique<datasketches::kll_sketch<T>>(kDefaultK);
        }
        sketch->merge(otherSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      auto serialized = sketch->serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      // Return NULL if sketch was never initialized (no data)
      if (!sketch || !nonNullGroup) {
        return false;
      }
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      // Return NULL if sketch was never initialized (no data)
      if (!sketch) {
        return false;
      }
      return getResult(out);
    }
  };
};

// Specialization for VARCHAR (velox::StringView)
// VARCHAR is backed by StringView, not std::string
template <>
struct KllSketchAggregate<velox::StringView>::AccumulatorType {
  std::unique_ptr<datasketches::kll_sketch<std::string>> sketch;

  AccumulatorType() = delete;

  explicit AccumulatorType(
      velox::HashStringAllocator* /*allocator*/,
      KllSketchAggregate* /*fn*/)
      : sketch(nullptr) {}

  bool addInput(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::StringView> data) {
    if (data.has_value()) {
      // Lazy initialization - only create sketch when we have data
      if (!sketch) {
        sketch =
            std::make_unique<datasketches::kll_sketch<std::string>>(kDefaultK);
      }
      const auto& strView = data.value();
      sketch->update(std::string(strView.data(), strView.size()));
    }
    return true;
  }

  bool combine(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::Varbinary> other) {
    if (other.has_value() && other->size() > 0) {
      auto otherSketch = datasketches::kll_sketch<std::string>::deserialize(
          other->data(), other->size());
      if (!sketch) {
        sketch =
            std::make_unique<datasketches::kll_sketch<std::string>>(kDefaultK);
      }
      sketch->merge(otherSketch);
    }
    return true;
  }

  bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
    auto serialized = sketch->serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
    return true;
  }

  bool writeFinalResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    // Return NULL if sketch was never initialized (no data)
    if (!sketch || !nonNullGroup) {
      return false;
    }
    return getResult(out);
  }

  bool writeIntermediateResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    // Return NULL if sketch was never initialized (no data)
    if (!sketch) {
      return false;
    }
    return getResult(out);
  }
};

template <>
bool KllSketchAggregate<velox::StringView>::toIntermediate(
    velox::exec::out_type<IntermediateType>& out,
    velox::exec::optional_arg_type<velox::StringView> in) {
  if (in.has_value()) {
    auto sketch = datasketches::kll_sketch<std::string>(kDefaultK);
    const auto& strView = in.value();
    sketch.update(std::string(strView.data(), strView.size()));
    auto serialized = sketch.serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
    return true;
  }
  // Return false (NULL) if no input
  return false;
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
      int64_t kValue = k.value();
      VELOX_USER_CHECK(
          kValue >= 8 && kValue <= kMaxK,
          "k value must satisfy 8 <= k <= {}: {}",
          kMaxK,
          kValue);

      auto sketch = datasketches::kll_sketch<T>(static_cast<int>(kValue));
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
        : sketch(nullptr), k(kDefaultK) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data,
        velox::exec::optional_arg_type<int64_t> kValue) {
      // Initialize sketch on first call with k value
      if (kValue.has_value() && !sketch) {
        int64_t kInt = kValue.value();
        VELOX_USER_CHECK(
            kInt >= 8 && kInt <= kMaxK,
            "k value must satisfy 8 <= k <= {}: {}",
            kMaxK,
            kInt);
        k = static_cast<int>(kInt);
        sketch = std::make_unique<datasketches::kll_sketch<T>>(k);
      }

      // Always update if we have data and sketch is initialized
      if (data.has_value()) {
        // Initialize with default k if not yet initialized
        if (!sketch) {
          sketch = std::make_unique<datasketches::kll_sketch<T>>(kDefaultK);
        }
        sketch->update(data.value());
      }
      return true;
    }

    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value() && other->size() > 0) {
        auto otherSketch = datasketches::kll_sketch<T>::deserialize(
            other->data(), other->size());
        if (!sketch) {
          // Initialize with same k as incoming sketch
          k = otherSketch.get_k();
          sketch = std::make_unique<datasketches::kll_sketch<T>>(k);
        }
        sketch->merge(otherSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      // Return false (NULL) if sketch was never initialized (no data)
      if (!sketch) {
        return false;
      }
      auto serialized = sketch->serialize();
      out.resize(serialized.size());
      std::memcpy(out.data(), serialized.data(), serialized.size());
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      if (!nonNullGroup) {
        return false; // Return NULL for empty groups
      }
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }
  };
};

// Specialization for VARCHAR with K (velox::StringView)
template <>
struct KllSketchWithKAggregate<velox::StringView>::AccumulatorType {
  std::unique_ptr<datasketches::kll_sketch<std::string>> sketch;
  int k;

  AccumulatorType() = delete;

  explicit AccumulatorType(
      velox::HashStringAllocator* /*allocator*/,
      KllSketchWithKAggregate* /*fn*/)
      : sketch(nullptr), k(kDefaultK) {}

  bool addInput(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::StringView> data,
      velox::exec::optional_arg_type<int64_t> kValue) {
    // Initialize sketch on first call with k value
    if (kValue.has_value() && !sketch) {
      int64_t kInt = kValue.value();
      VELOX_USER_CHECK_GE(kInt, 8, "k value must be at least 8");
      VELOX_USER_CHECK_LE(kInt, kMaxK, "k value must be at most {}", kMaxK);
      k = static_cast<int>(kInt);
      sketch = std::make_unique<datasketches::kll_sketch<std::string>>(k);
    }

    // Always update if we have data and sketch is initialized
    if (data.has_value()) {
      // Initialize with default k if not yet initialized
      if (!sketch) {
        sketch =
            std::make_unique<datasketches::kll_sketch<std::string>>(kDefaultK);
      }
      const auto& strView = data.value();
      sketch->update(std::string(strView.data(), strView.size()));
    }
    return true;
  }

  bool combine(
      velox::HashStringAllocator* /*allocator*/,
      velox::exec::optional_arg_type<velox::Varbinary> other) {
    if (other.has_value() && other->size() > 0) {
      auto otherSketch = datasketches::kll_sketch<std::string>::deserialize(
          other->data(), other->size());
      if (!sketch) {
        // Initialize with same k as incoming sketch
        k = otherSketch.get_k();
        sketch = std::make_unique<datasketches::kll_sketch<std::string>>(k);
      }
      sketch->merge(otherSketch);
    }
    return true;
  }

  bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
    // Return false (NULL) if sketch was never initialized (no data)
    if (!sketch) {
      return false;
    }
    auto serialized = sketch->serialize();
    out.resize(serialized.size());
    std::memcpy(out.data(), serialized.data(), serialized.size());
    return true;
  }

  bool writeFinalResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    if (!nonNullGroup) {
      return false; // Return NULL for empty groups
    }
    return getResult(out);
  }

  bool writeIntermediateResult(
      bool nonNullGroup,
      velox::exec::out_type<velox::Varbinary>& out) {
    return getResult(out);
  }
};

template <>
bool KllSketchWithKAggregate<velox::StringView>::toIntermediate(
    velox::exec::out_type<IntermediateType>& out,
    velox::exec::optional_arg_type<velox::StringView> in,
    velox::exec::optional_arg_type<int64_t> k) {
  if (in.has_value() && k.has_value()) {
    int64_t kValue = k.value();
    VELOX_USER_CHECK_GE(kValue, 8, "k value must be at least 8");
    VELOX_USER_CHECK_LE(kValue, kMaxK, "k value must be at most {}", kMaxK);

    auto sketch =
        datasketches::kll_sketch<std::string>(static_cast<int>(kValue));
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
  std::string intermediateType = "varbinary";

  for (const auto& inputType : {"bigint", "double", "varchar", "boolean"}) {
    // Return type is kllsketch(T) where T is the input type
    std::string returnType = "kllsketch(" + std::string(inputType) + ")";
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
        VELOX_USER_CHECK_LE(
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
                  KllSketchAggregate<velox::StringView>>>(
                  step, argTypes, resultType);
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
          // For intermediate steps, extract type from resultType (kllsketch(T))
          auto kllType =
              std::dynamic_pointer_cast<const velox::RowType>(resultType);
          VELOX_USER_CHECK_NOT_NULL(kllType, "Result type must be kllsketch");
          VELOX_USER_CHECK_EQ(
              kllType->size(),
              1,
              "kllsketch must have exactly one type parameter");
          auto elementType = kllType->childAt(0);

          switch (elementType->kind()) {
            case velox::TypeKind::BIGINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<int64_t>>>(step, argTypes, resultType);
            case velox::TypeKind::DOUBLE:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<double>>>(step, argTypes, resultType);
            case velox::TypeKind::VARCHAR:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<velox::StringView>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::BOOLEAN:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchAggregate<bool>>>(step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown element type for {} aggregation {}",
                  name,
                  elementType->kindName());
          }
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
  std::string intermediateType = "varbinary";

  for (const auto& inputType : {"bigint", "double", "varchar", "boolean"}) {
    // Return type is kllsketch(T) where T is the input type
    std::string returnType = "kllsketch(" + std::string(inputType) + ")";
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
        VELOX_USER_CHECK_EQ(
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
                  KllSketchWithKAggregate<velox::StringView>>>(
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
          // For intermediate steps, extract type from resultType (kllsketch(T))
          auto kllType =
              std::dynamic_pointer_cast<const velox::RowType>(resultType);
          VELOX_USER_CHECK_NOT_NULL(kllType, "Result type must be kllsketch");
          VELOX_USER_CHECK_EQ(
              kllType->size(),
              1,
              "kllsketch must have exactly one type parameter");
          auto elementType = kllType->childAt(0);

          switch (elementType->kind()) {
            case velox::TypeKind::BIGINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<int64_t>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::DOUBLE:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<double>>>(step, argTypes, resultType);
            case velox::TypeKind::VARCHAR:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<velox::StringView>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::BOOLEAN:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  KllSketchWithKAggregate<bool>>>(step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown element type for {} aggregation {}",
                  name,
                  elementType->kindName());
          }
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::presto::functions::aggregate
