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
#include "presto_cpp/main/functions/kll_sketch/KllSketchTypeTraits.h"
#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

#include "DataSketches/kll_sketch.hpp"

namespace facebook::presto::functions::aggregate {

namespace {

const char* const kKllSketch = "sketch_kll";
const char* const kKllSketchWithK = "sketch_kll_with_k";

// K parameter controls the accuracy and size of the KLL sketch.
// Higher k values provide better accuracy but use more memory.
// k=200 provides ~1.65% error at 99% confidence, which is a good balance
// between accuracy and memory usage for most use cases.
// This matches the default used in the Java implementation.
constexpr int kDefaultK{200};

// Minimum and maximum allowed k values, as defined by the DataSketches library.
// Values must satisfy: kMinK <= k <= kMaxK
constexpr int64_t kMinK{8};
constexpr int64_t kMaxK{65535};

// Helper function to serialize a sketch into a Varbinary output.
// This encapsulates the common pattern of serializing and copying sketch data.
template <typename SketchType>
void serializeSketch(
    const SketchType* sketch,
    velox::exec::out_type<velox::Varbinary>& out) {
  auto serialized = sketch->serialize();
  out.resize(serialized.size());
  std::memcpy(out.data(), serialized.data(), serialized.size());
}

using kll_sketch::SketchTypeMapper;

// Base template for KLL sketch aggregates, parameterized by:
// - T: The input type (e.g., int64_t, double, velox::StringView)
// - WithK: Whether the aggregate accepts a k parameter
template <typename T, bool WithK>
struct KllSketchAggregateBase {
  using IntermediateType = velox::Varbinary;
  using OutputType = velox::Varbinary;
  using SketchType = typename SketchTypeMapper<T>::type;

  static constexpr bool default_null_behavior_ = false;

  struct AccumulatorType {
    std::unique_ptr<datasketches::kll_sketch<SketchType>> sketch;

    typename std::conditional<WithK, int, std::monostate>::type k_storage;

    AccumulatorType() = delete;

    explicit AccumulatorType(
        velox::HashStringAllocator* /*allocator*/,
        KllSketchAggregateBase* /*fn*/)
        : sketch(nullptr), k_storage(initKStorage()) {}

    static auto initKStorage() {
      if constexpr (WithK) {
        return kDefaultK;
      } else {
        return std::monostate{};
      }
    }

    int getK() const {
      if constexpr (WithK) {
        return k_storage;
      } else {
        return kDefaultK;
      }
    }

    void setK(int k) {
      if constexpr (WithK) {
        k_storage = k;
      }
    }

    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value() && other->size() > 0) {
        auto otherSketch = datasketches::kll_sketch<SketchType>::deserialize(
            other->data(), other->size());
        if (!sketch) {
          setK(otherSketch.get_k());
          sketch = std::make_unique<datasketches::kll_sketch<SketchType>>(
              otherSketch.get_k());
        } else {
          VELOX_USER_CHECK(
              otherSketch.get_k() == sketch->get_k(),
              "Cannot merge KLL sketches with different k values. "
              "Expected k={}, got k={}",
              sketch->get_k(),
              otherSketch.get_k());
        }
        sketch->merge(otherSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      if (!sketch) {
        return false;
      }
      serializeSketch(sketch.get(), out);
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      if (!sketch || !nonNullGroup) {
        return false;
      }
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      // Return NULL if sketch was never initialized (no data)
      // Note: nonNullGroup is intentionally not checked here because
      // intermediate results must always propagate sketches for merging in
      // distributed aggregation
      if (!sketch) {
        return false;
      }
      return getResult(out);
    }
  };
};

// Specialization for aggregates without k parameter
template <typename T>
struct KllSketchAggregate : KllSketchAggregateBase<T, false> {
  using InputType = velox::Row<T>;
  using Base = KllSketchAggregateBase<T, false>;
  using Base::default_null_behavior_;
  using typename Base::IntermediateType;
  using typename Base::OutputType;
  using typename Base::SketchType;

  static bool toIntermediate(
      velox::exec::out_type<IntermediateType>& out,
      velox::exec::optional_arg_type<T> in) {
    if (in.has_value()) {
      auto sketch = datasketches::kll_sketch<SketchType>(kDefaultK);
      sketch.update(std::move(SketchTypeMapper<T>::toSketchType(in.value())));
      serializeSketch(&sketch, out);
      return true;
    }
    return false;
  }

  struct AccumulatorType : Base::AccumulatorType {
    using BaseAccumulator = typename Base::AccumulatorType;

    explicit AccumulatorType(
        velox::HashStringAllocator* allocator,
        KllSketchAggregate* fn)
        : BaseAccumulator(allocator, fn) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data) {
      if (data.has_value()) {
        // Lazy initialization - only create sketch when we have data
        if (!this->sketch) {
          this->sketch =
              std::make_unique<datasketches::kll_sketch<SketchType>>(kDefaultK);
        }
        this->sketch->update(
            std::move(SketchTypeMapper<T>::toSketchType(data.value())));
      }
      return true;
    }
  };
};

// Specialization for aggregates with k parameter
template <typename T>
struct KllSketchWithKAggregate : KllSketchAggregateBase<T, true> {
  using InputType = velox::Row<T, int64_t>;
  using Base = KllSketchAggregateBase<T, true>;
  using Base::default_null_behavior_;
  using typename Base::IntermediateType;
  using typename Base::OutputType;
  using typename Base::SketchType;

  static bool toIntermediate(
      velox::exec::out_type<IntermediateType>& out,
      velox::exec::optional_arg_type<T> in,
      velox::exec::optional_arg_type<int64_t> k) {
    if (in.has_value() && k.has_value()) {
      int64_t kValue = k.value();
      VELOX_USER_CHECK(
          kValue >= kMinK && kValue <= kMaxK,
          "k value must satisfy {} <= k <= {}",
          kMinK,
          kMaxK);

      auto sketch =
          datasketches::kll_sketch<SketchType>(static_cast<int>(kValue));
      sketch.update(std::move(SketchTypeMapper<T>::toSketchType(in.value())));
      serializeSketch(&sketch, out);
      return true;
    }
    return false;
  }

  struct AccumulatorType : Base::AccumulatorType {
    using BaseAccumulator = typename Base::AccumulatorType;

    explicit AccumulatorType(
        velox::HashStringAllocator* allocator,
        KllSketchWithKAggregate* fn)
        : BaseAccumulator(allocator, fn) {}

    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data,
        velox::exec::optional_arg_type<int64_t> kValue) {
      if (!data.has_value()) {
        return true;
      }

      VELOX_USER_CHECK(
          kValue.has_value(),
          "k parameter cannot be NULL for sketch_kll_with_k");

      int64_t kInt = kValue.value();

      if (!this->sketch) {
        VELOX_USER_CHECK(
            kInt >= kMinK && kInt <= kMaxK,
            "k value must satisfy {} <= k <= {}",
            kMinK,
            kMaxK);
        this->setK(static_cast<int>(kInt));
        this->sketch = std::make_unique<datasketches::kll_sketch<SketchType>>(
            this->getK());
      } else {
        VELOX_USER_CHECK(
            kInt == this->getK(),
            "k parameter must be constant within a group. Expected {}, got {}",
            this->getK(),
            kInt);
      }

      this->sketch->update(
          std::move(SketchTypeMapper<T>::toSketchType(data.value())));
      return true;
    }
  };
};

} // namespace

velox::exec::AggregateRegistrationResult registerKllSketchAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures;
  std::string intermediateType = "varbinary";

  for (const auto& inputType : {"bigint", "double", "varchar", "boolean"}) {
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
        VELOX_USER_CHECK_EQ(
            argTypes.size(), 1, "{} takes exactly one argument", name);
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
