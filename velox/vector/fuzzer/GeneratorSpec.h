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

#pragma once

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorTypeUtils.h"
#include "velox/vector/fuzzer/Utils.h"

namespace facebook::velox {

using namespace generator_spec_utils;

class GeneratorSpec {
  // Blueprint for generating a Velox vector of random data
 public:
  explicit GeneratorSpec(const TypePtr& type, double nullProbability)
      : type_(type), nullProbability_(nullProbability) {}

  VectorPtr generateData(
      FuzzerGenerator& rng,
      memory::MemoryPool* pool,
      size_t vectorLength = 100) const {
    auto randomVector = generateDataImpl(rng, pool, vectorLength);
    auto nullsBuffer =
        generateNullsBuffer(rng, pool, vectorLength, nullProbability_);
    randomVector->setNulls(nullsBuffer);
    return randomVector;
  }

  virtual ~GeneratorSpec() {}

  const TypePtr& type() const {
    return type_;
  }

 protected:
  virtual VectorPtr generateDataImpl(
      FuzzerGenerator&,
      memory::MemoryPool*,
      size_t vectorLength) const = 0;

  TypePtr type_;
  double nullProbability_;
};

using GeneratorSpecPtr = std::shared_ptr<const GeneratorSpec>;

template <TypeKind KIND, typename Distribution>
class ScalarGeneratorSpec : public GeneratorSpec {
 public:
  ScalarGeneratorSpec(
      TypePtr type,
      Distribution&& distribution,
      double nullProbability)
      : GeneratorSpec(type, nullProbability),
        distribution_(std::forward<Distribution>(distribution)) {
    using TCpp = typename TypeTraits<KIND>::NativeType;
    using Ret = std::result_of_t<Distribution(FuzzerGenerator&)>;
    static_assert(std::is_convertible_v<Ret, TCpp>);
  }

  ~ScalarGeneratorSpec() {}

 protected:
  VectorPtr generateDataImpl(
      FuzzerGenerator& rng,
      memory::MemoryPool* pool,
      size_t vectorSize) const override {
    using TFlat = typename KindToFlatVector<KIND>::type;
    VectorPtr vector = BaseVector::create(type_, vectorSize, pool);
    auto flatVector = vector->as<TFlat>();
    for (size_t i = 0; i < vectorSize; ++i) {
      flatVector->set(i, distribution_(rng));
    }
    return vector;
  }

 private:
  Distribution distribution_;
};

class RowGeneratorSpec : public GeneratorSpec {
 public:
  RowGeneratorSpec(
      TypePtr type,
      std::vector<GeneratorSpecPtr>&& generatorSpecVector,
      double nullProbability)
      : GeneratorSpec(type, nullProbability),
        children_(std::move(generatorSpecVector)) {}

  ~RowGeneratorSpec() {}

 protected:
  VectorPtr generateDataImpl(
      FuzzerGenerator& rng,
      memory::MemoryPool* pool,
      size_t vectorSize) const override {
    std::vector<VectorPtr> children;
    for (auto child : children_) {
      children.push_back(child->generateData(rng, pool, vectorSize));
    }
    auto rowType = std::dynamic_pointer_cast<const RowType>(type_);
    return std::make_shared<RowVector>(
        pool, rowType, nullptr, vectorSize, std::move(children));
  }

 private:
  std::vector<GeneratorSpecPtr> children_;
};

template <typename Distribution>
class ArrayGeneratorSpec : public GeneratorSpec {
 public:
  ArrayGeneratorSpec(
      TypePtr type,
      GeneratorSpecPtr elements,
      Distribution&& lengthDistribution,
      double nullProbability)
      : GeneratorSpec(type, nullProbability),
        elements_(elements),
        lengthDistribution_(std::forward<Distribution>(lengthDistribution)) {
    using Ret = std::result_of_t<Distribution(FuzzerGenerator&)>;
    static_assert(std::is_convertible_v<Ret, vector_size_t>);
  }

  ~ArrayGeneratorSpec() {}

 protected:
  VectorPtr generateDataImpl(
      FuzzerGenerator& rng,
      memory::MemoryPool* pool,
      size_t vectorSize) const override {
    auto offsets = allocateOffsets(vectorSize, pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto sizes = allocateSizes(vectorSize, pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();
    vector_size_t numElements = 0;

    // Randomly creates container size.
    for (auto i = 0; i < vectorSize; ++i) {
      rawOffsets[i] = numElements;
      vector_size_t length = lengthDistribution_(rng);
      rawSizes[i] = length;
      numElements += length;
    }
    VectorPtr elementsVector = elements_->generateData(rng, pool, numElements);
    return std::make_shared<ArrayVector>(
        pool, type_, nullptr, vectorSize, offsets, sizes, elementsVector);
  }

 private:
  GeneratorSpecPtr elements_;
  Distribution lengthDistribution_;
};

template <typename Distribution>
class MapGeneratorSpec : public GeneratorSpec {
 public:
  MapGeneratorSpec(
      TypePtr type,
      GeneratorSpecPtr keys,
      GeneratorSpecPtr values,
      Distribution&& lengthDistribution,
      double nullProbability)
      : GeneratorSpec(type, nullProbability),
        keys_(keys),
        values_(values),
        lengthDistribution_(std::forward<Distribution>(lengthDistribution)) {
    using Ret = std::result_of_t<Distribution(FuzzerGenerator&)>;
    static_assert(std::is_convertible_v<Ret, vector_size_t>);
  }

  ~MapGeneratorSpec() {}

 protected:
  VectorPtr generateDataImpl(
      FuzzerGenerator& rng,
      memory::MemoryPool* pool,
      size_t vectorSize) const override {
    auto offsets = allocateOffsets(vectorSize, pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto sizes = allocateSizes(vectorSize, pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();
    vector_size_t childSize = 0;

    // Randomly creates container size.
    for (auto i = 0; i < vectorSize; ++i) {
      rawOffsets[i] = childSize;
      auto length = lengthDistribution_(rng);
      rawSizes[i] = length;
      childSize += length;
    }
    VectorPtr keys = keys_->generateData(rng, pool, childSize);
    VectorPtr values = values_->generateData(rng, pool, childSize);
    return std::make_shared<MapVector>(
        pool, type_, nullptr, vectorSize, offsets, sizes, keys, values);
  }

 private:
  GeneratorSpecPtr keys_;
  GeneratorSpecPtr values_;
  Distribution lengthDistribution_;
};

namespace generator_spec_maker {

#ifdef DEFINE_RANDOM_SCALAR_FACTORY
#error "Macro name collision: DEFINE_RANDOM_SCALAR_FACTORY"
#endif

#define DEFINE_RANDOM_SCALAR_FACTORY(FACTORY_NAME, KIND)                    \
  template <typename Distribution>                                          \
  inline std::shared_ptr<                                                   \
      const ScalarGeneratorSpec<TypeKind::KIND, Distribution>>              \
  FACTORY_NAME(Distribution&& distribution, double nullProbability = 0.0) { \
    return std::make_shared<                                                \
        const ScalarGeneratorSpec<TypeKind::KIND, Distribution>>(           \
        KIND(), std::forward<Distribution>(distribution), nullProbability); \
  }

DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_BOOLEAN, BOOLEAN)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_TINYINT, TINYINT)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_SMALLINT, SMALLINT)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_INTEGER, INTEGER)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_BIGINT, BIGINT)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_REAL, REAL)
DEFINE_RANDOM_SCALAR_FACTORY(RANDOM_DOUBLE, DOUBLE)

#undef DEFINE_RANDOM_SCALAR_FACTORY

inline GeneratorSpecPtr RANDOM_ROW(
    std::vector<GeneratorSpecPtr>&& generatorSpecVector,
    double nullProbability = 0.0) {
  std::vector<TypePtr> types;
  for (auto generatorSpec : generatorSpecVector) {
    types.push_back(generatorSpec->type());
  }
  auto rowType = ROW(std::move(types));
  return std::make_shared<const RowGeneratorSpec>(
      rowType, std::move(generatorSpecVector), nullProbability);
}

template <typename Distribution>
inline GeneratorSpecPtr RANDOM_ARRAY(
    GeneratorSpecPtr generatorSpec,
    Distribution&& distribution,
    double nullProbability = 0.0) {
  auto arrayType = ARRAY(generatorSpec->type());
  return std::make_shared<const ArrayGeneratorSpec<Distribution>>(
      arrayType,
      generatorSpec,
      std::forward<Distribution>(distribution),
      nullProbability);
}

template <typename Distribution>
inline GeneratorSpecPtr RANDOM_MAP(
    GeneratorSpecPtr keys,
    GeneratorSpecPtr values,
    Distribution&& distribution,
    double nullProbability = 0.0) {
  auto mapType = MAP(keys->type(), values->type());
  return std::make_shared<const MapGeneratorSpec<Distribution>>(
      mapType,
      keys,
      values,
      std::forward<Distribution>(distribution),
      nullProbability);
}

} // namespace generator_spec_maker

} // namespace facebook::velox
