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

#include "velox/expression/tests/VectorFuzzer.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

namespace {

// Generate random values for the different supported types.
template <typename T>
T rand(folly::Random::DefaultGenerator&) {
  VELOX_NYI();
}

template <>
int8_t rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int16_t rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int32_t rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int64_t rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
double rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::randDouble01(rng);
}

template <>
float rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::randDouble01(rng);
}

template <>
bool rand(folly::Random::DefaultGenerator& rng) {
  return folly::Random::oneIn(2, rng);
}

template <>
Timestamp rand(folly::Random::DefaultGenerator& rng) {
  return Timestamp(folly::Random::rand32(rng), folly::Random::rand32(rng));
}

// TODO: Properly randomize this.
template <>
StringView rand(folly::Random::DefaultGenerator& rng) {
  return StringView("my_str");
}

template <TypeKind kind>
variant randVariantImpl(folly::Random::DefaultGenerator& rng) {
  using TCpp = typename TypeTraits<kind>::NativeType;
  return variant(rand<TCpp>(rng));
}

variant randVariant(const TypePtr& arg, folly::Random::DefaultGenerator& rng) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(randVariantImpl, arg->kind(), rng);
}

template <TypeKind kind>
void fuzzFlatImpl(
    const VectorPtr& vector,
    folly::Random::DefaultGenerator& rng) {
  using TFlat = typename KindToFlatVector<kind>::type;
  using TCpp = typename TypeTraits<kind>::NativeType;

  auto flatVector = vector->as<TFlat>();
  auto* rawValues = flatVector->mutableRawValues();

  for (size_t i = 0; i < vector->size(); ++i) {
    if constexpr (std::is_same_v<TCpp, bool>) {
      bits::setBit(rawValues, i, rand<TCpp>(rng));
    } else {
      // TODO: Handle StringView buffers.
      rawValues[i] = rand<TCpp>(rng);
    }
  }
}

} // namespace

VectorPtr VectorFuzzer::fuzz(const TypePtr& type, memory::MemoryPool* pool) {
  VectorPtr vector;

  // One in 5 chance of adding a constant vector.
  if (oneIn(5)) {
    // One in 5 chance of adding a NULL constant vector.
    if (oneIn(5)) {
      vector = BaseVector::createNullConstant(type, batchSize_, pool);
    } else {
      vector =
          BaseVector::createConstant(randVariant(type, rng_), batchSize_, pool);
    }
  } else {
    vector = fuzzFlat(type, pool);
  }

  // Toss a coin and add dictionary indirections.
  while (oneIn(2)) {
    vector = fuzzDictionary(vector, pool);
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzFlat(
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto vector = BaseVector::create(type, batchSize_, pool);

  // First, fill it with random values.
  // TODO: We should bias towards edge cases (min, max, Nan, etc).
  auto kind = vector->typeKind();
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(fuzzFlatImpl, kind, vector, rng_);

  // Second, generate a random null vector.
  for (size_t i = 0; i < vector->size(); ++i) {
    // 1 in 10 chance of setting one element as null.
    if (oneIn(10)) {
      vector->setNull(i, true);
    }
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzDictionary(
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  const size_t vectorSize = vector->size();
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(vectorSize, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < vectorSize; ++i) {
    rawIndices[i] = rand<vector_size_t>(rng_) % vectorSize;
  }

  // TODO: We can fuzz nulls here as well.
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, vectorSize, vector);
}

} // namespace facebook::velox
