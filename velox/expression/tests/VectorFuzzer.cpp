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

constexpr folly::StringPiece kAsciiChars{
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};

// TODO: Improve the random utf8 char generation.
constexpr folly::StringPiece kUtf8Chars{
    u8"0123456789\u0041\u0042\u0043\u0044\u0045\u0046\u0047\u0048"
    "\u0049\u0050\u0051\u0052\u0053\u0054\u0056\u0057"};

/// Generates a random string (string size and encoding are passed through
/// Options). Returns a StringView which uses `buf` as the underlying buffer.
StringView randString(
    folly::Random::DefaultGenerator& rng,
    const VectorFuzzer::Options& opts,
    std::string& buf) {
  const size_t stringLength = opts.stringVariableLength
      ? folly::Random::rand32(rng) % opts.stringLength
      : opts.stringLength;
  buf.resize(stringLength);
  auto chars = opts.stringUtf8 ? kUtf8Chars : kAsciiChars;

  for (size_t i = 0; i < stringLength; ++i) {
    buf[i] = chars[folly::Random::rand32(rng) % chars.size()];
  }
  return StringView(buf);
}

template <TypeKind kind>
variant randVariantImpl(
    folly::Random::DefaultGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TCpp = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<TCpp, StringView>) {
    std::string buf;
    return variant(randString(rng, opts, buf));
  } else {
    return variant(rand<TCpp>(rng));
  }
}

variant randVariant(
    const TypePtr& arg,
    folly::Random::DefaultGenerator& rng,
    const VectorFuzzer::Options& opts) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      randVariantImpl, arg->kind(), rng, opts);
}

template <TypeKind kind>
void fuzzFlatImpl(
    const VectorPtr& vector,
    folly::Random::DefaultGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TFlat = typename KindToFlatVector<kind>::type;
  using TCpp = typename TypeTraits<kind>::NativeType;

  auto flatVector = vector->as<TFlat>();
  auto* rawValues = flatVector->mutableRawValues();
  std::string strBuf;

  for (size_t i = 0; i < vector->size(); ++i) {
    if constexpr (std::is_same_v<TCpp, StringView>) {
      flatVector->set(i, randString(rng, opts, strBuf));
    } else {
      flatVector->set(i, rand<TCpp>(rng));
    }
  }
}

} // namespace

VectorPtr VectorFuzzer::fuzz(const TypePtr& type) {
  VectorPtr vector;

  // One in 5 chance of adding a constant vector.
  if (oneIn(5)) {
    // One in 5 chance of adding a NULL constant vector.
    if (oneIn(5)) {
      vector = BaseVector::createNullConstant(type, opts_.vectorSize, pool_);
    } else {
      vector = BaseVector::createConstant(
          randVariant(type, rng_, opts_), opts_.vectorSize, pool_);
    }
  } else {
    vector = fuzzFlat(type);
  }

  // Toss a coin and add dictionary indirections.
  while (oneIn(2)) {
    vector = fuzzDictionary(vector);
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type) {
  auto vector = BaseVector::create(type, opts_.vectorSize, pool_);

  // First, fill it with random values.
  // TODO: We should bias towards edge cases (min, max, Nan, etc).
  auto kind = vector->typeKind();
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(fuzzFlatImpl, kind, vector, rng_, opts_);

  // Second, generate a random null vector.
  for (size_t i = 0; i < vector->size(); ++i) {
    if (oneIn(opts_.nullChance)) {
      vector->setNull(i, true);
    }
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzDictionary(const VectorPtr& vector) {
  const size_t vectorSize = vector->size();
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(vectorSize, pool_);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < vectorSize; ++i) {
    rawIndices[i] = rand<vector_size_t>(rng_) % vectorSize;
  }

  // TODO: We can fuzz nulls here as well.
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, vectorSize, vector);
}

} // namespace facebook::velox
