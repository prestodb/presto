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

#include "velox/vector/fuzzer/VectorFuzzer.h"

#include <fmt/format.h>
#include <codecvt>
#include <locale>

#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

namespace {

// DWRF requires nano to be in a certain range. Hardcode the value here to avoid
// the dependency on DWRF.
constexpr int64_t MAX_NANOS = 1'000'000'000;

// Generate random values for the different supported types.
template <typename T>
T rand(FuzzerGenerator&) {
  VELOX_NYI();
}

template <>
int8_t rand(FuzzerGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int16_t rand(FuzzerGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int32_t rand(FuzzerGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
int64_t rand(FuzzerGenerator& rng) {
  return folly::Random::rand32(rng);
}

template <>
double rand(FuzzerGenerator& rng) {
  return folly::Random::randDouble01(rng);
}

template <>
float rand(FuzzerGenerator& rng) {
  return folly::Random::randDouble01(rng);
}

template <>
bool rand(FuzzerGenerator& rng) {
  return folly::Random::oneIn(2, rng);
}

template <>
uint32_t rand(FuzzerGenerator& rng) {
  return folly::Random::rand32(rng);
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    bool useMicrosecondPrecisionTimestamp = false) {
  return useMicrosecondPrecisionTimestamp
      ? Timestamp::fromMicros(folly::Random::rand32(rng))
      : Timestamp(
            folly::Random::rand32(rng),
            (folly::Random::rand32(rng) % MAX_NANOS));
}

Date randDate(FuzzerGenerator& rng) {
  return Date(folly::Random::rand32(rng));
}

IntervalDayTime randIntervalDayTime(FuzzerGenerator& rng) {
  return IntervalDayTime(folly::Random::rand64(rng));
}

size_t genContainerLength(
    const VectorFuzzer::Options& opts,
    FuzzerGenerator& rng) {
  return opts.containerVariableLength
      ? folly::Random::rand32(rng) % opts.containerLength
      : opts.containerLength;
}

/// Unicode character ranges.
/// Source: https://jrgraphix.net/research/unicode_blocks.php
const std::map<UTF8CharList, std::vector<std::pair<char16_t, char16_t>>>
    kUTFChatSetMap{
        {UTF8CharList::ASCII,
         {
             /*Numbers*/ {'0', '9'},
             /*Upper*/ {'A', 'Z'},
             /*Lower*/ {'a', 'z'},
         }},
        {UTF8CharList::UNICODE_CASE_SENSITIVE,
         {
             /*Basic Latin*/ {u'\u0020', u'\u007F'},
             /*Cyrillic*/ {u'\u0400', u'\u04FF'},
         }},
        {UTF8CharList::EXTENDED_UNICODE,
         {
             /*Greek*/ {u'\u03F0', u'\u03FF'},
             /*Latin Extended A*/ {u'\u0100', u'\u017F'},
             /*Arabic*/ {u'\u0600', u'\u06FF'},
             /*Devanagari*/ {u'\u0900', u'\u097F'},
             /*Hebrew*/ {u'\u0600', u'\u06FF'},
             /*Hiragana*/ {u'\u3040', u'\u309F'},
             /*Punctuation*/ {u'\u2000', u'\u206F'},
             /*Sub/Super Script*/ {u'\u2070', u'\u209F'},
             /*Currency*/ {u'\u20A0', u'\u20CF'},
         }},
        {UTF8CharList::MATHEMATICAL_SYMBOLS,
         {
             /*Math Operators*/ {u'\u2200', u'\u22FF'},
             /*Number Forms*/ {u'\u2150', u'\u218F'},
             /*Geometric Shapes*/ {u'\u25A0', u'\u25FF'},
             /*Math Symbols*/ {u'\u27C0', u'\u27EF'},
             /*Supplemental*/ {u'\u2A00', u'\u2AFF'},
         }}};

FOLLY_ALWAYS_INLINE char16_t getRandomChar(
    FuzzerGenerator& rng,
    const std::vector<std::pair<char16_t, char16_t>>& charSet) {
  const auto& chars = charSet[rand<uint32_t>(rng) % charSet.size()];
  auto size = chars.second - chars.first;
  auto inc = (rand<uint32_t>(rng) % size);
  char16_t res = chars.first + inc;
  return res;
}

/// Generates a random string (string size and encoding are passed through
/// Options). Returns a StringView which uses `buf` as the underlying buffer.
StringView randString(
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts,
    std::string& buf,
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t>& converter) {
  buf.clear();
  std::u16string wbuf;
  const size_t stringLength = opts.stringVariableLength
      ? folly::Random::rand32(rng) % opts.stringLength
      : opts.stringLength;
  wbuf.resize(stringLength);

  for (size_t i = 0; i < stringLength; ++i) {
    auto encoding =
        opts.charEncodings[rand<uint32_t>(rng) % opts.charEncodings.size()];
    wbuf[i] = getRandomChar(rng, kUTFChatSetMap.at(encoding));
  }

  buf.append(converter.to_bytes(wbuf));
  return StringView(buf);
}

template <TypeKind kind>
variant randVariantImpl(
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TCpp = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<TCpp, StringView>) {
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> converter;
    std::string buf;
    auto stringView = randString(rng, opts, buf, converter);

    if constexpr (kind == TypeKind::VARCHAR) {
      return variant(stringView);
    } else if constexpr (kind == TypeKind::VARBINARY) {
      return variant::binary(stringView);
    } else {
      VELOX_UNREACHABLE();
    }
  }
  if constexpr (std::is_same_v<TCpp, Timestamp>) {
    return variant(randTimestamp(rng, opts.useMicrosecondPrecisionTimestamp));
  } else if constexpr (std::is_same_v<TCpp, Date>) {
    return variant(randDate(rng));
  } else if constexpr (std::is_same_v<TCpp, IntervalDayTime>) {
    return variant(randIntervalDayTime(rng));
  } else {
    return variant(rand<TCpp>(rng));
  }
}

template <TypeKind kind>
void fuzzFlatImpl(
    const VectorPtr& vector,
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TFlat = typename KindToFlatVector<kind>::type;
  using TCpp = typename TypeTraits<kind>::NativeType;

  auto flatVector = vector->as<TFlat>();
  std::string strBuf;

  std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> converter;
  for (size_t i = 0; i < vector->size(); ++i) {
    if constexpr (std::is_same_v<TCpp, StringView>) {
      flatVector->set(i, randString(rng, opts, strBuf, converter));
    } else if constexpr (std::is_same_v<TCpp, Timestamp>) {
      flatVector->set(
          i, randTimestamp(rng, opts.useMicrosecondPrecisionTimestamp));
    } else if constexpr (std::is_same_v<TCpp, Date>) {
      flatVector->set(i, randDate(rng));
    } else if constexpr (std::is_same_v<TCpp, IntervalDayTime>) {
      flatVector->set(i, randIntervalDayTime(rng));
    } else {
      flatVector->set(i, rand<TCpp>(rng));
    }
  }
}

} // namespace

VectorPtr VectorFuzzer::fuzz(const TypePtr& type) {
  return fuzz(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzz(const TypePtr& type, vector_size_t size) {
  VectorPtr vector;

  // 20% chance of adding a constant vector.
  if (coinToss(0.2)) {
    // If adding a constant vector, 50% of chance between:
    // - generate a regular constant vector (only for primitive types).
    // - generate a random vector and wrap it using a constant vector.
    if (type->isPrimitiveType() && coinToss(0.5)) {
      vector = fuzzConstant(type, size);
    } else {
      // Vector size can't be zero.
      auto innerVectorSize =
          folly::Random::rand32(1, opts_.vectorSize + 1, rng_);
      auto constantIndex = rand<vector_size_t>(rng_) % innerVectorSize;
      vector = BaseVector::wrapInConstant(
          size, constantIndex, fuzz(type, innerVectorSize));
    }
  } else {
    vector = type->isPrimitiveType() ? fuzzFlat(type, size)
                                     : fuzzComplex(type, size);
  }

  // Toss a coin and add dictionary indirections.
  while (coinToss(0.5)) {
    vector = fuzzDictionary(vector);
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type) {
  return fuzzConstant(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type, vector_size_t size) {
  if (coinToss(opts_.nullRatio)) {
    return BaseVector::createNullConstant(type, size, pool_);
  }
  return BaseVector::createConstant(randVariant(type), size, pool_);
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type) {
  return fuzzFlat(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type, vector_size_t size) {
  auto vector = BaseVector::create(type, size, pool_);

  // First, fill it with random values.
  // TODO: We should bias towards edge cases (min, max, Nan, etc).
  auto kind = vector->typeKind();
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(fuzzFlatImpl, kind, vector, rng_, opts_);

  // Second, generate a random null vector.
  for (size_t i = 0; i < vector->size(); ++i) {
    if (coinToss(opts_.nullRatio)) {
      vector->setNull(i, true);
    }
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzComplex(const TypePtr& type) {
  return fuzzComplex(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzComplex(const TypePtr& type, vector_size_t size) {
  VectorPtr vector;
  if (type->kind() == TypeKind::ROW) {
    vector =
        fuzzRow(std::dynamic_pointer_cast<const RowType>(type), size, true);
  } else {
    auto offsets = allocateOffsets(size, pool_);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto sizes = allocateSizes(size, pool_);
    auto rawSizes = sizes->asMutable<vector_size_t>();
    vector_size_t childSize = 0;
    // Randomly creates container size.
    for (auto i = 0; i < size; ++i) {
      rawOffsets[i] = childSize;
      auto length = genContainerLength(opts_, rng_);
      rawSizes[i] = length;
      childSize += length;
    }

    auto nulls = fuzzNulls(size);
    if (type->kind() == TypeKind::ARRAY) {
      vector = std::make_shared<ArrayVector>(
          pool_,
          type,
          nulls,
          size,
          offsets,
          sizes,
          fuzz(type->asArray().elementType(), childSize));
    } else if (type->kind() == TypeKind::MAP) {
      auto& mapType = type->asMap();
      vector = std::make_shared<MapVector>(
          pool_,
          type,
          nulls,
          size,
          offsets,
          sizes,
          fuzz(mapType.keyType(), childSize),
          fuzz(mapType.valueType(), childSize));
    } else {
      VELOX_UNREACHABLE();
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

RowVectorPtr VectorFuzzer::fuzzRow(const RowTypePtr& rowType) {
  return fuzzRow(rowType, opts_.vectorSize, false);
}

RowVectorPtr VectorFuzzer::fuzzRow(
    const RowTypePtr& rowType,
    vector_size_t size,
    bool mayHaveNulls) {
  std::vector<VectorPtr> children;
  for (auto i = 0; i < rowType->size(); ++i) {
    children.push_back(fuzz(rowType->childAt(i), size));
  }

  auto nulls = mayHaveNulls ? fuzzNulls(size) : nullptr;
  return std::make_shared<RowVector>(
      pool_, rowType, nulls, size, std::move(children));
}

BufferPtr VectorFuzzer::fuzzNulls(vector_size_t size) {
  NullsBuilder builder{size, pool_};
  for (size_t i = 0; i < size; ++i) {
    if (coinToss(opts_.nullRatio)) {
      builder.setNull(i);
    }
  }
  return builder.build();
}

variant VectorFuzzer::randVariant(const TypePtr& arg) {
  if (arg->isArray()) {
    auto arrayType = arg->asArray();
    std::vector<variant> variantArray;
    auto length = genContainerLength(opts_, rng_);
    variantArray.reserve(length);

    for (size_t i = 0; i < length; ++i) {
      variantArray.emplace_back(randVariant(arrayType.elementType()));
    }
    return variant::array(std::move(variantArray));
  } else if (arg->isMap()) {
    auto mapType = arg->asMap();
    std::map<variant, variant> variantMap;
    auto length = genContainerLength(opts_, rng_);

    for (size_t i = 0; i < length; ++i) {
      variantMap.emplace(
          randVariant(mapType.keyType()), randVariant(mapType.valueType()));
    }
    return variant::map(std::move(variantMap));
  } else if (arg->isRow()) {
    auto rowType = arg->asRow();
    std::vector<variant> variantArray;
    auto length = genContainerLength(opts_, rng_);
    variantArray.reserve(length);

    for (size_t i = 0; i < length; ++i) {
      variantArray.emplace_back(randVariant(rowType.childAt(i)));
    }
    return variant::row(std::move(variantArray));
  } else {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        randVariantImpl, arg->kind(), rng_, opts_);
  }
}

TypePtr VectorFuzzer::randType(int maxDepth) {
  static TypePtr kScalarTypes[]{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
  };
  static constexpr int kNumScalarTypes =
      sizeof(kScalarTypes) / sizeof(kScalarTypes[0]);
  // Should we generate a scalar type?
  if (maxDepth <= 1 || folly::Random::rand32(2, rng_)) {
    return kScalarTypes[folly::Random::rand32(kNumScalarTypes, rng_)];
  }
  switch (folly::Random::rand32(3, rng_)) {
    case 0:
      return MAP(randType(0), randType(maxDepth - 1));
    case 1:
      return ARRAY(randType(maxDepth - 1));
    default:
      return randRowType(maxDepth - 1);
  }
}

RowTypePtr VectorFuzzer::randRowType(int maxDepth) {
  int numFields = folly::Random::rand32(rng_) % 7;
  std::vector<std::string> names;
  std::vector<TypePtr> fields;
  for (int i = 0; i < numFields; ++i) {
    names.push_back(fmt::format("f{}", i));
    fields.push_back(randType(maxDepth));
  }
  return ROW(std::move(names), std::move(fields));
}

} // namespace facebook::velox
