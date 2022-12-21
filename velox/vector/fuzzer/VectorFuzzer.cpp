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

#include <boost/random/uniform_int_distribution.hpp>
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

// Structure to help temporary changes to Options. This objects saves the
// current state of the Options object, and restores it when it's destructed.
// For instance, if you would like to temporarily disable nulls for a particular
// recursive call:
//
//  {
//    ScopedOptions scopedOptions(this);
//    opts_.nullRatio = 0;
//    // perhaps change other opts_ values.
//    vector = fuzzFlat(...);
//  }
//  // At this point, opts_ would have the original values again.
//
struct ScopedOptions {
  explicit ScopedOptions(VectorFuzzer* fuzzer)
      : fuzzer(fuzzer), savedOpts(fuzzer->getOptions()) {}

  ~ScopedOptions() {
    fuzzer->setOptions(savedOpts);
  }

  // Stores a copy of Options so we can restore at destruction time.
  VectorFuzzer* fuzzer;
  VectorFuzzer::Options savedOpts;
};

// Generate random values for the different supported types.
template <typename T>
T rand(FuzzerGenerator&) {
  VELOX_NYI();
}

template <>
int8_t rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<int8_t>()(rng);
}

template <>
int16_t rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<int16_t>()(rng);
}

template <>
int32_t rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<int32_t>()(rng);
}

template <>
int64_t rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<int64_t>()(rng);
}

template <>
double rand(FuzzerGenerator& rng) {
  return boost::random::uniform_01<double>()(rng);
}

template <>
float rand(FuzzerGenerator& rng) {
  return boost::random::uniform_01<float>()(rng);
}

template <>
bool rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng);
}

template <>
uint32_t rand(FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<uint32_t>()(rng);
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    bool useMicrosecondPrecisionTimestamp = false) {
  return useMicrosecondPrecisionTimestamp
      ? Timestamp::fromMicros(rand<int64_t>(rng))
      : Timestamp(rand<int32_t>(rng), (rand<int64_t>(rng) % MAX_NANOS));
}

Date randDate(FuzzerGenerator& rng) {
  return Date(rand<int32_t>(rng));
}

IntervalDayTime randIntervalDayTime(FuzzerGenerator& rng) {
  return IntervalDayTime(rand<int64_t>(rng));
}

size_t genContainerLength(
    const VectorFuzzer::Options& opts,
    FuzzerGenerator& rng) {
  return opts.containerVariableLength
      ? rand<uint32_t>(rng) % opts.containerLength
      : opts.containerLength;
}

/// Unicode character ranges. Ensure the vector indexes match the UTF8CharList
/// enum values.
///
/// Source: https://jrgraphix.net/research/unicode_blocks.php
const std::vector<std::vector<std::pair<char16_t, char16_t>>> kUTFChatSets{
    // UTF8CharList::ASCII
    {
        {33, 127}, // All ASCII printable chars.
    },
    // UTF8CharList::UNICODE_CASE_SENSITIVE
    {
        {u'\u0020', u'\u007F'}, // Basic Latin.
        {u'\u0400', u'\u04FF'}, // Cyrillic.
    },
    // UTF8CharList::EXTENDED_UNICODE
    {
        {u'\u03F0', u'\u03FF'}, // Greek.
        {u'\u0100', u'\u017F'}, // Latin Extended A.
        {u'\u0600', u'\u06FF'}, // Arabic.
        {u'\u0900', u'\u097F'}, // Devanagari.
        {u'\u0600', u'\u06FF'}, // Hebrew.
        {u'\u3040', u'\u309F'}, // Hiragana.
        {u'\u2000', u'\u206F'}, // Punctuation.
        {u'\u2070', u'\u209F'}, // Sub/Super Script.
        {u'\u20A0', u'\u20CF'}, // Currency.
    },
    // UTF8CharList::MATHEMATICAL_SYMBOLS
    {
        {u'\u2200', u'\u22FF'}, // Math Operators.
        {u'\u2150', u'\u218F'}, // Number Forms.
        {u'\u25A0', u'\u25FF'}, // Geometric Shapes.
        {u'\u27C0', u'\u27EF'}, // Math Symbols.
        {u'\u2A00', u'\u2AFF'}, // Supplemental.
    },
};

FOLLY_ALWAYS_INLINE char16_t getRandomChar(
    FuzzerGenerator& rng,
    const std::vector<std::pair<char16_t, char16_t>>& charSet) {
  const auto& chars = charSet.size() == 1
      ? charSet.front()
      : charSet[rand<uint32_t>(rng) % charSet.size()];
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
      ? rand<uint32_t>(rng) % opts.stringLength
      : opts.stringLength;
  wbuf.resize(stringLength);

  for (size_t i = 0; i < stringLength; ++i) {
    // First choose a random encoding from the list of input acceptable
    // encodings.
    const auto& encoding = (opts.charEncodings.size() == 1)
        ? opts.charEncodings.front()
        : opts.charEncodings[rand<uint32_t>(rng) % opts.charEncodings.size()];

    wbuf[i] = getRandomChar(rng, kUTFChatSets[encoding]);
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
void fuzzFlatPrimitiveImpl(
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

// Servers as a wrapper around a vector that will be used to load a lazyVector.
// Ensures that the loaded vector will only contain valid rows for the row set
// that it was loaded for. NOTE: If the vector is a multi-level dictionary, the
// indices from all the dictionaries are combined.
class VectorLoaderWrap : public VectorLoader {
 public:
  explicit VectorLoaderWrap(VectorPtr vector) : vector_(vector) {}

  void loadInternal(RowSet rowSet, ValueHook* hook, VectorPtr* result)
      override {
    VELOX_CHECK(!hook, "VectorLoaderWrap doesn't support ValueHook");
    SelectivityVector rows(rowSet.back() + 1, false);
    for (auto row : rowSet) {
      rows.setValid(row, true);
    }
    rows.updateBounds();
    *result = makeEncodingPreservedCopy(rows);
  }

 private:
  // Returns a copy of 'vector_' while retaining dictionary encoding if present.
  // Multiple dictionary layers are collapsed into one.
  VectorPtr makeEncodingPreservedCopy(SelectivityVector& rows);
  VectorPtr vector_;
};

bool hasNestedDictionaryLayers(const VectorPtr& baseVector) {
  return baseVector && VectorEncoding::isDictionary(baseVector->encoding()) &&
      VectorEncoding::isDictionary(baseVector->valueVector()->encoding());
}

} // namespace

VectorPtr VectorFuzzer::fuzzNotNull(const TypePtr& type) {
  return fuzzNotNull(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzNotNull(const TypePtr& type, vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.nullRatio = 0;
  return fuzz(type, size);
}

VectorPtr VectorFuzzer::fuzz(const TypePtr& type) {
  return fuzz(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzz(const TypePtr& type, vector_size_t size) {
  VectorPtr vector;
  vector_size_t vectorSize = size;

  bool usingLazyVector = opts_.allowLazyVector && coinToss(0.1);
  // Lazy Vectors cannot be sliced, so we skip this if using lazy wrapping.
  if (!usingLazyVector && coinToss(0.1)) {
    // Extend the underlying vector to allow slicing later.
    vectorSize += rand<uint32_t>(rng_) % 8;
  }

  // 20% chance of adding a constant vector.
  if (coinToss(0.2)) {
    vector = fuzzConstant(type, vectorSize);
  } else {
    vector = type->isPrimitiveType() ? fuzzFlatPrimitive(type, vectorSize)
                                     : fuzzComplex(type, vectorSize);
  }

  if (vectorSize > size) {
    auto offset = rand<uint32_t>(rng_) % (vectorSize - size + 1);
    vector = vector->slice(offset, size);
  }

  if (usingLazyVector) {
    vector = wrapInLazyVector(vector);
  }

  // Toss a coin and add dictionary indirections.
  while (coinToss(0.5)) {
    vectorSize = size;
    if (!usingLazyVector && vectorSize > 0 && coinToss(0.05)) {
      vectorSize += rand<uint32_t>(rng_) % 8;
    }
    vector = fuzzDictionary(vector, vectorSize);
    if (vectorSize > size) {
      auto offset = rand<uint32_t>(rng_) % (vectorSize - size + 1);
      vector = vector->slice(offset, size);
    }
  }
  VELOX_CHECK_EQ(vector->size(), size);
  return vector;
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type) {
  return fuzzConstant(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type, vector_size_t size) {
  // For constants, there are two possible cases:
  // - generate a regular constant vector (only for primitive types).
  // - generate a random vector and wrap it using a constant vector.
  if (type->isPrimitiveType() && coinToss(0.5)) {
    // For regular constant vectors, toss a coin to determine its nullability.
    if (coinToss(opts_.nullRatio)) {
      return BaseVector::createNullConstant(type, size, pool_);
    }
    if (type->isUnKnown()) {
      return BaseVector::createNullConstant(type, size, pool_);
    } else {
      return BaseVector::createConstant(randVariant(type), size, pool_);
    }
  }

  // Otherwise, create constant by wrapping around another vector. This will
  // return a null constant if the element being wrapped is null in the
  // generated inner vector.

  // Inner vector size can't be zero.
  auto innerVectorSize = rand<uint32_t>(rng_) % opts_.vectorSize + 1;
  auto constantIndex = rand<vector_size_t>(rng_) % innerVectorSize;

  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;
  return BaseVector::wrapInConstant(
      size, constantIndex, fuzz(type, innerVectorSize));
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type) {
  return fuzzFlat(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzFlatNotNull(const TypePtr& type) {
  return fuzzFlatNotNull(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzFlatNotNull(
    const TypePtr& type,
    vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.nullRatio = 0;
  return fuzzFlat(type, size);
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type, vector_size_t size) {
  // Primitive types.
  if (type->isPrimitiveType()) {
    return fuzzFlatPrimitive(type, size);
  }
  // Arrays.
  else if (type->isArray()) {
    return fuzzArray(
        fuzzFlat(type->asArray().elementType(), size * opts_.containerLength),
        size);
  }
  // Maps.
  else if (type->isMap()) {
    // Do not initialize keys and values inline in the fuzzMap call as C++ does
    // not specify the order they'll be called in, leading to inconsistent
    // results across platforms.
    auto keys = opts_.normalizeMapKeys
        ? fuzzFlatNotNull(type->asMap().keyType(), size * opts_.containerLength)
        : fuzzFlat(type->asMap().keyType(), size * opts_.containerLength);
    auto values =
        fuzzFlat(type->asMap().valueType(), size * opts_.containerLength);
    return fuzzMap(keys, values, size);
  }
  // Rows.
  else if (type->isRow()) {
    const auto& rowType = type->asRow();
    std::vector<VectorPtr> childrenVectors;
    childrenVectors.reserve(rowType.children().size());

    for (const auto& childType : rowType.children()) {
      childrenVectors.emplace_back(fuzzFlat(childType, size));
    }
    return fuzzRow(std::move(childrenVectors), size);
  } else {
    VELOX_UNREACHABLE();
  }
}

VectorPtr VectorFuzzer::fuzzFlatPrimitive(
    const TypePtr& type,
    vector_size_t size) {
  VELOX_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool_);

  if (type->isUnKnown()) {
    auto* rawNulls = vector->mutableRawNulls();
    bits::fillBits(rawNulls, 0, size, bits::kNull);
  } else {
    // First, fill it with random values.
    // TODO: We should bias towards edge cases (min, max, Nan, etc).
    auto kind = vector->typeKind();
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        fuzzFlatPrimitiveImpl, kind, vector, rng_, opts_);

    // Second, generate a random null vector.
    for (size_t i = 0; i < vector->size(); ++i) {
      if (coinToss(opts_.nullRatio)) {
        vector->setNull(i, true);
      }
    }
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzComplex(const TypePtr& type, vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;

  switch (type->kind()) {
    case TypeKind::ROW:
      return fuzzRow(std::dynamic_pointer_cast<const RowType>(type), size);

    case TypeKind::ARRAY:
      return fuzzArray(
          fuzz(type->asArray().elementType(), size * opts_.containerLength),
          size);

    case TypeKind::MAP: {
      // Do not initialize keys and values inline in the fuzzMap call as C++
      // does not specify the order they'll be called in, leading to
      // inconsistent results across platforms.
      auto keys = opts_.normalizeMapKeys
          ? fuzzNotNull(type->asMap().keyType(), size * opts_.containerLength)
          : fuzz(type->asMap().keyType(), size * opts_.containerLength);
      auto values =
          fuzz(type->asMap().valueType(), size * opts_.containerLength);
      return fuzzMap(keys, values, size);
    }

    default:
      VELOX_UNREACHABLE();
  }
  return nullptr; // no-op.
}

VectorPtr VectorFuzzer::fuzzDictionary(const VectorPtr& vector) {
  return fuzzDictionary(vector, vector->size());
}

VectorPtr VectorFuzzer::fuzzDictionary(
    const VectorPtr& vector,
    vector_size_t size) {
  const size_t vectorSize = vector->size();
  VELOX_CHECK(
      vectorSize > 0 || size == 0,
      "Cannot build a non-empty dictionary on an empty underlying vector");
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < size; ++i) {
    rawIndices[i] = rand<vector_size_t>(rng_) % vectorSize;
  }

  auto nulls = opts_.dictionaryHasNulls ? fuzzNulls(size) : nullptr;
  return BaseVector::wrapInDictionary(nulls, indices, size, vector);
}

void VectorFuzzer::fuzzOffsetsAndSizes(
    BufferPtr& offsets,
    BufferPtr& sizes,
    size_t elementsSize,
    size_t size) {
  offsets = allocateOffsets(size, pool_);
  sizes = allocateSizes(size, pool_);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  size_t containerAvgLength = std::max(elementsSize / size, 1UL);
  size_t childSize = 0;
  size_t length = 0;

  for (auto i = 0; i < size; ++i) {
    rawOffsets[i] = childSize;

    // If variable length, generate a random number between zero and 2 *
    // containerAvgLength (so that the average of generated containers size is
    // equal to number of input elements).
    if (opts_.containerVariableLength) {
      length = rand<uint32_t>(rng_) % (containerAvgLength * 2);
    } else {
      length = containerAvgLength;
    }

    // If we exhausted the available elements, add empty arrays.
    if ((childSize + length) > elementsSize) {
      length = 0;
    }
    rawSizes[i] = length;
    childSize += length;
  }
}

ArrayVectorPtr VectorFuzzer::fuzzArray(
    const VectorPtr& elements,
    vector_size_t size) {
  BufferPtr offsets, sizes;
  fuzzOffsetsAndSizes(offsets, sizes, elements->size(), size);
  return std::make_shared<ArrayVector>(
      pool_,
      ARRAY(elements->type()),
      opts_.containerHasNulls ? fuzzNulls(size) : nullptr,
      size,
      offsets,
      sizes,
      elements);
}

VectorPtr VectorFuzzer::normalizeMapKeys(
    const VectorPtr& keys,
    size_t mapSize,
    BufferPtr& offsets,
    BufferPtr& sizes) {
  // Map keys cannot be null.
  const auto& nulls = keys->nulls();
  if (nulls) {
    VELOX_CHECK_EQ(
        BaseVector::countNulls(nulls, 0, keys->size()),
        0,
        "Map keys cannot be null when opt.normalizeMapKeys is true");
  }

  auto rawOffsets = offsets->as<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  // Looks for duplicate key values.
  std::unordered_set<uint64_t> set;
  for (size_t i = 0; i < mapSize; ++i) {
    set.clear();

    for (size_t j = 0; j < rawSizes[i]; ++j) {
      vector_size_t idx = rawOffsets[i] + j;
      uint64_t hash = keys->hashValueAt(idx);

      // If we find the same hash (either same key value or hash colision), we
      // cut it short by reducing this element's map size. This should not
      // happen frequently.
      auto it = set.find(hash);
      if (it != set.end()) {
        rawSizes[i] = j;
        break;
      }
      set.insert(hash);
    }
  }
  return keys;
}

MapVectorPtr VectorFuzzer::fuzzMap(
    const VectorPtr& keys,
    const VectorPtr& values,
    vector_size_t size) {
  size_t elementsSize = std::min(keys->size(), values->size());
  BufferPtr offsets, sizes;
  fuzzOffsetsAndSizes(offsets, sizes, elementsSize, size);
  return std::make_shared<MapVector>(
      pool_,
      MAP(keys->type(), values->type()),
      opts_.containerHasNulls ? fuzzNulls(size) : nullptr,
      size,
      offsets,
      sizes,
      opts_.normalizeMapKeys ? normalizeMapKeys(keys, size, offsets, sizes)
                             : keys,
      values);
}

RowVectorPtr VectorFuzzer::fuzzInputRow(const RowTypePtr& rowType) {
  ScopedOptions restorer(this);
  opts_.containerHasNulls = false;
  return fuzzRow(rowType, opts_.vectorSize);
}

RowVectorPtr VectorFuzzer::fuzzRow(
    std::vector<VectorPtr>&& children,
    vector_size_t size) {
  std::vector<TypePtr> types;
  types.reserve(children.size());

  for (const auto& child : children) {
    types.emplace_back(child->type());
  }

  return std::make_shared<RowVector>(
      pool_,
      ROW(std::move(types)),
      opts_.containerHasNulls ? fuzzNulls(size) : nullptr,
      size,
      std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRow(const RowTypePtr& rowType) {
  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;
  return fuzzRow(rowType, opts_.vectorSize);
}

RowVectorPtr VectorFuzzer::fuzzRow(
    const RowTypePtr& rowType,
    vector_size_t size) {
  std::vector<VectorPtr> children;
  for (auto i = 0; i < rowType->size(); ++i) {
    children.push_back(fuzz(rowType->childAt(i), size));
  }

  auto nulls = opts_.containerHasNulls ? fuzzNulls(size) : nullptr;
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
  VELOX_CHECK(arg->isPrimitiveType());
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      randVariantImpl, arg->kind(), rng_, opts_);
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
  if (maxDepth <= 1 || rand<bool>(rng_)) {
    return kScalarTypes[rand<uint32_t>(rng_) % kNumScalarTypes];
  }
  switch (rand<uint32_t>(rng_) % 3) {
    case 0:
      return MAP(randType(0), randType(maxDepth - 1));
    case 1:
      return ARRAY(randType(maxDepth - 1));
    default:
      return randRowType(maxDepth - 1);
  }
}

RowTypePtr VectorFuzzer::randRowType(int maxDepth) {
  int numFields = 1 + rand<uint32_t>(rng_) % 7;
  std::vector<std::string> names;
  std::vector<TypePtr> fields;
  for (int i = 0; i < numFields; ++i) {
    names.push_back(fmt::format("f{}", i));
    fields.push_back(randType(maxDepth));
  }
  return ROW(std::move(names), std::move(fields));
}

VectorPtr VectorFuzzer::wrapInLazyVector(VectorPtr baseVector) {
  if (hasNestedDictionaryLayers(baseVector)) {
    auto indices = baseVector->wrapInfo();
    auto values = baseVector->valueVector();
    auto nulls = baseVector->nulls();

    auto copiedNulls = AlignedBuffer::copy(baseVector->pool(), nulls);

    return BaseVector::wrapInDictionary(
        copiedNulls, indices, baseVector->size(), wrapInLazyVector(values));
  }
  return std::make_shared<LazyVector>(
      baseVector->pool(),
      baseVector->type(),
      baseVector->size(),
      std::make_unique<VectorLoaderWrap>(baseVector));
}

RowVectorPtr VectorFuzzer::fuzzRowChildrenToLazy(RowVectorPtr rowVector) {
  std::vector<VectorPtr> children;
  VELOX_CHECK_NULL(rowVector->nulls());
  for (auto child : rowVector->children()) {
    VELOX_CHECK_NOT_NULL(child);
    VELOX_CHECK(!child->isLazy());
    // TODO: If child has dictionary wrappings, add ability to insert lazy wrap
    // between those layers.
    children.push_back(coinToss(0.5) ? wrapInLazyVector(child) : child);
  }
  return std::make_shared<RowVector>(
      pool_,
      rowVector->type(),
      nullptr,
      rowVector->size(),
      std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRowChildrenToLazy(
    RowVectorPtr rowVector,
    const std::vector<column_index_t>& columnsToWrapInLazy) {
  if (columnsToWrapInLazy.empty()) {
    return rowVector;
  }
  std::vector<VectorPtr> children;
  int listIndex = 0;
  for (column_index_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    VELOX_USER_CHECK_NOT_NULL(child);
    VELOX_USER_CHECK(!child->isLazy());
    if (listIndex < columnsToWrapInLazy.size() &&
        i == columnsToWrapInLazy[listIndex]) {
      listIndex++;
      child = VectorFuzzer::wrapInLazyVector(child);
    }
    children.push_back(child);
  }

  BufferPtr newNulls = nullptr;
  if (rowVector->nulls()) {
    newNulls = AlignedBuffer::copy(rowVector->pool(), rowVector->nulls());
  }

  return std::make_shared<RowVector>(
      rowVector->pool(),
      rowVector->type(),
      newNulls,
      rowVector->size(),
      std::move(children));
}

VectorPtr VectorLoaderWrap::makeEncodingPreservedCopy(SelectivityVector& rows) {
  VectorPtr result;
  DecodedVector decoded;
  decoded.decode(*vector_, rows, false);

  if (decoded.isConstantMapping() || decoded.isIdentityMapping()) {
    BaseVector::ensureWritable(rows, vector_->type(), vector_->pool(), result);
    result->copy(vector_.get(), rows, nullptr);
    return result;
  }

  SelectivityVector baseRows;
  auto baseVector = decoded.base();

  baseRows.resize(baseVector->size(), false);
  rows.applyToSelected([&](auto row) {
    if (!decoded.isNullAt(row)) {
      baseRows.setValid(decoded.index(row), true);
    }
  });
  baseRows.updateBounds();

  BaseVector::ensureWritable(
      baseRows, baseVector->type(), vector_->pool(), result);
  result->copy(baseVector, baseRows, nullptr);

  BufferPtr indices = allocateIndices(rows.end(), vector_->pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  auto decodedIndices = decoded.indices();
  rows.applyToSelected(
      [&](auto row) { rawIndices[row] = decodedIndices[row]; });

  BufferPtr nulls = nullptr;
  if (decoded.nulls()) {
    if (!baseRows.hasSelections()) {
      nulls = allocateNulls(rows.end(), vector_->pool(), bits::kNull);
    } else {
      nulls = AlignedBuffer::allocate<bool>(rows.end(), vector_->pool());
      std::memcpy(
          nulls->asMutable<uint64_t>(),
          decoded.nulls(),
          bits::nbytes(rows.end()));
    }
  }

  return BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), rows.end(), result);
}

} // namespace facebook::velox
