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
#include "velox/functions/sparksql/Hash.h"

#include <folly/CPortability.h>

#include "velox/common/base/BitUtil.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {

const int32_t kDefaultSeed = 42;

// Computes the hash value of input using the hash function in HashClass.
template <typename HashClass>
typename HashClass::ReturnType hashOne(
    int32_t input,
    typename HashClass::SeedType seed) {
  return HashClass::hashInt32(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    int64_t input,
    typename HashClass::SeedType seed) {
  return HashClass::hashInt64(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    float input,
    typename HashClass::SeedType seed) {
  return HashClass::hashFloat(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    double input,
    typename HashClass::SeedType seed) {
  return HashClass::hashDouble(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    int128_t input,
    typename HashClass::SeedType seed) {
  return HashClass::hashLongDecimal(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    Timestamp input,
    typename HashClass::SeedType seed) {
  return HashClass::hashTimestamp(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    StringView input,
    typename HashClass::SeedType seed) {
  return HashClass::hashBytes(input, seed);
}

template <typename HashClass>
typename HashClass::ReturnType hashOne(
    UnknownValue /*input*/,
    typename HashClass::SeedType seed) {
  return seed;
}

template <typename HashClass, TypeKind kind>
class PrimitiveVectorHasher;

template <typename HashClass>
class ArrayVectorHasher;

template <typename HashClass>
class MapVectorHasher;

template <typename HashClass>
class RowVectorHasher;

template <typename HashClass>
class UnknowTypeVectorHasher;

// Class to compute hashes identical to one produced by Spark.
// Hashes are computed using the algorithm implemented in HashClass.
template <typename HashClass>
class SparkVectorHasher {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  SparkVectorHasher(DecodedVector& decoded) : decoded_(decoded) {}

  virtual ~SparkVectorHasher() = default;

  // Compute the hash value of input vector at index.
  ReturnType hashAt(vector_size_t index, SeedType seed) {
    if (decoded_.isNullAt(index)) {
      return seed;
    }
    return hashNotNullAt(index, seed);
  }

  // Compute the hash value of input vector at index for non-null values.
  virtual ReturnType hashNotNullAt(vector_size_t index, SeedType seed) = 0;

 protected:
  const DecodedVector& decoded_;
};

template <typename HashClass, TypeKind kind>
std::shared_ptr<SparkVectorHasher<HashClass>> createPrimitiveVectorHasher(
    DecodedVector& decoded) {
  return std::make_shared<PrimitiveVectorHasher<HashClass, kind>>(decoded);
}

template <typename HashClass>
std::shared_ptr<SparkVectorHasher<HashClass>> createVectorHasher(
    DecodedVector& decoded) {
  switch (decoded.base()->typeKind()) {
    case TypeKind::ARRAY:
      return std::make_shared<ArrayVectorHasher<HashClass>>(decoded);
    case TypeKind::MAP:
      return std::make_shared<MapVectorHasher<HashClass>>(decoded);
    case TypeKind::ROW:
      return std::make_shared<RowVectorHasher<HashClass>>(decoded);
    case TypeKind::UNKNOWN:
      return std::make_shared<UnknowTypeVectorHasher<HashClass>>(decoded);
    default:
      return VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
          createPrimitiveVectorHasher,
          HashClass,
          decoded.base()->typeKind(),
          decoded);
  }
}

template <typename HashClass, TypeKind kind>
class PrimitiveVectorHasher : public SparkVectorHasher<HashClass> {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  PrimitiveVectorHasher(DecodedVector& decoded)
      : SparkVectorHasher<HashClass>(decoded) {}

  ReturnType hashNotNullAt(vector_size_t index, SeedType seed) override {
    return hashOne<HashClass>(
        this->decoded_.template valueAt<typename TypeTraits<kind>::NativeType>(
            index),
        seed);
  }
};

template <typename HashClass>
class UnknowTypeVectorHasher : public SparkVectorHasher<HashClass> {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  explicit UnknowTypeVectorHasher(DecodedVector& decoded)
      : SparkVectorHasher<HashClass>(decoded) {}

  ReturnType hashNotNullAt(vector_size_t /*index*/, SeedType /*seed*/)
      override {
    VELOX_FAIL("hashNotNullAt should not be called for unknown type.");
  }
};

template <typename HashClass>
class ArrayVectorHasher : public SparkVectorHasher<HashClass> {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  ArrayVectorHasher(DecodedVector& decoded)
      : SparkVectorHasher<HashClass>(decoded) {
    base_ = decoded.base()->as<ArrayVector>();
    indices_ = decoded.indices();

    SelectivityVector rows(base_->elements()->size());
    decodedElements_.decode(*base_->elements(), rows);
    elementHasher_ = createVectorHasher<HashClass>(decodedElements_);
  }

  ReturnType hashNotNullAt(vector_size_t index, SeedType seed) override {
    auto size = base_->sizeAt(indices_[index]);
    auto offset = base_->offsetAt(indices_[index]);

    ReturnType result = seed;
    for (auto i = 0; i < size; ++i) {
      result = elementHasher_->hashAt(i + offset, result);
    }
    return result;
  }

 private:
  const ArrayVector* base_;
  const int32_t* indices_;
  DecodedVector decodedElements_;
  std::shared_ptr<SparkVectorHasher<HashClass>> elementHasher_;
};

template <typename HashClass>
class MapVectorHasher : public SparkVectorHasher<HashClass> {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  MapVectorHasher(DecodedVector& decoded)
      : SparkVectorHasher<HashClass>(decoded) {
    base_ = decoded.base()->as<MapVector>();
    indices_ = decoded.indices();

    SelectivityVector rows(base_->mapKeys()->size());
    decodedKeys_.decode(*base_->mapKeys(), rows);
    decodedValues_.decode(*base_->mapValues(), rows);
    keyHasher_ = createVectorHasher<HashClass>(decodedKeys_);
    valueHasher_ = createVectorHasher<HashClass>(decodedValues_);
  }

  ReturnType hashNotNullAt(vector_size_t index, SeedType seed) override {
    auto size = base_->sizeAt(indices_[index]);
    auto offset = base_->offsetAt(indices_[index]);

    ReturnType result = seed;
    for (auto i = 0; i < size; ++i) {
      result = keyHasher_->hashAt(i + offset, result);
      result = valueHasher_->hashAt(i + offset, result);
    }
    return result;
  }

 private:
  const MapVector* base_;
  const int32_t* indices_;
  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  std::shared_ptr<SparkVectorHasher<HashClass>> keyHasher_;
  std::shared_ptr<SparkVectorHasher<HashClass>> valueHasher_;
};

template <typename HashClass>
class RowVectorHasher : public SparkVectorHasher<HashClass> {
 public:
  using SeedType = typename HashClass::SeedType;
  using ReturnType = typename HashClass::ReturnType;

  RowVectorHasher(DecodedVector& decoded)
      : SparkVectorHasher<HashClass>(decoded) {
    base_ = decoded.base()->as<RowVector>();
    indices_ = decoded.indices();

    SelectivityVector rows(base_->size());
    decodedChildren_.resize(base_->childrenSize());
    hashers_.resize(base_->childrenSize());
    for (auto i = 0; i < base_->childrenSize(); ++i) {
      decodedChildren_[i].decode(*base_->childAt(i), rows);
      hashers_[i] = createVectorHasher<HashClass>(decodedChildren_[i]);
    }
  }

  ReturnType hashNotNullAt(vector_size_t index, SeedType seed) override {
    ReturnType result = seed;
    for (auto i = 0; i < base_->childrenSize(); ++i) {
      result = hashers_[i]->hashAt(indices_[index], result);
    }
    return result;
  }

 private:
  const RowVector* base_;
  const int32_t* indices_;
  std::vector<DecodedVector> decodedChildren_;
  std::vector<std::shared_ptr<SparkVectorHasher<HashClass>>> hashers_;
};

template <typename HashClass, typename ReturnType, typename ArgType>
void hashSimdTyped(
    const SelectivityVector* rows,
    std::vector<VectorPtr>& args,
    FlatVector<ReturnType>& result,
    const int32_t hashIdx) {
  const ArgType* __restrict rawA =
      args[hashIdx]->asUnchecked<FlatVector<ArgType>>()->rawValues();
  auto* __restrict rawResult = result.template mutableRawValues<ReturnType>();
  rows->applyToSelected([&](auto row) {
    rawResult[row] = hashOne<HashClass>(rawA[row], rawResult[row]);
  });
}

template <typename HashClass, typename ReturnType>
void hashSimd(
    const SelectivityVector* rows,
    std::vector<VectorPtr>& args,
    FlatVector<ReturnType>& result,
    const int32_t hashIdx) {
  switch (args[hashIdx]->typeKind()) {
#define SCALAR_CASE(kind) \
  case TypeKind::kind:    \
    return hashSimdTyped< \
        HashClass,        \
        ReturnType,       \
        TypeTraits<TypeKind::kind>::NativeType>(rows, args, result, hashIdx);
    SCALAR_CASE(TINYINT)
    SCALAR_CASE(SMALLINT)
    SCALAR_CASE(INTEGER)
    SCALAR_CASE(BIGINT)
    SCALAR_CASE(HUGEINT)
    SCALAR_CASE(REAL)
    SCALAR_CASE(DOUBLE)
    SCALAR_CASE(VARCHAR)
    SCALAR_CASE(VARBINARY)
    SCALAR_CASE(TIMESTAMP)
    SCALAR_CASE(UNKNOWN)
#undef SCALAR_CASE
    default:
      VELOX_UNREACHABLE();
  }
}

// ReturnType can be either int32_t or int64_t
// HashClass contains the function like hashInt32
template <
    typename HashClass,
    typename SeedType = typename HashClass::SeedType,
    typename ReturnType = typename HashClass::ReturnType>
void applyWithType(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
    std::optional<SeedType> seed,
    exec::EvalCtx& context,
    VectorPtr& resultRef) {
  size_t hashIdx = seed ? 1 : 0;
  SeedType hashSeed = seed ? *seed : kDefaultSeed;

  auto& result = *resultRef->as<FlatVector<ReturnType>>();
  rows.applyToSelected([&](auto row) { result.set(row, hashSeed); });

  exec::LocalSelectivityVector selectedMinusNulls(context);

  exec::DecodedArgs decodedArgs(rows, args, context);
  for (auto i = hashIdx; i < args.size(); i++) {
    auto decoded = decodedArgs.at(i);
    const SelectivityVector* selected = &rows;
    if (args[i]->mayHaveNulls()) {
      *selectedMinusNulls.get(rows.end()) = rows;
      selectedMinusNulls->deselectNulls(
          decoded->nulls(&rows), rows.begin(), rows.end());
      selected = selectedMinusNulls.get();
    }

    auto kind = args[i]->typeKind();
    if ((kind == TypeKind::TINYINT || kind == TypeKind::SMALLINT ||
         kind == TypeKind::INTEGER || kind == TypeKind::BIGINT ||
         kind == TypeKind::REAL || kind == TypeKind::DOUBLE ||
         kind == TypeKind::TIMESTAMP || kind == TypeKind::VARCHAR ||
         kind == TypeKind::VARBINARY || kind == TypeKind::HUGEINT ||
         kind == TypeKind::UNKNOWN) &&
        args[i]->isFlatEncoding()) {
      hashSimd<HashClass, ReturnType>(selected, args, result, i);
      continue;
    }

    auto hasher = createVectorHasher<HashClass>(*decoded);
    selected->applyToSelected([&](auto row) {
      result.set(row, hasher->hashNotNullAt(row, result.valueAt(row)));
    });
  }
}

// Derived from
// src/main/java/org/apache/spark/unsafe/hash/Murmur3_x86_32.java.
//
// Spark's Murmur3 seems slightly different from the original from Austin
// Appleby: in particular the fmix function's first line is different. The
// original can be found here:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
//
// Signed integer types have been remapped to unsigned types (as in the
// original) to avoid undefined signed integer overflow and sign extension.

class Murmur3Hash final {
 public:
  using SeedType = int32_t;
  using ReturnType = int32_t;

  static uint32_t hashInt32(int32_t input, uint32_t seed) {
    uint32_t k1 = mixK1(input);
    uint32_t h1 = mixH1(seed, k1);
    return fmix(h1, 4);
  }

  static uint32_t hashInt64(uint64_t input, uint32_t seed) {
    uint32_t low = input;
    uint32_t high = input >> 32;

    uint32_t k1 = mixK1(low);
    uint32_t h1 = mixH1(seed, k1);

    k1 = mixK1(high);
    h1 = mixH1(h1, k1);

    return fmix(h1, 8);
  }

  // Floating point numbers are hashed as if they are integers, with
  // -0f defined to have the same output as +0f.
  static uint32_t hashFloat(float input, uint32_t seed) {
    return hashInt32(
        input == -0.f ? 0 : *reinterpret_cast<uint32_t*>(&input), seed);
  }

  static uint32_t hashDouble(double input, uint32_t seed) {
    return hashInt64(
        input == -0. ? 0 : *reinterpret_cast<uint64_t*>(&input), seed);
  }

  // Spark also has an hashUnsafeBytes2 function, but it was not used at the
  // time of implementation.
  static uint32_t hashBytes(const StringView& input, uint32_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();
    uint32_t h1 = seed;
    for (; i <= end - 4; i += 4) {
      h1 = mixH1(h1, mixK1(*reinterpret_cast<const uint32_t*>(i)));
    }
    for (; i != end; ++i) {
      h1 = mixH1(h1, mixK1(*i));
    }
    return fmix(h1, input.size());
  }

  static uint32_t hashLongDecimal(int128_t input, uint32_t seed) {
    char out[sizeof(int128_t)];
    int32_t length = DecimalUtil::toByteArray(input, out);
    return hashBytes(StringView(out, length), seed);
  }

  static uint32_t hashTimestamp(Timestamp input, uint32_t seed) {
    return hashInt64(input.toMicros(), seed);
  }

 private:
  static uint32_t mixK1(uint32_t k1) {
    k1 *= 0xcc9e2d51;
    k1 = bits::rotateLeft(k1, 15);
    k1 *= 0x1b873593;
    return k1;
  }

  static uint32_t mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = bits::rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  static uint32_t fmix(uint32_t h1, uint32_t length) {
    h1 ^= length;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    return h1;
  }
};

class Murmur3HashFunction final : public exec::VectorFunction {
 public:
  Murmur3HashFunction() = default;
  explicit Murmur3HashFunction(int32_t seed) : seed_(seed) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, INTEGER(), resultRef);
    applyWithType<Murmur3Hash>(rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int32_t> seed_;
};

class XxHash64 final {
 public:
  using SeedType = int64_t;
  using ReturnType = int64_t;

  static uint64_t hashInt32(const int32_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 4L;
    hash ^= static_cast<int64_t>((input & 0xFFFFFFFFL) * PRIME64_1);
    hash = bits::rotateLeft64(hash, 23) * PRIME64_2 + PRIME64_3;
    return fmix(hash);
  }

  static uint64_t hashInt64(int64_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 8L;
    hash ^= bits::rotateLeft64(input * PRIME64_2, 31) * PRIME64_1;
    hash = bits::rotateLeft64(hash, 27) * PRIME64_1 + PRIME64_4;
    return fmix(hash);
  }

  // Floating point numbers are hashed as if they are integers, with
  // -0f defined to have the same output as +0f.
  static uint64_t hashFloat(float input, uint64_t seed) {
    return hashInt32(
        input == -0.f ? 0 : *reinterpret_cast<uint32_t*>(&input), seed);
  }

  static uint64_t hashDouble(double input, uint64_t seed) {
    return hashInt64(
        input == -0. ? 0 : *reinterpret_cast<uint64_t*>(&input), seed);
  }

  static uint64_t hashBytes(const StringView& input, uint64_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();

    uint64_t hash = hashBytesByWords(input, seed);
    uint32_t length = input.size();
    auto offset = i + (length & -8);
    if (offset + 4L <= end) {
      hash ^= (*reinterpret_cast<const uint64_t*>(offset) & 0xFFFFFFFFL) *
          PRIME64_1;
      hash = bits::rotateLeft64(hash, 23) * PRIME64_2 + PRIME64_3;
      offset += 4L;
    }

    while (offset < end) {
      hash ^= (*reinterpret_cast<const uint64_t*>(offset) & 0xFFL) * PRIME64_5;
      hash = bits::rotateLeft64(hash, 11) * PRIME64_1;
      offset++;
    }
    return fmix(hash);
  }

  static uint64_t hashLongDecimal(int128_t input, uint64_t seed) {
    char out[sizeof(int128_t)];
    int32_t length = DecimalUtil::toByteArray(input, out);
    return hashBytes(StringView(out, length), seed);
  }

  static uint64_t hashTimestamp(Timestamp input, uint64_t seed) {
    return hashInt64(input.toMicros(), seed);
  }

 private:
  static const uint64_t PRIME64_1 = 0x9E3779B185EBCA87L;
  static const uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  static const uint64_t PRIME64_3 = 0x165667B19E3779F9L;
  static const uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63L;
  static const uint64_t PRIME64_5 = 0x27D4EB2F165667C5L;

  static uint64_t fmix(uint64_t hash) {
    hash ^= hash >> 33;
    hash *= PRIME64_2;
    hash ^= hash >> 29;
    hash *= PRIME64_3;
    hash ^= hash >> 32;
    return hash;
  }

  static uint64_t hashBytesByWords(const StringView& input, uint64_t seed) {
    const char* i = input.data();
    const char* const end = input.data() + input.size();
    uint32_t length = input.size();
    uint64_t hash;
    if (length >= 32) {
      uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
      uint64_t v2 = seed + PRIME64_2;
      uint64_t v3 = seed;
      uint64_t v4 = seed - PRIME64_1;
      for (; i <= end - 32; i += 32) {
        v1 = bits::rotateLeft64(
                 v1 + (*reinterpret_cast<const uint64_t*>(i) * PRIME64_2), 31) *
            PRIME64_1;
        v2 = bits::rotateLeft64(
                 v2 + (*reinterpret_cast<const uint64_t*>(i + 8) * PRIME64_2),
                 31) *
            PRIME64_1;
        v3 = bits::rotateLeft64(
                 v3 + (*reinterpret_cast<const uint64_t*>(i + 16) * PRIME64_2),
                 31) *
            PRIME64_1;
        v4 = bits::rotateLeft64(
                 v4 + (*reinterpret_cast<const uint64_t*>(i + 24) * PRIME64_2),
                 31) *
            PRIME64_1;
      }
      hash = bits::rotateLeft64(v1, 1) + bits::rotateLeft64(v2, 7) +
          bits::rotateLeft64(v3, 12) + bits::rotateLeft64(v4, 18);
      v1 *= PRIME64_2;
      v1 = bits::rotateLeft64(v1, 31);
      v1 *= PRIME64_1;
      hash ^= v1;
      hash = hash * PRIME64_1 + PRIME64_4;

      v2 *= PRIME64_2;
      v2 = bits::rotateLeft64(v2, 31);
      v2 *= PRIME64_1;
      hash ^= v2;
      hash = hash * PRIME64_1 + PRIME64_4;

      v3 *= PRIME64_2;
      v3 = bits::rotateLeft64(v3, 31);
      v3 *= PRIME64_1;
      hash ^= v3;
      hash = hash * PRIME64_1 + PRIME64_4;

      v4 *= PRIME64_2;
      v4 = bits::rotateLeft64(v4, 31);
      v4 *= PRIME64_1;
      hash ^= v4;
      hash = hash * PRIME64_1 + PRIME64_4;
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;

    for (; i <= end - 8; i += 8) {
      hash ^= bits::rotateLeft64(
                  *reinterpret_cast<const uint64_t*>(i) * PRIME64_2, 31) *
          PRIME64_1;
      hash = bits::rotateLeft64(hash, 27) * PRIME64_1 + PRIME64_4;
    }
    return hash;
  }
};

class XxHash64Function final : public exec::VectorFunction {
 public:
  XxHash64Function() = default;
  explicit XxHash64Function(int64_t seed) : seed_(seed) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, BIGINT(), resultRef);
    applyWithType<XxHash64>(rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int64_t> seed_;
};

bool checkHashElementType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::HUGEINT:
    case TypeKind::TIMESTAMP:
    case TypeKind::UNKNOWN:
      return true;
    case TypeKind::ARRAY:
      return checkHashElementType(type->asArray().elementType());
    case TypeKind::MAP:
      return checkHashElementType(type->asMap().keyType()) &&
          checkHashElementType(type->asMap().valueType());
    case TypeKind::ROW: {
      const auto& children = type->asRow().children();
      return std::all_of(
          children.begin(), children.end(), [](const auto& child) {
            return checkHashElementType(child);
          });
    }
    default:
      return false;
  }
}

void checkArgTypes(const std::vector<exec::VectorFunctionArg>& args) {
  for (const auto& arg : args) {
    if (!checkHashElementType(arg.type)) {
      VELOX_USER_FAIL("Unsupported type for hash: {}", arg.type->toString());
    }
  }
}

} // namespace

// Not all types are supported by now. Check types when making hash function.
// See checkArgTypes.
std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .variableArity("any")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  static const auto kHashFunction = std::make_shared<Murmur3HashFunction>();
  return kHashFunction;
}

std::shared_ptr<exec::VectorFunction> makeHashWithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  const auto& constantSeed = inputArgs[0].constantValue;
  if (!constantSeed || constantSeed->isNullAt(0)) {
    VELOX_USER_FAIL("{} requires a constant non-null seed argument.", name);
  }
  auto seed = constantSeed->as<ConstantVector<int32_t>>()->valueAt(0);
  return std::make_shared<Murmur3HashFunction>(seed);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> hashWithSeedSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .constantArgumentType("integer")
              .variableArity("any")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> xxhash64Signatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .variableArity("any")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
xxhash64WithSeedSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .constantArgumentType("bigint")
              .variableArity("any")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeXxHash64(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  checkArgTypes(inputArgs);
  static const auto kXxHash64Function = std::make_shared<XxHash64Function>();
  return kXxHash64Function;
}

std::shared_ptr<exec::VectorFunction> makeXxHash64WithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& constantSeed = inputArgs[0].constantValue;
  if (!constantSeed || constantSeed->isNullAt(0)) {
    VELOX_USER_FAIL("{} requires a constant non-null seed argument.", name);
  }
  auto seed = constantSeed->as<ConstantVector<int64_t>>()->valueAt(0);
  return std::make_shared<XxHash64Function>(seed);
}

exec::VectorFunctionMetadata hashMetadata() {
  return exec::VectorFunctionMetadataBuilder()
      .defaultNullBehavior(false)
      .build();
}

} // namespace facebook::velox::functions::sparksql
