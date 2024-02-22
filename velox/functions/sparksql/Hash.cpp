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

// ReturnType can be either int32_t or int64_t
// HashClass contains the function like hashInt32
template <typename ReturnType, typename HashClass, typename SeedType>
void applyWithType(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
    std::optional<SeedType> seed,
    exec::EvalCtx& context,
    VectorPtr& resultRef) {
  HashClass hash;
  size_t hashIdx = seed ? 1 : 0;
  SeedType hashSeed = seed ? *seed : kDefaultSeed;

  auto& result = *resultRef->as<FlatVector<ReturnType>>();
  rows.applyToSelected([&](int row) { result.set(row, hashSeed); });

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
    switch (args[i]->type()->kind()) {
// Derived from InterpretedHashFunction.hash:
// https://github.com/apache/spark/blob/382b66e/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L532
#define CASE(typeEnum, hashFn, inputType)                                      \
  case TypeKind::typeEnum:                                                     \
    selected->applyToSelected([&](int row) {                                   \
      result.set(                                                              \
          row, hashFn(decoded->valueAt<inputType>(row), result.valueAt(row))); \
    });                                                                        \
    break;
      CASE(BOOLEAN, hash.hashInt32, bool);
      CASE(TINYINT, hash.hashInt32, int8_t);
      CASE(SMALLINT, hash.hashInt32, int16_t);
      CASE(INTEGER, hash.hashInt32, int32_t);
      CASE(BIGINT, hash.hashInt64, int64_t);
      CASE(VARCHAR, hash.hashBytes, StringView);
      CASE(VARBINARY, hash.hashBytes, StringView);
      CASE(REAL, hash.hashFloat, float);
      CASE(DOUBLE, hash.hashDouble, double);
      CASE(HUGEINT, hash.hashLongDecimal, int128_t);
      CASE(TIMESTAMP, hash.hashTimestamp, Timestamp);
#undef CASE
      default:
        VELOX_NYI(
            "Unsupported type for HASH(): {}", args[i]->type()->toString());
    }
  }
}

// Derived from src/main/java/org/apache/spark/unsafe/hash/Murmur3_x86_32.java.
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
  uint32_t hashInt32(int32_t input, uint32_t seed) {
    uint32_t k1 = mixK1(input);
    uint32_t h1 = mixH1(seed, k1);
    return fmix(h1, 4);
  }

  uint32_t hashInt64(uint64_t input, uint32_t seed) {
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
  uint32_t hashFloat(float input, uint32_t seed) {
    return hashInt32(
        input == -0.f ? 0 : *reinterpret_cast<uint32_t*>(&input), seed);
  }

  uint32_t hashDouble(double input, uint32_t seed) {
    return hashInt64(
        input == -0. ? 0 : *reinterpret_cast<uint64_t*>(&input), seed);
  }

  // Spark also has an hashUnsafeBytes2 function, but it was not used at the
  // time of implementation.
  uint32_t hashBytes(const StringView& input, uint32_t seed) {
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

  uint32_t hashLongDecimal(int128_t input, uint32_t seed) {
    char out[sizeof(int128_t)];
    int32_t length = DecimalUtil::toByteArray(input, out);
    return hashBytes(StringView(out, length), seed);
  }

  uint32_t hashTimestamp(Timestamp input, uint32_t seed) {
    return hashInt64(input.toMicros(), seed);
  }

 private:
  uint32_t mixK1(uint32_t k1) {
    k1 *= 0xcc9e2d51;
    k1 = bits::rotateLeft(k1, 15);
    k1 *= 0x1b873593;
    return k1;
  }

  uint32_t mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = bits::rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  uint32_t fmix(uint32_t h1, uint32_t length) {
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

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, INTEGER(), resultRef);
    applyWithType<int32_t, Murmur3Hash>(rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int32_t> seed_;
};

class XxHash64 final {
  const uint64_t PRIME64_1 = 0x9E3779B185EBCA87L;
  const uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  const uint64_t PRIME64_3 = 0x165667B19E3779F9L;
  const uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63L;
  const uint64_t PRIME64_5 = 0x27D4EB2F165667C5L;

 public:
  int64_t hashInt32(const int32_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 4L;
    hash ^= static_cast<int64_t>((input & 0xFFFFFFFFL) * PRIME64_1);
    hash = bits::rotateLeft64(hash, 23) * PRIME64_2 + PRIME64_3;
    return fmix(hash);
  }

  int64_t hashInt64(int64_t input, uint64_t seed) {
    int64_t hash = seed + PRIME64_5 + 8L;
    hash ^= bits::rotateLeft64(input * PRIME64_2, 31) * PRIME64_1;
    hash = bits::rotateLeft64(hash, 27) * PRIME64_1 + PRIME64_4;
    return fmix(hash);
  }

  // Floating point numbers are hashed as if they are integers, with
  // -0f defined to have the same output as +0f.
  int64_t hashFloat(float input, uint64_t seed) {
    return hashInt32(
        input == -0.f ? 0 : *reinterpret_cast<uint32_t*>(&input), seed);
  }

  int64_t hashDouble(double input, uint64_t seed) {
    return hashInt64(
        input == -0. ? 0 : *reinterpret_cast<uint64_t*>(&input), seed);
  }

  uint64_t hashBytes(const StringView& input, uint64_t seed) {
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

  int64_t hashLongDecimal(int128_t input, uint32_t seed) {
    char out[sizeof(int128_t)];
    int32_t length = DecimalUtil::toByteArray(input, out);
    return hashBytes(StringView(out, length), seed);
  }

  int64_t hashTimestamp(Timestamp input, uint32_t seed) {
    return hashInt64(input.toMicros(), seed);
  }

 private:
  uint64_t fmix(uint64_t hash) {
    hash ^= hash >> 33;
    hash *= PRIME64_2;
    hash ^= hash >> 29;
    hash *= PRIME64_3;
    hash ^= hash >> 32;
    return hash;
  }

  uint64_t hashBytesByWords(const StringView& input, uint64_t seed) {
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

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    context.ensureWritable(rows, BIGINT(), resultRef);
    applyWithType<int64_t, XxHash64>(rows, args, seed_, context, resultRef);
  }

 private:
  const std::optional<int64_t> seed_;
};

} // namespace

// Not all types are supported by now. Check types when making hash function.
// See checkArgTypes.
std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .argumentType("any")
              .variableArity()
              .build()};
}

void checkArgTypes(const std::vector<exec::VectorFunctionArg>& args) {
  for (const auto& arg : args) {
    switch (arg.type->kind()) {
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
        break;
      default:
        VELOX_USER_FAIL("Unsupported type for hash: {}", arg.type->toString())
    }
  }
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
              .argumentType("any")
              .variableArity()
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> xxhash64Signatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
xxhash64WithSeedSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .constantArgumentType("bigint")
              .argumentType("any")
              .variableArity()
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

} // namespace facebook::velox::functions::sparksql
