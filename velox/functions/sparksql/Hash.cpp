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

#include <stdint.h>

#include <folly/CPortability.h>

#include "velox/common/base/BitUtil.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {

// Derived from src/main/java/org/apache/spark/unsafe/hash/Murmur3_x86_32.java.
//
// Spark's Murmur3 seems slightly different from the original from Austin
// Appleby: in particular the fmix function's first line is different. The
// original can be found here:
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
//
// Signed integer types have been remapped to unsigned types (as in the
// original) to avoid undefined signed integer overflow and sign extension.

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

// Spark also has an hashUnsafeBytes2 function, but it was not used at the time
// of implementation.
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

class HashFunction final : public exec::VectorFunction {
  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* resultRef) const final {
    constexpr int32_t kSeed = 42;

    context->ensureWritable(rows, INTEGER(), *resultRef);

    FlatVector<int32_t>& result = *(*resultRef)->as<FlatVector<int32_t>>();
    rows.applyToSelected([&](int row) { result.set(row, kSeed); });

    exec::LocalSelectivityVector selectedMinusNulls(context);

    for (auto& arg : args) {
      exec::LocalDecodedVector decoded(context, *arg, rows);
      const SelectivityVector* selected = &rows;
      if (arg->mayHaveNulls()) {
        *selectedMinusNulls.get(rows.end()) = rows;
        selectedMinusNulls->deselectNulls(
            arg->flatRawNulls(rows), rows.begin(), rows.end());
        selected = selectedMinusNulls.get();
      }
      switch (arg->type()->kind()) {
#define CASE(typeEnum, hashFn, inputType)                                      \
  case TypeKind::typeEnum:                                                     \
    selected->applyToSelected([&](int row) {                                   \
      result.set(                                                              \
          row, hashFn(decoded->valueAt<inputType>(row), result.valueAt(row))); \
    });                                                                        \
    break;
        // Derived from InterpretedHashFunction.hash:
        // https://github.com/apache/spark/blob/382b66e/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L532
        CASE(BOOLEAN, hashInt32, bool);
        CASE(TINYINT, hashInt32, int8_t);
        CASE(SMALLINT, hashInt32, int16_t);
        CASE(INTEGER, hashInt32, int32_t);
        CASE(BIGINT, hashInt64, int64_t);
        CASE(VARCHAR, hashBytes, StringView);
        CASE(VARBINARY, hashBytes, StringView);
        CASE(REAL, hashFloat, float);
        CASE(DOUBLE, hashDouble, double);
#undef CASE
        default:
          VELOX_NYI("Unsupported type for HASH(): {}", arg->type()->toString());
      }
    }
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("integer")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  static const auto kHashFunction = std::make_shared<HashFunction>();
  return kHashFunction;
}

} // namespace facebook::velox::functions::sparksql
