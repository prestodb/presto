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
#include <velox/common/base/Exceptions.h>
#include <velox/type/Timestamp.h>
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/VectorTypeUtils.h"

// The description of the map function in Spark
// https://kontext.tech/article/586/spark-sql-map-functions
//
// Example:
// Select map(1,'a',2,'b',3,'c');
// map(1, a, 2, b, 3, c)
//
// Result:
// {1:"a",2:"b",3:"c"}

namespace facebook::velox::functions::sparksql {
namespace {

template <TypeKind kind>
void setKeysResultTyped(
    vector_size_t mapSize,
    std::vector<VectorPtr>& args,
    const VectorPtr& keysResult,
    exec::EvalCtx* context,
    const SelectivityVector& rows) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto flatKeys = keysResult->asFlatVector<T>()
                      ->mutableValues(rows.size() * mapSize)
                      ->template asMutable<T>();

  exec::LocalDecodedVector decoded(context);
  for (vector_size_t i = 0; i < mapSize; i++) {
    decoded.get()->decode(*args[i * 2], rows);
    // For efficiency traverse one arg at the time
    rows.applyToSelected([&](vector_size_t row) {
      VELOX_CHECK(!decoded->isNullAt(row), "Cannot use null as map key!");
      flatKeys[row * mapSize + i] = decoded->valueAt<T>(row);
    });
  }
}

template <TypeKind kind>
void setValuesResultTyped(
    vector_size_t mapSize,
    std::vector<VectorPtr>& args,
    const VectorPtr& valuesResult,
    exec::EvalCtx* context,
    const SelectivityVector& rows) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto flatValues = valuesResult->asFlatVector<T>()
                        ->mutableValues(rows.size() * mapSize)
                        ->template asMutable<T>();

  exec::LocalDecodedVector decoded(context);
  for (vector_size_t i = 0; i < mapSize; i++) {
    decoded.get()->decode(*args[i * 2 + 1], rows);
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded->isNullAt(row)) {
        valuesResult->asFlatVector<T>()->setNull(row * mapSize + i, true);
      } else {
        flatValues[row * mapSize + i] = decoded->valueAt<T>(row);
      }
    });
  }
}

class MapFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(
        args.size() >= 2 && args.size() % 2 == 0,
        "Map function must take an even number of arguments");
    auto mapSize = args.size() / 2;

    auto keyType = args[0]->type();
    auto valueType = args[1]->type();

    // Check key and value types
    for (auto i = 0; i < mapSize; i++) {
      VELOX_CHECK_EQ(
          args[i * 2]->type(),
          keyType,
          "All the key arguments in Map function must be the same!");
      VELOX_CHECK_EQ(
          args[i * 2 + 1]->type(),
          valueType,
          "All the key arguments in Map function must be the same!");
    }

    // Initializing input
    context->ensureWritable(
        rows, std::make_shared<MapType>(keyType, valueType), *result);

    auto mapResult = (*result)->as<MapVector>();
    auto sizes = mapResult->mutableSizes(rows.size());
    auto rawSizes = sizes->asMutable<int32_t>();
    auto offsets = mapResult->mutableOffsets(rows.size());
    auto rawOffsets = offsets->asMutable<int32_t>();

    // Setting size and offsets
    rows.applyToSelected([&](vector_size_t row) {
      rawSizes[row] = mapSize;
      rawOffsets[row] = row * mapSize;
    });

    // Setting keys and value elements
    auto keysResult = mapResult->mapKeys();
    auto valuesResult = mapResult->mapValues();
    keysResult->resize(rows.size() * mapSize);
    valuesResult->resize(rows.size() * mapSize);

    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        setKeysResultTyped,
        keyType->kind(),
        mapSize,
        args,
        keysResult,
        context,
        rows);

    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        setValuesResultTyped,
        valueType->kind(),
        mapSize,
        args,
        valuesResult,
        context,
        rows);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // For the purpose of testing we introduce up to 6 inputs
    // array(K), array(V) -> map(K,V)
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    constexpr int kNumberOfSignatures = 3;
    signatures.reserve(kNumberOfSignatures);
    for (int i = 1; i <= kNumberOfSignatures; i++) {
      auto builder = exec::FunctionSignatureBuilder()
                         .typeVariable("K")
                         .typeVariable("V")
                         .returnType("map(K,V)");
      for (int arg = 0; arg < i; arg++) {
        builder.argumentType("K").argumentType("V");
      }
      signatures.push_back(builder.build());
    }
    return signatures;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map,
    MapFunction::signatures(),
    std::make_unique<MapFunction>());
} // namespace facebook::velox::functions::sparksql
