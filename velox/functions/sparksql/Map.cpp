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

void setKeysAndValuesResult(
    vector_size_t mapSize,
    vector_size_t baseOffset,
    std::vector<VectorPtr>& args,
    const VectorPtr& keysResult,
    const VectorPtr& valuesResult,
    exec::EvalCtx& context,
    const SelectivityVector& rows) {
  exec::LocalDecodedVector decoded(context);
  SelectivityVector targetRows(keysResult->size(), false);
  std::vector<vector_size_t> toSourceRow(keysResult->size());
  for (vector_size_t i = 0; i < mapSize; i++) {
    decoded.get()->decode(*args[i * 2], rows);
    auto offset = baseOffset;
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      VELOX_USER_CHECK(!decoded->isNullAt(row), "Cannot use null as map key!");
      targetRows.setValid(offset + i, true);
      toSourceRow[offset + i] = row;
      offset += mapSize;
    });
    targetRows.updateBounds();
    keysResult->copy(args[i * 2].get(), targetRows, toSourceRow.data());
    valuesResult->copy(args[i * 2 + 1].get(), targetRows, toSourceRow.data());
    targetRows.clearAll();
  }
}

class MapFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_USER_CHECK(
        args.size() >= 2 && args.size() % 2 == 0,
        "Map function must take an even number of arguments");
    auto mapSize = args.size() / 2;

    auto keyType = args[0]->type();
    auto valueType = args[1]->type();

    // Check key and value types
    for (auto i = 0; i < mapSize; i++) {
      VELOX_USER_CHECK(
          args[i * 2]->type()->equivalent(*keyType),
          "All the key arguments in Map function must be the same!");
      VELOX_USER_CHECK(
          args[i * 2 + 1]->type()->equivalent(*valueType),
          "All the value arguments in Map function must be the same!");
    }

    // Initializing input
    context.ensureWritable(
        rows, std::make_shared<MapType>(keyType, valueType), result);

    auto mapResult = result->as<MapVector>();
    auto sizes = mapResult->mutableSizes(rows.end());
    auto rawSizes = sizes->asMutable<int32_t>();
    auto offsets = mapResult->mutableOffsets(rows.end());
    auto rawOffsets = offsets->asMutable<int32_t>();

    // Setting keys and value elements
    auto& keysResult = mapResult->mapKeys();
    auto& valuesResult = mapResult->mapValues();
    const auto baseOffset =
        std::max<vector_size_t>(keysResult->size(), valuesResult->size());

    // Setting size and offsets
    vector_size_t offset = baseOffset;
    rows.applyToSelected([&](vector_size_t row) {
      rawSizes[row] = mapSize;
      rawOffsets[row] = offset;
      offset += mapSize;
    });

    keysResult->resize(offset);
    valuesResult->resize(offset);
    setKeysAndValuesResult(
        mapSize, baseOffset, args, keysResult, valuesResult, context, rows);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // For the purpose of testing we introduce up to 6 inputs
    // array(K), array(V) -> map(K,V)
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    constexpr int kNumberOfSignatures = 3;
    signatures.reserve(kNumberOfSignatures);
    for (int i = 1; i <= kNumberOfSignatures; i++) {
      auto builder = exec::FunctionSignatureBuilder()
                         .knownTypeVariable("K")
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

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_map,
    MapFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<MapFunction>());
} // namespace facebook::velox::functions::sparksql
