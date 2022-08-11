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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
class TransformKeysFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // transform_keys is null preserving for the map. But
    // since an expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    // Flatten input map.
    exec::LocalDecodedVector mapDecoder(context, *args[0], rows);
    auto& decodedMap = *mapDecoder.get();

    auto flatMap = flattenMap(rows, args[0], decodedMap);

    std::vector<VectorPtr> lambdaArgs = {
        flatMap->mapKeys(), flatMap->mapValues()};
    auto numKeys = flatMap->mapKeys()->size();

    VectorPtr transformedKeys;

    // Loop over lambda functions and apply these to keys of the map.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto keyRows =
          toElementRows<MapVector>(numKeys, *entry.rows, flatMap.get());
      auto wrapCapture = toWrapCapture<MapVector>(
          numKeys, entry.callable, *entry.rows, flatMap);

      entry.callable->apply(
          keyRows, wrapCapture, context, lambdaArgs, &transformedKeys);
    }

    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        flatMap->nulls(),
        flatMap->size(),
        flatMap->offsets(),
        flatMap->sizes(),
        transformedKeys,
        flatMap->mapValues());

    checkDuplicateKeys(localResult, rows);

    context->moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K1, V), function(K1, V) -> K2 -> map(K2, V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K1")
                .typeVariable("K2")
                .typeVariable("V")
                .returnType("map(K2,V)")
                .argumentType("map(K1,V)")
                .argumentType("function(K1,V,K2)")
                .build()};
  }

 private:
  void checkDuplicateKeys(
      const MapVectorPtr& mapVector,
      const SelectivityVector& rows) const {
    static const char* kDuplicateKey =
        "Duplicate map keys ({}) are not allowed";

    MapVector::canonicalize(mapVector);

    auto offsets = mapVector->rawOffsets();
    auto sizes = mapVector->rawSizes();
    auto mapKeys = mapVector->mapKeys();
    rows.applyToSelected([&](auto row) {
      auto offset = offsets[row];
      auto size = sizes[row];
      for (auto i = 1; i < size; i++) {
        if (mapKeys->equalValueAt(mapKeys.get(), offset + i, offset + i - 1)) {
          auto duplicateKey = mapKeys->wrappedVector()->toString(
              mapKeys->wrappedIndex(offset + i));
          VELOX_USER_FAIL(kDuplicateKey, duplicateKey);
        }
      }
    });
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_transform_keys,
    TransformKeysFunction::signatures(),
    std::make_unique<TransformKeysFunction>());

} // namespace facebook::velox::functions
