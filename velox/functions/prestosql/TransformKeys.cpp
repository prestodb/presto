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
#include "velox/functions/lib/CheckDuplicateKeys.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
class TransformKeysFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    // Flatten input map.
    exec::LocalDecodedVector mapDecoder(context, *args[0], rows);
    auto& decodedMap = *mapDecoder.get();

    auto flatMap = flattenMap(rows, args[0], decodedMap);

    std::vector<VectorPtr> lambdaArgs = {
        flatMap->mapKeys(), flatMap->mapValues()};
    auto numKeys = flatMap->mapKeys()->size();

    SelectivityVector validRowsInReusedResult =
        toElementRows<MapVector>(numKeys, rows, flatMap.get());

    VectorPtr transformedKeys;

    auto elementToTopLevelRows =
        getElementToTopLevelRows(numKeys, rows, flatMap.get(), context.pool());
    const auto* rawElementToTopLevelRows =
        elementToTopLevelRows->as<vector_size_t>();

    // Loop over lambda functions and apply these to keys of the map.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto keyRows =
          toElementRows<MapVector>(numKeys, *entry.rows, flatMap.get());
      auto wrapCapture = toWrapCapture<MapVector>(
          numKeys, entry.callable, *entry.rows, flatMap);

      entry.callable->apply(
          keyRows,
          &validRowsInReusedResult,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &transformedKeys);

      if (transformedKeys->mayHaveNulls() ||
          transformedKeys->mayHaveNullsRecursive()) {
        static const char* kNullKeyErrorMessage = "map key cannot be null";
        static const char* kIndeterminateKeyErrorMessage =
            "map key cannot be indeterminate";

        keyRows.applyToSelected([&](vector_size_t keyRow) {
          try {
            VELOX_USER_CHECK(
                !transformedKeys->isNullAt(keyRow), kNullKeyErrorMessage);

            VELOX_USER_CHECK(
                !transformedKeys->containsNullAt(keyRow),
                "{}: {}",
                kIndeterminateKeyErrorMessage,
                transformedKeys->toString(keyRow));
          } catch (VeloxException&) {
            context.setVeloxExceptionError(
                rawElementToTopLevelRows[keyRow], std::current_exception());
          }
        });
      }
    }

    exec::LocalSelectivityVector remainingRows(context, rows);
    context.deselectErrors(*remainingRows);

    // Set nulls for rows not present in 'remainingRows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatMap, *remainingRows);

    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        std::move(newNulls),
        rows.end(),
        flatMap->offsets(),
        flatMap->sizes(),
        transformedKeys,
        flatMap->mapValues());

    checkDuplicateKeys(localResult, *remainingRows, context);

    context.moveOrCopyResult(localResult, rows, result);
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
};
} // namespace

/// transform_keys is null preserving for the map. But
/// since an expr tree with a lambda depends on all named fields, including
/// captures, a null in a capture does not automatically make a
/// null result.

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_transform_keys,
    TransformKeysFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<TransformKeysFunction>());

} // namespace facebook::velox::functions
