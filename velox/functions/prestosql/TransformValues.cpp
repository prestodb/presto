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
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
class TransformValuesFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // transform_values is null preserving for the map. But
    // since an expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

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
    auto numValues = flatMap->mapValues()->size();

    SelectivityVector finalSelection;
    if (!context.isFinalSelection()) {
      finalSelection = toElementRows<MapVector>(
          numValues, *context.finalSelection(), flatMap.get());
    }

    VectorPtr transformedValues;

    auto elementToTopLevelRows = getElementToTopLevelRows(
        numValues, rows, flatMap.get(), context.pool());

    // Loop over lambda functions and apply these to values of the map.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto valueRows =
          toElementRows<MapVector>(numValues, *entry.rows, flatMap.get());
      auto wrapCapture = toWrapCapture<MapVector>(
          numValues, entry.callable, *entry.rows, flatMap);

      entry.callable->apply(
          valueRows,
          finalSelection,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &transformedValues);
    }

    auto localResult = std::make_shared<MapVector>(
        flatMap->pool(),
        outputType,
        flatMap->nulls(),
        flatMap->size(),
        flatMap->offsets(),
        flatMap->sizes(),
        flatMap->mapKeys(),
        transformedValues);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K, V1), function(K, V1) -> V2 -> map(K, V2)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V1")
                .typeVariable("V2")
                .returnType("map(K,V2)")
                .argumentType("map(K,V1)")
                .argumentType("function(K,V1,V2)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_transform_values,
    TransformValuesFunction::signatures(),
    std::make_unique<TransformValuesFunction>());

} // namespace facebook::velox::functions
