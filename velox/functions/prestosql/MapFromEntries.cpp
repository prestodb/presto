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
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/CheckDuplicateKeys.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {
// See documentation at https://prestodb.io/docs/current/functions/map.html
class MapFromEntriesFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    auto& arg = args[0];
    VectorPtr localResult;

    // Input can be constant or flat.
    if (arg->isConstantEncoding()) {
      auto* constantArray = arg->as<ConstantVector<ComplexType>>();
      const auto& flatArray = constantArray->valueVector();
      const auto flatIndex = constantArray->index();

      exec::LocalSelectivityVector singleRow(context, flatIndex + 1);
      singleRow->clearAll();
      singleRow->setValid(flatIndex, true);
      singleRow->updateBounds();

      localResult = applyFlat(
          *singleRow.get(), flatArray->as<ArrayVector>(), outputType, context);
      localResult =
          BaseVector::wrapInConstant(rows.size(), flatIndex, localResult);
    } else {
      localResult =
          applyFlat(rows, arg->as<ArrayVector>(), outputType, context);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {// array(unknown) -> map(unknown, unknown)
            exec::FunctionSignatureBuilder()
                .returnType("map(unknown, unknown)")
                .argumentType("array(unknown)")
                .build(),
            // array(row(K,V)) -> map(K,V)
            exec::FunctionSignatureBuilder()
                .knownTypeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("array(row(K,V))")
                .build()};
  }

 private:
  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const ArrayVector* inputArray,
      const TypePtr& outputType,
      exec::EvalCtx& context) const {
    auto& inputRowVector = inputArray->elements();
    exec::LocalDecodedVector decodedRow(context);
    decodedRow.get()->decode(*inputRowVector);
    auto rowVector = decodedRow->base()->as<RowVector>();
    auto rowKeyVector = rowVector->childAt(0);

    // Validate all map entries and map keys are not null.
    if (decodedRow->mayHaveNulls() || rowKeyVector->mayHaveNulls()) {
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        auto size = inputArray->sizeAt(row);
        auto offset = inputArray->offsetAt(row);
        for (auto i = 0; i < size; ++i) {
          bool isMapEntryNull = decodedRow->isNullAt(offset + i);
          VELOX_USER_CHECK(!isMapEntryNull, "map entry cannot be null");
          bool isMapKeyNull =
              rowKeyVector->isNullAt(decodedRow->index(offset + i));
          VELOX_USER_CHECK(!isMapKeyNull, "map key cannot be null");
        }
      });
    }

    VectorPtr wrappedKeys;
    VectorPtr wrappedValues;
    if (decodedRow->isIdentityMapping()) {
      wrappedKeys = rowVector->childAt(0);
      wrappedValues = rowVector->childAt(1);
    } else {
      wrappedKeys = decodedRow->wrap(
          rowVector->childAt(0), *inputRowVector, inputRowVector->size());
      wrappedValues = decodedRow->wrap(
          rowVector->childAt(1), *inputRowVector, inputRowVector->size());
    }

    // To avoid creating new buffers, we try to reuse the input's buffers
    // as many as possible.
    auto mapVector = std::make_shared<MapVector>(
        context.pool(),
        outputType,
        inputArray->nulls(),
        rows.end(),
        inputArray->offsets(),
        inputArray->sizes(),
        wrappedKeys,
        wrappedValues);

    checkDuplicateKeys(mapVector, rows, context);
    return mapVector;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_from_entries,
    MapFromEntriesFunction::signatures(),
    std::make_unique<MapFromEntriesFunction>());
} // namespace facebook::velox::functions
