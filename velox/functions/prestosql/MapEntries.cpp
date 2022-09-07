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

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
class MapEntriesFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto& arg = args[0];

    VectorPtr localResult;

    // Input can be constant or flat.
    if (arg->isConstantEncoding()) {
      auto* constantMap = arg->as<ConstantVector<ComplexType>>();
      const auto& flatMap = constantMap->valueVector();
      const auto flatIndex = constantMap->index();

      SelectivityVector singleRow(flatIndex + 1, false);
      singleRow.setValid(flatIndex, true);
      singleRow.updateBounds();

      localResult = applyFlat(singleRow, flatMap, outputType, context);
      localResult =
          BaseVector::wrapInConstant(rows.size(), flatIndex, localResult);
    } else {
      localResult = applyFlat(rows, arg, outputType, context);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V) -> array(row(K,V))
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(row(K,V))")
                .argumentType("map(K,V)")
                .build()};
  }

 private:
  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      const TypePtr& outputType,
      exec::EvalCtx& context) const {
    const auto inputMap = arg->as<MapVector>();

    VectorPtr resultElements = std::make_shared<RowVector>(
        context.pool(),
        outputType->childAt(0),
        BufferPtr(nullptr),
        inputMap->mapKeys()->size(),
        std::vector<VectorPtr>{inputMap->mapKeys(), inputMap->mapValues()});

    return std::make_shared<ArrayVector>(
        context.pool(),
        outputType,
        inputMap->nulls(),
        rows.size(),
        inputMap->offsets(),
        inputMap->sizes(),
        resultElements);
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_entries,
    MapEntriesFunction::signatures(),
    std::make_unique<MapEntriesFunction>());
} // namespace facebook::velox::functions
