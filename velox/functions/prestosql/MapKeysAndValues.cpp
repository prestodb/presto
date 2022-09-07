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

#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class MapKeyValueFunction : public exec::VectorFunction {
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

      localResult = applyFlat(singleRow, flatMap, context);
      localResult =
          BaseVector::wrapInConstant(rows.size(), flatIndex, localResult);
    } else {
      localResult = applyFlat(rows, arg, context);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

 protected:
  explicit MapKeyValueFunction(const std::string& name) : name_(name) {}

  virtual VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const = 0;

 private:
  const std::string name_;
};

class MapKeysFunction : public MapKeyValueFunction {
 public:
  MapKeysFunction() : MapKeyValueFunction("map_keys") {}

  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const override {
    VELOX_CHECK(
        arg->typeKind() == TypeKind::MAP,
        "Unsupported type for map_keys function {}",
        mapTypeKindToName(arg->typeKind()));

    auto mapVector = arg->as<MapVector>();
    auto mapKeys = mapVector->mapKeys();
    return std::make_shared<ArrayVector>(
        context.pool(),
        ARRAY(mapKeys->type()),
        mapVector->nulls(),
        rows.size(),
        mapVector->offsets(),
        mapVector->sizes(),
        mapKeys,
        mapVector->getNullCount());
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V) -> array(K)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(K)")
                .argumentType("map(K,V)")
                .build()};
  }
};

class MapValuesFunction : public MapKeyValueFunction {
 public:
  MapValuesFunction() : MapKeyValueFunction("map_values") {}

  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const override {
    VELOX_CHECK(
        arg->typeKind() == TypeKind::MAP,
        "Unsupported type for map_keys function {}",
        mapTypeKindToName(arg->typeKind()));

    auto mapVector = arg->as<MapVector>();
    auto mapValues = mapVector->mapValues();
    return std::make_shared<ArrayVector>(
        context.pool(),
        ARRAY(mapValues->type()),
        mapVector->nulls(),
        rows.size(),
        mapVector->offsets(),
        mapVector->sizes(),
        mapValues,
        mapVector->getNullCount());
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V) -> array(V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(V)")
                .argumentType("map(K,V)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_keys,
    MapKeysFunction::signatures(),
    std::make_unique<MapKeysFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_values,
    MapValuesFunction::signatures(),
    std::make_unique<MapValuesFunction>());
} // namespace facebook::velox::functions
