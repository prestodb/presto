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
#include "MapConcat.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
template <bool EmptyForNull>
class MapConcatFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_GE(args.size(), 2);
    const TypePtr& mapType = args[0]->type();
    VELOX_CHECK_EQ(mapType->kind(), TypeKind::MAP);
    for (const VectorPtr& arg : args) {
      VELOX_CHECK(mapType->kindEquals(arg->type()));
    }
    VELOX_CHECK(mapType->kindEquals(outputType));

    const uint64_t numArgs = args.size();
    exec::DecodedArgs decodedArgs(rows, args, context);
    vector_size_t maxSize = 0;
    for (int i = 0; i < numArgs; i++) {
      const DecodedVector* decodedArg = decodedArgs.at(i);
      const MapVector* inputMap = decodedArg->base()->as<MapVector>();
      const vector_size_t* rawSizes = inputMap->rawSizes();
      rows.applyToSelected([&](vector_size_t row) {
        if (EmptyForNull && decodedArg->isNullAt(row)) {
          return;
        }
        maxSize += rawSizes[decodedArg->index(row)];
      });
    }

    const TypePtr& keyType = outputType->asMap().keyType();
    const TypePtr& valueType = outputType->asMap().valueType();

    memory::MemoryPool* pool = context.pool();
    auto combinedKeys = BaseVector::create(keyType, maxSize, pool);
    auto combinedValues = BaseVector::create(valueType, maxSize, pool);

    // Initialize offsets and sizes to 0 so that canonicalize() will
    // work also for sparse 'rows'.
    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    int* rawOffsets = offsets->asMutable<vector_size_t>();

    BufferPtr sizes = allocateSizes(rows.end(), pool);
    int* rawSizes = sizes->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    rows.applyToSelected([&](vector_size_t row) {
      rawOffsets[row] = offset;
      // Reuse the last offset and size if null key must create empty map
      for (int i = 0; i < numArgs; i++) {
        const DecodedVector* decodedArg = decodedArgs.at(i);
        if (EmptyForNull && decodedArg->isNullAt(row)) {
          continue; // Treat NULL maps as empty.
        }
        const MapVector* inputMap = decodedArg->base()->as<MapVector>();
        const vector_size_t index = decodedArg->index(row);
        const vector_size_t inputOffset = inputMap->offsetAt(index);
        const vector_size_t inputSize = inputMap->sizeAt(index);
        combinedKeys->copy(
            inputMap->mapKeys().get(), offset, inputOffset, inputSize);
        combinedValues->copy(
            inputMap->mapValues().get(), offset, inputOffset, inputSize);
        offset += inputSize;
      }
      rawSizes[row] = offset - rawOffsets[row];
    });

    auto combinedMap = std::make_shared<MapVector>(
        pool,
        outputType,
        BufferPtr(nullptr),
        rows.end(),
        offsets,
        sizes,
        combinedKeys,
        combinedValues);

    MapVector::canonicalize(combinedMap, true);

    combinedKeys = combinedMap->mapKeys();
    combinedValues = combinedMap->mapValues();

    // Check for duplicate keys
    SelectivityVector uniqueKeys(offset);
    vector_size_t duplicateCnt = 0;
    rows.applyToSelected([&](vector_size_t row) {
      const int mapOffset = rawOffsets[row];
      const int mapSize = rawSizes[row];
      if (duplicateCnt) {
        rawOffsets[row] -= duplicateCnt;
      }
      for (vector_size_t i = 1; i < mapSize; i++) {
        if (combinedKeys->equalValueAt(
                combinedKeys.get(), mapOffset + i, mapOffset + i - 1)) {
          duplicateCnt++;
          // "remove" duplicate entry
          uniqueKeys.setValid(mapOffset + i - 1, false);
          rawSizes[row]--;
        }
      }
    });

    if (duplicateCnt) {
      uniqueKeys.updateBounds();
      const vector_size_t uniqueCount = uniqueKeys.countSelected();

      BufferPtr uniqueIndices = allocateIndices(uniqueCount, pool);
      vector_size_t* rawUniqueIndices =
          uniqueIndices->asMutable<vector_size_t>();
      vector_size_t index = 0;
      uniqueKeys.applyToSelected(
          [&](vector_size_t row) { rawUniqueIndices[index++] = row; });

      VectorPtr keys =
          BaseVector::transpose(uniqueIndices, std::move(combinedKeys));
      VectorPtr values =
          BaseVector::transpose(uniqueIndices, std::move(combinedValues));

      combinedMap = std::make_shared<MapVector>(
          pool,
          outputType,
          BufferPtr(nullptr),
          rows.end(),
          offsets,
          sizes,
          std::move(keys),
          std::move(values));
    }

    context.moveOrCopyResult(combinedMap, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V), map(K,V), ... -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("map(K,V)")
                .variableArity("map(K,V)")
                .build()};
  }
};
} // namespace

void registerMapConcatFunction(const std::string& name) {
  exec::registerVectorFunction(
      name,
      MapConcatFunction</*EmptyForNull=*/false>::signatures(),
      std::make_unique<MapConcatFunction</*EmptyForNull=*/false>>());
}

void registerMapConcatEmptyNullsFunction(const std::string& name) {
  exec::registerVectorFunction(
      name,
      MapConcatFunction</*EmptyForNull=*/true>::signatures(),
      std::make_unique<MapConcatFunction</*EmptyForNull=*/true>>(),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());
}
} // namespace facebook::velox::functions
