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

template <bool AllowDuplicateKeys>
class MapFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    auto keys = args[0];
    auto values = args[1];

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decodedKeys = decodedArgs.at(0);
    auto decodedValues = decodedArgs.at(1);

    static const char* kArrayLengthsMismatch =
        "Key and value arrays must be the same length";
    static const char* kDuplicateKey =
        "Duplicate map keys ({}) are not allowed";

    MapVectorPtr mapVector;

    // Take the fast path only if:
    // - both keys and values are identity mapping (no other indirections).
    // - the offsets in both keys and values vectors are the same.
    //
    // (if sizes are different keys and values the function will throw).
    if (decodedKeys->isIdentityMapping() &&
        decodedValues->isIdentityMapping() &&
        offsetsAligned(
            keys->as<ArrayVector>(), values->as<ArrayVector>(), rows)) {
      auto keysArray = keys->as<ArrayVector>();
      auto valuesArray = values->as<ArrayVector>();

      // Check array lengths
      rows.applyToSelected([&](vector_size_t row) {
        VELOX_USER_CHECK_EQ(
            keysArray->sizeAt(row),
            valuesArray->sizeAt(row),
            "{}",
            kArrayLengthsMismatch);
      });

      mapVector = std::make_shared<MapVector>(
          context.pool(),
          outputType,
          BufferPtr(nullptr),
          rows.size(),
          keysArray->offsets(),
          keysArray->sizes(),
          keysArray->elements(),
          valuesArray->elements());
    } else {
      auto keyIndices = decodedKeys->indices();
      auto valueIndices = decodedValues->indices();

      auto keysArray = decodedKeys->base()->as<ArrayVector>();
      auto valuesArray = decodedValues->base()->as<ArrayVector>();

      // Check array lengths
      rows.applyToSelected([&](vector_size_t row) {
        VELOX_USER_CHECK_EQ(
            keysArray->sizeAt(keyIndices[row]),
            valuesArray->sizeAt(valueIndices[row]),
            "{}",
            kArrayLengthsMismatch);
      });

      vector_size_t totalElements = 0;
      rows.applyToSelected([&](auto row) {
        totalElements += keysArray->sizeAt(keyIndices[row]);
      });

      BufferPtr offsets = allocateOffsets(rows.size(), context.pool());
      auto rawOffsets = offsets->asMutable<vector_size_t>();

      BufferPtr sizes = allocateSizes(rows.size(), context.pool());
      auto rawSizes = sizes->asMutable<vector_size_t>();

      BufferPtr valuesIndices = allocateIndices(totalElements, context.pool());
      auto rawValuesIndices = valuesIndices->asMutable<vector_size_t>();

      BufferPtr keysIndices = allocateIndices(totalElements, context.pool());
      auto rawKeysIndices = keysIndices->asMutable<vector_size_t>();

      vector_size_t offset = 0;
      rows.applyToSelected([&](vector_size_t row) {
        auto size = keysArray->sizeAt(keyIndices[row]);
        rawOffsets[row] = offset;
        rawSizes[row] = size;

        auto keysOffset = keysArray->offsetAt(keyIndices[row]);
        auto valuesOffset = valuesArray->offsetAt(valueIndices[row]);
        for (vector_size_t i = 0; i < size; i++) {
          rawKeysIndices[offset + i] = keysOffset + i;
          rawValuesIndices[offset + i] = valuesOffset + i;
        }

        offset += size;
      });

      auto wrappedKeys = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          keysIndices,
          totalElements,
          keysArray->elements());

      auto wrappedValues = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          valuesIndices,
          totalElements,
          valuesArray->elements());

      mapVector = std::make_shared<MapVector>(
          context.pool(),
          outputType,
          BufferPtr(nullptr),
          rows.size(),
          offsets,
          sizes,
          wrappedKeys,
          wrappedValues);
    }

    if constexpr (!AllowDuplicateKeys) {
      // Check for duplicate keys
      MapVector::canonicalize(mapVector);

      auto offsets = mapVector->rawOffsets();
      auto sizes = mapVector->rawSizes();
      auto mapKeys = mapVector->mapKeys();
      rows.applyToSelected([&](vector_size_t row) {
        auto offset = offsets[row];
        auto size = sizes[row];
        for (vector_size_t i = 1; i < size; i++) {
          if (mapKeys->equalValueAt(
                  mapKeys.get(), offset + i, offset + i - 1)) {
            auto duplicateKey = mapKeys->wrappedVector()->toString(
                mapKeys->wrappedIndex(offset + i));
            VELOX_USER_FAIL(kDuplicateKey, duplicateKey);
          }
        }
      });
    }
    context.moveOrCopyResult(mapVector, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(K), array(V) -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("array(K)")
                .argumentType("array(V)")
                .build()};
  }

 private:
  // Given two ArrayVectors, return whether their offset buffer values are the
  // same.
  bool offsetsAligned(
      ArrayVector* keys,
      ArrayVector* values,
      const SelectivityVector& rows) const {
    VELOX_CHECK_GE(keys->size(), rows.size());
    VELOX_CHECK_GE(values->size(), rows.size());
    return rows.testSelected([&](vector_size_t row) {
      return keys->offsetAt(row) == values->offsetAt(row);
    });
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map,
    MapFunction</*AllowDuplicateKeys=*/false>::signatures(),
    std::make_unique<MapFunction</*AllowDuplicateKeys=*/false>>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_allow_duplicates,
    MapFunction</*AllowDuplicateKeys=*/true>::signatures(),
    std::make_unique<MapFunction</*AllowDuplicateKeys=*/true>>());
} // namespace facebook::velox::functions
