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
    static const char* kNullKey = "map key cannot be null";

    // If both vectors have identity mapping, check if we can take the zero-copy
    // fast-path.
    if (decodedKeys->isIdentityMapping() &&
        decodedValues->isIdentityMapping() &&
        canTakeFastPath(
            keys->as<ArrayVector>(), values->as<ArrayVector>(), rows)) {
      auto keysArray = keys->as<ArrayVector>();
      auto valuesArray = values->as<ArrayVector>();

      // Verify there are no null keys.
      auto keysElements = keysArray->elements();
      if (keysElements->mayHaveNulls()) {
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          auto offset = keysArray->offsetAt(row);
          auto size = keysArray->sizeAt(row);
          for (auto i = 0; i < size; ++i) {
            VELOX_USER_CHECK(!keysElements->isNullAt(offset + i), kNullKey);
          }
        });
      }

      auto mapVector = std::make_shared<MapVector>(
          context.pool(),
          outputType,
          BufferPtr(nullptr),
          rows.end(),
          keysArray->offsets(),
          keysArray->sizes(),
          keysArray->elements(),
          valuesArray->elements());

      if constexpr (!AllowDuplicateKeys) {
        checkDuplicateKeys(mapVector, rows, context);
      }
      context.moveOrCopyResult(mapVector, rows, result);
    } else {
      auto keyIndices = decodedKeys->indices();
      auto valueIndices = decodedValues->indices();

      auto keysArray = decodedKeys->base()->as<ArrayVector>();
      auto valuesArray = decodedValues->base()->as<ArrayVector>();

      // When context.throwOnError is false, some rows will be marked as
      // 'failed'. These rows should not be processed further. 'remainingRows'
      // will contain a subset of 'rows' that have passed all the checks (e.g.
      // keys are not nulls and number of keys and values is the same).
      SelectivityVector remainingRows = rows;

      // Verify there are no null keys.
      auto keysElements = keysArray->elements();
      if (keysElements->mayHaveNulls()) {
        context.applyToSelectedNoThrow(remainingRows, [&](auto row) {
          auto offset = keysArray->offsetAt(keyIndices[row]);
          auto size = keysArray->sizeAt(keyIndices[row]);
          for (auto i = 0; i < size; ++i) {
            VELOX_USER_CHECK(!keysElements->isNullAt(offset + i), kNullKey);
          }
        });
        context.deselectErrors(remainingRows);
      }

      // Check array lengths
      context.applyToSelectedNoThrow(remainingRows, [&](vector_size_t row) {
        VELOX_USER_CHECK_EQ(
            keysArray->sizeAt(keyIndices[row]),
            valuesArray->sizeAt(valueIndices[row]),
            "{}",
            kArrayLengthsMismatch);
      });

      context.deselectErrors(remainingRows);

      vector_size_t totalElements = 0;
      remainingRows.applyToSelected([&](auto row) {
        totalElements += keysArray->sizeAt(keyIndices[row]);
      });

      BufferPtr offsets = allocateOffsets(rows.end(), context.pool());
      auto rawOffsets = offsets->asMutable<vector_size_t>();

      BufferPtr sizes = allocateSizes(rows.end(), context.pool());
      auto rawSizes = sizes->asMutable<vector_size_t>();

      BufferPtr valuesIndices = allocateIndices(totalElements, context.pool());
      auto rawValuesIndices = valuesIndices->asMutable<vector_size_t>();

      BufferPtr keysIndices = allocateIndices(totalElements, context.pool());
      auto rawKeysIndices = keysIndices->asMutable<vector_size_t>();

      vector_size_t offset = 0;
      remainingRows.applyToSelected([&](vector_size_t row) {
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

      auto mapVector = std::make_shared<MapVector>(
          context.pool(),
          outputType,
          BufferPtr(nullptr),
          rows.end(),
          offsets,
          sizes,
          wrappedKeys,
          wrappedValues);
      if constexpr (!AllowDuplicateKeys) {
        checkDuplicateKeys(mapVector, remainingRows, context);
      }
      context.moveOrCopyResult(mapVector, remainingRows, result);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(K), array(V) -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .knownTypeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("array(K)")
                .argumentType("array(V)")
                .build()};
  }

 private:
  // Can only take the fast path if keys and values have an equal
  // number of arrays and the offsets and sizes of these arrays match
  // 1:1. The map must be well formed for all elements, also ones not
  // in 'rows' in apply(). This is because canonicalize() will touch
  // all elements in any case.
  bool canTakeFastPath(
      ArrayVector* keys,
      ArrayVector* values,
      const SelectivityVector& rows) const {
    VELOX_CHECK_GE(keys->size(), rows.end());
    VELOX_CHECK_GE(values->size(), rows.end());
    // the fast path takes a reference to the keys and values and the
    // offsets and sizes from keys. This is valid only if the keys and
    // values align for all rows for both size and offset. Anything
    // else will break canonicalize().
    if (keys->size() != values->size()) {
      return false;
    }
    for (auto row = 0; row < keys->size(); ++row) {
      if (keys->offsetAt(row) != values->offsetAt(row) ||
          keys->sizeAt(row) != values->sizeAt(row)) {
        return false;
      }
    }
    return true;
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
