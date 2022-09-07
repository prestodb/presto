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

#pragma once

#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/NullsBuilder.h"

namespace facebook::velox::functions {

/// Generic subscript/element_at implementation for both array and map data
/// types.
///
/// Provides four template parameters to configure the behavior:
/// - allowNegativeIndices: if allowed, negative indices accesses elements from
///   last to the first; otherwise, throws.
/// - nullOnNegativeIndices: returns NULL for negative indices instead of the
///   behavior described above.
/// - allowOutOfBound: if allowed, returns NULL for out of bound accesses; if
///   false, throws an exception.
/// - indexStartsAtOne: whether indices start at zero or one.
template <
    bool allowNegativeIndices,
    bool nullOnNegativeIndices,
    bool allowOutOfBound,
    bool indexStartsAtOne>
class SubscriptImpl : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    VectorPtr localResult;

    switch (args[0]->typeKind()) {
      case TypeKind::ARRAY:
        localResult = applyArray(rows, args, context);
        break;

      case TypeKind::MAP:
        localResult = applyMap(rows, args, context);
        break;

      default:
        VELOX_UNREACHABLE();
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // array(T), integer -> T
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("T")
            .argumentType("array(T)")
            .argumentType("integer")
            .build(),
        // array(T), bigint -> T
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("T")
            .argumentType("array(T)")
            .argumentType("bigint")
            .build(),
        // map(K,V), K -> V
        exec::FunctionSignatureBuilder()
            .typeVariable("K")
            .typeVariable("V")
            .returnType("V")
            .argumentType("map(K,V)")
            .argumentType("K")
            .build(),
    };
  }

 private:
  VectorPtr applyArray(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context) const {
    VELOX_CHECK_EQ(args[0]->typeKind(), TypeKind::ARRAY);

    auto arrayArg = args[0];
    auto indexArg = args[1];

    switch (indexArg->typeKind()) {
      case TypeKind::INTEGER:
        return applyArrayTyped<int32_t>(rows, arrayArg, indexArg, context);

      case TypeKind::BIGINT:
        return applyArrayTyped<int64_t>(rows, arrayArg, indexArg, context);

      default:
        VELOX_UNSUPPORTED(
            "Unsupported type for element_at index {}",
            mapTypeKindToName(indexArg->typeKind()));
    }
  }

  VectorPtr applyMap(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context) const {
    VELOX_CHECK_EQ(args[0]->typeKind(), TypeKind::MAP);

    auto mapArg = args[0];
    auto indexArg = args[1];

    // Ensure map key type and second argument are the same.
    VELOX_CHECK_EQ(mapArg->type()->childAt(0), indexArg->type());

    switch (indexArg->typeKind()) {
      case TypeKind::BIGINT:
        return applyMapTyped<int64_t>(rows, mapArg, indexArg, context);
      case TypeKind::INTEGER:
        return applyMapTyped<int32_t>(rows, mapArg, indexArg, context);
      case TypeKind::SMALLINT:
        return applyMapTyped<int16_t>(rows, mapArg, indexArg, context);
      case TypeKind::TINYINT:
        return applyMapTyped<int8_t>(rows, mapArg, indexArg, context);
      case TypeKind::VARCHAR:
        return applyMapTyped<StringView>(rows, mapArg, indexArg, context);
      default:
        VELOX_UNSUPPORTED(
            "Unsupported map key type for element_at: {}",
            mapTypeKindToName(indexArg->typeKind()));
    }
  }

  /// Decode arguments and transform result into a dictionaryVector where the
  /// dictionary maintains a mapping from a given row to the index of the input
  /// elements vector. This allows us to ensure that element_at is zero-copy.
  template <typename I>
  VectorPtr applyArrayTyped(
      const SelectivityVector& rows,
      const VectorPtr& arrayArg,
      const VectorPtr& indexArg,
      exec::EvalCtx& context) const {
    auto* pool = context.pool();

    BufferPtr indices = allocateIndices(rows.size(), pool);
    auto rawIndices = indices->asMutable<vector_size_t>();

    // Create nulls for lazy initialization.
    NullsBuilder nullsBuilder(rows.size(), pool);

    exec::LocalDecodedVector arrayHolder(context, *arrayArg, rows);
    auto decodedArray = arrayHolder.get();
    auto baseArray = decodedArray->base()->as<ArrayVector>();
    auto arrayIndices = decodedArray->indices();

    exec::LocalDecodedVector indexHolder(context, *indexArg, rows);
    auto decodedIndices = indexHolder.get();

    auto rawSizes = baseArray->rawSizes();
    auto rawOffsets = baseArray->rawOffsets();

    // Optimize for constant encoding case.
    if (decodedIndices->isConstantMapping()) {
      vector_size_t adjustedIndex = -1;
      bool allFailed = false;
      // If index is invalid, capture the error and mark all rows as failed.
      try {
        adjustedIndex = adjustIndex(decodedIndices->valueAt<I>(0));
      } catch (const std::exception& e) {
        rows.applyToSelected(
            [&](auto row) { context.setError(row, std::current_exception()); });
        allFailed = true;
      }

      if (!allFailed) {
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          auto elementIndex =
              getIndex(adjustedIndex, row, rawSizes, rawOffsets, arrayIndices);
          rawIndices[row] = elementIndex;
          if (elementIndex == -1) {
            nullsBuilder.setNull(row);
          }
        });
      }
    } else {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        auto adjustedIndex = adjustIndex(decodedIndices->valueAt<I>(row));
        auto elementIndex =
            getIndex(adjustedIndex, row, rawSizes, rawOffsets, arrayIndices);
        rawIndices[row] = elementIndex;
        if (elementIndex == -1) {
          nullsBuilder.setNull(row);
        }
      });
    }
    return BaseVector::wrapInDictionary(
        nullsBuilder.build(), indices, rows.size(), baseArray->elements());
  }

  // Normalize indices from 1 or 0-based into always 0-based (according to
  // indexStartsAtOne template parameter - no-op if it's false).
  template <typename I>
  vector_size_t adjustIndex(I index) const {
    // If array indices start at 1.
    if constexpr (indexStartsAtOne) {
      // If it's zero, throw.
      if (UNLIKELY(index == 0)) {
        VELOX_USER_FAIL("SQL array indices start at 1");
      }

      // If larger than zero, adjust it.
      if (index > 0) {
        index--;
      }
    }
    return index;
  }

  // Returns the actual Vector index given an array index. Checks and adjusts
  // negative indices, in addition to bound checks.
  // `index` is always a 0-based array index (see `adjustIndex` function above).
  template <typename I>
  vector_size_t getIndex(
      I index,
      vector_size_t row,
      const vector_size_t* rawSizes,
      const vector_size_t* rawOffsets,
      const vector_size_t* indices) const {
    auto arraySize = rawSizes[indices[row]];

    if (index < 0) {
      // Check if we allow negative indices. If so, adjust.
      if constexpr (allowNegativeIndices) {
        if constexpr (nullOnNegativeIndices) {
          return -1;
        } else {
          index += arraySize;
        }
      } else {
        VELOX_USER_FAIL("Array subscript is negative.");
      }
    }

    // Check if index is within bound.
    if ((index >= arraySize) || (index < 0)) {
      // If we allow it, return null.
      if constexpr (allowOutOfBound) {
        return -1;
      }
      // Otherwise, throw.
      else {
        VELOX_USER_FAIL("Array subscript out of bounds.");
      }
    }

    // Resultant index is the sum of the offset in the input array and the
    // index.
    return rawOffsets[indices[row]] + index;
  }

  /// Decode arguments and transform result into a dictionaryVector where the
  /// dictionary maintains a mapping from a given row to the index of the input
  /// map value vector. This allows us to ensure that element_at is zero-copy.
  template <typename TKey>
  VectorPtr applyMapTyped(
      const SelectivityVector& rows,
      const VectorPtr& mapArg,
      const VectorPtr& indexArg,
      exec::EvalCtx& context) const {
    auto* pool = context.pool();

    BufferPtr indices = allocateIndices(rows.size(), pool);
    auto rawIndices = indices->asMutable<vector_size_t>();

    // Create nulls for lazy initialization.
    NullsBuilder nullsBuilder(rows.size(), pool);

    // Get base MapVector.
    // TODO: Optimize the case when indices are identity.
    exec::LocalDecodedVector mapHolder(context, *mapArg, rows);
    auto decodedMap = mapHolder.get();
    auto baseMap = decodedMap->base()->as<MapVector>();
    auto mapIndices = decodedMap->indices();

    // Get map keys.
    auto mapKeys = baseMap->mapKeys();
    exec::LocalSelectivityVector allElementRows(context, mapKeys->size());
    allElementRows->setAll();
    exec::LocalDecodedVector mapKeysHolder(context, *mapKeys, *allElementRows);
    auto decodedMapKeys = mapKeysHolder.get();

    // Get index vector (second argument).
    exec::LocalDecodedVector indexHolder(context, *indexArg, rows);
    auto decodedIndices = indexHolder.get();

    auto rawSizes = baseMap->rawSizes();
    auto rawOffsets = baseMap->rawOffsets();

    // Lambda that does the search for a key, for each row.
    auto processRow = [&](vector_size_t row, TKey searchKey) {
      size_t mapIndex = mapIndices[row];
      size_t offsetStart = rawOffsets[mapIndex];
      size_t offsetEnd = offsetStart + rawSizes[mapIndex];
      bool found = false;

      // Sequentially check each key on this map for a match. We use a
      // sequential scan over the keys because it's easier to express (and
      // likely has good memory locality), but if we find that this is slow
      // for large maps we could try to canonicalize the whole vector and binary
      // search `searchKey`.
      for (size_t offset = offsetStart; offset < offsetEnd; ++offset) {
        if (decodedMapKeys->valueAt<TKey>(offset) == searchKey) {
          rawIndices[row] = offset;
          found = true;
          break;
        }
      }

      // NB: We still allow non-existent map keys, even if out of bounds is
      // disabled for arrays.

      // Handle NULLs.
      if (!found) {
        nullsBuilder.setNull(row);
      }
    };

    // When second argument ("at") is a constant.
    if (decodedIndices->isConstantMapping()) {
      auto searchKey = decodedIndices->valueAt<TKey>(0);
      rows.applyToSelected(
          [&](vector_size_t row) { processRow(row, searchKey); });
    }
    // When the second argument ("at") is also a variable vector.
    else {
      rows.applyToSelected([&](vector_size_t row) {
        auto searchKey = decodedIndices->valueAt<TKey>(row);
        processRow(row, searchKey);
      });
    }
    return BaseVector::wrapInDictionary(
        nullsBuilder.build(), indices, rows.size(), baseMap->mapValues());
  }
};

} // namespace facebook::velox::functions
