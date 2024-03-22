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

#include <memory>
#include "velox/expression/ComplexViewTypes.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorReaders.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::functions {

// Below functions return a stock instance of each of the possible errors in
// SubscriptImpl
const std::exception_ptr& zeroSubscriptError();
const std::exception_ptr& badSubscriptError();
const std::exception_ptr& negativeSubscriptError();

template <TypeKind kind>
class LookupTable;

class LookupTableBase {
 public:
  template <TypeKind kind>
  LookupTable<kind>* typedTable() {
    return static_cast<LookupTable<kind>*>(this);
  }
  virtual ~LookupTableBase() {}
};

template <TypeKind kind>
class LookupTable : public LookupTableBase {
  using key_t = typename TypeTraits<kind>::NativeType;

 public:
  LookupTable(memory::MemoryPool& pool)
      : pool_(pool),
        map_(std::make_unique<outer_map_t>(outer_allocator_t(pool))) {}

  auto& map() {
    return map_;
  }

  bool containsMapAtIndex(vector_size_t rowIndex) const {
    return map_->count(rowIndex) != 0;
  }

  void ensureMapAtIndex(vector_size_t rowIndex) const {
    map_->emplace(rowIndex, pool_);
  }

  auto& getMapAtIndex(vector_size_t rowIndex) {
    VELOX_DCHECK(containsMapAtIndex(rowIndex));
    return map_->find(rowIndex)->second;
  }

 private:
  using inner_allocator_t =
      memory::StlAllocator<std::pair<key_t const, vector_size_t>>;

  using inner_map_t = folly::F14FastMap<
      key_t,
      vector_size_t,
      folly::f14::DefaultHasher<key_t>,
      folly::f14::DefaultKeyEqual<key_t>,
      inner_allocator_t>;

  using outer_allocator_t =
      memory::StlAllocator<std::pair<vector_size_t const, inner_map_t>>;

  // [rowindex][key] -> offset of value.
  using outer_map_t = folly::F14FastMap<
      vector_size_t,
      inner_map_t,
      folly::f14::DefaultHasher<vector_size_t>,
      folly::f14::DefaultKeyEqual<vector_size_t>,
      outer_allocator_t>;

  memory::MemoryPool& pool_;
  std::unique_ptr<outer_map_t> map_;
};

class MapSubscript {
 public:
  explicit MapSubscript(bool allowCaching) : allowCaching_(allowCaching) {}

  VectorPtr applyMap(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context) const;

  bool cachingEnabled() const {
    return allowCaching_;
  }

  auto& lookupTable() const {
    return lookupTable_;
  }

  auto& firstSeenMap() const {
    return firstSeenMap_;
  }

 private:
  bool shouldTriggerCaching(const VectorPtr& mapArg) const {
    if (!allowCaching_) {
      return false;
    }

    if (!mapArg->type()->childAt(0)->isPrimitiveType() &&
        !!mapArg->type()->childAt(0)->isBoolean()) {
      // Disable caching if the key type is not primitive or is boolean.
      allowCaching_ = false;
      return false;
    }

    if (!firstSeenMap_) {
      firstSeenMap_ = mapArg;
      return false;
    }

    if (firstSeenMap_->wrappedVector() == mapArg->wrappedVector()) {
      return true;
    }

    // Disable caching forever.
    allowCaching_ = false;
    lookupTable_.reset();
    firstSeenMap_.reset();
    return false;
  }

  // When true the function is allowed to cache a materialized version of the
  // processed map.
  mutable bool allowCaching_;

  // This is used to check if the same base map is being passed over and over
  // in the function. A shared_ptr is used to guarantee that if the map is
  // seen again then it was not modified.
  mutable VectorPtr firstSeenMap_;

  // Materialized cached version of firstSeenMap_ used to optimize the lookup.
  mutable std::shared_ptr<LookupTableBase> lookupTable_;
};

/// Generic subscript/element_at implementation for both array and map data
/// types.
///
/// Provides four template parameters to configure the behavior:
/// - allowNegativeIndices: if allowed, negative indices accesses elements
/// from
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
class SubscriptImpl : public exec::Subscript {
 public:
  explicit SubscriptImpl(bool allowCaching)
      : mapSubscript_(MapSubscript(allowCaching)) {}

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
        localResult = mapSubscript_.applyMap(rows, args, context);
        break;

      default:
        VELOX_UNREACHABLE();
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

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

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    // array(T), integer|bigint -> T
    for (const auto& indexType : {"integer", "bigint"}) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .typeVariable("T")
                               .returnType("T")
                               .argumentType("array(T)")
                               .argumentType(indexType)
                               .build());
    }

    // map(K,V), K -> V
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .typeVariable("K")
                             .typeVariable("V")
                             .returnType("V")
                             .argumentType("map(K,V)")
                             .argumentType("K")
                             .build());

    return signatures;
  }

  template <typename I>
  VectorPtr applyArrayTyped(
      const SelectivityVector& rows,
      const VectorPtr& arrayArg,
      const VectorPtr& indexArg,
      exec::EvalCtx& context) const {
    auto* pool = context.pool();

    BufferPtr indices = allocateIndices(rows.end(), pool);
    auto rawIndices = indices->asMutable<vector_size_t>();

    // Create nulls for lazy initialization.
    NullsBuilder nullsBuilder(rows.end(), pool);

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
      bool allFailed = false;
      // If index is invalid, capture the error and mark all rows as failed.
      bool isZeroSubscriptError = false;
      const auto adjustedIndex =
          adjustIndex(decodedIndices->valueAt<I>(0), isZeroSubscriptError);
      if (isZeroSubscriptError) {
        context.setErrors(rows, zeroSubscriptError());
        allFailed = true;
      }

      if (!allFailed) {
        rows.applyToSelected([&](auto row) {
          const auto elementIndex = getIndex(
              adjustedIndex, row, rawSizes, rawOffsets, arrayIndices, context);
          rawIndices[row] = elementIndex;
          if (elementIndex == -1) {
            nullsBuilder.setNull(row);
          }
        });
      }
    } else {
      rows.applyToSelected([&](auto row) {
        const auto originalIndex = decodedIndices->valueAt<I>(row);
        bool isZeroSubscriptError = false;
        const auto adjustedIndex =
            adjustIndex(originalIndex, isZeroSubscriptError);
        if (isZeroSubscriptError) {
          context.setVeloxExceptionError(row, zeroSubscriptError());
          return;
        }
        const auto elementIndex = getIndex(
            adjustedIndex, row, rawSizes, rawOffsets, arrayIndices, context);
        rawIndices[row] = elementIndex;
        if (elementIndex == -1) {
          nullsBuilder.setNull(row);
        }
      });
    }

    // Subscript into empty arrays always returns NULLs. Check added at the end
    // to ensure user error checks for indices are not skipped.
    if (baseArray->elements()->size() == 0) {
      return BaseVector::createNullConstant(
          baseArray->elements()->type(), rows.end(), context.pool());
    }

    return BaseVector::wrapInDictionary(
        nullsBuilder.build(), indices, rows.end(), baseArray->elements());
  }

  // Normalize indices from 1 or 0-based into always 0-based (according to
  // indexStartsAtOne template parameter - no-op if it's false).
  template <typename I>
  vector_size_t adjustIndex(I index, bool& isZeroSubscriptError) const {
    // If array indices start at 1.
    if constexpr (indexStartsAtOne) {
      if (UNLIKELY(index == 0)) {
        isZeroSubscriptError = true;
        return 0;
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
  // `index` is always a 0-based array index (see `adjustIndex` function
  // above).
  template <typename I>
  vector_size_t getIndex(
      I index,
      vector_size_t row,
      const vector_size_t* rawSizes,
      const vector_size_t* rawOffsets,
      const vector_size_t* indices,
      exec::EvalCtx& context) const {
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
        context.setVeloxExceptionError(row, negativeSubscriptError());
        return -1;
      }
    }

    // Check if index is within bound.
    if ((index >= arraySize) || (index < 0)) {
      // If we allow it, return null.
      if constexpr (allowOutOfBound) {
        return -1;
      } else {
        context.setVeloxExceptionError(row, badSubscriptError());
        return -1;
      }
    }

    // Resultant index is the sum of the offset in the input array and the
    // index.
    return rawOffsets[indices[row]] + index;
  }

 private:
  MapSubscript mapSubscript_;
};

} // namespace facebook::velox::functions
