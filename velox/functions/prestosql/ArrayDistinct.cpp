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
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions {
namespace {

template <typename T>
struct ValueSet {
  util::floating_point::HashSetNaNAware<T> values;

  bool insert(const T& value) {
    return values.insert(value).second;
  }

  void reset() {
    values.clear();
  }
};

template <>
struct ValueSet<ComplexType> {
  struct Key {
    const uint64_t hash;
    const BaseVector* vector;
    const vector_size_t index;
  };

  struct Hash {
    size_t operator()(const Key& key) const {
      return key.hash;
    }
  };

  struct EqualTo {
    bool operator()(const Key& left, const Key& right) const {
      return left.vector
          ->equalValueAt(
              right.vector,
              left.index,
              right.index,
              CompareFlags::NullHandlingMode::kNullAsValue)
          .value();
    }
  };

  folly::F14FastSet<Key, Hash, EqualTo> values;

  bool insert(const BaseVector* vector, vector_size_t index) {
    const uint64_t hash = vector->hashValueAt(index);
    return values.insert({hash, vector, index}).second;
  }

  void reset() {
    values.clear();
  }
};

/// See documentation at https://prestodb.io/docs/current/functions/array.html
///
/// array_distinct SQL function.
///
/// Along with the set, we maintain a `hasNull` flag that indicates whether
/// null is present in the array.
///
/// Zero element copy:
///
/// In order to prevent copies of array elements, the function reuses the
/// internal elements() vector from the original ArrayVector.
///
/// First a new vector is created containing the indices of the elements
/// which will be present in the output, and wrapped into a DictionaryVector.
/// Next the `lengths` and `offsets` vectors that control where output arrays
/// start and end are wrapped into the output ArrayVector.template <typename T>
template <typename T, bool useCustomComparison = false>
class ArrayDistinctFunction : public exec::VectorFunction {
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
      auto* constantArray = arg->as<ConstantVector<ComplexType>>();
      const auto& flatArray = constantArray->valueVector();
      const auto flatIndex = constantArray->index();

      exec::LocalSingleRow singleRow(context, flatIndex);
      localResult = applyFlat(*singleRow, flatArray, context);
      localResult =
          BaseVector::wrapInConstant(rows.end(), flatIndex, localResult);
    } else {
      localResult = applyFlat(rows, arg, context);
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  // We want to use ValueSet<ComplexType> when we need custom comparison because
  // it uses the Vector's implementation of compare and hash which it gets from
  // the type.
  using ValueSetT = std::
      conditional_t<useCustomComparison, ValueSet<ComplexType>, ValueSet<T>>;

  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const {
    auto arrayVector = arg->as<ArrayVector>();
    auto elementsVector = arrayVector->elements();
    auto elementsRows =
        toElementRows(elementsVector->size(), rows, arrayVector);
    exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

    vector_size_t elementsCount = elementsRows.end();
    vector_size_t rowCount = rows.end();

    // Allocate new vectors for indices, length and offsets.
    memory::MemoryPool* pool = context.pool();
    BufferPtr newIndices = allocateIndices(elementsCount, pool);
    BufferPtr newLengths = allocateSizes(rowCount, pool);
    BufferPtr newOffsets = allocateOffsets(rowCount, pool);

    // Pointers and cursors to the raw data.
    vector_size_t indicesCursor = 0;
    auto* rawNewIndices = newIndices->asMutable<vector_size_t>();
    auto* rawNewSizes = newLengths->asMutable<vector_size_t>();
    auto* rawNewOffsets = newOffsets->asMutable<vector_size_t>();

    // Process the rows: store unique values in the hash table.
    ValueSetT uniqueSet;

    rows.applyToSelected([&](vector_size_t row) {
      auto size = arrayVector->sizeAt(row);
      auto offset = arrayVector->offsetAt(row);

      rawNewOffsets[row] = indicesCursor;
      bool hasNulls = false;
      for (vector_size_t i = offset; i < offset + size; ++i) {
        if (elements->isNullAt(i)) {
          if (!hasNulls) {
            hasNulls = true;
            rawNewIndices[indicesCursor++] = i;
          }
        } else {
          bool unique;
          if constexpr (std::is_same_v<ValueSetT, ValueSet<ComplexType>>) {
            unique = uniqueSet.insert(elements->base(), elements->index(i));
          } else {
            auto value = elements->valueAt<T>(i);
            unique = uniqueSet.insert(value);
          }

          if (unique) {
            rawNewIndices[indicesCursor++] = i;
          }
        }
      }

      uniqueSet.reset();
      rawNewSizes[row] = indicesCursor - rawNewOffsets[row];
    });

    newIndices->setSize(indicesCursor * sizeof(vector_size_t));
    auto newElements =
        BaseVector::transpose(newIndices, std::move(elementsVector));

    return std::make_shared<ArrayVector>(
        pool,
        arrayVector->type(),
        nullptr,
        rowCount,
        std::move(newOffsets),
        std::move(newLengths),
        std::move(newElements),
        0);
  }
};

template <>
VectorPtr ArrayDistinctFunction<UnknownType, false>::applyFlat(
    const SelectivityVector& rows,
    const VectorPtr& arg,
    exec::EvalCtx& context) const {
  auto arrayVector = arg->as<ArrayVector>();
  auto elementsVector = arrayVector->elements();
  vector_size_t rowCount = rows.end();

  // Allocate new vectors for indices, length and offsets.
  memory::MemoryPool* pool = context.pool();
  BufferPtr newIndices = allocateIndices(rowCount, pool);
  BufferPtr newLengths = allocateSizes(rowCount, pool);
  BufferPtr newOffsets = allocateOffsets(rowCount, pool);

  // Pointers and cursors to the raw data.
  vector_size_t indicesCursor = 0;
  auto* rawNewIndices = newIndices->asMutable<vector_size_t>();
  auto* rawNewSizes = newLengths->asMutable<vector_size_t>();
  auto* rawNewOffsets = newOffsets->asMutable<vector_size_t>();

  rows.applyToSelected([&](vector_size_t row) {
    auto size = arrayVector->sizeAt(row);
    auto offset = arrayVector->offsetAt(row);

    rawNewOffsets[row] = indicesCursor;
    if (size > 0) {
      if (FOLLY_UNLIKELY(indicesCursor == 0)) {
        rawNewIndices[0] = offset;
      }
      rawNewSizes[row] = 1;
      rawNewIndices[indicesCursor++] = rawNewIndices[0];
    } else {
      rawNewSizes[row] = 0;
    }
  });

  newIndices->setSize(indicesCursor * sizeof(vector_size_t));
  auto newElements =
      BaseVector::transpose(newIndices, std::move(elementsVector));

  return std::make_shared<ArrayVector>(
      pool,
      arrayVector->type(),
      nullptr,
      rowCount,
      std::move(newOffsets),
      std::move(newLengths),
      std::move(newElements),
      0);
}

// Validate number of parameters and types.
void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(), 1, "array_distinct requires exactly one parameter");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_distinct requires arguments of type ARRAY");
}

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const TypePtr& elementType) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);

  using T = typename TypeTraits<kind>::NativeType;

  if (elementType->providesCustomComparison()) {
    return std::make_shared<ArrayDistinctFunction<T, true>>();
  } else {
    return std::make_shared<ArrayDistinctFunction<T, false>>();
  }
}

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  validateType(inputArgs);
  auto elementType = inputArgs.front().type->childAt(0);
  if (elementType->isUnKnown()) {
    return std::make_shared<ArrayDistinctFunction<UnknownType>>();
  }

  if (elementType->isArray() || elementType->isMap() || elementType->isRow()) {
    return std::make_shared<ArrayDistinctFunction<ComplexType>>();
  }

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createTyped, elementType->kind(), inputArgs, elementType);
}

// Define function signature.
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T) -> array(T)
  return std::vector<std::shared_ptr<exec::FunctionSignature>>{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .argumentType("array(T)")
          .build(),
  };
}

} // namespace

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_distinct,
    signatures(),
    create);

} // namespace facebook::velox::functions
