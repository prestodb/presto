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
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions {
namespace {
constexpr vector_size_t kInitialSetSize{128};

template <typename T>
struct SetWithNull {
  SetWithNull(vector_size_t initialSetSize = kInitialSetSize) {
    set.reserve(initialSetSize);
  }

  bool insert(const DecodedVector* decodedElements, vector_size_t offset) {
    return set.insert(decodedElements->valueAt<T>(offset)).second;
  }

  size_t count(const DecodedVector* decodedElements, vector_size_t offset)
      const {
    return set.count(decodedElements->valueAt<T>(offset));
  }

  void reset() {
    set.clear();
    hasNull = false;
  }

  bool empty() const {
    return !hasNull && set.empty();
  }

  util::floating_point::HashSetNaNAware<T> set;
  bool hasNull{false};
};

// This class is used as the entry in a set when the native type cannot be used
// directly. In particular, for complex types and custom types that provide
// custom comparison operators.
struct WrappedVectorEntry {
  const uint64_t hash;
  const BaseVector* baseVector;
  const vector_size_t index;
};

template <>
struct SetWithNull<WrappedVectorEntry> {
  struct Hash {
    size_t operator()(const WrappedVectorEntry& entry) const {
      return entry.hash;
    }
  };

  struct EqualTo {
    bool operator()(
        const WrappedVectorEntry& left,
        const WrappedVectorEntry& right) const {
      return left.baseVector
          ->equalValueAt(
              right.baseVector,
              left.index,
              right.index,
              CompareFlags::NullHandlingMode::kNullAsValue)
          .value();
    }
  };

  folly::F14FastSet<WrappedVectorEntry, Hash, EqualTo> set;
  bool hasNull{false};

  SetWithNull(vector_size_t initialSetSize = kInitialSetSize) {
    set.reserve(initialSetSize);
  }

  bool insert(const DecodedVector* decodedElements, vector_size_t offset) {
    const auto vector = decodedElements->base();
    const auto index = decodedElements->index(offset);
    const uint64_t hash = vector->hashValueAt(index);
    return set.insert(WrappedVectorEntry{hash, vector, index}).second;
  }

  size_t count(const DecodedVector* decodedElements, vector_size_t offset)
      const {
    const auto vector = decodedElements->base();
    const auto index = decodedElements->index(offset);
    const uint64_t hash = vector->hashValueAt(index);
    return set.count(WrappedVectorEntry{hash, vector, index});
  }

  void reset() {
    set.clear();
    hasNull = false;
  }

  bool empty() const {
    return !hasNull && set.empty();
  }
};

// Generates a set based on the elements of an ArrayVector. Note that we take
// rightSet as a parameter (instead of returning a new one) to reuse the
// allocated memory.
template <typename T>
void generateSet(
    const ArrayVector* arrayVector,
    const DecodedVector* arrayElements,
    vector_size_t idx,
    SetWithNull<T>& rightSet) {
  auto size = arrayVector->sizeAt(idx);
  auto offset = arrayVector->offsetAt(idx);
  rightSet.reset();

  for (vector_size_t i = offset; i < (offset + size); ++i) {
    if (arrayElements->isNullAt(i)) {
      rightSet.hasNull = true;
    } else {
      rightSet.insert(arrayElements, i);
    }
  }
}

DecodedVector* decodeArrayElements(
    exec::LocalDecodedVector& arrayDecoder,
    exec::LocalDecodedVector& elementsDecoder,
    const SelectivityVector& rows) {
  auto decodedVector = arrayDecoder.get();
  auto baseArrayVector = arrayDecoder->base()->as<ArrayVector>();

  // Decode and acquire array elements vector.
  auto elementsVector = baseArrayVector->elements();
  auto elementsSelectivityRows = toElementRows(
      elementsVector->size(), rows, baseArrayVector, decodedVector->indices());
  elementsDecoder.get()->decode(*elementsVector, elementsSelectivityRows);
  auto decodedElementsVector = elementsDecoder.get();
  return decodedElementsVector;
}

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <bool isIntersect, typename T>
class ArrayIntersectExceptFunction : public exec::VectorFunction {
 public:
  /// This class is used for both array_intersect and array_except functions
  /// (behavior controlled at compile time by the isIntersect template
  /// variable). Both these functions take two ArrayVectors as inputs (left and
  /// right) and leverage two sets to calculate the intersection (or except):
  ///
  /// - rightSet: a set that contains all (distinct) elements from the
  ///   right-hand side array.
  /// - outputSet: a set that contains the elements that were already added to
  ///   the output (to prevent duplicates).
  ///
  /// Along with each set, we maintain a `hasNull` flag that indicates whether
  /// null is present in the arrays, to prevent the use of optional types or
  /// special values.
  ///
  /// Zero element copy:
  ///
  /// In order to prevent copies of array elements, the function reuses the
  /// internal elements() vector from the left-hand side ArrayVector.
  ///
  /// First a new vector is created containing the indices of the elements
  /// which will be present in the output, and wrapped into a DictionaryVector.
  /// Next the `lengths` and `offsets` vectors that control where output arrays
  /// start and end are wrapped into the output ArrayVector.
  ///
  /// Constant optimization:
  ///
  /// If the rhs values passed to either array_intersect() or array_except()
  /// are constant (array literals) we create a set before instantiating the
  /// object and pass as a constructor parameter (constantSet).

  ArrayIntersectExceptFunction() = default;

  explicit ArrayIntersectExceptFunction(SetWithNull<T> constantSet)
      : constantSet_(std::move(constantSet)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    memory::MemoryPool* pool = context.pool();
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();

    exec::LocalDecodedVector leftHolder(context, *left, rows);
    auto decodedLeftArray = leftHolder.get();
    auto baseLeftArray = decodedLeftArray->base()->as<ArrayVector>();

    // Decode and acquire array elements vector.
    exec::LocalDecodedVector leftElementsDecoder(context);
    auto decodedLeftElements =
        decodeArrayElements(leftHolder, leftElementsDecoder, rows);

    auto leftElementsCount =
        countElements<ArrayVector>(rows, *decodedLeftArray);
    vector_size_t rowCount = left->size();

    // Allocate new vectors for indices, nulls, length and offsets.
    BufferPtr newIndices = allocateIndices(leftElementsCount, pool);
    BufferPtr newElementNulls =
        AlignedBuffer::allocate<bool>(leftElementsCount, pool, bits::kNotNull);
    BufferPtr newLengths = allocateSizes(rowCount, pool);
    BufferPtr newOffsets = allocateOffsets(rowCount, pool);

    // Pointers and cursors to the raw data.
    auto rawNewIndices = newIndices->asMutable<vector_size_t>();
    auto rawNewElementNulls = newElementNulls->asMutable<uint64_t>();
    auto rawNewLengths = newLengths->asMutable<vector_size_t>();
    auto rawNewOffsets = newOffsets->asMutable<vector_size_t>();

    vector_size_t indicesCursor = 0;

    // Lambda that process each row. This is detached from the code so we can
    // apply it differently based on whether the right-hand side set is constant
    // or not.
    auto processRow = [&](vector_size_t row,
                          const SetWithNull<T>& rightSet,
                          SetWithNull<T>& outputSet) {
      auto idx = decodedLeftArray->index(row);
      auto size = baseLeftArray->sizeAt(idx);
      auto offset = baseLeftArray->offsetAt(idx);

      outputSet.reset();
      rawNewOffsets[row] = indicesCursor;
      // Scans the array elements on the left-hand side.
      for (vector_size_t i = offset; i < (offset + size); ++i) {
        if (decodedLeftElements->isNullAt(i)) {
          // For a NULL value not added to the output row yet, insert in
          // array_intersect if it was found on the rhs (and not found in the
          // case of array_except).
          if (!outputSet.hasNull) {
            bool setNull = false;
            if constexpr (isIntersect) {
              setNull = rightSet.hasNull;
            } else {
              setNull = !rightSet.hasNull;
            }
            if (setNull) {
              bits::setNull(rawNewElementNulls, indicesCursor++, true);
              outputSet.hasNull = true;
            }
          }
        } else {
          // For array_intersect, add the element if it is found (not found
          // for array_except) in the right-hand side, and wasn't added already
          // (check outputSet).
          bool addValue = false;
          if constexpr (isIntersect) {
            addValue = rightSet.count(decodedLeftElements, i) > 0;
          } else {
            addValue = rightSet.count(decodedLeftElements, i) == 0;
          }
          if (addValue) {
            if (outputSet.insert(decodedLeftElements, i)) {
              rawNewIndices[indicesCursor++] = i;
            }
          }
        }
      }
      rawNewLengths[row] = indicesCursor - rawNewOffsets[row];
    };

    SetWithNull<T> outputSet;

    // Optimized case when the right-hand side array is constant.
    if (constantSet_.has_value()) {
      rows.applyToSelected([&](vector_size_t row) {
        processRow(row, *constantSet_, outputSet);
      });
    }
    // General case when no arrays are constant and both sets need to be
    // computed for each row.
    else {
      exec::LocalDecodedVector rightHolder(context, *right, rows);
      // Decode and acquire array elements vector.
      exec::LocalDecodedVector rightElementsHolder(context);
      auto decodedRightElements =
          decodeArrayElements(rightHolder, rightElementsHolder, rows);
      SetWithNull<T> rightSet;
      auto rightArrayVector = rightHolder.get()->base()->as<ArrayVector>();
      rows.applyToSelected([&](vector_size_t row) {
        auto idx = rightHolder.get()->index(row);
        generateSet<T>(rightArrayVector, decodedRightElements, idx, rightSet);
        processRow(row, rightSet, outputSet);
      });
    }

    auto newElements = BaseVector::wrapInDictionary(
        newElementNulls, newIndices, indicesCursor, baseLeftArray->elements());
    auto resultArray = std::make_shared<ArrayVector>(
        pool,
        outputType,
        nullptr,
        rowCount,
        newOffsets,
        newLengths,
        newElements);
    context.moveOrCopyResult(resultArray, rows, result);
  }

  // If one of the arrays is constant, this member will store a pointer to the
  // set generated from its elements, which is calculated only once, before
  // instantiating this object.
  std::optional<SetWithNull<T>> constantSet_;
}; // class ArrayIntersectExcept

template <typename T>
class ArraysOverlapFunction : public exec::VectorFunction {
 public:
  ArraysOverlapFunction() = default;

  ArraysOverlapFunction(SetWithNull<T> constantSet, bool isLeftConstant)
      : constantSet_(std::move(constantSet)), isLeftConstant_(isLeftConstant) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();
    if (constantSet_.has_value() && isLeftConstant_) {
      std::swap(left, right);
    }
    exec::LocalDecodedVector arrayDecoder(context, *left, rows);
    exec::LocalDecodedVector elementsDecoder(context);
    auto decodedLeftElements =
        decodeArrayElements(arrayDecoder, elementsDecoder, rows);
    auto decodedLeftArray = arrayDecoder.get();
    auto baseLeftArray = decodedLeftArray->base()->as<ArrayVector>();
    context.ensureWritable(rows, BOOLEAN(), result);
    auto resultBoolVector = result->template asFlatVector<bool>();
    auto processRow = [&](auto row, const SetWithNull<T>& rightSet) {
      auto idx = decodedLeftArray->index(row);
      auto offset = baseLeftArray->offsetAt(idx);
      auto size = baseLeftArray->sizeAt(idx);
      bool hasNull = rightSet.hasNull;
      if (size == 0 || rightSet.empty()) {
        resultBoolVector->set(row, false);
        return;
      }
      for (auto i = offset; i < (offset + size); ++i) {
        // For each element in the current row search for it in the rightSet.
        if (decodedLeftElements->isNullAt(i)) {
          // Arrays overlap function skips null values.
          hasNull = true;
          continue;
        }
        if (rightSet.count(decodedLeftElements, i) > 0) {
          // Found an overlapping element. Add to result set.
          resultBoolVector->set(row, true);
          return;
        }
      }
      if (hasNull) {
        // If encountered a NULL, insert NULL in the result.
        resultBoolVector->setNull(row, true);
      } else {
        // If there is no overlap and no nulls, then insert false.
        resultBoolVector->set(row, false);
      }
    };

    if (constantSet_.has_value()) {
      rows.applyToSelected(
          [&](vector_size_t row) { processRow(row, *constantSet_); });
    }
    // General case when no arrays are constant and both sets need to be
    // computed for each row.
    else {
      exec::LocalDecodedVector rightDecoder(context, *right, rows);
      exec::LocalDecodedVector rightElementsDecoder(context);
      auto decodedRightElements =
          decodeArrayElements(rightDecoder, rightElementsDecoder, rows);
      SetWithNull<T> rightSet;
      auto baseRightArray = rightDecoder.get()->base()->as<ArrayVector>();
      rows.applyToSelected([&](vector_size_t row) {
        auto idx = rightDecoder.get()->index(row);
        generateSet<T>(baseRightArray, decodedRightElements, idx, rightSet);
        processRow(row, rightSet);
      });
    }
  }

 private:
  // If one of the arrays is constant, this member will store a pointer to the
  // set generated from its elements, which is calculated only once, before
  // instantiating this object.
  std::optional<SetWithNull<T>> constantSet_;

  // If there's a `constantSet`, whether it refers to left or right-hand side.
  const bool isLeftConstant_{false};
}; // class ArraysOverlapFunction

void validateMatchingArrayTypes(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const std::string& name,
    vector_size_t expectedArgCount) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(),
      expectedArgCount,
      "{} requires exactly {} parameters",
      name,
      expectedArgCount);

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "{} requires arguments of type ARRAY",
      name);

  for (auto& arg : inputArgs) {
    VELOX_USER_CHECK(
        arrayType->kindEquals(arg.type),
        "{} function requires all arguments of the same type: {} vs. {}",
        name,
        arg.type->toString(),
        arrayType->toString());
  }
}

template <typename T>
SetWithNull<T> validateConstantVectorAndGenerateSet(
    const BaseVector* baseVector) {
  auto constantVector = baseVector->as<ConstantVector<velox::ComplexType>>();
  auto constantArray = constantVector->as<ConstantVector<velox::ComplexType>>();
  VELOX_CHECK_NOT_NULL(constantArray, "wrong constant type found");
  VELOX_CHECK_NOT_NULL(constantVector, "wrong constant type found");
  auto arrayVecPtr = constantVector->valueVector()->as<ArrayVector>();
  VELOX_CHECK_NOT_NULL(arrayVecPtr, "wrong array literal type");

  auto idx = constantArray->index();
  auto elementBegin = arrayVecPtr->offsetAt(idx);
  auto elementEnd = elementBegin + arrayVecPtr->sizeAt(idx);

  SelectivityVector rows{elementEnd, false};
  rows.setValidRange(elementBegin, elementEnd, true);
  rows.updateBounds();

  DecodedVector decodedElements{*arrayVecPtr->elements(), rows};

  SetWithNull<T> constantSet;
  generateSet<T>(arrayVecPtr, &decodedElements, idx, constantSet);
  return constantSet;
}

template <bool isIntersect, typename SetEntryT>
std::shared_ptr<exec::VectorFunction> createTypedArraysIntersectExcept(
    const BaseVector* rhs) {
  // We don't optimize the case where lhs is a constant expression for
  // array_intersect() because that would make this function non-deterministic.
  // For example, a constant lhs would mean the constantSet is created based on
  // lhs; the same data encoded as a regular column could result in the set
  // being created based on rhs. Running this function with different sets could
  // results in arrays in different orders.
  //
  // If rhs is a constant value:
  if (rhs != nullptr) {
    return std::make_shared<
        ArrayIntersectExceptFunction<isIntersect, SetEntryT>>(
        validateConstantVectorAndGenerateSet<SetEntryT>(rhs));
  } else {
    return std::make_shared<
        ArrayIntersectExceptFunction<isIntersect, SetEntryT>>();
  }
}

template <bool isIntersect, TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTypedArraysIntersectExcept(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const TypePtr& elementType) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  const BaseVector* rhs = inputArgs[1].constantValue.get();

  if (elementType->providesCustomComparison()) {
    return createTypedArraysIntersectExcept<isIntersect, WrappedVectorEntry>(
        rhs);
  } else {
    using T = std::conditional_t<
        TypeTraits<kind>::isPrimitiveType,
        typename TypeTraits<kind>::NativeType,
        WrappedVectorEntry>;
    return createTypedArraysIntersectExcept<isIntersect, T>(rhs);
  }
}

std::shared_ptr<exec::VectorFunction> createArrayIntersect(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
      createTypedArraysIntersectExcept,
      /* isIntersect */ true,
      elementType->kind(),
      inputArgs,
      elementType);
}

std::shared_ptr<exec::VectorFunction> createArrayExcept(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(
      createTypedArraysIntersectExcept,
      /* isIntersect */ false,
      elementType->kind(),
      inputArgs,
      elementType);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures(
    const std::string& returnType) {
  return std::vector<std::shared_ptr<exec::FunctionSignature>>{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType(returnType)
          .argumentType("array(T)")
          .argumentType("array(T)")
          .build(),
  };
}

template <TypeKind kind>
const std::shared_ptr<exec::VectorFunction> createTypedArraysOverlap(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  auto left = inputArgs[0].constantValue.get();
  auto right = inputArgs[1].constantValue.get();
  bool usesCustomComparison =
      inputArgs[0].type->childAt(0)->providesCustomComparison();
  using T = std::conditional_t<
      TypeTraits<kind>::isPrimitiveType,
      typename TypeTraits<kind>::NativeType,
      WrappedVectorEntry>;

  if (left == nullptr && right == nullptr) {
    if (usesCustomComparison) {
      return std::make_shared<ArraysOverlapFunction<WrappedVectorEntry>>();
    } else {
      return std::make_shared<ArraysOverlapFunction<T>>();
    }
  }
  auto isLeftConstant = (left != nullptr);
  auto baseVector = isLeftConstant ? left : right;

  if (usesCustomComparison) {
    auto constantSet =
        validateConstantVectorAndGenerateSet<WrappedVectorEntry>(baseVector);
    return std::make_shared<ArraysOverlapFunction<WrappedVectorEntry>>(
        std::move(constantSet), isLeftConstant);
  } else {
    auto constantSet = validateConstantVectorAndGenerateSet<T>(baseVector);
    return std::make_shared<ArraysOverlapFunction<T>>(
        std::move(constantSet), isLeftConstant);
  }
}

std::shared_ptr<exec::VectorFunction> createArraysOverlapFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_TYPE_DISPATCH(
      createTypedArraysOverlap, elementType->kind(), inputArgs);
}
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_arrays_overlap,
    signatures("boolean"),
    createArraysOverlapFunction);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_intersect,
    signatures("array(T)"),
    createArrayIntersect);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_except,
    signatures("array(T)"),
    createArrayExcept);
} // namespace facebook::velox::functions
