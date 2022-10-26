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

namespace facebook::velox::functions {
namespace {
template <typename T>

struct SetWithNull {
  SetWithNull(vector_size_t initialSetSize = kInitialSetSize) {
    set.reserve(initialSetSize);
  }

  void reset() {
    set.clear();
    hasNull = false;
  }

  std::unordered_set<T> set;
  bool hasNull{false};
  static constexpr vector_size_t kInitialSetSize{128};
};
// Generates a set based on the elements of an ArrayVector. Note that we take
// rightSet as a parameter (instead of returning a new one) to reuse the
// allocated memory.
template <typename T, typename TVector>
void generateSet(
    const ArrayVector* arrayVector,
    const TVector* arrayElements,
    vector_size_t idx,
    SetWithNull<T>& rightSet) {
  auto size = arrayVector->sizeAt(idx);
  auto offset = arrayVector->offsetAt(idx);
  rightSet.reset();

  for (vector_size_t i = offset; i < (offset + size); ++i) {
    if (arrayElements->isNullAt(i)) {
      rightSet.hasNull = true;
    } else {
      // Function can be called with either FlatVector or DecodedVector, but
      // their APIs are slightly different.
      if constexpr (std::is_same_v<TVector, DecodedVector>) {
        rightSet.set.insert(arrayElements->template valueAt<T>(i));
      } else {
        rightSet.set.insert(arrayElements->valueAt(i));
      }
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
  /// If any of the values passed to array_intersect() or rhs for array_except()
  /// are constant (array literals) we create a set before instantiating the
  /// object and pass as a constructor parameter (constantSet).

  ArrayIntersectExceptFunction() = default;

  explicit ArrayIntersectExceptFunction(
      SetWithNull<T> constantSet,
      bool isLeftConstant)
      : constantSet_(std::move(constantSet)), isLeftConstant_(isLeftConstant) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    memory::MemoryPool* pool = context.pool();
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();

    // For array_intersect, if there's a constant input, then require it is on
    // the right side. For array_except, the constant optimization only applies
    // if the constant is on the rhs, so this swap is not applicable.
    if constexpr (isIntersect) {
      if (constantSet_.has_value() && isLeftConstant_) {
        std::swap(left, right);
      }
    }

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
      *rawNewOffsets = indicesCursor;

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
          auto val = decodedLeftElements->valueAt<T>(i);
          // For array_intersect, add the element if it is found (not found
          // for array_except) in the right-hand side, and wasn't added already
          // (check outputSet).
          bool addValue = false;
          if constexpr (isIntersect) {
            addValue = rightSet.set.count(val) > 0;
          } else {
            addValue = rightSet.set.count(val) == 0;
          }
          if (addValue) {
            auto it = outputSet.set.insert(val);
            if (it.second) {
              rawNewIndices[indicesCursor++] = i;
            }
          }
        }
      }
      *rawNewLengths = indicesCursor - *rawNewOffsets;
      ++rawNewLengths;
      ++rawNewOffsets;
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
        ARRAY(CppToType<T>::create()),
        BufferPtr(nullptr),
        rowCount,
        newOffsets,
        newLengths,
        newElements,
        0);
    context.moveOrCopyResult(resultArray, rows, result);
  }

  // If one of the arrays is constant, this member will store a pointer to the
  // set generated from its elements, which is calculated only once, before
  // instantiating this object.
  std::optional<SetWithNull<T>> constantSet_;

  // If there's a `constantSet`, whether it refers to left or right-hand side.
  const bool isLeftConstant_{false};
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
      for (auto i = offset; i < (offset + size); ++i) {
        // For each element in the current row search for it in the rightSet.
        if (decodedLeftElements->isNullAt(i)) {
          // Arrays overlap function skips null values.
          hasNull = true;
          continue;
        }
        if (rightSet.set.count(decodedLeftElements->valueAt<T>(i)) > 0) {
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

template <bool isIntersect, TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTypedArraysIntersectExcept(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  BaseVector* left = inputArgs[0].constantValue.get();
  BaseVector* right = inputArgs[1].constantValue.get();
  using T = typename TypeTraits<kind>::NativeType;
  // No constant values.
  if ((left == nullptr) && (right == nullptr)) {
    return std::make_shared<ArrayIntersectExceptFunction<isIntersect, T>>();
  }

  // Constant optimization is not supported for constant lhs for array_except
  const bool isLeftConstant = (left != nullptr);
  if (isLeftConstant) {
    if constexpr (!isIntersect) {
      return std::make_shared<ArrayIntersectExceptFunction<isIntersect, T>>();
    }
  }
  BaseVector* constantVector = isLeftConstant ? left : right;
  SetWithNull<T> constantSet =
      validateConstantVectorAndGenerateSet<T>(constantVector);
  return std::make_shared<ArrayIntersectExceptFunction<isIntersect, T>>(
      std::move(constantSet), isLeftConstant);
}

std::shared_ptr<exec::VectorFunction> createArrayIntersect(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      createTypedArraysIntersectExcept,
      /* isIntersect */ true,
      elementType->kind(),
      inputArgs);
}

std::shared_ptr<exec::VectorFunction> createArrayExcept(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      createTypedArraysIntersectExcept,
      /* isIntersect */ false,
      elementType->kind(),
      inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures(
    const std::string& returnType) {
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType(returnType)
              .argumentType("array(T)")
              .argumentType("array(T)")
              .build()};
}

template <TypeKind kind>
const std::shared_ptr<exec::VectorFunction> createTypedArraysOverlap(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  auto left = inputArgs[0].constantValue.get();
  auto right = inputArgs[1].constantValue.get();
  using T = typename TypeTraits<kind>::NativeType;
  if (left == nullptr && right == nullptr) {
    return std::make_shared<ArraysOverlapFunction<T>>();
  }
  auto isLeftConstant = (left != nullptr);
  auto baseVector = isLeftConstant ? left : right;
  auto constantSet = validateConstantVectorAndGenerateSet<T>(baseVector);
  return std::make_shared<ArraysOverlapFunction<T>>(
      std::move(constantSet), isLeftConstant);
}

std::shared_ptr<exec::VectorFunction> createArraysOverlapFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateMatchingArrayTypes(inputArgs, name, 2);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
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
