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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"

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

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <typename T>
class ArrayIntersectFunction : public exec::VectorFunction {
 public:
  /// Array intersection takes two ArrayVectors as inputs (left and right) and
  /// leverages two sets to calculate the intersection:
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
  /// If any of the values passed to array_intersect() are constant (array
  /// literals) we create a set before instantiating the object and pass as a
  /// constructor parameter (constantSet).

  ArrayIntersectFunction() {}

  explicit ArrayIntersectFunction(
      SetWithNull<T> constantSet,
      bool isLeftConstant)
      : constantSet_(std::move(constantSet)), isLeftConstant_(isLeftConstant) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* caller */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    memory::MemoryPool* pool = context->pool();
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();

    // Ensure that if there's a constant input, it's on the right side.
    if (constantSet_.has_value() && isLeftConstant_) {
      std::swap(left, right);
    }

    // Decode and acquire pointers to the left-hand side vector.
    exec::LocalDecodedVector leftHolder(context, *left, rows);
    auto decodedLeftArray = leftHolder.get();
    auto baseLeftArray = decodedLeftArray->base()->as<ArrayVector>();

    // Decode and acquire array elements vector.
    auto leftElementsVector = baseLeftArray->elements();
    auto leftElementsRows =
        toElementRows(leftElementsVector->size(), rows, baseLeftArray);
    exec::LocalDecodedVector leftElementsHolder(
        context, *leftElementsVector, leftElementsRows);
    auto decodedLeftElements = leftElementsHolder.get();

    vector_size_t arrayElementsCount = decodedLeftElements->size();
    vector_size_t rowCount = left->size();

    // Allocate new vectors for indices, nulls, length and offsets.
    BufferPtr newIndices =
        AlignedBuffer::allocate<vector_size_t>(arrayElementsCount, pool);
    BufferPtr newElementNulls =
        AlignedBuffer::allocate<bool>(arrayElementsCount, pool, bits::kNotNull);
    BufferPtr newLengths =
        AlignedBuffer::allocate<vector_size_t>(rowCount, pool);
    BufferPtr newOffsets =
        AlignedBuffer::allocate<vector_size_t>(rowCount, pool);

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
        // If found a NULL value, insert only if it was contained in the
        // right-hande side wasn't added to this output row yet.
        if (decodedLeftElements->isNullAt(i)) {
          if (rightSet.hasNull && !outputSet.hasNull) {
            bits::setNull(rawNewElementNulls, indicesCursor++, true);
            outputSet.hasNull = true;
          }
        } else {
          // If element exists in the right-hand side, only add if it wasn't
          // added already (check outputSet).
          auto val = decodedLeftElements->valueAt<T>(i);
          if (rightSet.set.find(val) != rightSet.set.end()) {
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
      auto decodedRightArray = rightHolder.get();
      auto baseRightArray = decodedRightArray->base()->as<ArrayVector>();

      // Decode and acquire array elements vector.
      auto rightElementsVector = baseRightArray->elements();
      auto rightElementsRows =
          toElementRows(rightElementsVector->size(), rows, baseRightArray);
      exec::LocalDecodedVector rightElementsHolder(
          context, *rightElementsVector, rightElementsRows);
      auto decodedRightElements = rightElementsHolder.get();
      SetWithNull<T> rightSet;

      rows.applyToSelected([&](vector_size_t row) {
        auto idx = decodedRightArray->index(row);
        generateSet<T>(baseRightArray, decodedRightElements, idx, rightSet);
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
    context->moveOrCopyResult(resultArray, rows, result);
  }

  // If one of the arrays is constant, this member will store a pointer to the
  // set generated from its elements, which is calculated only once, before
  // instantiating this object.
  std::optional<SetWithNull<T>> constantSet_;

  // If there's a `constantSet`, whether it refers to left or right-hand side.
  const bool isLeftConstant_{false};
};

void validateType(const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_USER_CHECK_EQ(
      inputArgs.size(), 2, "array_intersect requires exactly two parameters");

  auto arrayType = inputArgs.front().type;
  VELOX_USER_CHECK_EQ(
      arrayType->kind(),
      TypeKind::ARRAY,
      "array_intersect requires arguments of type ARRAY");

  for (auto& arg : inputArgs) {
    VELOX_USER_CHECK(
        arrayType->kindEquals(arg.type),
        "array_intersect function requires all arguments of the same type: {} vs. {}",
        arg.type->toString(),
        arrayType->toString());
  }
}

template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  BaseVector* left = inputArgs[0].constantValue.get();
  BaseVector* right = inputArgs[1].constantValue.get();
  using T = typename TypeTraits<kind>::NativeType;
  // No constant values.
  if ((left == nullptr) && (right == nullptr)) {
    return std::make_shared<ArrayIntersectFunction<T>>();
  }

  // From now on either left or right is constant; generate a set based on its
  // elements.
  const bool isLeftConstant = (left != nullptr);
  BaseVector* constantVector = isLeftConstant ? left : right;

  auto constantArray = constantVector->as<ConstantVector<velox::ComplexType>>();
  VELOX_CHECK_NOT_NULL(constantArray, "wrong constant type found");

  auto constantArrayVector = constantArray->valueVector()->as<ArrayVector>();
  VELOX_CHECK_NOT_NULL(constantArrayVector, "wrong array literal type");

  auto flatVectorElements =
      constantArrayVector->elements()->as<FlatVector<T>>();
  VELOX_CHECK_NOT_NULL(
      flatVectorElements, "constant value must be encoded as flat");

  auto idx = constantArray->index();
  SetWithNull<T> constantSet;
  generateSet<T>(constantArrayVector, flatVectorElements, idx, constantSet);
  return std::make_shared<ArrayIntersectFunction<T>>(
      std::move(constantSet), isLeftConstant);
}

std::shared_ptr<exec::VectorFunction> create(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  validateType(inputArgs);
  auto elementType = inputArgs.front().type->childAt(0);

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createTyped, elementType->kind(), inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T), array(T) -> array(T)
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("array(T)")
              .argumentType("array(T)")
              .argumentType("array(T)")
              .build()};
}

} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_intersect,
    signatures(),
    create);

} // namespace facebook::velox::functions
