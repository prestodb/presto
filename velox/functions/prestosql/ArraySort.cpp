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

#include <folly/container/F14Set.h>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/functions/prestosql/SimpleComparisonMatcher.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions {
namespace {

BufferPtr sortElements(
    const SelectivityVector& rows,
    const ArrayVector& inputArray,
    const BaseVector& inputElements,
    bool ascending,
    exec::EvalCtx& context,
    bool throwOnNestedNull) {
  const SelectivityVector inputElementRows =
      toElementRows(inputElements.size(), rows, &inputArray);
  exec::LocalDecodedVector decodedElements(
      context, inputElements, inputElementRows);
  const auto* baseElementsVector = decodedElements->base();

  // Allocate new vectors for indices.
  BufferPtr indices = allocateIndices(inputElements.size(), context.pool());
  vector_size_t* rawIndices = indices->asMutable<vector_size_t>();

  CompareFlags flags{.nullsFirst = false, .ascending = ascending};
  if (throwOnNestedNull) {
    flags.nullHandlingMode =
        CompareFlags::NullHandlingMode::kNullAsIndeterminate;
  }

  auto decodedIndices = decodedElements->indices();
  context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
    const auto size = inputArray.sizeAt(row);
    const auto offset = inputArray.offsetAt(row);

    for (auto i = offset; i < offset + size; ++i) {
      rawIndices[i] = i;
    }

    std::sort(
        rawIndices + offset,
        rawIndices + offset + size,
        [&](vector_size_t& a, vector_size_t& b) {
          if (a == b) {
            return false;
          }
          bool aNull = decodedElements->isNullAt(a);
          bool bNull = decodedElements->isNullAt(b);
          if (aNull) {
            return false;
          }
          if (bNull) {
            return true;
          }

          std::optional<int32_t> result = baseElementsVector->compare(
              baseElementsVector, decodedIndices[a], decodedIndices[b], flags);

          if (!result.has_value()) {
            VELOX_USER_FAIL("Ordering nulls is not supported");
          }

          return result.value() < 0;
        });
  });

  return indices;
}

void applyComplexType(
    const SelectivityVector& rows,
    ArrayVector* inputArray,
    bool ascending,
    exec::EvalCtx& context,
    VectorPtr& resultElements,
    bool throwOnNestedNull) {
  auto inputElements = inputArray->elements();
  auto indices = sortElements(
      rows, *inputArray, *inputElements, ascending, context, throwOnNestedNull);
  resultElements = BaseVector::transpose(indices, std::move(inputElements));
}

template <typename T>
inline void swapWithNull(
    FlatVector<T>* vector,
    vector_size_t index,
    vector_size_t nullIndex) {
  // Values are already present in vector stringBuffers. Don't create additional
  // copy.
  if constexpr (std::is_same_v<T, StringView>) {
    vector->setNoCopy(nullIndex, vector->valueAt(index));
  } else {
    vector->set(nullIndex, vector->valueAt(index));
  }
  vector->setNull(index, true);
}

template <TypeKind kind>
void applyScalarType(
    const SelectivityVector& rows,
    const ArrayVector* inputArray,
    bool ascending,
    exec::EvalCtx& context,
    VectorPtr& resultElements) {
  using T = typename TypeTraits<kind>::NativeType;

  // Copy array elements to new vector.
  const VectorPtr& inputElements = inputArray->elements();
  VELOX_DCHECK(kind == inputElements->typeKind());
  const SelectivityVector inputElementRows =
      toElementRows(inputElements->size(), rows, inputArray);
  const vector_size_t elementsCount = inputElementRows.size();

  // TODO: consider to use dictionary wrapping to avoid the direct sorting on
  // the scalar values as we do for complex data type if this runs slow in
  // practice.
  resultElements =
      BaseVector::create(inputElements->type(), elementsCount, context.pool());
  resultElements->copy(
      inputElements.get(), inputElementRows, /*toSourceRow=*/nullptr);

  auto flatResults = resultElements->asFlatVector<T>();

  auto processRow = [&](vector_size_t row) {
    const auto size = inputArray->sizeAt(row);
    const auto offset = inputArray->offsetAt(row);
    if (size == 0) {
      return;
    }
    vector_size_t numNulls = 0;
    // Move nulls to end of array.
    for (vector_size_t i = size - 1; i >= 0; --i) {
      if (flatResults->isNullAt(offset + i)) {
        swapWithNull<T>(flatResults, offset + size - numNulls - 1, offset + i);
        ++numNulls;
      }
    }
    // Exclude null values while sorting.
    const auto startRow = offset;
    const auto endRow = startRow + size - numNulls;

    if constexpr (kind == TypeKind::BOOLEAN) {
      uint64_t* rawBits = flatResults->template mutableRawValues<uint64_t>();
      const auto numOneBits = bits::countBits(rawBits, startRow, endRow);
      const auto endZeroRow = endRow - numOneBits;

      if (ascending) {
        bits::fillBits(rawBits, startRow, endZeroRow, false);
        bits::fillBits(rawBits, endZeroRow, endRow, true);
      } else {
        bits::fillBits(rawBits, startRow, startRow + numOneBits, true);
        bits::fillBits(rawBits, endZeroRow, endRow, false);
      }
    } else if constexpr (kind == TypeKind::REAL || kind == TypeKind::DOUBLE) {
      T* resultRawValues = flatResults->mutableRawValues();
      if (ascending) {
        std::sort(
            resultRawValues + startRow,
            resultRawValues + endRow,
            util::floating_point::NaNAwareLessThan<T>());
      } else {
        std::sort(
            resultRawValues + startRow,
            resultRawValues + endRow,
            util::floating_point::NaNAwareGreaterThan<T>());
      }
    } else {
      T* resultRawValues = flatResults->mutableRawValues();
      if (ascending) {
        std::sort(resultRawValues + startRow, resultRawValues + endRow);
      } else {
        std::sort(
            resultRawValues + startRow,
            resultRawValues + endRow,
            std::greater<T>());
      }
    }
  };
  rows.applyToSelected(processRow);
}

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <TypeKind T>
class ArraySortFunction : public exec::VectorFunction {
 public:
  /// This class implements the array_sort query function. Takes an array as
  /// input and sorts it in ascending order and null elements will be placed at
  /// the end of the returned array.
  ///
  /// Along with the set, we maintain a `hasNull` flag that indicates whether
  /// null is present in the array.
  ///
  /// Zero element copy for complex data type:
  ///
  /// In order to prevent copies of array elements with complex data type, the
  /// function reuses the internal elements() vector from the original
  /// ArrayVector. A new vector is created containing the indices of the sorted
  /// elements in the output, and wrapped into a DictionaryVector. The 'lengths'
  /// and 'offsets' vectors that control where output arrays start and end
  /// remain the same in the output ArrayVector.

  explicit ArraySortFunction(bool ascending, bool throwOnNestedNull)
      : ascending_{ascending}, throwOnNestedNull_(throwOnNestedNull) {}

  // Execute function.
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
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
  VectorPtr applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context) const {
    // Acquire the array elements vector.
    auto inputArray = arg->as<ArrayVector>();
    VectorPtr resultElements;

    if (velox::TypeTraits<T>::isPrimitiveType) {
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          applyScalarType,
          T,
          rows,
          inputArray,
          ascending_,
          context,
          resultElements);

    } else {
      applyComplexType(
          rows,
          inputArray,
          ascending_,
          context,
          resultElements,
          throwOnNestedNull_);
    }

    return std::make_shared<ArrayVector>(
        context.pool(),
        inputArray->type(),
        inputArray->nulls(),
        rows.end(),
        inputArray->offsets(),
        inputArray->sizes(),
        resultElements,
        inputArray->getNullCount());
  }

  const bool ascending_;
  const bool throwOnNestedNull_;
};

class ArraySortLambdaFunction : public exec::VectorFunction {
 public:
  explicit ArraySortLambdaFunction(bool ascending, bool throwOnNestedNull)
      : ascending_{ascending}, throwOnNestedNull_(throwOnNestedNull) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Flatten input array.
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    auto newNumElements = flatArray->elements()->size();

    SelectivityVector validRowsInReusedResult =
        toElementRows<ArrayVector>(newNumElements, rows, flatArray.get());

    // Compute sorting keys.
    VectorPtr newElements;

    auto elementToTopLevelRows = getElementToTopLevelRows(
        newNumElements, rows, flatArray.get(), context.pool());

    // Loop over lambda functions and apply these to elements of the base array.
    // In most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows = toElementRows<ArrayVector>(
          newNumElements, *entry.rows, flatArray.get());
      auto wrapCapture = toWrapCapture<ArrayVector>(
          newNumElements, entry.callable, *entry.rows, flatArray);

      entry.callable->apply(
          elementRows,
          &validRowsInReusedResult,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &newElements);
    }

    // Sort 'newElements'.
    auto indices = sortElements(
        rows,
        *flatArray,
        *newElements,
        ascending_,
        context,
        throwOnNestedNull_);
    auto sortedElements = BaseVector::wrapInDictionary(
        nullptr,
        indices,
        indices->size() / sizeof(vector_size_t),
        flatArray->elements());

    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatArray, rows);

    VectorPtr localResult = std::make_shared<ArrayVector>(
        flatArray->pool(),
        flatArray->type(),
        std::move(newNulls),
        rows.end(),
        flatArray->offsets(),
        flatArray->sizes(),
        sortedElements);
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  const bool ascending_;
  const bool throwOnNestedNull_;
};

// Create function template based on type.
template <TypeKind kind>
std::shared_ptr<exec::VectorFunction> createTyped(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    bool ascending,
    bool throwOnNestedNull = true) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return std::make_shared<ArraySortFunction<kind>>(
      ascending, throwOnNestedNull);
}

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    bool ascending,
    bool throwOnNestedNull = true) {
  if (inputArgs.size() == 2) {
    return std::make_shared<ArraySortLambdaFunction>(
        ascending, throwOnNestedNull);
  }

  auto elementType = inputArgs.front().type->childAt(0);
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      createTyped,
      elementType->kind(),
      inputArgs,
      ascending,
      throwOnNestedNull);
}

std::shared_ptr<exec::VectorFunction> createAsc(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return create(inputArgs, true, true);
}

std::shared_ptr<exec::VectorFunction> createDesc(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return create(inputArgs, false, true);
}

// Define function signature.
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures(
    bool withComparator) {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures = {
      // array(T) -> array(T)
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .returnType("array(T)")
          .argumentType("array(T)")
          .build(),
      // array(T), function(T,U), boolean -> array(T)
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .orderableTypeVariable("U")
          .returnType("array(T)")
          .argumentType("array(T)")
          .constantArgumentType("function(T,U)")
          .build(),
  };

  if (withComparator) {
    signatures.push_back(
        // array(T), function(T,T,bigint) -> array(T)
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("array(T)")
            .argumentType("array(T)")
            .constantArgumentType("function(T,T,bigint)")
            .build());
  }
  return signatures;
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
internalCanonicalizeSignatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures = {
      // array(T) -> array(T)
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("array(T)")
          .argumentType("array(T)")
          .build()};
  return signatures;
}

std::shared_ptr<exec::VectorFunction> createAscNoThrowOnNestedNull(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return create(inputArgs, true, false);
}

core::CallTypedExprPtr asArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    if (call->name() == prefix + "array_sort") {
      return call;
    }
  }
  return nullptr;
}

} // namespace

core::TypedExprPtr rewriteArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  auto call = asArraySortCall(prefix, expr);
  if (call == nullptr || call->inputs().size() != 2) {
    return nullptr;
  }

  auto lambda =
      dynamic_cast<const core::LambdaTypedExpr*>(call->inputs()[1].get());
  VELOX_CHECK_NOT_NULL(lambda);

  // Extract 'transform' from the comparison lambda:
  //  (x, y) -> if(func(x) < func(y),...) ===> x -> func(x).
  if (lambda->signature()->size() != 2) {
    return nullptr;
  }

  static const std::string kNotSupported =
      "array_sort with comparator lambda that cannot be rewritten "
      "into a transform is not supported: {}";

  if (auto comparison =
          functions::prestosql::isSimpleComparison(prefix, *lambda)) {
    std::string name = comparison->isLessThen ? prefix + "array_sort"
                                              : prefix + "array_sort_desc";

    if (!comparison->expr->type()->isOrderable()) {
      VELOX_USER_FAIL(kNotSupported, lambda->toString())
    }

    auto rewritten = std::make_shared<core::CallTypedExpr>(
        call->type(),
        std::vector<core::TypedExprPtr>{
            call->inputs()[0],
            std::make_shared<core::LambdaTypedExpr>(
                ROW({lambda->signature()->nameOf(0)},
                    {lambda->signature()->childAt(0)}),
                comparison->expr),
        },
        name);

    return rewritten;
  }

  VELOX_USER_FAIL(kNotSupported, lambda->toString())
}

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_sort,
    signatures(true),
    createAsc);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_array_sort_desc,
    signatures(false),
    createDesc);

// An internal function to canonicalize an array to allow for comparisons. Used
// in AggregationFuzzerTest. Details in
// https://github.com/facebookincubator/velox/issues/6999.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_$internal$canonicalize,
    internalCanonicalizeSignatures(),
    createAscNoThrowOnNestedNull);

} // namespace facebook::velox::functions
