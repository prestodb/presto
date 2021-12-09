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
#include "velox/functions/sparksql/ArraySort.h"

#include <memory>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/sparksql/Comparisons.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <template <typename> class Cmp, TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    const ArrayVector* inputArray,
    bool nullsFirst,
    exec::EvalCtx* context,
    VectorPtr result) {
  using T = typename TypeTraits<kind>::NativeType;

  // Decode and acquire array elements vector.
  const VectorPtr& elementsVector = inputArray->elements();
  SelectivityVector elementRows =
      toElementRows(elementsVector->size(), rows, inputArray);
  exec::LocalDecodedVector inputElements(context, *elementsVector, elementRows);

  // Initialize elements vectors for result.
  ArrayVector* resultArray = result->as<ArrayVector>();
  auto resultElements = resultArray->elements();
  resultElements->resize(inputArray->elements()->size());
  auto resultFlatElements = resultElements->asFlatVector<T>();
  T* resultRawValues = resultFlatElements->mutableRawValues();

  vector_size_t resultOffset = 0;
  auto processRow = [&](vector_size_t row) {
    auto size = inputArray->sizeAt(row);
    auto inputOffset = inputArray->offsetAt(row);
    resultArray->setOffsetAndSize(row, resultOffset, size);
    if (size == 0) {
      return;
    }
    T* rowValues = resultRawValues + resultOffset;
    for (int j = 0; j < size; ++j) {
      *(rowValues + j) = inputElements->valueAt<T>(j + inputOffset);
    }
    if (nullsFirst) {
      // Move nulls to beginning of array.
      vector_size_t numNulls = 0;
      for (vector_size_t i = 0; i < size; ++i) {
        if (inputElements->isNullAt(i + inputOffset)) {
          std::swap(*(rowValues + i), *(rowValues + numNulls));
          resultElements->setNull(resultOffset + numNulls, true);
          ++numNulls;
        }
      }
      std::sort(rowValues + numNulls, rowValues + size, Cmp<T>());
    } else {
      // Move nulls to end of array.
      vector_size_t numNulls = 0;
      for (vector_size_t i = size - 1; i >= 0; --i) {
        if (inputElements->isNullAt(i + inputOffset)) {
          std::swap(*(rowValues + i), *(rowValues + size - numNulls - 1));
          resultElements->setNull(resultOffset + size - numNulls - 1, true);
          ++numNulls;
        }
      }
      std::sort(rowValues, rowValues + size - numNulls, Cmp<T>());
    }
    resultOffset += size;
  };

  rows.applyToSelected(processRow);
}
} // namespace

template <template <typename> class Cmp>
void ArraySort<Cmp>::apply(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    const TypePtr& /*outputType*/,
    exec::EvalCtx* context,
    VectorPtr* result) const {
  const ArrayVector* inputArray = args[0]->as<ArrayVector>();

  // Initialize result ArrayVector.
  VectorPtr resultArray = BaseVector::create(
      inputArray->type(), inputArray->size(), context->pool());
  VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      applyTyped,
      Cmp,
      inputArray->elements()->typeKind(),
      rows,
      inputArray,
      nullsFirst_,
      context,
      resultArray);
  context->moveOrCopyResult(resultArray, rows, result);
}

// Signature: array_sort(array(T)) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> arraySortSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeArraySort(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);
  // Nulls are considered largest.
  return std::make_shared<ArraySort<Less>>(/*nullsFirst=*/false);
}

// Signatures:
//   sort_array(array(T)) -> array(T)
//   sort_array(array(T), boolean) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> sortArraySignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .argumentType("array(T)")
          .argumentType("boolean")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeSortArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK(
      inputArgs.size() == 1 || inputArgs.size() == 2,
      "Invalid number of arguments {}, expected 1 or 2",
      inputArgs.size());
  bool ascending = true;
  // Read optional sort ascending flag.
  if (inputArgs.size() == 2) {
    BaseVector* boolVector = inputArgs[1].constantValue.get();
    if (!boolVector || !boolVector->isConstantEncoding()) {
      VELOX_USER_FAIL(
          "{} requires a constant bool as the second argument.", name);
    }
    ascending = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  }
  // Nulls are considered smallest.
  bool nullsFirst = ascending;
  if (ascending) {
    return std::make_shared<ArraySort<Less>>(nullsFirst);
  } else {
    return std::make_shared<ArraySort<Greater>>(nullsFirst);
  }
}
} // namespace facebook::velox::functions::sparksql
