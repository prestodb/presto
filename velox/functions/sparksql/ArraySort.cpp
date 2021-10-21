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

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    const ArrayVector* inputArray,
    bool sortDescending,
    exec::EvalCtx* context,
    VectorPtr result) {
  using T = typename TypeTraits<kind>::NativeType;

  // Decode and acquire array elements vector.
  const VectorPtr& elementsVector = inputArray->elements();
  SelectivityVector elementRows =
      toElementRows(elementsVector->size(), rows, inputArray);
  exec::LocalDecodedVector inputElements(context, *elementsVector, elementRows);

  // Initialize elements vectors for result
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
    if (sortDescending) {
      // Move nulls to end of array
      vector_size_t numNulls = 0;
      for (vector_size_t i = size - 1; i >= 0; --i) {
        if (inputElements->isNullAt(i + inputOffset)) {
          std::swap(*(rowValues + i), *(rowValues + size - numNulls - 1));
          resultElements->setNull(resultOffset + size - numNulls - 1, true);
          ++numNulls;
        }
      }
      std::sort(rowValues, rowValues + size - numNulls, Greater<T>());
    } else {
      // Move nulls to beginning of array
      vector_size_t numNulls = 0;
      for (vector_size_t i = 0; i < size; ++i) {
        if (inputElements->isNullAt(i + inputOffset)) {
          std::swap(*(rowValues + i), *(rowValues + numNulls));
          resultElements->setNull(resultOffset + numNulls, true);
          ++numNulls;
        }
      }
      std::sort(rowValues + numNulls, rowValues + size, Less<T>());
    }
    resultOffset += size;
  };

  rows.applyToSelected(processRow);
}

class ArraySort : public exec::VectorFunction {
  /// This class implements array_sort function. Takes an array as input
  /// and sorts it in ascending order as per following sematics
  /// NULL < -Inf < Inf < NaN
  /// If optional second parameter is set to true, then the elements will
  /// sorted in descending order instead.
  ///
  /// Signature:
  ///   array_sort(array(T)) -> array(T)
  ///   array_sort(array(T), boolean) -> array(T)
  ///
  /// NOTE: Supports all scalar types except BOOLEAN, VARCHAR and VARBINARY.

 public:
  explicit ArraySort(bool sortDescending) : sortDescending_(sortDescending) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    const ArrayVector* inputArray = args[0]->as<ArrayVector>();

    // initialize result ArrayVector
    VectorPtr resultArray = BaseVector::create(
        inputArray->type(), inputArray->size(), context->pool());
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        applyTyped,
        inputArray->elements()->typeKind(),
        rows,
        inputArray,
        sortDescending_,
        context,
        resultArray);
    context->moveOrCopyResult(resultArray, rows, result);
  }

 private:
  bool sortDescending_;
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> arraySortSignatures() {
  return {// array(T) -> array(T)
          exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .argumentType("array(T)")
              .returnType("array(T)")
              .build(),
          // array(T, boolean) -> array(T)
          exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .argumentType("array(T)")
              .argumentType("boolean")
              .returnType("array(T)")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeArraySort(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK(
      inputArgs.size() == 1 || inputArgs.size() == 2,
      "Invalid number of arguments {}, expected 1 or 2",
      inputArgs.size());
  bool sortDescending = false;
  // Read optional sort descending flag.
  if (inputArgs.size() == 2) {
    BaseVector* boolVector = inputArgs[1].constantValue.get();
    if (!boolVector || !boolVector->isConstantEncoding()) {
      VELOX_USER_FAIL(
          "{} requires a constant bool as the second argument.", name);
    }
    sortDescending = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  }
  return std::make_shared<ArraySort>(sortDescending);
}
} // namespace facebook::velox::functions::sparksql
