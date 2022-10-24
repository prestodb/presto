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

#include <velox/vector/TypeAliases.h>
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"

namespace facebook::velox::functions {
namespace {
///
/// Implements the array_sum function.
/// See documentation at https://prestodb.io/docs/current/functions/array.html
///

template <typename TInput, typename TOutput>
class ArraySumFunction : public exec::VectorFunction {
 public:
  template <bool mayHaveNulls, typename DataAtFunc, typename IsNullFunc>
  TOutput applyCore(
      vector_size_t row,
      const ArrayVector* arrayVector,
      DataAtFunc&& dataAtFunc,
      IsNullFunc&& isNullFunc) const {
    auto start = arrayVector->offsetAt(row);
    auto end = start + arrayVector->sizeAt(row);
    TOutput sum = 0;

    auto addElement = [](TOutput& sum, TInput value) {
      if constexpr (std::is_same_v<TOutput, int64_t>) {
        sum = checkedPlus<TOutput>(sum, value);
      } else {
        sum += value;
      }
    };

    for (auto i = start; i < end; i++) {
      if constexpr (mayHaveNulls) {
        bool isNull = isNullFunc(i);
        if (!isNull) {
          addElement(sum, dataAtFunc(i));
        }
      } else {
        addElement(sum, dataAtFunc(i));
      }
    }
    return sum;
  }

  template <bool mayHaveNulls, typename DataAtFunc, typename IsNullFunc>
  void applyCore(
      const SelectivityVector& rows,
      const ArrayVector* arrayVector,
      FlatVector<TOutput>* resultValues,
      DataAtFunc&& dataAtFunc,
      IsNullFunc&& isNullFunc) const {
    rows.template applyToSelected([&](vector_size_t row) {
      resultValues->set(
          row,
          applyCore<mayHaveNulls>(row, arrayVector, dataAtFunc, isNullFunc));
    });
  }

  template <bool mayHaveNulls>
  void applyFlat(
      const SelectivityVector& rows,
      ArrayVector* arrayVector,
      const uint64_t* rawNulls,
      const TInput* rawElements,
      FlatVector<TOutput>* resultValues) const {
    applyCore<mayHaveNulls>(
        rows,
        arrayVector,
        resultValues,
        [&](vector_size_t index) { return rawElements[index]; },
        [&](vector_size_t index) { return bits::isBitNull(rawNulls, index); });
  }

  template <bool mayHaveNulls>
  void applyNonFlat(
      const SelectivityVector& rows,
      ArrayVector* arrayVector,
      exec::LocalDecodedVector& elements,
      FlatVector<TOutput>* resultValues) const {
    applyCore<mayHaveNulls>(
        rows,
        arrayVector,
        resultValues,
        [&](vector_size_t index) {
          return elements->template valueAt<TInput>(index);
        },
        [&](vector_size_t index) { return elements->isNullAt(index); });
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Input is either flat or constant.

    if (args[0]->isConstantEncoding()) {
      auto arrayVector = args[0]->wrappedVector()->as<ArrayVector>();
      auto arrayRow = args[0]->wrappedIndex(rows.begin());
      VELOX_CHECK(arrayVector);
      auto elementsVector = arrayVector->elements();

      SelectivityVector elementsRows(elementsVector->size());
      exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

      TOutput sum;
      if (elementsVector->mayHaveNulls()) {
        sum = applyCore<true>(
            arrayRow,
            arrayVector,
            [&](auto index) { return elements->valueAt<TInput>(index); },
            [&](auto index) { return elements->isNullAt(index); });
      } else {
        sum = applyCore<false>(
            arrayRow,
            arrayVector,
            [&](auto index) { return elements->valueAt<TInput>(index); },
            [&](auto index) { return elements->isNullAt(index); });
      }

      context.moveOrCopyResult(
          BaseVector::createConstant(sum, rows.end(), context.pool()),
          rows,
          result);
      return;
    }

    VELOX_CHECK_EQ(
        args[0]->encoding(),
        VectorEncoding::Simple::ARRAY,
        "Expected flat or constant encoding");

    // Prepare result vector for writing
    BaseVector::ensureWritable(rows, outputType, context.pool(), result);
    auto resultValues = result->template asFlatVector<TOutput>();

    auto arrayVector = args[0]->as<ArrayVector>();
    VELOX_CHECK(arrayVector);
    auto elementsVector = arrayVector->elements();

    if (elementsVector->isFlatEncoding()) {
      const TInput* __restrict rawElements =
          elementsVector->as<FlatVector<TInput>>()->rawValues();
      const uint64_t* __restrict rawNulls = elementsVector->rawNulls();

      if (elementsVector->mayHaveNulls()) {
        applyFlat<true>(rows, arrayVector, rawNulls, rawElements, resultValues);
      } else {
        applyFlat<false>(
            rows, arrayVector, rawNulls, rawElements, resultValues);
      }
    } else {
      SelectivityVector elementsRows(elementsVector->size());
      exec::LocalDecodedVector elements(context, *elementsVector, elementsRows);

      if (elementsVector->mayHaveNulls()) {
        applyNonFlat<true>(rows, arrayVector, elements, resultValues);
      } else {
        applyNonFlat<false>(rows, arrayVector, elements, resultValues);
      }
    }
  }
};

// Create function.
std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto elementType = inputArgs.front().type->childAt(0);

  switch (elementType->kind()) {
    case TypeKind::TINYINT: {
      return std::make_shared<ArraySumFunction<
          TypeTraits<TypeKind::TINYINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::SMALLINT: {
      return std::make_shared<ArraySumFunction<
          TypeTraits<TypeKind::SMALLINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::INTEGER: {
      return std::make_shared<ArraySumFunction<
          TypeTraits<TypeKind::INTEGER>::NativeType,
          int64_t>>();
    }
    case TypeKind::BIGINT: {
      return std::make_shared<ArraySumFunction<
          TypeTraits<TypeKind::BIGINT>::NativeType,
          int64_t>>();
    }
    case TypeKind::REAL: {
      return std::make_shared<
          ArraySumFunction<TypeTraits<TypeKind::REAL>::NativeType, double>>();
    }
    case TypeKind::DOUBLE: {
      return std::make_shared<
          ArraySumFunction<TypeTraits<TypeKind::DOUBLE>::NativeType, double>>();
    }
    default: {
      VELOX_FAIL("Unsupported Type")
    }
  }
}

// Define function signature.
// array(T1) -> T2 where T1 must be coercible to bigint or double, and
// T2 is bigint or double
std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  static const std::map<std::string, std::string> typePairs = {
      {"tinyint", "bigint"},
      {"smallint", "bigint"},
      {"integer", "bigint"},
      {"bigint", "bigint"},
      {"real", "double"},
      {"double", "double"}};
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(typePairs.size());
  for (const auto& [argType, returnType] : typePairs) {
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType(returnType)
                                .argumentType(fmt::format("array({})", argType))
                                .build());
  }
  return signatures;
}
} // namespace

// Register function.
VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(udf_array_sum, signatures(), create);

} // namespace facebook::velox::functions
