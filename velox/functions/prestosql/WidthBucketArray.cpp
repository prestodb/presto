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
#include "velox/functions/prestosql/WidthBucketArray.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::functions {
namespace {

template <typename T>
int64_t widthBucket(
    double operand,
    DecodedVector& elementsHolder,
    int offset,
    int binCount) {
  VELOX_USER_CHECK_GT(binCount, 0, "Bins cannot be an empty array");
  VELOX_USER_CHECK(!std::isnan(operand), "Operand cannot be NaN");

  int lower = 0;
  int upper = binCount;
  while (lower < upper) {
    VELOX_USER_CHECK_LE(
        elementsHolder.valueAt<T>(offset + lower),
        elementsHolder.valueAt<T>(offset + upper - 1),
        "Bin values are not sorted in ascending order");

    int index = (lower + upper) / 2;
    auto bin = elementsHolder.valueAt<T>(offset + index);

    VELOX_USER_CHECK(std::isfinite(bin), "Bin value must be finite");

    if (operand < bin) {
      upper = index;
    } else {
      lower = index + 1;
    }
  }
  return lower;
}

template <typename T>
class WidthBucketArrayFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    context->ensureWritable(rows, BIGINT(), *result);
    auto flatResult = (*result)->asFlatVector<int64_t>()->mutableRawValues();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto operand = decodedArgs.at(0);
    auto bins = decodedArgs.at(1);

    auto binsArray = bins->base()->as<ArrayVector>();
    auto rawSizes = binsArray->rawSizes();
    auto rawOffsets = binsArray->rawOffsets();
    auto elementsVector = binsArray->elements();
    auto elementsRows =
        toElementRows(elementsVector->size(), rows, binsArray, bins->indices());
    exec::LocalDecodedVector elementsHolder(
        context, *elementsVector, elementsRows);

    auto indices = bins->indices();
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];
      try {
        flatResult[row] = widthBucket<T>(
            operand->valueAt<double>(row), *elementsHolder.get(), offset, size);
      } catch (const std::exception& e) {
        context->setError(row, std::current_exception());
      }
    });
  }
};

class WidthBucketArrayFunctionConstantBins : public exec::VectorFunction {
 public:
  explicit WidthBucketArrayFunctionConstantBins(std::vector<double> bins)
      : bins_(std::move(bins)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    context->ensureWritable(rows, BIGINT(), *result);
    auto flatResult = (*result)->asFlatVector<int64_t>()->mutableRawValues();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto operand = decodedArgs.at(0);

    rows.applyToSelected([&](auto row) {
      try {
        flatResult[row] = widthBucket(operand->valueAt<double>(row), bins_);
      } catch (const std::exception& e) {
        context->setError(row, std::current_exception());
      }
    });
  }

 private:
  const std::vector<double> bins_;

  static int64_t widthBucket(double operand, const std::vector<double>& bins) {
    VELOX_USER_CHECK(!std::isnan(operand), "Operand cannot be NaN");

    int lower = 0;
    int upper = (int)bins.size();
    while (lower < upper) {
      int index = (lower + upper) / 2;
      auto bin = bins.at(index);

      if (operand < bin) {
        upper = index;
      } else {
        lower = index + 1;
      }
    }
    return lower;
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>>
widthBucketArraySignature() {
  // double, array(double|bigint) -> bigint
  return {
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("double")
          .argumentType("array(double)")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("double")
          .argumentType("array(bigint)")
          .build(),
  };
}

template <typename T>
std::vector<double> toBinValues(
    const VectorPtr& binsVector,
    vector_size_t offset,
    vector_size_t size) {
  std::vector<double> binValues;
  binValues.reserve(size);
  auto simpleVector = binsVector->asUnchecked<SimpleVector<T>>();

  for (int i = 0; i < size; i++) {
    VELOX_USER_CHECK(
        !simpleVector->isNullAt(offset + i), "Bin value cannot be null");
    auto value = simpleVector->valueAt(offset + i);
    VELOX_USER_CHECK(std::isfinite(value), "Bin value must be finite");
    if (i > 0) {
      VELOX_USER_CHECK_GT(
          value,
          simpleVector->valueAt(offset + i - 1),
          "Bin values are not sorted in ascending order")
    }
    binValues.push_back(value);
  }

  return binValues;
}

std::shared_ptr<exec::VectorFunction> makeWidthBucketArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  const auto& operandVector = inputArgs[0];
  const auto& binsVector = inputArgs[1];

  VELOX_CHECK_EQ(operandVector.type->kind(), TypeKind::DOUBLE);
  VELOX_CHECK_EQ(binsVector.type->kind(), TypeKind::ARRAY);

  auto binsTypeKind = binsVector.type->asArray().elementType()->kind();

  auto constantBins = binsVector.constantValue.get();
  if (constantBins != nullptr && !constantBins->isNullAt(0)) {
    auto binsArrayVector = constantBins->wrappedVector()->as<ArrayVector>();
    auto binsArrayIndex = constantBins->wrappedIndex(0);
    auto size = binsArrayVector->sizeAt(binsArrayIndex);
    VELOX_USER_CHECK_GT(size, 0, "Bins cannot be an empty array");
    auto offset = binsArrayVector->offsetAt(binsArrayIndex);

    // This is a different behavior comparing to non-constant implementation:
    //
    // In non-constant bins implementation, we only do these checks during
    // binary search, which means we might ignore even if there are infinite
    // value or non-ascending order.
    //
    // In constant bins implementation, we first check bins, so if the bins is
    // invalid, they will fail directly.

    std::vector<double> binValues;
    if (binsTypeKind == TypeKind::DOUBLE) {
      binValues =
          toBinValues<double>(binsArrayVector->elements(), offset, size);
    } else if (binsTypeKind == TypeKind::BIGINT) {
      binValues =
          toBinValues<int64_t>(binsArrayVector->elements(), offset, size);
    } else {
      VELOX_UNSUPPORTED(
          "Unsupported type of 'bins' argument: {}",
          binsArrayVector->type()->toString());
    }

    return std::make_shared<WidthBucketArrayFunctionConstantBins>(
        std::move(binValues));
  }

  if (binsTypeKind == TypeKind::DOUBLE) {
    return std::make_shared<WidthBucketArrayFunction<double>>();
  } else if (binsTypeKind == TypeKind::BIGINT) {
    return std::make_shared<WidthBucketArrayFunction<int64_t>>();
  } else {
    VELOX_UNSUPPORTED(
        "Unsupported type of 'bins' argument: {}", binsVector.type->toString());
  }
}

} // namespace facebook::velox::functions
