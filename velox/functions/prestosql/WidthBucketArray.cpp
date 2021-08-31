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

class WidthBucketArrayFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /*caller*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
    auto flatResult = (*result)->asFlatVector<int64_t>()->mutableRawValues();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto operand = decodedArgs.at(0);
    auto bins = decodedArgs.at(1);

    auto binsArray = bins->base()->as<ArrayVector>();
    auto rawSizes = binsArray->rawSizes();
    auto rawOffsets = binsArray->rawOffsets();
    auto elementsVector = binsArray->elements();
    auto elementsRows = toElementRows(elementsVector->size(), rows, binsArray);
    exec::LocalDecodedVector elementsHolder(
        context, *elementsVector, elementsRows);

    auto indices = bins->indices();
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];
      try {
        flatResult[row] = widthBucket(
            operand->valueAt<double>(row), *elementsHolder.get(), offset, size);
      } catch (const std::exception& e) {
        context->setError(row, std::current_exception());
      }
    });
  }

 private:
  static int64_t widthBucket(
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
          elementsHolder.valueAt<double>(offset + lower),
          elementsHolder.valueAt<double>(offset + upper - 1),
          "Bin values are not sorted in ascending order");

      int index = (lower + upper) / 2;
      auto bin = elementsHolder.valueAt<double>(offset + index);

      VELOX_USER_CHECK(std::isfinite(bin), "Bin value must be finite");

      if (operand < bin) {
        upper = index;
      } else {
        lower = index + 1;
      }
    }
    return lower;
  }
};

class WidthBucketArrayFunctionConstantBins : public exec::VectorFunction {
 public:
  explicit WidthBucketArrayFunctionConstantBins(std::vector<double> bins)
      : bins_(std::move(bins)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /*caller*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
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
  // double, array(double) -> bigint
  return {exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("double")
              .argumentType("array(double)")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeWidthBucketArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  const auto& operandVector = inputArgs[0];
  const auto& binsVector = inputArgs[1];

  VELOX_CHECK(operandVector.type->isDouble());
  VELOX_CHECK(binsVector.type->isArray());
  VELOX_CHECK(binsVector.type->asArray().elementType()->isDouble());

  auto constantBins = binsVector.constantValue.get();
  if (constantBins != nullptr && !constantBins->isNullAt(0)) {
    auto binsArrayVector = constantBins->wrappedVector()->as<ArrayVector>();
    auto binsArrayIndex = constantBins->wrappedIndex(0);
    auto size = binsArrayVector->rawSizes()[binsArrayIndex];
    VELOX_USER_CHECK_GT(size, 0, "Bins cannot be an empty array");
    auto offset = binsArrayVector->rawOffsets()[binsArrayIndex];
    auto elementVector =
        binsArrayVector->elements()->asUnchecked<SimpleVector<double>>();

    std::vector<double> binValues;
    binValues.reserve(size);
    for (int i = 0; i < size; i++) {
      auto value = elementVector->valueAt(offset + i);
      // This is a different behavior comparing to non-constant implementation:
      //
      // In non-constant bins implementation, we only do these checks during
      // binary search, which means we might ignore even if there are infinite
      // value or non-ascending order.
      //
      // In constant bins implementation, we first check bins, so if the bins is
      // invalid, they will fail directly.
      VELOX_USER_CHECK(std::isfinite(value), "Bin value must be finite");
      if (i > 0) {
        VELOX_USER_CHECK_GT(
            value,
            elementVector->valueAt(offset + i - 1),
            "Bin values are not sorted in ascending order")
      }
      binValues.push_back(value);
    }
    return std::make_shared<WidthBucketArrayFunctionConstantBins>(
        std::move(binValues));
  }
  return std::make_shared<WidthBucketArrayFunction>();
}

} // namespace facebook::velox::functions
