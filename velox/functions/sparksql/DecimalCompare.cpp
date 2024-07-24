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

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/SIMDComparisonUtil.h"
#include "velox/functions/sparksql/DecimalUtil.h"

namespace facebook::velox::functions::sparksql {
namespace {
// Rescale two inputs as the same scale and compare. Returns 0 when a is equal
// with b. Returns -1 when a is less than b. Returns 1 when a is greater than b.
template <typename T>
int32_t rescaleAndCompare(T a, T b, int32_t deltaScale) {
  T aScaled = a;
  T bScaled = b;
  if (deltaScale < 0) {
    aScaled = a * velox::DecimalUtil::kPowersOfTen[-deltaScale];
  } else if (deltaScale > 0) {
    bScaled = b * velox::DecimalUtil::kPowersOfTen[deltaScale];
  }
  if (aScaled == bScaled) {
    return 0;
  } else if (aScaled < bScaled) {
    return -1;
  } else {
    return 1;
  }
}

// Compare two decimals. Rescale one of them if they are of different scales.
int32_t
decimalCompare(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
  if (need256) {
    return rescaleAndCompare<int256_t>(
        static_cast<int256_t>(a), static_cast<int256_t>(b), deltaScale);
  }
  return rescaleAndCompare<int128_t>(a, b, deltaScale);
}

struct GreaterThan {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) > 0;
  }
};

struct GreaterThanOrEqual {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) >= 0;
  }
};

struct LessThan {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) < 0;
  }
};

struct LessThanOrEqual {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) <= 0;
  }
};

struct Equal {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) == 0;
  }
};

struct NotEqual {
  inline static bool
  apply(int128_t a, int128_t b, int8_t deltaScale, bool need256) {
    return decimalCompare(a, b, deltaScale, need256) != 0;
  }
};

template <typename A, typename B, typename Operation /* Arithmetic operation */>
class DecimalCompareFunction : public exec::VectorFunction {
 public:
  DecimalCompareFunction(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale)
      : aPrecision_(aPrecision),
        bPrecision_(bPrecision),
        deltaScale_(aScale - bScale),
        need256_(
            (deltaScale_ < 0 &&
             aPrecision_ - deltaScale_ > LongDecimalType::kMaxPrecision) ||
            (bPrecision_ + deltaScale_ > LongDecimalType::kMaxPrecision)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    prepareResults(rows, resultType, context, result);

    if (shouldApplyAutoSimdComparison<A, B>(rows, args)) {
      applyAutoSimdComparison<A, B, Operation, int8_t, bool>(
          rows, args, context, result, deltaScale_, need256_);
      return;
    }

    // Fast path when the first argument is a flat vector.
    if (args[0]->isFlatEncoding()) {
      auto rawA = args[0]->asUnchecked<FlatVector<A>>()->mutableRawValues();

      if (args[1]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)rawA[row],
                  (int128_t)constantB,
                  deltaScale_,
                  need256_));
        });
        return;
      }

      if (args[1]->isFlatEncoding()) {
        auto rawB = args[1]->asUnchecked<FlatVector<B>>()->mutableRawValues();
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)rawA[row],
                  (int128_t)rawB[row],
                  deltaScale_,
                  need256_));
        });
        return;
      }
    } else {
      // Fast path when the first argument is encoded but the second is
      // constant.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto aDecoded = decodedArgs.at(0);
      auto aDecodedData = aDecoded->data<A>();

      if (args[1]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          auto value = aDecodedData[aDecoded->index(row)];
          result->asUnchecked<FlatVector<bool>>()->set(
              row,
              Operation::apply(
                  (int128_t)value, (int128_t)constantB, deltaScale_, need256_));
        });
        return;
      }
    }

    // Decode the input in all other cases.
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto aDecoded = decodedArgs.at(0);
    auto bDecoded = decodedArgs.at(1);

    auto aDecodedData = aDecoded->data<A>();
    auto bDecodedData = bDecoded->data<B>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto aValue = aDecodedData[aDecoded->index(row)];
      auto bValue = bDecodedData[bDecoded->index(row)];
      result->asUnchecked<FlatVector<bool>>()->set(
          row,
          Operation::apply(
              (int128_t)aValue, (int128_t)bValue, deltaScale_, need256_));
    });
  }

 private:
  void prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
  }

  const uint8_t aPrecision_;
  const uint8_t bPrecision_;
  const int8_t deltaScale_;
  // If 256 bits are needed after adjusting the scale.
  const bool need256_;
};

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalCompareFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& aType = inputArgs[0].type;
  const auto& bType = inputArgs[1].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  if (aType->isShortDecimal()) {
    if (bType->isShortDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int64_t, int64_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    } else {
      return std::make_shared<
          DecimalCompareFunction<int64_t, int128_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    }
  }
  if (aType->isLongDecimal()) {
    if (bType->isShortDecimal()) {
      return std::make_shared<
          DecimalCompareFunction<int128_t, int64_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    } else {
      return std::make_shared<
          DecimalCompareFunction<int128_t, int128_t, Operation>>(
          aPrecision, aScale, bPrecision, bScale);
    }
  }
  VELOX_UNREACHABLE();
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalCompareSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .returnType("boolean")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_gt,
    decimalCompareSignature(),
    createDecimalCompareFunction<GreaterThan>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_gte,
    decimalCompareSignature(),
    createDecimalCompareFunction<GreaterThanOrEqual>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_lt,
    decimalCompareSignature(),
    createDecimalCompareFunction<LessThan>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_lte,
    decimalCompareSignature(),
    createDecimalCompareFunction<LessThanOrEqual>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_eq,
    decimalCompareSignature(),
    createDecimalCompareFunction<Equal>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_neq,
    decimalCompareSignature(),
    createDecimalCompareFunction<NotEqual>);
} // namespace facebook::velox::functions::sparksql
