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
#pragma once

#include <charconv>

#include "velox/common/base/CountBits.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/StringWriter.h"
#include "velox/external/date/tz.h"
#include "velox/type/Type.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {
namespace {

inline std::string makeErrorMessage(
    const BaseVector& input,
    vector_size_t row,
    const TypePtr& toType,
    const std::string& details = "") {
  return fmt::format(
      "Cannot cast {} '{}' to {}. {}",
      input.type()->toString(),
      input.toString(row),
      toType->toString(),
      details);
}

inline std::exception_ptr makeBadCastException(
    const TypePtr& resultType,
    const BaseVector& input,
    vector_size_t row,
    const std::string& errorDetails) {
  return std::make_exception_ptr(VeloxUserError(
      std::current_exception(),
      makeErrorMessage(input, row, resultType, errorDetails),
      false));
}

/// @brief Convert the unscaled value of a decimal to varchar and write to raw
/// string buffer from start position.
/// @tparam T The type of input value.
/// @param unscaledValue The input unscaled value.
/// @param scale The scale of decimal.
/// @param maxVarcharSize The estimated max size of a varchar.
/// @param startPosition The start position to write from.
/// @return A string view.
template <typename T>
StringView convertToStringView(
    T unscaledValue,
    int32_t scale,
    int32_t maxVarcharSize,
    char* const startPosition) {
  char* writePosition = startPosition;
  if (unscaledValue == 0) {
    *writePosition++ = '0';
    if (scale > 0) {
      *writePosition++ = '.';
      // Append leading zeros.
      std::memset(writePosition, '0', scale);
      writePosition += scale;
    }
  } else {
    if (unscaledValue < 0) {
      *writePosition++ = '-';
      unscaledValue = -unscaledValue;
    }
    auto [position, errorCode] = std::to_chars(
        writePosition,
        writePosition + maxVarcharSize,
        unscaledValue / DecimalUtil::kPowersOfTen[scale]);
    VELOX_DCHECK_EQ(
        errorCode,
        std::errc(),
        "Failed to cast decimal to varchar: {}",
        std::make_error_code(errorCode).message());
    writePosition = position;

    if (scale > 0) {
      *writePosition++ = '.';
      uint128_t fraction = unscaledValue % DecimalUtil::kPowersOfTen[scale];
      // Append leading zeros.
      int numLeadingZeros = std::max(scale - countDigits(fraction), 0);
      std::memset(writePosition, '0', numLeadingZeros);
      writePosition += numLeadingZeros;
      // Append remaining fraction digits.
      auto result = std::to_chars(
          writePosition, writePosition + maxVarcharSize, fraction);
      VELOX_DCHECK_EQ(
          result.ec,
          std::errc(),
          "Failed to cast decimal to varchar: {}",
          std::make_error_code(result.ec).message());
      writePosition = result.ptr;
    }
  }
  return StringView(startPosition, writePosition - startPosition);
}

} // namespace

namespace detail {

/// Represent the varchar fragment.
///
/// For example:
/// | value | wholeDigits | fractionalDigits | exponent | sign |
/// | 9999999999.99 | 9999999999 | 99 | nullopt | 1 |
/// | 15 | 15 |  | nullopt | 1 |
/// | 1.5 | 1 | 5 | nullopt | 1 |
/// | -1.5 | 1 | 5 | nullopt | -1 |
/// | 31.523e-2 | 31 | 523 | -2 | 1 |
struct DecimalComponents {
  std::string_view wholeDigits;
  std::string_view fractionalDigits;
  std::optional<int32_t> exponent = std::nullopt;
  int8_t sign = 1;
};

// Extract a string view of continuous digits.
std::string_view extractDigits(const char* s, size_t start, size_t size);

/// Parse decimal components, including whole digits, fractional digits,
/// exponent and sign, from input chars. Returns error status if input chars
/// do not represent a valid value.
Status
parseDecimalComponents(const char* s, size_t size, DecimalComponents& out);

/// Parse huge int from decimal components. The fractional part is scaled up by
/// required power of 10, and added with the whole part. Returns error status if
/// overflows.
Status parseHugeInt(const DecimalComponents& decimalComponents, int128_t& out);

/// Converts string view to decimal value of given precision and scale.
/// Derives from Arrow function DecimalFromString. Arrow implementation:
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/decimal.cc#L637.
///
/// Firstly, it parses the varchar to DecimalComponents which contains the
/// message that can represent a decimal value. Secondly, processes the exponent
/// to get the scale. Thirdly, compute the rescaled value. Returns status for
/// the outcome of computing.
template <typename T>
Status toDecimalValue(
    const StringView s,
    int toPrecision,
    int toScale,
    T& decimalValue) {
  DecimalComponents decimalComponents;
  if (auto status =
          parseDecimalComponents(s.data(), s.size(), decimalComponents);
      !status.ok()) {
    return Status::UserError("Value is not a number. " + status.message());
  }

  // Count number of significant digits (without leading zeros).
  const size_t firstNonZero =
      decimalComponents.wholeDigits.find_first_not_of('0');
  size_t significantDigits = decimalComponents.fractionalDigits.size();
  if (firstNonZero != std::string::npos) {
    significantDigits += decimalComponents.wholeDigits.size() - firstNonZero;
  }
  int32_t parsedPrecision = static_cast<int32_t>(significantDigits);

  int32_t parsedScale = 0;
  bool roundUp = false;
  const int32_t fractionSize = decimalComponents.fractionalDigits.size();
  if (!decimalComponents.exponent.has_value()) {
    if (fractionSize > toScale) {
      if (decimalComponents.fractionalDigits[toScale] >= '5') {
        roundUp = true;
      }
      parsedScale = toScale;
      decimalComponents.fractionalDigits =
          std::string_view(decimalComponents.fractionalDigits.data(), toScale);
    } else {
      parsedScale = fractionSize;
    }
  } else {
    const auto exponent = decimalComponents.exponent.value();
    parsedScale = -exponent + fractionSize;
    // Truncate the fractionalDigits.
    if (parsedScale > toScale) {
      if (-exponent >= toScale) {
        // The fractional digits could be dropped.
        if (fractionSize > 0 && decimalComponents.fractionalDigits[0] >= '5') {
          roundUp = true;
        }
        decimalComponents.fractionalDigits = "";
        parsedScale -= fractionSize;
      } else {
        const auto reduceDigits = exponent + toScale;
        if (fractionSize > reduceDigits &&
            decimalComponents.fractionalDigits[reduceDigits] >= '5') {
          roundUp = true;
        }
        decimalComponents.fractionalDigits = std::string_view(
            decimalComponents.fractionalDigits.data(),
            std::min(reduceDigits, fractionSize));
        parsedScale -= fractionSize - decimalComponents.fractionalDigits.size();
      }
    }
  }

  int128_t out = 0;
  if (auto status = parseHugeInt(decimalComponents, out); !status.ok()) {
    return status;
  }

  if (roundUp) {
    bool overflow = __builtin_add_overflow(out, 1, &out);
    if (UNLIKELY(overflow)) {
      return Status::UserError("Value too large.");
    }
  }
  out *= decimalComponents.sign;

  if (parsedScale < 0) {
    /// Force the scale to be zero, to avoid negative scales (due to
    /// compatibility issues with external systems such as databases).
    if (-parsedScale + toScale > LongDecimalType::kMaxPrecision) {
      return Status::UserError("Value too large.");
    }

    bool overflow = __builtin_mul_overflow(
        out, DecimalUtil::kPowersOfTen[-parsedScale + toScale], &out);
    if (UNLIKELY(overflow)) {
      return Status::UserError("Value too large.");
    }
    parsedPrecision -= parsedScale;
    parsedScale = toScale;
  }
  const auto status = DecimalUtil::rescaleWithRoundUp<int128_t, T>(
      out,
      std::min((uint8_t)parsedPrecision, LongDecimalType::kMaxPrecision),
      parsedScale,
      toPrecision,
      toScale,
      decimalValue);
  if (!status.ok()) {
    return Status::UserError("Value too large.");
  }
  return status;
}
} // namespace detail

template <typename Func>
void CastExpr::applyToSelectedNoThrowLocal(
    EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result,
    Func&& func) {
  if (setNullInResultAtError()) {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (...) {
        result->setNull(row, true);
      }
    });
  } else {
    rows.template applyToSelected([&](auto row) INLINE_LAMBDA {
      try {
        func(row);
      } catch (const VeloxException& e) {
        if (!e.isUserError()) {
          throw;
        }
        // Avoid double throwing.
        context.setVeloxExceptionError(row, std::current_exception());
      } catch (const std::exception&) {
        context.setError(row, std::current_exception());
      }
    });
  }
}

/// The per-row level Kernel
/// @tparam ToKind The cast target type
/// @tparam FromKind The expression type
/// @tparam TPolicy The policy used by the cast
/// @param row The index of the current row
/// @param input The input vector (of type FromKind)
/// @param result The output vector (of type ToKind)
template <TypeKind ToKind, TypeKind FromKind, typename TPolicy>
void CastExpr::applyCastKernel(
    vector_size_t row,
    EvalCtx& context,
    const SimpleVector<typename TypeTraits<FromKind>::NativeType>* input,
    FlatVector<typename TypeTraits<ToKind>::NativeType>* result) {
  bool wrapException = true;
  auto setError = [&](const std::string& details) INLINE_LAMBDA {
    if (setNullInResultAtError()) {
      result->setNull(row, true);
    } else {
      wrapException = false;
      if (context.captureErrorDetails()) {
        const auto errorDetails =
            makeErrorMessage(*input, row, result->type(), details);
        context.setStatus(row, Status::UserError("{}", errorDetails));
      } else {
        context.setStatus(row, Status::UserError());
      }
    }
  };

  // If castResult has an error, set the error in context. Otherwise, set the
  // value in castResult directly to result. This lambda should be called only
  // when ToKind is primitive and is not VARCHAR or VARBINARY.
  auto setResultOrError = [&](const auto& castResult, vector_size_t row)
                              INLINE_LAMBDA {
                                if (castResult.hasError()) {
                                  setError(castResult.error().message());
                                } else {
                                  result->set(row, castResult.value());
                                }
                              };

  try {
    auto inputRowValue = input->valueAt(row);

    // Optimize empty input strings casting by avoiding throwing exceptions.
    if constexpr (
        FromKind == TypeKind::VARCHAR || FromKind == TypeKind::VARBINARY) {
      if constexpr (
          TypeTraits<ToKind>::isPrimitiveType &&
          TypeTraits<ToKind>::isFixedWidth) {
        inputRowValue = hooks_->removeWhiteSpaces(inputRowValue);
        if (inputRowValue.size() == 0) {
          setError("Empty string");
          return;
        }
      }
      if constexpr (ToKind == TypeKind::TIMESTAMP) {
        const auto castResult = hooks_->castStringToTimestamp(inputRowValue);
        setResultOrError(castResult, row);
        return;
      }
      if constexpr (ToKind == TypeKind::REAL) {
        const auto castResult = hooks_->castStringToReal(inputRowValue);
        setResultOrError(castResult, row);
        return;
      }
      if constexpr (ToKind == TypeKind::DOUBLE) {
        const auto castResult = hooks_->castStringToDouble(inputRowValue);
        setResultOrError(castResult, row);
        return;
      }
    }

    const auto castResult =
        util::Converter<ToKind, void, TPolicy>::tryCast(inputRowValue);
    if (castResult.hasError()) {
      setError(castResult.error().message());
      return;
    }

    const auto output = castResult.value();

    if constexpr (
        ToKind == TypeKind::VARCHAR || ToKind == TypeKind::VARBINARY) {
      // Write the result output to the output vector
      auto writer = exec::StringWriter<>(result, row);
      writer.copy_from(output);
      writer.finalize();
    } else {
      result->set(row, output);
    }

  } catch (const VeloxException& ue) {
    if (!ue.isUserError() || !wrapException) {
      throw;
    }
    setError(ue.message());
  } catch (const std::exception& e) {
    setError(e.what());
  }
}

template <typename TInput, typename TOutput>
void CastExpr::applyDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        TOutput rescaledValue;
        const auto status = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
            sourceVector->valueAt(row),
            fromPrecisionScale.first,
            fromPrecisionScale.second,
            toPrecisionScale.first,
            toPrecisionScale.second,
            rescaledValue);
        if (status.ok()) {
          castResultRawBuffer[row] = rescaledValue;
        } else {
          if (setNullInResultAtError()) {
            castResult->setNull(row, true);
          } else {
            context.setVeloxExceptionError(
                row,
                std::make_exception_ptr(VeloxUserError(
                    std::current_exception(), status.message(), false)));
          }
        }
      });
}

template <typename TInput, typename TOutput>
void CastExpr::applyIntToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& castResult) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
  applyToSelectedNoThrowLocal(
      context, rows, castResult, [&](vector_size_t row) {
        auto rescaledValue = DecimalUtil::rescaleInt<TInput, TOutput>(
            sourceVector->valueAt(row),
            toPrecisionScale.first,
            toPrecisionScale.second);
        if (rescaledValue.has_value()) {
          castResultRawBuffer[row] = rescaledValue.value();
        } else {
          castResult->setNull(row, true);
        }
      });
}

template <typename TInput, typename TOutput>
void CastExpr::applyFloatingPointToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  const auto floatingInput = input.as<SimpleVector<TInput>>();
  auto rawResults =
      result->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    TOutput output;
    const auto status = DecimalUtil::rescaleFloatingPoint<TInput, TOutput>(
        floatingInput->valueAt(row),
        toPrecisionScale.first,
        toPrecisionScale.second,
        output);
    if (status.ok()) {
      rawResults[row] = output;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setVeloxExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename T>
void CastExpr::applyVarcharToDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& toType,
    VectorPtr& result) {
  auto sourceVector = input.as<SimpleVector<StringView>>();
  auto rawBuffer = result->asUnchecked<FlatVector<T>>()->mutableRawValues();
  const auto toPrecisionScale = getDecimalPrecisionScale(*toType);

  rows.applyToSelected([&](auto row) {
    T decimalValue;
    const auto status = detail::toDecimalValue<T>(
        hooks_->removeWhiteSpaces(sourceVector->valueAt(row)),
        toPrecisionScale.first,
        toPrecisionScale.second,
        decimalValue);
    if (status.ok()) {
      rawBuffer[row] = decimalValue;
    } else {
      if (setNullInResultAtError()) {
        result->setNull(row, true);
      } else {
        context.setVeloxExceptionError(
            row, makeBadCastException(toType, input, row, status.message()));
      }
    }
  });
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExpr::applyDecimalToFloatCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  const auto scaleFactor = DecimalUtil::kPowersOfTen[precisionScale.second];
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    const auto output =
        util::Converter<ToKind>::tryCast(simpleInput->valueAt(row))
            .thenOrThrow(folly::identity, [&](const Status& status) {
              VELOX_USER_FAIL("{}", status.message());
            });
    resultBuffer[row] = output / scaleFactor;
  });
  return result;
}

template <typename FromNativeType, TypeKind ToKind>
VectorPtr CastExpr::applyDecimalToIntegralCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  using To = typename TypeTraits<ToKind>::NativeType;

  VectorPtr result;
  context.ensureWritable(rows, toType, result);
  (*result).clearNulls(rows);
  auto resultBuffer = result->asUnchecked<FlatVector<To>>()->mutableRawValues();
  const auto precisionScale = getDecimalPrecisionScale(*fromType);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  const auto scaleFactor = DecimalUtil::kPowersOfTen[precisionScale.second];
  if (hooks_->truncate()) {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      resultBuffer[row] =
          static_cast<To>(simpleInput->valueAt(row) / scaleFactor);
    });
  } else {
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      auto value = simpleInput->valueAt(row);
      auto integralPart = value / scaleFactor;
      auto fractionPart = value % scaleFactor;
      auto sign = value >= 0 ? 1 : -1;
      bool needsRoundUp =
          (scaleFactor != 1) && (sign * fractionPart >= (scaleFactor >> 1));
      integralPart += needsRoundUp ? sign : 0;
      if (integralPart > std::numeric_limits<To>::max() ||
          integralPart < std::numeric_limits<To>::min()) {
        if (setNullInResultAtError()) {
          result->setNull(row, true);
        } else {
          context.setVeloxExceptionError(
              row,
              makeBadCastException(
                  result->type(),
                  input,
                  row,
                  makeErrorMessage(input, row, toType) + "Out of bounds."));
        }
        return;
      }

      resultBuffer[row] = static_cast<To>(integralPart);
    });
  }
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToBooleanCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context) {
  VectorPtr result;
  context.ensureWritable(rows, BOOLEAN(), result);
  (*result).clearNulls(rows);
  auto resultBuffer =
      result->asUnchecked<FlatVector<bool>>()->mutableRawValues<uint64_t>();
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
    auto value = simpleInput->valueAt(row);
    bits::setBit(resultBuffer, row, value != 0);
  });
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToVarcharCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType) {
  VectorPtr result;
  context.ensureWritable(rows, VARCHAR(), result);
  (*result).clearNulls(rows);
  const auto simpleInput = input.as<SimpleVector<FromNativeType>>();
  int precision = getDecimalPrecisionScale(*fromType).first;
  int scale = getDecimalPrecisionScale(*fromType).second;
  // A varchar's size is estimated with unscaled value digits, dot, leading
  // zero, and possible minus sign.
  int32_t rowSize = precision + 1;
  if (scale > 0) {
    ++rowSize; // A dot.
  }
  if (precision == scale) {
    ++rowSize; // Leading zero.
  }

  auto flatResult = result->asFlatVector<StringView>();
  if (StringView::isInline(rowSize)) {
    char inlined[StringView::kInlineSize];
    applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
      flatResult->setNoCopy(
          row,
          convertToStringView<FromNativeType>(
              simpleInput->valueAt(row), scale, rowSize, inlined));
    });
    return result;
  }

  Buffer* buffer =
      flatResult->getBufferWithSpace(rows.countSelected() * rowSize);
  char* rawBuffer = buffer->asMutable<char>() + buffer->size();

  applyToSelectedNoThrowLocal(context, rows, result, [&](vector_size_t row) {
    auto stringView = convertToStringView<FromNativeType>(
        simpleInput->valueAt(row), scale, rowSize, rawBuffer);
    flatResult->setNoCopy(row, stringView);
    if (!stringView.isInline()) {
      // If string view is inline, corresponding bytes on the raw string buffer
      // are not needed.
      rawBuffer += stringView.size();
    }
  });
  // Update the exact buffer size.
  buffer->setSize(rawBuffer - buffer->asMutable<char>());
  return result;
}

template <typename FromNativeType>
VectorPtr CastExpr::applyDecimalToPrimitiveCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  switch (toType->kind()) {
    case TypeKind::BOOLEAN:
      return applyDecimalToBooleanCast<FromNativeType>(rows, input, context);
    case TypeKind::TINYINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::TINYINT>(
          rows, input, context, fromType, toType);
    case TypeKind::SMALLINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::SMALLINT>(
          rows, input, context, fromType, toType);
    case TypeKind::INTEGER:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::INTEGER>(
          rows, input, context, fromType, toType);
    case TypeKind::BIGINT:
      return applyDecimalToIntegralCast<FromNativeType, TypeKind::BIGINT>(
          rows, input, context, fromType, toType);
    case TypeKind::REAL:
      return applyDecimalToFloatCast<FromNativeType, TypeKind::REAL>(
          rows, input, context, fromType, toType);
    case TypeKind::DOUBLE:
      return applyDecimalToFloatCast<FromNativeType, TypeKind::DOUBLE>(
          rows, input, context, fromType, toType);
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          fromType->toString(),
          toType->toString());
  }
}

template <TypeKind ToKind, TypeKind FromKind>
void CastExpr::applyCastPrimitives(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  using To = typename TypeTraits<ToKind>::NativeType;
  using From = typename TypeTraits<FromKind>::NativeType;
  auto* resultFlatVector = result->as<FlatVector<To>>();
  auto* inputSimpleVector = input.as<SimpleVector<From>>();

  switch (hooks_->getPolicy()) {
    case LegacyCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::LegacyCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;
    case PrestoCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::PrestoCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;
    case SparkCastPolicy:
      applyToSelectedNoThrowLocal(context, rows, result, [&](int row) {
        applyCastKernel<ToKind, FromKind, util::SparkCastPolicy>(
            row, context, inputSimpleVector, resultFlatVector);
      });
      break;

    default:
      VELOX_NYI("Policy {} not yet implemented.", hooks_->getPolicy());
  }
}

template <TypeKind ToKind>
void CastExpr::applyCastPrimitivesDispatch(
    const TypePtr& fromType,
    const TypePtr& toType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  context.ensureWritable(rows, toType, result);

  // This already excludes complex types, hugeint and unknown from type kinds.
  VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
      applyCastPrimitives,
      ToKind,
      fromType->kind() /*dispatched*/,
      rows,
      context,
      input,
      result);
}

} // namespace facebook::velox::exec
