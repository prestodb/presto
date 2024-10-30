#include "presto_cpp/external/magen-cpp/include/DLPMasking.h"
#include "velox/expression/CastExpr.h"
#include "velox/expression/CastExpr-inl.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/prestosql/DateTimeFunctions.h"
#include "velox/type/Type.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::presto::governance {

using namespace facebook::velox;

namespace {
const MaskType getMaskType(int8_t inputMask) {
  auto maskType(REDACT);
  auto inputMaskType = static_cast<MaskType>(inputMask);
  if (inputMaskType >= REDACT && inputMaskType <= NUMERIC_SHIFT) {
    maskType = inputMaskType;
  }
  return maskType;
}
}; // namespace

template <typename TExecParams>
struct MaskDate {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);
  TimestampToStringOptions options;
  int32_t maxDateLength;
  std::unique_ptr<char[]> outData;

  MaskDate() {
    options.mode = TimestampToStringOptions::Mode::kDateOnly;
    maxDateLength = getMaxStringLength(options);
    outData = std::make_unique<char[]>(maxDateLength + 1);
  }

  std::string performMaskDate(
      const char* date,
      const int32_t inputMask,
      const char* maskParams,
      const char* format,
      const char* seed,
      const int32_t outLength,
      const int32_t nullIndicator = 0) {
    auto maskType = getMaskType(inputMask);
    // Ignore the length passed in outLength if it exceeds a useful string
    // length.
    const uint32_t bufSize = std::min(maxDateLength, outLength) + 1;
    short returnValue = maskDate(
        maskType,
        maskParams,
        format,
        seed,
        date,
        bufSize - 1,
        outData.get(),
        &nullIndicator);
    return std::string(outData.get());
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<Date>& result,
      const arg_type<Date>& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    std::string dateStr = DATE()->toString(input);
    std::string outData = performMaskDate(
        dateStr.c_str(),
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
    result = DATE()->toDays(outData.c_str());
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Date>& result,
      const arg_type<Date>* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      std::string outData = performMaskDate(
          "",
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
      result = DATE()->toDays(outData.c_str());
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<Date>& result,
      const arg_type<Varchar>& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    std::string dateStr = input.getString();
    std::string outData = performMaskDate(
        dateStr.c_str(),
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
    result = DATE()->toDays(outData.c_str());
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Date>& result,
      const arg_type<Varchar>* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      std::string outData = performMaskDate(
          "",
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
      result = DATE()->toDays(outData.c_str());
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }
};

template <typename TExecParams>
struct MaskTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);
  TimestampToStringOptions options;
  int32_t maxTimestampLength;
  std::unique_ptr<char[]> outData;

  MaskTimestamp() {
    TimestampToStringOptions options;
    options.mode = TimestampToStringOptions::Mode::kFull;
    maxTimestampLength = getMaxStringLength(options);
    outData = std::make_unique<char[]>(maxTimestampLength + 1);
  }

  out_type<Timestamp> parseTimestamp(StringView timestamp) {
    return util::fromTimestampString(
               timestamp, util::TimestampParseMode::kPrestoCast)
        .thenOrThrow(folly::identity, [&](const Status& status) {
          VELOX_USER_FAIL("{}", status.message());
        });
  }

  std::string performMaskTimestamp(
      const char* timestamp,
      const int32_t inputMask,
      const char* maskParams,
      const char* format,
      const char* seed,
      const int32_t outLength,
      const int32_t nullIndicator = 0) {
    auto maskType = getMaskType(inputMask);
    // Ignore the length passed in outLength if it exceeds a useful string
    // length.
    const uint32_t bufSize = std::min(maxTimestampLength, outLength) + 1;
    short returnValue = maskDatetime(
        maskType,
        maskParams,
        format,
        seed,
        timestamp,
        bufSize - 1,
        outData.get(),
        &nullIndicator);
    return std::string(outData.get());
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    auto timestampStr = input.toString();
    std::string outData = performMaskTimestamp(
        timestampStr.c_str(),
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
    result = parseTimestamp(outData.c_str());
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      std::string outData = performMaskTimestamp(
          "",
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
      result = parseTimestamp(outData.c_str());
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<Timestamp>& result,
      const arg_type<Varchar>& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    auto timestampStr = input.getString();
    std::string outData = performMaskTimestamp(
        timestampStr.c_str(),
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
    result = parseTimestamp(outData.c_str());
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Timestamp>& result,
      const arg_type<Varchar>* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      std::string outData = performMaskTimestamp(
          "",
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
      result = parseTimestamp(outData.c_str());
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }
};

template <typename TExecParams>
struct MaskDecimal {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  template <typename T>
  FOLLY_ALWAYS_INLINE void call(
      T& out,
      const T& in,
      const int32_t& maskType,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& formatName,
      const arg_type<Varchar>& seed,
      const int32_t& precision,
      const int32_t& scale,
      const arg_type<Varchar>& dbType) {
    auto maskTypeValue = static_cast<MaskType>(maskType);
    auto decimalInput = DecimalUtil::toString(in, DECIMAL(precision, scale));
    auto dbTypeValue = ("DB2" == dbType) ? DB2 : OTHERS;
    auto nullIndicator = -1;
    char outData[kMaxOutputLength_] = {};
    auto maskedResult = maskNumerics(
        maskTypeValue,
        maskParams.data(),
        formatName.data(),
        seed.data(),
        decimalInput.c_str(),
        &precision,
        &scale,
        kMaxOutputLength_,
        outData,
        &nullIndicator,
        dbTypeValue);
    if (maskedResult != 0) {
      VELOX_USER_FAIL("Failed to mask decimal: {}", decimalInput);
    } else {
      T unscaledValue;
      auto castResult = exec::detail::toDecimalValue<T>(
          outData, precision, scale, unscaledValue);
      VELOX_USER_CHECK_EQ(castResult, Status::OK(), "Cast to decimal failed");
      out = unscaledValue;
    }
  }

 private:
  // Max value of outDataLen is maxDecimalPrecision + 3 (considering possible
  // characters, '.', '-', and '\0' in the output char buffer). It is set to 48
  // to ensure there is no overflow in output buffer.
  static constexpr int64_t kMaxOutputLength_ = 48;
};

template <typename TExecParams>
struct Mask {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  template <typename T>
  T performMask(
      const T& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength,
      const int32_t nullIndicator = 0) {
    auto maskType = getMaskType(inputMask);
    return mask(
        maskType,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        input,
        &nullIndicator);
  }

  // Specialization for int64_t to be cast to long long and call the correct
  // masking function.
  int64_t performMask(
      const int64_t& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength,
      const int32_t nullIndicator = 0) {
    auto maskType = getMaskType(inputMask);
    return mask(
        maskType,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        static_cast<long long>(input),
        &nullIndicator);
  }

  std::string performMaskVarchar(
      const char* input,
      const int32_t inputMask,
      const char* maskParams,
      const char* format,
      const char* seed,
      const int32_t outLength,
      const int32_t nullIndicator = 0) {
    auto maskType = getMaskType(inputMask);
    // The masking output is either the same length as input,
    // a subset of input, a sha256, or the maskParams string depending on the
    // inputMask. It is not completely arbitrary based on the outLength. So
    // determine the maximum length needed to handle the result without
    // truncation.
    size_t bufSize = strlen(input);
    if (inputMask == REDACT) {
      // This takes the paramMask length as it can redact with a fixed value.
      bufSize = strlen(maskParams);
    } else if (inputMask == SUBSTITUTE) {
      // SHA256 uses 44 bytes base64 encoding (256 bits / 6 bits).
      static const int32_t sha256CharLength = 44;
      bufSize = sha256CharLength;
    }
    // The outLength, if smaller than the determined required length, can
    // override the actual output length and cause truncation.
    bufSize = std::min(bufSize, static_cast<size_t>(outLength)) + 1;
    auto outData = std::make_unique<char[]>(bufSize);
    mask(
        maskType,
        maskParams,
        format,
        seed,
        input,
        bufSize - 1,
        outData.get(),
        &nullIndicator);
    return std::string(outData.get());
  }

  template <typename T>
  FOLLY_ALWAYS_INLINE void callNullFree(
      T& result,
      const T& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    result = performMask(
        input,
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
  }

  template <typename T>
  FOLLY_ALWAYS_INLINE void callNullable(
      T& result,
      const T* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      result = performMask(
          0,
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }

  FOLLY_ALWAYS_INLINE void callNullFree(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      const int32_t inputMask,
      const arg_type<Varchar>& maskParams,
      const arg_type<Varchar>& format,
      const arg_type<Varchar>& seed,
      const int32_t outLength) {
    result = performMaskVarchar(
        input.getString().c_str(),
        inputMask,
        maskParams.getString().c_str(),
        format.getString().c_str(),
        seed.getString().c_str(),
        outLength);
  }

  FOLLY_ALWAYS_INLINE void callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* input,
      const int32_t* inputMask,
      const arg_type<Varchar>* maskParams,
      const arg_type<Varchar>* format,
      const arg_type<Varchar>* seed,
      const int32_t* outLength) {
    int32_t nullIndicator = (input) ? 0 : -1;
    if (!input) {
      result = performMaskVarchar(
          "",
          *inputMask,
          maskParams->getString().c_str(),
          format->getString().c_str(),
          seed->getString().c_str(),
          *outLength,
          nullIndicator);
    } else {
      callNullFree(
          result, *input, *inputMask, *maskParams, *format, *seed, *outLength);
    }
  }
};

} // namespace facebook::presto::governance
