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
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::functions {
namespace {

template <typename TExec>
struct DecimalPlusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    auto aType = inputTypes[0];
    auto bType = inputTypes[1];
    auto aScale = getDecimalPrecisionScale(*aType).second;
    auto bScale = getDecimalPrecisionScale(*bType).second;
    aRescale_ = computeRescaleFactor(aScale, bScale);
    bRescale_ = computeRescaleFactor(bScale, aScale);
  }

  template <typename R, typename A, typename B>
  void call(R& out, const A& a, const B& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a, DecimalUtil::kPowersOfTen[aRescale_], &aRescaled) ||
        __builtin_mul_overflow(
            b, DecimalUtil::kPowersOfTen[bRescale_], &bRescaled)) {
      VELOX_ARITHMETIC_ERROR("Decimal overflow: {} + {}", a, b);
    }
    out = checkedPlus<R>(R(aRescaled), R(bRescaled));
    DecimalUtil::valueInRange(out);
  }

 private:
  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t toScale) {
    return std::max(0, toScale - fromScale);
  }

  uint8_t aRescale_;
  uint8_t bRescale_;
};

template <typename TExec>
struct DecimalMinusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    const auto& aType = inputTypes[0];
    const auto& bType = inputTypes[1];
    auto aScale = getDecimalPrecisionScale(*aType).second;
    auto bScale = getDecimalPrecisionScale(*bType).second;
    aRescale_ = computeRescaleFactor(aScale, bScale);
    bRescale_ = computeRescaleFactor(bScale, aScale);
  }

  template <typename R, typename A, typename B>
  void call(R& out, const A& a, const B& b)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a, DecimalUtil::kPowersOfTen[aRescale_], &aRescaled) ||
        __builtin_mul_overflow(
            b, DecimalUtil::kPowersOfTen[bRescale_], &bRescaled)) {
      VELOX_ARITHMETIC_ERROR("Decimal overflow: {} - {}", a, b);
    }
    out = checkedMinus<R>(R(aRescaled), R(bRescaled));
    DecimalUtil::valueInRange(out);
  }

 private:
  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t toScale) {
    return std::max(0, toScale - fromScale);
  }

  uint8_t aRescale_;
  uint8_t bRescale_;
};

template <typename TExec>
struct DecimalMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename R, typename A, typename B>
  void call(R& out, const A& a, const B& b) {
    out = checkedMultiply<R>(checkedMultiply<R>(R(a), R(b)), R(1));
    DecimalUtil::valueInRange(out);
  }
};

template <typename TExec>
struct DecimalDivideFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    auto aType = inputTypes[0];
    auto bType = inputTypes[1];
    auto aScale = getDecimalPrecisionScale(*aType).second;
    auto bScale = getDecimalPrecisionScale(*bType).second;
    auto rScale = std::max(aScale, bScale);
    aRescale_ = rScale - aScale + bScale;
    VELOX_USER_CHECK_LE(
        aRescale_, LongDecimalType::kMaxPrecision, "Decimal overflow");
  }

  template <typename R, typename A, typename B>
  void call(R& out, const A& a, const B& b) {
    DecimalUtil::divideWithRoundUp<R, A, B>(out, a, b, false, aRescale_, 0);
    DecimalUtil::valueInRange(out);
  }

 private:
  uint8_t aRescale_;
};

template <typename TExec>
struct DecimalRoundFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      A* a) {
    initialize(inputTypes, config, a, nullptr);
  }

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      const int32_t* /*n*/) {
    const auto [precision, scale] = getDecimalPrecisionScale(*inputTypes[0]);
    precision_ = precision;
    scale_ = scale;
  }

  template <typename R, typename A>
  void call(R& out, const A& a) {
    DecimalUtil::divideWithRoundUp<R, A, int128_t>(
        out, a, DecimalUtil::kPowersOfTen[scale_], false, 0, 0);
  }

  template <typename R, typename A>
  void call(R& out, const A& a, int32_t n) {
    if (a == 0 || precision_ - scale_ + n <= 0) {
      out = 0;
      return;
    }
    if (n >= scale_) {
      out = a;
      return;
    }
    auto reScaleFactor = DecimalUtil::kPowersOfTen[scale_ - n];
    DecimalUtil::divideWithRoundUp<R, A, int128_t>(
        out, a, reScaleFactor, false, 0, 0);
    out *= reScaleFactor;
  }

 private:
  uint8_t precision_;
  uint8_t scale_;
};

template <typename TExec>
struct DecimalFloorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/) {
    scale_ = getDecimalPrecisionScale(*inputTypes[0]).second;
  }

  template <typename R, typename A>
  void call(R& out, const A& a) {
    const auto rescaleFactor = DecimalUtil::kPowersOfTen[scale_];
    // Function interpretation is the same as ceil for negative numbers, and as
    // floor for positive numbers.
    const auto increment = (a % rescaleFactor) < 0 ? -1 : 0;
    out = a / rescaleFactor + increment;
  }

 private:
  uint8_t scale_;
};

template <template <class> typename Func>
void registerDecimalBinary(
    const std::string& name,
    const std::vector<exec::SignatureVariable>& constraints) {
  // (long, long) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      LongDecimal<P2, S2>>({name}, constraints);

  // (short, short) -> short
  registerFunction<
      Func,
      ShortDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);

  // (short, short) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);

  // (short, long) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      LongDecimal<P2, S2>>({name}, constraints);

  // (long, short) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);
}

template <template <class> typename Func>
void registerDecimalPlusMinus(const std::string& name) {
  std::vector<exec::SignatureVariable> constraints = {
      exec::SignatureVariable(
          P3::name(),
          fmt::format(
              "min(38, max({a_precision} - {a_scale}, {b_precision} - {b_scale}) + max({a_scale}, {b_scale}) + 1)",
              fmt::arg("a_precision", P1::name()),
              fmt::arg("b_precision", P2::name()),
              fmt::arg("a_scale", S1::name()),
              fmt::arg("b_scale", S2::name())),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S3::name(),
          fmt::format(
              "max({a_scale}, {b_scale})",
              fmt::arg("a_scale", S1::name()),
              fmt::arg("b_scale", S2::name())),
          exec::ParameterType::kIntegerParameter),
  };

  registerDecimalBinary<Func>(name, constraints);
}

} // namespace

void registerDecimalPlus(const std::string& prefix) {
  registerDecimalPlusMinus<DecimalPlusFunction>(prefix + "plus");
}

void registerDecimalMinus(const std::string& prefix) {
  registerDecimalPlusMinus<DecimalMinusFunction>(prefix + "minus");
}

void registerDecimalMultiply(const std::string& prefix) {
  std::vector<exec::SignatureVariable> constraints = {
      exec::SignatureVariable(
          P3::name(),
          fmt::format(
              "min(38, {a_precision} + {b_precision})",
              fmt::arg("a_precision", P1::name()),
              fmt::arg("b_precision", P2::name())),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S3::name(),
          // Result type resolution fails if sum of input scales exceeds 38.
          fmt::format(
              "{a_scale} + {b_scale}",
              fmt::arg("a_scale", S1::name()),
              fmt::arg("b_scale", S2::name())),
          exec::ParameterType::kIntegerParameter),
  };

  registerDecimalBinary<DecimalMultiplyFunction>(
      prefix + "multiply", constraints);
}

void registerDecimalDivide(const std::string& prefix) {
  std::vector<exec::SignatureVariable> constraints = {
      exec::SignatureVariable(
          P3::name(),
          fmt::format(
              "min(38, {a_precision} + {b_scale} + max(0, {b_scale} - {a_scale}))",
              fmt::arg("a_precision", P1::name()),
              fmt::arg("a_scale", S1::name()),
              fmt::arg("b_scale", S2::name())),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S3::name(),
          fmt::format(
              "max({a_scale}, {b_scale})",
              fmt::arg("a_scale", S1::name()),
              fmt::arg("b_scale", S2::name())),
          exec::ParameterType::kIntegerParameter),
  };

  registerDecimalBinary<DecimalDivideFunction>(prefix + "divide", constraints);

  // (short, long) -> short
  registerFunction<
      DecimalDivideFunction,
      ShortDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      LongDecimal<P2, S2>>({prefix + "divide"}, constraints);

  // (long, short) -> short
  registerFunction<
      DecimalDivideFunction,
      ShortDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({prefix + "divide"}, constraints);
}

void registerDecimalFloor(const std::string& prefix) {
  std::vector<exec::SignatureVariable> constraints = {
      exec::SignatureVariable(
          P2::name(),
          fmt::format(
              "min(38, {p} - {s} + min({s}, 1))",
              fmt::arg("p", P1::name()),
              fmt::arg("s", S1::name())),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S2::name(), "0", exec::ParameterType::kIntegerParameter),
  };

  registerFunction<
      DecimalFloorFunction,
      LongDecimal<P2, S2>,
      LongDecimal<P1, S1>>({prefix + "floor"}, constraints);

  registerFunction<
      DecimalFloorFunction,
      ShortDecimal<P2, S2>,
      LongDecimal<P1, S1>>({prefix + "floor"}, constraints);

  registerFunction<
      DecimalFloorFunction,
      ShortDecimal<P2, S2>,
      ShortDecimal<P1, S1>>({prefix + "floor"}, constraints);
}

void registerDecimalRound(const std::string& prefix) {
  // round(decimal) -> decimal
  {
    std::vector<exec::SignatureVariable> constraints = {
        exec::SignatureVariable(
            P2::name(),
            fmt::format(
                "min(38, {p} - {s} + min({s}, 1))",
                fmt::arg("p", P1::name()),
                fmt::arg("s", S1::name())),
            exec::ParameterType::kIntegerParameter),
        exec::SignatureVariable(
            S2::name(), "0", exec::ParameterType::kIntegerParameter),
    };

    registerFunction<
        DecimalRoundFunction,
        LongDecimal<P2, S2>,
        LongDecimal<P1, S1>>({prefix + "round"}, constraints);

    registerFunction<
        DecimalRoundFunction,
        ShortDecimal<P2, S2>,
        LongDecimal<P1, S1>>({prefix + "round"}, constraints);

    registerFunction<
        DecimalRoundFunction,
        ShortDecimal<P2, S2>,
        ShortDecimal<P1, S1>>({prefix + "round"}, constraints);
  }

  // round(decimal, n) -> decimal
  {
    std::vector<exec::SignatureVariable> constraints = {
        exec::SignatureVariable(
            P2::name(),
            fmt::format("min(38, {p} + 1)", fmt::arg("p", P1::name())),
            exec::ParameterType::kIntegerParameter),
    };

    registerFunction<
        DecimalRoundFunction,
        LongDecimal<P2, S1>,
        LongDecimal<P1, S1>,
        int32_t>({prefix + "round"}, constraints);

    registerFunction<
        DecimalRoundFunction,
        ShortDecimal<P2, S1>,
        ShortDecimal<P1, S1>,
        int32_t>({prefix + "round"}, constraints);

    registerFunction<
        DecimalRoundFunction,
        LongDecimal<P2, S1>,
        ShortDecimal<P1, S1>,
        int32_t>({prefix + "round"}, constraints);
  }
}

} // namespace facebook::velox::functions
