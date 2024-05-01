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

#include <limits>
#include "velox/buffer/Buffer.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"

using namespace facebook::velox;
namespace facebook::velox::test {
namespace {
template <typename TExecParams>
struct ErrorOnOddFunctionElseUnknown {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  FOLLY_ALWAYS_INLINE void call(
      out_type<UnknownValue> /*out*/,
      const int32_t input) {
    // Function will always throw an error on odd input.
    if (input % 2 != 0) {
      VELOX_USER_FAIL("ErrorOnOddElseUnknown Function: {}", input);
    }
  }
};

class CastExprTest : public functions::test::CastBaseTest {
 protected:
  CastExprTest() {
    registerFunction<ErrorOnOddFunctionElseUnknown, UnknownValue, int32_t>(
        {"error_on_odd_else_unknown"});
  }

  void setLegacyCast(bool value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kLegacyCast, std::to_string(value)},
    });
  }

  void setCastMatchStructByName(bool value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kCastMatchStructByName, std::to_string(value)},
    });
  }

  void setTimezone(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, value},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  std::shared_ptr<core::ConstantTypedExpr> makeConstantNullExpr(TypeKind kind) {
    return std::make_shared<core::ConstantTypedExpr>(
        createType(kind, {}), variant(kind));
  }

  template <typename T>
  void testDecimalToFloatCasts() {
    // short to short, scale up.
    auto shortFlat = makeNullableFlatVector<int64_t>(
        {DecimalUtil::kShortDecimalMin,
         DecimalUtil::kShortDecimalMin,
         -3,
         0,
         55,
         DecimalUtil::kShortDecimalMax,
         DecimalUtil::kShortDecimalMax,
         std::nullopt},
        DECIMAL(18, 18));
    testCast(
        shortFlat,
        makeNullableFlatVector<T>(
            {-1,
             // the same DecimalUtil::kShortDecimalMin conversion, checking
             // floating point diff works on decimals
             -0.999999999999999999,
             -0.000000000000000003,
             0,
             0.000000000000000055,
             // the same DecimalUtil::kShortDecimalMax conversion, checking
             // floating point diff works on decimals
             0.999999999999999999,
             1,
             std::nullopt}));

    auto longFlat = makeNullableFlatVector<int128_t>(
        {DecimalUtil::kLongDecimalMin,
         0,
         DecimalUtil::kLongDecimalMax,
         HugeInt::build(0xffff, 0xffffffffffffffff),
         std::nullopt},
        DECIMAL(38, 5));
    testCast(
        longFlat,
        makeNullableFlatVector<T>(
            {-1e33, 0, 1e33, 1.2089258196146293E19, std::nullopt}));
  }

  template <TypeKind KIND>
  void testDecimalToIntegralCastsOutOfBounds() {
    using NativeType = typename TypeTraits<KIND>::NativeType;
    VELOX_CHECK(!(std::is_same<int64_t, NativeType>::value));
    const auto tooSmall =
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1;
    const auto tooBig =
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<int64_t>({0, tooSmall}, DECIMAL(10, 0)),
            makeFlatVector<NativeType>(0, 0)),
        fmt::format(
            "Cannot cast DECIMAL(10, 0) '-2147483649' to {}. Out of bounds.",
            TypeTraits<KIND>::name));

    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<int128_t>({0, tooSmall}, DECIMAL(19, 0)),
            makeFlatVector<NativeType>(0, 0)),
        fmt::format(
            "Cannot cast DECIMAL(19, 0) '-2147483649' to {}. Out of bounds.",
            TypeTraits<KIND>::name));

    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<int64_t>({0, tooBig}, DECIMAL(10, 0)),
            makeFlatVector<NativeType>(0, 0)),
        fmt::format(
            "Cannot cast DECIMAL(10, 0) '2147483648' to {}. Out of bounds.",
            TypeTraits<KIND>::name));

    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<int128_t>({0, tooBig}, DECIMAL(19, 0)),
            makeFlatVector<NativeType>(0, 0)),
        fmt::format(
            "Cannot cast DECIMAL(19, 0) '2147483648' to {}. Out of bounds.",
            TypeTraits<KIND>::name));
  }

  template <TypeKind KIND>
  void testDecimalToIntegralCastsOutOfBoundsSetNullOnFailure() {
    using NativeType = typename TypeTraits<KIND>::NativeType;
    VELOX_CHECK(!(std::is_same<int64_t, NativeType>::value));
    const auto tooSmall =
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1;
    const auto tooBig =
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

    testCast(
        makeNullableFlatVector<int64_t>(
            {0, tooSmall, 0, tooBig, 0, std::nullopt, 0}, DECIMAL(10, 0)),
        makeNullableFlatVector<NativeType>(
            {0, std::nullopt, 0, std::nullopt, 0, std::nullopt, 0}),
        true);

    testCast(
        makeNullableFlatVector<int128_t>(
            {0, tooSmall, 0, tooBig, 0, std::nullopt, 0}, DECIMAL(19, 0)),
        makeNullableFlatVector<NativeType>(
            {0, std::nullopt, 0, std::nullopt, 0, std::nullopt, 0}),
        true);
  }

  template <typename T>
  void testDecimalToIntegralCasts() {
    auto shortFlat = makeNullableFlatVector<int64_t>(
        {-300,
         -260,
         -230,
         -200,
         -100,
         0,
         5500,
         5749,
         5755,
         6900,
         7200,
         std::nullopt},
        DECIMAL(6, 2));
    testCast(
        shortFlat,
        makeNullableFlatVector<T>(
            {-3,
             -3 /*-2.6 rounds to -3*/,
             -2 /*-2.3 rounds to -2*/,
             -2,
             -1,
             0,
             55,
             57 /*57.49 rounds to 57*/,
             58 /*57.55 rounds to 58*/,
             69,
             72,
             std::nullopt}));
    auto longFlat = makeNullableFlatVector<int128_t>(
        {-30'000'000'000,
         -25'500'000'000,
         -24'500'000'000,
         -20'000'000'000,
         -10'000'000'000,
         0,
         550'000'000'000,
         554'900'000'000,
         559'900'000'000,
         690'000'000'000,
         720'000'000'000,
         std::nullopt},
        DECIMAL(20, 10));
    testCast(
        longFlat,
        makeNullableFlatVector<T>(
            {-3,
             -3 /*-2.55 rounds to -3*/,
             -2 /*-2.45 rounds to -2*/,
             -2,
             -1,
             0,
             55,
             55 /* 55.49 rounds to 55*/,
             56 /* 55.99 rounds to 56*/,
             69,
             72,
             std::nullopt}));
  }

  template <typename T>
  void testIntToDecimalCasts() {
    // integer to short decimal
    auto input = makeFlatVector<T>({-3, -2, -1, 0, 55, 69, 72});
    testCast(
        input,
        makeFlatVector<int64_t>(
            {-300, -200, -100, 0, 5'500, 6'900, 7'200}, DECIMAL(6, 2)));

    // integer to long decimal
    testCast(
        input,
        makeFlatVector<int128_t>(
            {-30'000'000'000,
             -20'000'000'000,
             -10'000'000'000,
             0,
             550'000'000'000,
             690'000'000'000,
             720'000'000'000},
            DECIMAL(20, 10)));

    // Expected failures: allowed # of integers (precision - scale) in the
    // target
    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<T>(std::vector<T>{std::numeric_limits<T>::min()}),
            makeFlatVector(std::vector<int64_t>{0}, DECIMAL(3, 1))),
        fmt::format(
            "Cannot cast {} '{}' to DECIMAL(3, 1)",
            CppToType<T>::name,
            std::to_string(std::numeric_limits<T>::min())));
    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<T>(std::vector<T>{-100}),
            makeFlatVector(std::vector<int64_t>{0}, DECIMAL(17, 16))),
        fmt::format(
            "Cannot cast {} '-100' to DECIMAL(17, 16)", CppToType<T>::name));
    VELOX_ASSERT_THROW(
        testCast(
            makeFlatVector<T>(std::vector<T>{100}),
            makeFlatVector(std::vector<int64_t>{0}, DECIMAL(17, 16))),
        fmt::format(
            "Cannot cast {} '100' to DECIMAL(17, 16)", CppToType<T>::name));
  }

  std::string zeros(uint32_t numZeros) {
    return std::string(numZeros, '0');
  }
};

TEST_F(CastExprTest, basics) {
  testCast<int32_t, double>(
      "double", {1, 2, 3, 100, -100}, {1.0, 2.0, 3.0, 100.0, -100.0});
  testCast<int32_t, std::string>(
      "string", {1, 2, 3, 100, -100}, {"1", "2", "3", "100", "-100"});
  testCast<std::string, int8_t>(
      "tinyint", {"1", "2", "3", "100", "-100"}, {1, 2, 3, 100, -100});
  testCast<double, int>(
      "int",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {2, 3, 4, 100, -100, 1, -2});
  testCast<double, double>(
      "double",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0});
  testCast<double, std::string>(
      "string",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {"1.888", "2.5", "3.6", "100.44", "-100.101", "1.0", "-2.0"});
  testCast<double, double>(
      "double",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0});
  testCast<double, float>(
      "float",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0});
  testCast<bool, std::string>("string", {true, false}, {"true", "false"});
  testCast<float, bool>(
      "boolean",
      {0.0,
       1.0,
       1.1,
       -1.1,
       0.5,
       -0.5,
       std::numeric_limits<float>::quiet_NaN(),
       std::numeric_limits<float>::infinity(),
       0.0000000000001},
      {false, true, true, true, true, true, true, true, true});
  testCast<std::string, float>(
      "float",
      {"1.888",
       "1.",
       "1",
       "1.7E308",
       "Infinity",
       "-Infinity",
       "infinity",
       "inf",
       "INFINITY",
       "NaN",
       "nan"},
      {1.888,
       1.0,
       1.0,
       std::numeric_limits<float>::infinity(),
       std::numeric_limits<float>::infinity(),
       -std::numeric_limits<float>::infinity(),
       std::numeric_limits<float>::infinity(),
       std::numeric_limits<float>::infinity(),
       std::numeric_limits<float>::infinity(),
       std::numeric_limits<float>::quiet_NaN(),
       std::numeric_limits<float>::quiet_NaN()});

  gflags::FlagSaver flagSaver;
  FLAGS_experimental_enable_legacy_cast = true;
  testCast<double, std::string>(
      "string",
      {1.888, 2.5, 3.6, 100.44, -100.101, 1.0, -2.0},
      {"1.888", "2.5", "3.6", "100.44", "-100.101", "1", "-2"});
}

TEST_F(CastExprTest, fromUnknownType) {
  testCast<UnknownValue, int8_t>(
      "tinyint", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, int16_t>(
      "smallint", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, int32_t>(
      "int", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, int64_t>(
      "bigint", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, float>(
      "float", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, double>(
      "double", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, std::string>(
      "string", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, bool>(
      "boolean", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, Timestamp>(
      "timestamp", {std::nullopt, std::nullopt}, {std::nullopt, std::nullopt});
  testCast<UnknownValue, int32_t>(
      "date",
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt},
      UNKNOWN(),
      DATE());
}

TEST_F(CastExprTest, realAndDoubleToString) {
  setLegacyCast(false);
  testCast<double, std::string>(
      "string",
      {
          12345678901234567000.0,
          123456789.01234567,
          10'000'000.0,
          12345.0,
          0.001,
          0.00012,
          0.0,
          -0.0,
          -0.00012,
          -0.001,
          -12345.0,
          -10'000'000.0,
          -123456789.01234567,
          -12345678901234567000.0,
          std::numeric_limits<double>::infinity(),
          -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN(),
          -std::numeric_limits<double>::quiet_NaN(),
      },
      {
          "1.2345678901234567E19",
          "1.2345678901234567E8",
          "1.0E7",
          "12345.0",
          "0.001",
          "1.2E-4",
          "0.0",
          "-0.0",
          "-1.2E-4",
          "-0.001",
          "-12345.0",
          "-1.0E7",
          "-1.2345678901234567E8",
          "-1.2345678901234567E19",
          "Infinity",
          "-Infinity",
          "NaN",
          "NaN",
      });
  testCast<float, std::string>(
      "string",
      {
          12345678000000000000.0,
          123456780.0,
          10'000'000.0,
          12345.0,
          0.001,
          0.00012,
          0.0,
          -0.0,
          -0.00012,
          -0.001,
          -12345.0,
          -10'000'000.0,
          -123456780.0,
          -12345678000000000000.0,
          std::numeric_limits<float>::infinity(),
          -std::numeric_limits<float>::infinity(),
          std::numeric_limits<float>::quiet_NaN(),
          -std::numeric_limits<float>::quiet_NaN(),
      },
      {
          "1.2345678E19",
          "1.2345678E8",
          "1.0E7",
          "12345.0",
          "0.001",
          "1.2E-4",
          "0.0",
          "-0.0",
          "-1.2E-4",
          "-0.001",
          "-12345.0",
          "-1.0E7",
          "-1.2345678E8",
          "-1.2345678E19",
          "Infinity",
          "-Infinity",
          "NaN",
          "NaN",
      });

  setLegacyCast(true);
  testCast<double, std::string>(
      "string",
      {
          12345678901234567000.0,
          123456789.01234567,
          10'000'000.0,
          12345.0,
          0.001,
          0.00012,
          0.0,
          -0.0,
          -0.00012,
          -0.001,
          -12345.0,
          -10'000'000.0,
          -123456789.01234567,
          -12345678901234567000.0,
          std::numeric_limits<double>::infinity(),
          -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN(),
          -std::numeric_limits<double>::quiet_NaN(),
      },
      {
          "12345678901234567000.0",
          "123456789.01234567",
          "10000000.0",
          "12345.0",
          "0.001",
          "0.00012",
          "0.0",
          "-0.0",
          "-0.00012",
          "-0.001",
          "-12345.0",
          "-10000000.0",
          "-123456789.01234567",
          "-12345678901234567000.0",
          "Infinity",
          "-Infinity",
          "NaN",
          "NaN",
      });
  testCast<float, std::string>(
      "string",
      {
          12345678000000000000.0,
          123456780.0,
          10'000'000.0,
          12345.0,
          0.001,
          0.00012,
          0.0,
          -0.0,
          -0.00012,
          -0.001,
          -12345.0,
          -10'000'000.0,
          -123456780.0,
          -12345678000000000000.0,
          std::numeric_limits<float>::infinity(),
          -std::numeric_limits<float>::infinity(),
          std::numeric_limits<float>::quiet_NaN(),
          -std::numeric_limits<float>::quiet_NaN(),
      },
      {
          "12345678295994466000.0",
          "123456784.0",
          "10000000.0",
          "12345.0",
          "0.0010000000474974513",
          "0.00011999999696854502",
          "0.0",
          "-0.0",
          "-0.00011999999696854502",
          "-0.0010000000474974513",
          "-12345.0",
          "-10000000.0",
          "-123456784.0",
          "-12345678295994466000.0",
          "Infinity",
          "-Infinity",
          "NaN",
          "NaN",
      });
}

TEST_F(CastExprTest, stringToTimestamp) {
  std::vector<std::optional<std::string>> input{
      "1970-01-01",
      "1970-01-01 00:00 America/Sao_Paulo",
      "2000-01-01",
      "1970-01-01 00:00:00",
      "2000-01-01 12:21:56",
      "1970-01-01 00:00:00-02:00",
      std::nullopt,
  };
  std::vector<std::optional<Timestamp>> expected{
      Timestamp(0, 0),
      Timestamp(10800, 0),
      Timestamp(946684800, 0),
      Timestamp(0, 0),
      Timestamp(946729316, 0),
      Timestamp(7200, 0),
      std::nullopt,
  };
  testCast<std::string, Timestamp>("timestamp", input, expected);

  // Try with a different session timezone.
  setTimezone("America/Los_Angeles");
  input = {
      "1970-01-01 00:00",
      "1970-01-01 00:00 +00:00",
      "1970-01-01 00:00 +01:00",
      "1970-01-01 00:00 America/Sao_Paulo",
      "2000-01-01 12:21:56Z",
  };
  expected = {
      Timestamp(28800, 0),
      Timestamp(0, 0),
      Timestamp(-3600, 0),
      Timestamp(10800, 0),
      Timestamp(946729316, 0),
  };
  testCast<std::string, Timestamp>("timestamp", input, expected);
}

TEST_F(CastExprTest, timestampToString) {
  setLegacyCast(false);
  testCast<Timestamp, std::string>(
      "string",
      {
          Timestamp(-946684800, 0),
          Timestamp(-7266, 0),
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(9466848000, 0),
          Timestamp(94668480000, 0),
          Timestamp(946729316, 0),
          Timestamp(946729316, 123),
          Timestamp(946729316, 129900000),
          Timestamp(7266, 0),
          Timestamp(-50049331200, 0),
          Timestamp(253405036800, 0),
          Timestamp(-62480037600, 0),
          std::nullopt,
      },
      {
          "1940-01-02 00:00:00.000",
          "1969-12-31 21:58:54.000",
          "1970-01-01 00:00:00.000",
          "2000-01-01 00:00:00.000",
          "2269-12-29 00:00:00.000",
          "4969-12-04 00:00:00.000",
          "2000-01-01 12:21:56.000",
          "2000-01-01 12:21:56.000",
          "2000-01-01 12:21:56.129",
          "1970-01-01 02:01:06.000",
          "0384-01-01 08:00:00.000",
          "10000-02-01 16:00:00.000",
          "-0010-02-01 10:00:00.000",
          std::nullopt,
      });

  setLegacyCast(true);
  testCast<Timestamp, std::string>(
      "string",
      {
          Timestamp(0, 0),
          Timestamp(946729316, 123),
          Timestamp(-50049331200, 0),
          Timestamp(253405036800, 0),
          Timestamp(-62480037600, 0),
          std::nullopt,
      },
      {
          "1970-01-01T00:00:00.000",
          "2000-01-01T12:21:56.000",
          "384-01-01T08:00:00.000",
          "10000-02-01T16:00:00.000",
          "-10-02-01T10:00:00.000",
          std::nullopt,
      });

  setLegacyCast(false);
  setTimezone("America/Los_Angeles");
  testCast<Timestamp, std::string>(
      "string",
      {
          Timestamp(0, 0),
          Timestamp(946729316, 123),
          Timestamp(-50049331622, 0),
          Timestamp(253405036800, 0),
          Timestamp(-62480038022, 0),
          std::nullopt,
      },
      {
          "1969-12-31 16:00:00.000",
          "2000-01-01 04:21:56.000",
          "0384-01-01 00:00:00.000",
          "10000-02-01 08:00:00.000",
          "-0010-02-01 02:00:00.000",
          std::nullopt,
      });
}

TEST_F(CastExprTest, dateToTimestamp) {
  testCast<int32_t, Timestamp>(
      "timestamp",
      {
          0,
          10957,
          14557,
          std::nullopt,
      },
      {
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(1257724800, 0),
          std::nullopt,
      },
      DATE(),
      TIMESTAMP());
}

TEST_F(CastExprTest, timestampToDate) {
  std::vector<std::optional<Timestamp>> inputTimestamps = {
      Timestamp(0, 0),
      Timestamp(946684800, 0),
      Timestamp(1257724800, 0),
      std::nullopt,
  };

  testCast<Timestamp, int32_t>(
      "date",
      inputTimestamps,
      {
          0,
          10957,
          14557,
          std::nullopt,
      },
      TIMESTAMP(),
      DATE());

  setTimezone("America/Los_Angeles");
  testCast<Timestamp, int32_t>(
      "date",
      inputTimestamps,
      {
          -1,
          10956,
          14556,
          std::nullopt,
      },
      TIMESTAMP(),
      DATE());
}

TEST_F(CastExprTest, timestampInvalid) {
  testInvalidCast<int8_t>(
      "timestamp", {12}, "Conversion to Timestamp is not supported");
  testInvalidCast<int16_t>(
      "timestamp", {1234}, "Conversion to Timestamp is not supported");
  testInvalidCast<int32_t>(
      "timestamp", {1234}, "Conversion to Timestamp is not supported");
  testInvalidCast<int64_t>(
      "timestamp", {1234}, "Conversion to Timestamp is not supported");

  testInvalidCast<float>(
      "timestamp", {12.99}, "Conversion to Timestamp is not supported");
  testInvalidCast<double>(
      "timestamp", {12.99}, "Conversion to Timestamp is not supported");

  testInvalidCast<std::string>(
      "timestamp",
      {"2012-Oct-01"},
      "Unable to parse timestamp value: \"2012-Oct-01\"");
}

TEST_F(CastExprTest, timestampAdjustToTimezone) {
  // Empty timezone is assumed to be GMT.
  testCast<std::string, Timestamp>(
      "timestamp", {"1970-01-01"}, {Timestamp(0, 0)});

  setTimezone("America/Los_Angeles");

  // Expect unix epochs to be converted to LA timezone (8h offset).
  testCast<std::string, Timestamp>(
      "timestamp",
      {
          "1970-01-01",
          "2000-01-01",
          "1969-12-31 16:00:00",
          "2000-01-01 12:21:56",
          "1970-01-01 00:00:00+14:00",
          std::nullopt,
          "2000-05-01", // daylight savings - 7h offset.
      },
      {
          Timestamp(28800, 0),
          Timestamp(946713600, 0),
          Timestamp(0, 0),
          Timestamp(946758116, 0),
          Timestamp(-50400, 0),
          std::nullopt,
          Timestamp(957164400, 0),
      });
}

TEST_F(CastExprTest, timestampAdjustToTimezoneInvalid) {
  VELOX_ASSERT_USER_THROW(setTimezone("bla"), "Unknown time zone: 'bla'");
}

TEST_F(CastExprTest, date) {
  testCast<std::string, int32_t>(
      "date",
      {"1970-01-01",
       "2020-01-01",
       "2135-11-09",
       "1969-12-27",
       "1812-04-15",
       "1920-01-02",
       "12345-12-18",
       "1970-1-2",
       "1970-01-2",
       "1970-1-02",
       "+1970-01-02",
       "-1-1-1",
       " 1970-01-01",
       std::nullopt},
      {0,
       18262,
       60577,
       -5,
       -57604,
       -18262,
       3789742,
       1,
       1,
       1,
       1,
       -719893,
       0,
       std::nullopt},
      VARCHAR(),
      DATE());
}

TEST_F(CastExprTest, invalidDate) {
  testInvalidCast<int8_t>(
      "date", {12}, "Cast from TINYINT to DATE is not supported", TINYINT());
  testInvalidCast<int16_t>(
      "date",
      {1234},
      "Cast from SMALLINT to DATE is not supported",
      SMALLINT());
  testInvalidCast<int32_t>(
      "date", {1234}, "Cast from INTEGER to DATE is not supported", INTEGER());
  testInvalidCast<int64_t>(
      "date", {1234}, "Cast from BIGINT to DATE is not supported", BIGINT());

  testInvalidCast<float>(
      "date", {12.99}, "Cast from REAL to DATE is not supported", REAL());
  testInvalidCast<double>(
      "date", {12.99}, "Cast from DOUBLE to DATE is not supported", DOUBLE());

  // Parsing ill-formated dates.
  testInvalidCast<std::string>(
      "date",
      {"2012-Oct-23"},
      "Unable to parse date value: \"2012-Oct-23\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03-18X"},
      "Unable to parse date value: \"2015-03-18X\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015/03/18"},
      "Unable to parse date value: \"2015/03/18\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015.03.18"},
      "Unable to parse date value: \"2015.03.18\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"20150318"},
      "Unable to parse date value: \"20150318\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-031-8"},
      "Unable to parse date value: \"2015-031-8\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date", {"12345"}, "Unable to parse date value: \"12345\"", VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03"},
      "Unable to parse date value: \"2015-03\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03-18 123412"},
      "Unable to parse date value: \"2015-03-18 123412\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03-18T"},
      "Unable to parse date value: \"2015-03-18T\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03-18T123412"},
      "Unable to parse date value: \"2015-03-18T123412\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"2015-03-18 (BC)"},
      "Unable to parse date value: \"2015-03-18 (BC)\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"1970-01-01 "},
      "Unable to parse date value: \"1970-01-01 \"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {" 1970-01-01 "},
      "Unable to parse date value: \" 1970-01-01 \"",
      VARCHAR());
}

TEST_F(CastExprTest, primitiveInvalidCornerCases) {
  // To integer.
  {
    // Overflow.
    testInvalidCast<int32_t>(
        "tinyint", {1234567}, "Overflow during arithmetic conversion");
    testInvalidCast<int32_t>(
        "tinyint",
        {-1234567},
        "Negative overflow during arithmetic conversion");
    testInvalidCast<double>(
        "tinyint",
        {12345.67},
        "Loss of precision during arithmetic conversion");
    testInvalidCast<double>(
        "tinyint",
        {-12345.67},
        "Loss of precision during arithmetic conversion");
    testInvalidCast<double>(
        "tinyint", {127.8}, "Loss of precision during arithmetic conversion");
    testInvalidCast<float>(
        "integer", {kInf}, "Loss of precision during arithmetic conversion");
    testInvalidCast<float>(
        "bigint", {kInf}, "Loss of precision during arithmetic conversion");
    // Presto throws on cast(nan() as bigint), but we let it return 0 to be
    // consistent with other cases.
    testInvalidCast<float>(
        "bigint", {kNan}, "Cannot cast NaN to an integral value");
    testInvalidCast<float>(
        "integer", {kNan}, "Cannot cast NaN to an integral value");
    testInvalidCast<float>(
        "smallint", {kNan}, "Cannot cast NaN to an integral value");
    testInvalidCast<float>(
        "tinyint", {kNan}, "Cannot cast NaN to an integral value");

    // Invalid strings.
    testInvalidCast<std::string>(
        "tinyint", {"1234567"}, "Overflow during conversion");
    testInvalidCast<std::string>(
        "tinyint",
        {"1.2"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "tinyint",
        {"1.23444"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "tinyint", {".2355"}, "Invalid leading character");
    testInvalidCast<std::string>(
        "tinyint",
        {"1a"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>("tinyint", {""}, "Empty string");
    testInvalidCast<std::string>(
        "integer",
        {"1'234'567"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "integer",
        {"1,234,567"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "bigint", {"infinity"}, "Invalid leading character");
    testInvalidCast<std::string>(
        "bigint", {"nan"}, "Invalid leading character");
  }

  // To floating-point.
  {
    // TODO: Presto returns Infinity in this case.
    testInvalidCast<double>(
        "real", {1.7E308}, "Overflow during arithmetic conversion");

    // Invalid strings.
    testInvalidCast<std::string>(
        "real",
        {"1.2a"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "real",
        {"1.2.3"},
        "Non-whitespace character found after end of conversion");
  }

  // To boolean.
  {
    testInvalidCast<std::string>(
        "boolean",
        {"1.7E308"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean",
        {"nan"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean", {"infinity"}, "Invalid value for bool");
    testInvalidCast<std::string>(
        "boolean",
        {"12"},
        "Integer overflow when parsing bool (must be 0 or 1)");
    testInvalidCast<std::string>("boolean", {"-1"}, "Invalid value for bool");
    testInvalidCast<std::string>(
        "boolean",
        {"tr"},
        "Non-whitespace character found after end of conversion");
    testInvalidCast<std::string>(
        "boolean",
        {"tru"},
        "Non-whitespace character found after end of conversion");
  }
}

TEST_F(CastExprTest, primitiveValidCornerCases) {
  // To integer.
  {
    testCast<double, int8_t>("tinyint", {127.1}, {127});
    testCast<double, int64_t>("bigint", {12345.12}, {12345});
    testCast<double, int64_t>("bigint", {12345.67}, {12346});
    testCast<std::string, int8_t>("tinyint", {"+1"}, {1});
  }

  // To floating-point.
  {
    testCast<std::string, float>("real", {"1.7E308"}, {kInf});
    testCast<std::string, float>("real", {"1."}, {1.0});
    testCast<std::string, float>("real", {"1"}, {1});
    // When casting from "Infinity" and "NaN", Presto is case sensitive. But we
    // let them be case insensitive to be consistent with other conversions.
    testCast<std::string, float>("real", {"infinity"}, {kInf});
    testCast<std::string, float>("real", {"-infinity"}, {-kInf});
    testCast<std::string, float>("real", {"InfiNiTy"}, {kInf});
    testCast<std::string, float>("real", {"-InfiNiTy"}, {-kInf});
    testCast<std::string, float>("real", {"nan"}, {kNan});
    testCast<std::string, float>("real", {"nAn"}, {kNan});
  }

  // To boolean.
  {
    testCast<int8_t, bool>("boolean", {1}, {true});
    testCast<int8_t, bool>("boolean", {0}, {false});
    testCast<int8_t, bool>("boolean", {12}, {true});
    testCast<int8_t, bool>("boolean", {-1}, {true});
    testCast<double, bool>("boolean", {1.0}, {true});
    testCast<double, bool>("boolean", {1.1}, {true});
    testCast<double, bool>("boolean", {0.1}, {true});
    testCast<double, bool>("boolean", {-0.1}, {true});
    testCast<double, bool>("boolean", {-1.0}, {true});
    testCast<float, bool>("boolean", {kNan}, {true});
    testCast<float, bool>("boolean", {kInf}, {true});
    testCast<double, bool>("boolean", {0.0000000000001}, {true});

    testCast<std::string, bool>("boolean", {"1"}, {true});
    testCast<std::string, bool>("boolean", {"0"}, {false});
    testCast<std::string, bool>("boolean", {"t"}, {true});
    testCast<std::string, bool>("boolean", {"true"}, {true});
  }

  // To string.
  {
    testCast<float, std::string>("varchar", {kInf}, {"Infinity"});
    testCast<float, std::string>("varchar", {kNan}, {"NaN"});
  }
}

TEST_F(CastExprTest, truncateVsRound) {
  // Testing round cast from double to int.
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});
  testCast<int8_t, int32_t>("int", {111, 2, 3, 10, -10}, {111, 2, 3, 10, -10});
  testCast<int32_t, int8_t>("tinyint", {2, 3}, {2, 3});
  testInvalidCast<int32_t>(
      "tinyint",
      {1111111, 1000, -100101},
      "Cannot cast INTEGER '1111111' to TINYINT. Overflow during arithmetic conversion: (signed char) 1111111");
}

TEST_F(CastExprTest, nullInputs) {
  // Testing null inputs
  testCast<double, double>(
      "double",
      {std::nullopt, std::nullopt, 3.6, 100.44, std::nullopt},
      {std::nullopt, std::nullopt, 3.6, 100.44, std::nullopt});
  testCast<double, float>(
      "float",
      {std::nullopt, 2.5, 3.6, 100.44, std::nullopt},
      {std::nullopt, 2.5, 3.6, 100.44, std::nullopt});
  testCast<double, std::string>(
      "string",
      {1.888, std::nullopt, std::nullopt, std::nullopt, -100.101},
      {"1.888", std::nullopt, std::nullopt, std::nullopt, "-100.101"});
}

TEST_F(CastExprTest, errorHandling) {
  // Making sure error cases lead to null outputs
  testTryCast<std::string, int8_t>(
      "tinyint",
      {"1abc", "2", "3", "100", std::nullopt},
      {std::nullopt, 2, 3, 100, std::nullopt});
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});

  testInvalidCast<std::string>(
      "tinyint",
      {"1abc", "2", "3", "100", "-100"},
      "Non-whitespace character found after end of conversion");

  testInvalidCast<std::string>(
      "tinyint",
      {"1", "2", "3", "100", "-100.5"},
      "Non-whitespace character found after end of conversion");
}

constexpr vector_size_t kVectorSize = 1'000;

TEST_F(CastExprTest, mapCast) {
  auto sizeAt = [](vector_size_t row) { return row % 5; };
  auto keyAt = [](vector_size_t row) { return row % 11; };
  auto valueAt = [](vector_size_t row) { return row % 13; };

  auto inputMap = makeMapVector<int64_t, int64_t>(
      kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3));

  // Cast map<bigint, bigint> -> map<integer, double>.
  {
    auto expectedMap = makeMapVector<int32_t, double>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3));

    testCast(inputMap, expectedMap);
  }

  // Cast map<bigint, bigint> -> map<bigint, varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row) {
      return StringView::makeInline(folly::to<std::string>(valueAt(row)));
    };

    auto expectedMap = makeMapVector<int64_t, StringView>(
        kVectorSize, sizeAt, keyAt, valueAtString, nullEvery(3));

    testCast(inputMap, expectedMap);
  }

  // Cast map<bigint, bigint> -> map<varchar, bigint>.
  {
    auto keyAtString = [&](vector_size_t row) {
      return StringView::makeInline(folly::to<std::string>(keyAt(row)));
    };

    auto expectedMap = makeMapVector<StringView, int64_t>(
        kVectorSize, sizeAt, keyAtString, valueAt, nullEvery(3));

    testCast(inputMap, expectedMap);
  }

  // null values
  {
    auto inputWithNullValues = makeMapVector<int64_t, int64_t>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    auto expectedMap = makeMapVector<int32_t, double>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    testCast(inputWithNullValues, expectedMap);
  }

  // Nulls in result keys are not allowed.
  {
    VELOX_ASSERT_THROW(
        testCast(
            inputMap,
            makeMapVector<Timestamp, int64_t>(
                kVectorSize,
                sizeAt,
                [](auto /*row*/) { return Timestamp(); },
                valueAt,
                nullEvery(3),
                nullEvery(7)),
            false),
        "Cannot cast BIGINT '0' to TIMESTAMP. Conversion to Timestamp is not supported");

    testCast(
        inputMap,
        makeMapVector<Timestamp, int64_t>(
            kVectorSize,
            sizeAt,
            [](auto /*row*/) { return Timestamp(); },
            valueAt,
            [](auto row) { return row % 3 == 0 || row % 5 != 0; }),
        true);
  }

  // Make sure that the output of map cast has valid(copyable) data even for
  // non selected rows.
  {
    auto mapVector = vectorMaker_.mapVector<int32_t, int32_t>(
        kVectorSize,
        sizeAt,
        keyAt,
        /*valueAt*/ nullptr,
        /*isNullAt*/ nullptr,
        /*valueIsNullAt*/ nullEvery(1));

    SelectivityVector rows(5);
    rows.setValid(2, false);
    mapVector->setOffsetAndSize(2, 100, 100);
    std::vector<VectorPtr> results(1);

    auto rowVector = makeRowVector({mapVector});
    auto castExpr =
        makeTypedExpr("c0::map(bigint, bigint)", asRowType(rowVector->type()));
    exec::ExprSet exprSet({castExpr}, &execCtx_);

    exec::EvalCtx evalCtx(&execCtx_, &exprSet, rowVector.get());
    exprSet.eval(rows, evalCtx, results);
    auto mapResults = results[0]->as<MapVector>();
    auto keysSize = mapResults->mapKeys()->size();
    auto valuesSize = mapResults->mapValues()->size();

    for (int i = 0; i < mapResults->size(); i++) {
      auto start = mapResults->offsetAt(i);
      auto size = mapResults->sizeAt(i);
      if (size == 0) {
        continue;
      }
      VELOX_CHECK(start + size - 1 < keysSize);
      VELOX_CHECK(start + size - 1 < valuesSize);
    }
  }

  // Error handling.
  {
    auto data = makeRowVector(
        {makeMapVector<StringView, StringView>({{{"1", "2"}}, {{"", "1"}}})});
    auto copy = createCopy(data);
    auto result1 = evaluate("try_cast(c0 as map(int, int))", data);
    auto result2 = evaluate("try(cast(c0 as map(int, int)))", data);
    ASSERT_FALSE(result1->isNullAt(0));
    ASSERT_TRUE(result1->isNullAt(1));

    ASSERT_FALSE(result2->isNullAt(0));
    ASSERT_TRUE(result2->isNullAt(1));
    ASSERT_THROW(evaluate("cast(c0 as map(int, int)", data), VeloxException);

    // Make sure the input vector does not change.
    assertEqualVectors(data, copy);
  }

  {
    auto result = evaluate(
        "try_cast(map(array_constructor('1'), array_constructor(''))  as map(int, int))",
        makeRowVector({makeFlatVector<int32_t>({1, 2})}));

    ASSERT_TRUE(result->isNullAt(0));
    ASSERT_TRUE(result->isNullAt(1));
  }
}

TEST_F(CastExprTest, arrayCast) {
  auto sizeAt = [](vector_size_t /* row */) { return 7; };
  auto valueAt = [](vector_size_t /* row */, vector_size_t idx) {
    return 1 + idx;
  };
  auto arrayVector =
      makeArrayVector<double>(kVectorSize, sizeAt, valueAt, nullEvery(3));

  // Cast array<double> -> array<bigint>.
  {
    auto expected =
        makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt, nullEvery(3));
    testCast(arrayVector, expected);
  }

  // Cast array<double> -> array<varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row, vector_size_t idx) {
      // Add .0 at the end since folly outputs 1.0 -> 1
      return StringView::makeInline(
          folly::to<std::string>(valueAt(row, idx)) + ".0");
    };
    auto expected = makeArrayVector<StringView>(
        kVectorSize, sizeAt, valueAtString, nullEvery(3));
    testCast(arrayVector, expected);
  }

  // Make sure that the output of array cast has valid(copyable) data even for
  // non selected rows.
  {
    // Array with all inner elements null.
    auto sizeAtLocal = [](vector_size_t /* row */) { return 5; };
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        kVectorSize, sizeAtLocal, nullptr, nullptr, nullEvery(1));

    SelectivityVector rows(5);
    rows.setValid(2, false);
    arrayVector->setOffsetAndSize(2, 100, 10);
    std::vector<VectorPtr> results(1);

    auto rowVector = makeRowVector({arrayVector});
    auto castExpr =
        makeTypedExpr("cast (c0 as bigint[])", asRowType(rowVector->type()));
    exec::ExprSet exprSet({castExpr}, &execCtx_);

    exec::EvalCtx evalCtx(&execCtx_, &exprSet, rowVector.get());
    exprSet.eval(rows, evalCtx, results);
    auto arrayResults = results[0]->as<ArrayVector>();
    auto elementsSize = arrayResults->elements()->size();
    for (int i = 0; i < arrayResults->size(); i++) {
      auto start = arrayResults->offsetAt(i);
      auto size = arrayResults->sizeAt(i);
      if (size == 0) {
        continue;
      }
      VELOX_CHECK(start + size - 1 < elementsSize);
    }
  }

  // Error handling.
  {
    auto data =
        makeRowVector({makeArrayVector<StringView>({{"1", "2"}, {"", "1"}})});
    auto copy = createCopy(data);
    auto result1 = evaluate("try_cast(c0 as bigint[])", data);
    auto result2 = evaluate("try(cast(c0 as bigint[]))", data);

    auto expected = makeNullableArrayVector<int64_t>({{{1, 2}}, std::nullopt});

    assertEqualVectors(result1, expected);
    assertEqualVectors(result2, expected);

    ASSERT_THROW(evaluate("cast(c0 as bigint[])", data), VeloxException);

    // Make sure the input vector does not change.
    assertEqualVectors(data, copy);
  }

  {
    auto data = makeNullableNestedArrayVector<StringView>({
        {{{{"1"_sv, "2"_sv}}, {{""_sv}}}}, // row0
        {{{{std::nullopt, "4"_sv}}}}, // row1
    });
    auto expected = makeNullableNestedArrayVector<int64_t>({
        std::nullopt, // row0
        {{{{std::nullopt, 4}}}}, // row1

    });
    testCast(data, expected, true);
  }
}

TEST_F(CastExprTest, rowCast) {
  auto valueAt = [](vector_size_t row) { return double(1 + row); };
  auto valueAtInt = [](vector_size_t row) { return int64_t(1 + row); };
  auto doubleVectorNullEvery3 =
      makeFlatVector<double>(kVectorSize, valueAt, nullEvery(3));
  auto intVectorNullEvery11 =
      makeFlatVector<int64_t>(kVectorSize, valueAtInt, nullEvery(11));
  auto doubleVectorNullEvery11 =
      makeFlatVector<double>(kVectorSize, valueAt, nullEvery(11));
  auto intVectorNullEvery3 =
      makeFlatVector<int64_t>(kVectorSize, valueAtInt, nullEvery(3));
  auto rowVector = makeRowVector(
      {intVectorNullEvery11, doubleVectorNullEvery3}, nullEvery(5));

  setCastMatchStructByName(false);
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double, c1:
  // bigint)
  {
    auto expectedRowVector = makeRowVector(
        {doubleVectorNullEvery11, intVectorNullEvery3}, nullEvery(5));
    testCast(rowVector, expectedRowVector);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(a: double, b:
  // bigint)
  {
    auto expectedRowVector = makeRowVector(
        {"a", "b"},
        {doubleVectorNullEvery11, intVectorNullEvery3},
        nullEvery(5));
    testCast(rowVector, expectedRowVector);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double)
  {
    auto expectedRowVector =
        makeRowVector({doubleVectorNullEvery11}, nullEvery(5));
    testCast(rowVector, expectedRowVector);
  }

  // Name-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double) dropping
  // b
  setCastMatchStructByName(true);
  {
    auto intVectorNullAll = makeFlatVector<int64_t>(
        kVectorSize, valueAtInt, [](vector_size_t /* row */) { return true; });
    auto expectedRowVector = makeRowVector(
        {"c0", "b"}, {doubleVectorNullEvery11, intVectorNullAll}, nullEvery(5));
    testCast(rowVector, expectedRowVector);
  }

  // Error handling.
  {
    auto data = makeRowVector(
        {makeFlatVector<StringView>({"1", ""}),
         makeFlatVector<StringView>({"2", "3"})});

    auto expected = makeRowVector(
        {makeFlatVector<int32_t>({1, 2}), makeFlatVector<int32_t>({2, 3})});
    expected->setNull(1, true);

    testCast(data, expected, true);
  }

  {
    auto data = makeRowVector(
        {makeArrayVector<StringView>({{"1", ""}, {"3", "4"}}),
         makeFlatVector<StringView>({"2", ""})});

    // expected1 is [null, struct{[3,4], ""}]
    auto expected1 = makeRowVector(
        {makeArrayVector<int32_t>({{1 /*will be null*/}, {3, 4}}),
         makeFlatVector<StringView>({"2" /*will be null*/, ""})});
    expected1->setNull(0, true);

    // expected2 is [struct{["1",""], 2}, null]
    auto expected2 = makeRowVector(
        {makeArrayVector<StringView>({{"1", ""}, {"3", "4"}}),
         makeFlatVector<int32_t>({2, 0 /*null*/})});
    expected2->setNull(1, true);

    // expected3 is [null, null]
    auto expected3 = makeRowVector(
        {makeArrayVector<int32_t>({{1}}), makeFlatVector<int32_t>(1)});
    expected3->resize(2);
    expected3->setNull(0, true);
    expected3->setNull(1, true);

    testCast(data, expected1, true);
    testCast(data, expected2, true);
    testCast(data, expected3, true);
  }

  // Null handling for nested structs.
  {
    auto data =
        makeRowVector({makeRowVector({makeFlatVector<StringView>({"1", ""})})});
    auto expected =
        makeRowVector({makeRowVector({makeFlatVector<int32_t>({1, 0})})});
    expected->setNull(1, true);
    testCast(data, expected, true);
  }
}

TEST_F(CastExprTest, nulls) {
  auto input =
      makeFlatVector<int32_t>(kVectorSize, [](auto row) { return row; });
  auto allNulls = makeFlatVector<int32_t>(
      kVectorSize, [](auto row) { return row; }, nullEvery(1));

  auto result = evaluate<FlatVector<int16_t>>(
      "cast(if(c0 % 2 = 0, c1, c0) as smallint)",
      makeRowVector({input, allNulls}));

  auto expectedResult = makeFlatVector<int16_t>(
      kVectorSize, [](auto row) { return row; }, nullEvery(2));
  assertEqualVectors(expectedResult, result);
}

TEST_F(CastExprTest, testNullOnFailure) {
  auto input =
      makeNullableFlatVector<std::string>({"1", "2", "", "3.4", std::nullopt});
  auto expected = makeNullableFlatVector<int32_t>(
      {1, 2, std::nullopt, std::nullopt, std::nullopt});

  // nullOnFailure is true, so we should return null instead of throwing.
  testCast(input, expected, true);

  // nullOnFailure is false, so we should throw.
  EXPECT_THROW(testCast(input, expected, false), VeloxUserError);
}

TEST_F(CastExprTest, toString) {
  auto input = std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "a");
  exec::ExprSet exprSet(
      {makeCastExpr(input, BIGINT(), false),
       makeCastExpr(input, ARRAY(VARCHAR()), false)},
      &execCtx_);
  ASSERT_EQ("cast((a) as BIGINT)", exprSet.exprs()[0]->toString());
  ASSERT_EQ("cast((a) as ARRAY<VARCHAR>)", exprSet.exprs()[1]->toString());
}

TEST_F(CastExprTest, decimalToIntegral) {
  testDecimalToIntegralCasts<int64_t>();
  testDecimalToIntegralCasts<int32_t>();
  testDecimalToIntegralCasts<int16_t>();
  testDecimalToIntegralCasts<int8_t>();
}

TEST_F(CastExprTest, decimalToIntegralOutOfBounds) {
  testDecimalToIntegralCastsOutOfBounds<TypeKind::INTEGER>();
  testDecimalToIntegralCastsOutOfBounds<TypeKind::SMALLINT>();
  testDecimalToIntegralCastsOutOfBounds<TypeKind::TINYINT>();
}

TEST_F(CastExprTest, decimalToIntegralOutOfBoundsSetNullOnFailure) {
  testDecimalToIntegralCastsOutOfBoundsSetNullOnFailure<TypeKind::INTEGER>();
  testDecimalToIntegralCastsOutOfBoundsSetNullOnFailure<TypeKind::SMALLINT>();
  testDecimalToIntegralCastsOutOfBoundsSetNullOnFailure<TypeKind::TINYINT>();
}

TEST_F(CastExprTest, decimalToFloat) {
  testDecimalToFloatCasts<float>();
  testDecimalToFloatCasts<double>();
}

TEST_F(CastExprTest, decimalToBool) {
  auto shortFlat = makeNullableFlatVector<int64_t>(
      {DecimalUtil::kShortDecimalMin, 0, std::nullopt}, DECIMAL(18, 18));
  testCast(shortFlat, makeNullableFlatVector<bool>({1, 0, std::nullopt}));

  auto longFlat = makeNullableFlatVector<int128_t>(
      {DecimalUtil::kLongDecimalMin, 0, std::nullopt}, DECIMAL(38, 5));
  testCast(longFlat, makeNullableFlatVector<bool>({1, 0, std::nullopt}));
}

TEST_F(CastExprTest, decimalToVarchar) {
  auto flatForInline = makeNullableFlatVector<int64_t>(
      {123456789, -333333333, 0, 5, -9, std::nullopt}, DECIMAL(9, 2));
  testCast(
      flatForInline,
      makeNullableFlatVector<StringView>(
          {"1234567.89",
           "-3333333.33",
           "0.00",
           "0.05",
           "-0.09",
           std::nullopt}));

  auto shortFlatForZero = makeNullableFlatVector<int64_t>({0}, DECIMAL(6, 0));
  testCast(shortFlatForZero, makeNullableFlatVector<StringView>({"0"}));

  auto shortFlat = makeNullableFlatVector<int64_t>(
      {DecimalUtil::kShortDecimalMin,
       -3,
       0,
       55,
       DecimalUtil::kShortDecimalMax,
       std::nullopt},
      DECIMAL(18, 18));
  testCast(
      shortFlat,
      makeNullableFlatVector<StringView>(
          {"-0.999999999999999999",
           "-0.000000000000000003",
           "0.000000000000000000",
           "0.000000000000000055",
           "0.999999999999999999",
           std::nullopt}));

  auto longFlat = makeNullableFlatVector<int128_t>(
      {DecimalUtil::kLongDecimalMin,
       0,
       DecimalUtil::kLongDecimalMax,
       HugeInt::build(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull),
       HugeInt::build(0xffff, 0xffffffffffffffff),
       std::nullopt},
      DECIMAL(38, 5));
  testCast(
      longFlat,
      makeNullableFlatVector<StringView>(
          {"-999999999999999999999999999999999.99999",
           "0.00000",
           "999999999999999999999999999999999.99999",
           "-0.00001",
           "12089258196146291747.06175",
           std::nullopt}));

  auto longFlatForZero = makeNullableFlatVector<int128_t>({0}, DECIMAL(25, 0));
  testCast(longFlatForZero, makeNullableFlatVector<StringView>({"0"}));
}

TEST_F(CastExprTest, decimalToDecimal) {
  // short to short, scale up.
  auto shortFlat =
      makeFlatVector<int64_t>({-3, -2, -1, 0, 55, 69, 72}, DECIMAL(2, 2));
  testCast(
      shortFlat,
      makeFlatVector<int64_t>(
          {-300, -200, -100, 0, 5'500, 6'900, 7'200}, DECIMAL(4, 4)));

  // short to short, scale down.
  testCast(
      shortFlat, makeFlatVector<int64_t>({0, 0, 0, 0, 6, 7, 7}, DECIMAL(4, 1)));

  // long to short, scale up.
  auto longFlat =
      makeFlatVector<int128_t>({-201, -109, 0, 105, 208}, DECIMAL(20, 2));
  testCast(
      longFlat,
      makeFlatVector<int64_t>(
          {-201'000, -109'000, 0, 105'000, 208'000}, DECIMAL(10, 5)));

  // long to short, scale down.
  testCast(
      longFlat, makeFlatVector<int64_t>({-20, -11, 0, 11, 21}, DECIMAL(10, 1)));

  // long to long, scale up.
  testCast(
      longFlat,
      makeFlatVector<int128_t>(
          {-20'100'000'000, -10'900'000'000, 0, 10'500'000'000, 20'800'000'000},
          DECIMAL(20, 10)));

  // long to long, scale down.
  testCast(
      longFlat,
      makeFlatVector<int128_t>({-20, -11, 0, 11, 21}, DECIMAL(20, 1)));

  // short to long, scale up.
  testCast(
      shortFlat,
      makeFlatVector<int128_t>(
          {-3'000'000'000,
           -2'000'000'000,
           -1'000'000'000,
           0,
           55'000'000'000,
           69'000'000'000,
           72'000'000'000},
          DECIMAL(20, 11)));

  // short to long, scale down.
  testCast(
      makeFlatVector<int64_t>({-20'500, -190, 12'345, 19'999}, DECIMAL(6, 4)),
      makeFlatVector<int128_t>({-21, 0, 12, 20}, DECIMAL(20, 1)));

  // NULLs and overflow.
  longFlat = makeNullableFlatVector<int128_t>(
      {-20'000, -1'000'000, 10'000, std::nullopt}, DECIMAL(20, 3));
  auto expectedShort = makeNullableFlatVector<int64_t>(
      {-200'000, std::nullopt, 100'000, std::nullopt}, DECIMAL(6, 4));

  // Throws exception if CAST fails.
  VELOX_ASSERT_THROW(
      testCast(longFlat, expectedShort),
      "Cannot cast DECIMAL '-1000.000' to DECIMAL(6, 4)");

  // nullOnFailure is true.
  testCast(longFlat, expectedShort, true);

  // long to short, big numbers.
  testCast(
      makeNullableFlatVector<int128_t>(
          {HugeInt::build(-2, 200),
           HugeInt::build(-1, 300),
           HugeInt::build(0, 400),
           HugeInt::build(1, 1),
           HugeInt::build(10, 100),
           std::nullopt},
          DECIMAL(23, 8)),
      makeNullableFlatVector<int64_t>(
          {-368934881474,
           -184467440737,
           0,
           184467440737,
           std::nullopt,
           std::nullopt},
          DECIMAL(12, 0)),
      true);

  // Overflow case.
  VELOX_ASSERT_THROW(
      testCast(
          makeNullableFlatVector<int128_t>(
              {DecimalUtil::kLongDecimalMax}, DECIMAL(38, 0)),
          makeNullableFlatVector<int128_t>({0}, DECIMAL(38, 1))),
      "Cannot cast DECIMAL '99999999999999999999999999999999999999' to DECIMAL(38, 1)");
  VELOX_ASSERT_THROW(
      testCast(
          makeNullableFlatVector<int128_t>(
              {DecimalUtil::kLongDecimalMin}, DECIMAL(38, 0)),
          makeNullableFlatVector<int128_t>({0}, DECIMAL(38, 1))),
      "Cannot cast DECIMAL '-99999999999999999999999999999999999999' to DECIMAL(38, 1)");
}

TEST_F(CastExprTest, integerToDecimal) {
  testIntToDecimalCasts<int8_t>();
  testIntToDecimalCasts<int16_t>();
  testIntToDecimalCasts<int32_t>();
  testIntToDecimalCasts<int64_t>();
}

TEST_F(CastExprTest, boolToDecimal) {
  // Bool to short decimal.
  auto input =
      makeFlatVector<bool>({true, false, false, true, true, true, false});
  testCast(
      input,
      makeFlatVector<int64_t>({100, 0, 0, 100, 100, 100, 0}, DECIMAL(6, 2)));

  // Bool to long decimal.
  testCast(
      input,
      makeFlatVector<int128_t>(
          {10'000'000'000,
           0,
           0,
           10'000'000'000,
           10'000'000'000,
           10'000'000'000,
           0},
          DECIMAL(20, 10)));
}

TEST_F(CastExprTest, varcharToDecimal) {
  testCast(
      makeFlatVector<StringView>(
          {"9999999999.99",
           "15",
           "1.5",
           "-1.5",
           "1.556",
           "1.554",
           ("1.556" + std::string(32, '1')).data(),
           ("1.556" + std::string(32, '9')).data(),
           "0000.123",
           ".12300000000",
           "+09",
           "9.",
           ".9",
           "3E2",
           "-3E+2",
           "3E+2",
           "3E+00002",
           "3E-2",
           "3e+2",
           "3e-2",
           "3.5E-2",
           "3.4E-2",
           "3.5E+2",
           "3.4E+2",
           "31.423e+2",
           "31.423e-2",
           "31.523e-2",
           "-3E-00000"}),
      makeFlatVector<int64_t>(
          {999'999'999'999,
           1500,
           150,
           -150,
           156,
           155,
           156,
           156,
           12,
           12,
           900,
           900,
           90,
           30000,
           -30000,
           30000,
           30000,
           3,
           30000,
           3,
           4,
           3,
           35000,
           34000,
           314230,
           31,
           32,
           -300},
          DECIMAL(12, 2)));

  // Truncates the fractional digits with exponent.
  testCast(
      makeFlatVector<StringView>(
          {"112345612.23e-6",
           "112345662.23e-6",
           "1.23e-6",
           "1.23e-3",
           "1.26e-3",
           "1.23456781e3",
           "1.23456789e3",
           "1.23456789123451789123456789e9",
           "1.23456789123456789123456789e9"}),
      makeFlatVector<int128_t>(
          {1123456,
           1123457,
           0,
           12,
           13,
           12345678,
           12345679,
           12345678912345,
           12345678912346},
          DECIMAL(20, 4)));

  const auto minDecimalStr = '-' + std::string(36, '9') + '.' + "99";
  const auto maxDecimalStr = std::string(36, '9') + '.' + "99";
  testCast(
      makeFlatVector<StringView>(
          {StringView(minDecimalStr),
           StringView(maxDecimalStr),
           "123456789012345678901234.567"}),
      makeFlatVector<int128_t>(
          {
              DecimalUtil::kLongDecimalMin,
              DecimalUtil::kLongDecimalMax,
              HugeInt::build(
                  669260, 10962463713375599297U), // 12345678901234567890123457
          },
          DECIMAL(38, 2)));

  const std::string fractionLarge = "1.9" + std::string(67, '9');
  const std::string fractionLargeExp = "1.9" + std::string(67, '9') + "e2";
  const std::string fractionLargeNegExp =
      "1000.9" + std::string(67, '9') + "e-2";
  testCast(
      makeFlatVector<StringView>(
          {StringView(('-' + std::string(38, '9')).data()),
           StringView(std::string(38, '9').data()),
           StringView(fractionLarge),
           StringView(fractionLargeExp),
           StringView(fractionLargeNegExp)}),
      makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMin,
           DecimalUtil::kLongDecimalMax,
           2,
           200,
           10},
          DECIMAL(38, 0)));

  const std::string fractionRoundDown = "0." + std::string(38, '9') + "2";
  const std::string fractionRoundDownExp =
      "99." + std::string(36, '9') + "2e-2";
  testCast(
      makeFlatVector<StringView>(
          {StringView(fractionRoundDown), StringView(fractionRoundDownExp)}),
      makeConstant<int128_t>(DecimalUtil::kLongDecimalMax, 2, DECIMAL(38, 38)));

  // Overflows when parsing whole digits.
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {std::string(280, '9')},
      fmt::format(
          "Cannot cast VARCHAR '{}' to DECIMAL(38, 0). Value too large.",
          std::string(280, '9')));

  // Overflows when parsing fractional digits.
  const std::string fractionOverflow = std::string(36, '9') + '.' + "23456";
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 10),
      {fractionOverflow},
      fmt::format(
          "Cannot cast VARCHAR '{}' to DECIMAL(38, 10). Value too large.",
          fractionOverflow));

  const std::string fractionRoundUp = "0." + std::string(38, '9') + "6";
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 38),
      {fractionRoundUp},
      fmt::format(
          "Cannot cast VARCHAR '{}' to DECIMAL(38, 38). Value too large.",
          fractionRoundUp));

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"0.0444a"},
      "Cannot cast VARCHAR '0.0444a' to DECIMAL(38, 0). Value is not a number. Chars 'a' are invalid.");

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {""},
      "Cannot cast VARCHAR '' to DECIMAL(38, 0). Value is not a number. Input is empty.");

  // Exponent > LongDecimalType::kMaxPrecision.
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"1.23e67"},
      "Cannot cast VARCHAR '1.23e67' to DECIMAL(38, 0). Value too large.");

  // Forcing the scale to be zero overflows.
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"20908.23e35"},
      "Cannot cast VARCHAR '20908.23e35' to DECIMAL(38, 0). Value too large.");

  // Rescale overflows.
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 38),
      {"111111111111111111.23"},
      "Cannot cast VARCHAR '111111111111111111.23' to DECIMAL(38, 38). Value too large.");

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"23e-5d"},
      "Cannot cast VARCHAR '23e-5d' to DECIMAL(38, 0). Value is not a number. Non-digit character 'd' is not allowed in the exponent part.");

  // Whitespaces.
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"1. 23"},
      "Cannot cast VARCHAR '1. 23' to DECIMAL(38, 0). Value is not a number. Chars ' 23' are invalid.");
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {"-3E+ 2"},
      "Cannot cast VARCHAR '-3E+ 2' to DECIMAL(12, 2). Value is not a number. Non-digit character ' ' is not allowed in the exponent part.");
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {"1.23 "},
      "Cannot cast VARCHAR '1.23 ' to DECIMAL(38, 0). Value is not a number. Chars ' ' are invalid.");
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {"-3E+2 "},
      "Cannot cast VARCHAR '-3E+2 ' to DECIMAL(12, 2). Value is not a number. Non-digit character ' ' is not allowed in the exponent part.");
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(38, 0),
      {" 1.23"},
      "Cannot cast VARCHAR ' 1.23' to DECIMAL(38, 0). Value is not a number. Extracted digits are empty.");
  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {" -3E+2"},
      "Cannot cast VARCHAR ' -3E+2' to DECIMAL(12, 2). Value is not a number. Extracted digits are empty.");

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {"-3E+2.1"},
      "Cannot cast VARCHAR '-3E+2.1' to DECIMAL(12, 2). Value is not a number. Non-digit character '.' is not allowed in the exponent part.");

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {"-3E+"},
      "Cannot cast VARCHAR '-3E+' to DECIMAL(12, 2). Value is not a number. The exponent part only contains sign.");

  testThrow<std::string>(
      VARCHAR(),
      DECIMAL(12, 2),
      {"-3E-"},
      "Cannot cast VARCHAR '-3E-' to DECIMAL(12, 2). Value is not a number. The exponent part only contains sign.");
}

TEST_F(CastExprTest, castInTry) {
  // Test try(cast(array(varchar) as array(bigint))) whose input vector is
  // wrapped in dictinary encoding. The row of ["2a"] should trigger an error
  // during casting and the try expression should turn this error into a null
  // at this row.
  auto input = makeRowVector({makeNullableArrayVector<StringView>(
      {{{"1"_sv}}, {{"2a"_sv}}, std::nullopt, std::nullopt})});
  auto expected = makeNullableArrayVector<int64_t>(
      {{{1}}, std::nullopt, std::nullopt, std::nullopt});

  evaluateAndVerifyCastInTryDictEncoding(
      ARRAY(VARCHAR()), ARRAY(BIGINT()), input, expected);

  // Test try(cast(map(varchar, bigint) as map(bigint, bigint))) where "3a"
  // should trigger an error at the first row.
  auto map = makeRowVector({makeMapVector<StringView, int64_t>(
      {{{"1", 2}, {"3a", 4}}, {{"5", 6}, {"7", 8}}})});
  auto mapExpected = makeNullableMapVector<int64_t, int64_t>(
      {std::nullopt, {{{5, 6}, {7, 8}}}});
  evaluateAndVerifyCastInTryDictEncoding(
      MAP(VARCHAR(), BIGINT()), MAP(BIGINT(), BIGINT()), map, mapExpected);

  // Test try(cast(array(varchar) as array(bigint))) where "2a" should trigger
  // an error at the first row.
  auto array =
      makeArrayVector<StringView>({{"1"_sv, "2a"_sv}, {"3"_sv, "4"_sv}});
  auto arrayExpected =
      vectorMaker_.arrayVectorNullable<int64_t>({std::nullopt, {{3, 4}}});
  evaluateAndVerifyCastInTryDictEncoding(
      ARRAY(VARCHAR()), ARRAY(BIGINT()), makeRowVector({array}), arrayExpected);

  arrayExpected = vectorMaker_.arrayVectorNullable<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  evaluateAndVerifyCastInTryDictEncoding(
      ARRAY(VARCHAR()),
      ARRAY(BIGINT()),
      makeRowVector({BaseVector::wrapInConstant(3, 0, array)}),
      arrayExpected);

  auto nested = makeRowVector({makeNullableNestedArrayVector<StringView>(
      {{{{{"1"_sv, "2"_sv}}, {{"3"_sv}}, {{"4a"_sv, "5"_sv}}}},
       {{{{"6"_sv, "7"_sv}}}}})});
  auto nestedExpected =
      makeNullableNestedArrayVector<int64_t>({std::nullopt, {{{{6, 7}}}}});
  evaluateAndVerifyCastInTryDictEncoding(
      ARRAY(ARRAY(VARCHAR())), ARRAY(ARRAY(BIGINT())), nested, nestedExpected);
}

TEST_F(CastExprTest, doubleToDecimal) {
  // Double to short decimal.
  const auto input = makeFlatVector<double>(
      {-3333.03,
       -2222.02,
       -1.0,
       0.00,
       100,
       99999.99,
       10.03,
       10.05,
       9.95,
       -2.123456789});
  testCast(
      input,
      makeFlatVector<int64_t>(
          {-33'330'300,
           -22'220'200,
           -10'000,
           0,
           1'000'000,
           999'999'900,
           100'300,
           100'500,
           99'500,
           -21'235},
          DECIMAL(10, 4)));

  // Double to long decimal.
  testCast(
      input,
      makeFlatVector<int128_t>(
          {
              HugeInt::parse("-333303" + zeros(16)),
              HugeInt::parse("-222202" + zeros(16)),
              -1'000'000'000'000'000'000,
              0,
              HugeInt::parse("100" + zeros(18)),
              HugeInt::parse("9999999" + zeros(16)),
              HugeInt::parse("1003" + zeros(16)),
              HugeInt::parse("1005" + zeros(16)),
              HugeInt::parse("995" + zeros(16)),
              HugeInt::parse("-2123456789" + zeros(9)),
          },
          DECIMAL(38, 18)));
  testCast(
      input,
      makeFlatVector<int128_t>(
          {-33'330, -22'220, -10, 0, 1'000, 1'000'000, 100, 101, 100, -21},
          DECIMAL(20, 1)));
  testCast(
      makeNullableFlatVector<double>(
          {0.13456789,
           0.00000015,
           0.000000000000001,
           0.999999999999999,
           0.123456789123123,
           std::nullopt}),
      makeNullableFlatVector<int128_t>(
          {134'567'890'000'000'000,
           150'000'000'000,
           1'000,
           999'999'999'999'999'000,
           123'456'789'123'123'000,
           std::nullopt},
          DECIMAL(38, 18)));

  testThrow<double>(
      DOUBLE(),
      DECIMAL(10, 2),
      {9999999999999999999999.99},
      "Cannot cast DOUBLE '1E22' to DECIMAL(10, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(10, 2),
      {static_cast<double>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::max()) + 1)},
      "Cannot cast DOUBLE '9223372036854776000' to DECIMAL(10, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(10, 2),
      {static_cast<double>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::min()) - 1)},
      "Cannot cast DOUBLE '-9223372036854776000' to DECIMAL(10, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(20, 2),
      {static_cast<double>(DecimalUtil::kLongDecimalMax)},
      "Cannot cast DOUBLE '1E38' to DECIMAL(20, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(20, 2),
      {static_cast<double>(DecimalUtil::kLongDecimalMin)},
      "Cannot cast DOUBLE '-1E38' to DECIMAL(20, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(38, 2),
      {std::numeric_limits<double>::max()},
      "Cannot cast DOUBLE '1.7976931348623157E308' to DECIMAL(38, 2). Result overflows.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(38, 2),
      {std::numeric_limits<double>::lowest()},
      "Cannot cast DOUBLE '-1.7976931348623157E308' to DECIMAL(38, 2). Result overflows.");
  testCast(
      makeConstant<double>(std::numeric_limits<double>::min(), 1),
      makeConstant<int128_t>(0, 1, DECIMAL(38, 2)));

  testThrow<double>(
      DOUBLE(),
      DECIMAL(38, 2),
      {INFINITY},
      "Cannot cast DOUBLE 'Infinity' to DECIMAL(38, 2). The input value should be finite.");
  testThrow<double>(
      DOUBLE(),
      DECIMAL(38, 2),
      {NAN},
      "Cannot cast DOUBLE 'NaN' to DECIMAL(38, 2). The input value should be finite.");
}

TEST_F(CastExprTest, realToDecimal) {
  // Real to short decimal.
  const auto input = makeFlatVector<float>(
      {-3333.03,
       -2222.02,
       -1.0,
       0.00,
       100,
       99999.9,
       10.03,
       10.05,
       9.95,
       -2.12345});
  testCast(
      input,
      makeFlatVector<int64_t>(
          {-33'330'300,
           -22'220'200,
           -10'000,
           0,
           1'000'000,
           999'999'000,
           100'300,
           100'500,
           99'500,
           -212'35},
          DECIMAL(10, 4)));

  // Real to long decimal.
  testCast(
      input,
      makeFlatVector<int128_t>(
          {HugeInt::parse("-333303" + zeros(16)),
           HugeInt::parse("-222202" + zeros(16)),
           -1'000'000'000'000'000'000,
           0,
           HugeInt::parse("100" + zeros(18)),
           HugeInt::parse("999999" + zeros(17)),
           HugeInt::parse("1003" + zeros(16)),
           HugeInt::parse("1005" + zeros(16)),
           HugeInt::parse("995" + zeros(16)),
           -2'123'450'000'000'000'000},
          DECIMAL(38, 18)));
  testCast(
      input,
      makeFlatVector<int128_t>(
          {-33'330, -22'220, -10, 0, 1'000, 999'999, 100, 101, 100, -21},
          DECIMAL(20, 1)));
  testCast(
      makeNullableFlatVector<float>(
          {0.134567, 0.000015, 0.000001, 0.999999, 0.123456, std::nullopt}),
      makeNullableFlatVector<int128_t>(
          {134'567'000'000'000'000,
           15'000'000'000'000,
           1'000'000'000'000,
           999'999'000'000'000'000,
           123'456'000'000'000'000,
           std::nullopt},
          DECIMAL(38, 18)));

  testThrow<float>(
      REAL(), DECIMAL(10, 2), {9999999999999999999999.99}, "Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(10, 2),
      {static_cast<float>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::max()) + 1)},
      "Cannot cast REAL '9223372036854776000' to DECIMAL(10, 2). Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(10, 2),
      {static_cast<float>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::min()) - 1)},
      "Cannot cast REAL '-9223372036854776000' to DECIMAL(10, 2). Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(20, 2),
      {static_cast<float>(DecimalUtil::kLongDecimalMax)},
      "Cannot cast REAL '9.999999680285692E37' to DECIMAL(20, 2). Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(20, 2),
      {static_cast<float>(DecimalUtil::kLongDecimalMin)},
      "Cannot cast REAL '-9.999999680285692E37' to DECIMAL(20, 2). Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(38, 2),
      {std::numeric_limits<float>::max()},
      "Cannot cast REAL '3.4028234663852886E38' to DECIMAL(38, 2). Result overflows.");
  testThrow<float>(
      REAL(),
      DECIMAL(38, 2),
      {std::numeric_limits<float>::lowest()},
      "Cannot cast REAL '-3.4028234663852886E38' to DECIMAL(38, 2). Result overflows.");
  testCast(
      makeConstant<float>(std::numeric_limits<float>::min(), 1),
      makeConstant<int128_t>(0, 1, DECIMAL(38, 2)));

  testThrow<float>(
      REAL(),
      DECIMAL(38, 2),
      {INFINITY},
      "Cannot cast REAL 'Infinity' to DECIMAL(38, 2). The input value should be finite.");
  testThrow<float>(
      REAL(),
      DECIMAL(38, 2),
      {NAN},
      "Cannot cast REAL 'NaN' to DECIMAL(38, 2). The input value should be finite.");
}

TEST_F(CastExprTest, primitiveNullConstant) {
  // Evaluate cast(NULL::double as bigint).
  auto cast =
      makeCastExpr(makeConstantNullExpr(TypeKind::DOUBLE), BIGINT(), false);

  auto result = evaluate(
      cast, makeRowVector({makeFlatVector<int64_t>(std::vector<int64_t>{1})}));
  auto expectedResult = makeNullableFlatVector<int64_t>({std::nullopt});
  assertEqualVectors(expectedResult, result);

  // Evaluate cast(try_cast(NULL::varchar as double) as bigint).
  auto innerCast =
      makeCastExpr(makeConstantNullExpr(TypeKind::VARCHAR), DOUBLE(), true);
  auto outerCast = makeCastExpr(innerCast, BIGINT(), false);

  result = evaluate(outerCast, makeRowVector(ROW({}, {}), 1));
  assertEqualVectors(expectedResult, result);
}

TEST_F(CastExprTest, primitiveWithDictionaryIntroducedNulls) {
  exec::registerVectorFunction(
      "add_dict",
      TestingDictionaryFunction::signatures(),
      std::make_unique<TestingDictionaryFunction>(2));

  {
    auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto result = evaluate(
        "cast(add_dict(add_dict(c0)) as smallint)", makeRowVector({data}));
    auto expected = makeNullableFlatVector<int16_t>(
        {std::nullopt,
         std::nullopt,
         3,
         4,
         5,
         6,
         7,
         std::nullopt,
         std::nullopt});
    assertEqualVectors(expected, result);
  }

  {
    auto data = makeNullableFlatVector<int64_t>(
        {1,
         2,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         std::nullopt,
         8,
         9});
    auto result = evaluate(
        "cast(add_dict(add_dict(c0)) as varchar)", makeRowVector({data}));
    auto expected = makeNullConstant(TypeKind::VARCHAR, 9);
    assertEqualVectors(expected, result);
  }
}

TEST_F(CastExprTest, castAsCall) {
  // Invoking cast through a CallExpr instead of a CastExpr
  const std::vector<std::optional<int32_t>> inputValues = {1, 2, 3, 100, -100};
  const std::vector<std::optional<double>> outputValues = {
      1.0, 2.0, 3.0, 100.0, -100.0};

  auto input = makeRowVector({makeNullableFlatVector(inputValues)});
  core::TypedExprPtr inputField =
      std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0");
  core::TypedExprPtr callExpr = std::make_shared<const core::CallTypedExpr>(
      DOUBLE(), std::vector<core::TypedExprPtr>{inputField}, "cast");

  auto result = evaluate(callExpr, input);
  auto expected = makeNullableFlatVector(outputValues);
  assertEqualVectors(expected, result);
}

namespace {
/// Wraps input in a constant encoding that repeats the first element and
/// then in dictionary that reverses the order of rows.
class TestingDictionaryOverConstFunction : public exec::VectorFunction {
 public:
  TestingDictionaryOverConstFunction() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto size = rows.size();
    auto constant = BaseVector::wrapInConstant(size, 0, args[0]);

    auto indices = functions::test::makeIndicesInReverse(size, context.pool());
    auto nulls = allocateNulls(size, context.pool());
    result =
        BaseVector::wrapInDictionary(nulls, indices, size, std::move(constant));
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};
} // namespace

TEST_F(CastExprTest, dictionaryOverConst) {
  // Verify that cast properly handles an input where the vector has a
  // dictionary layer wrapped over a constant layer.
  exec::registerVectorFunction(
      "dictionary_over_const",
      TestingDictionaryOverConstFunction::signatures(),
      std::make_unique<TestingDictionaryOverConstFunction>());

  auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  auto result = evaluate(
      "cast(dictionary_over_const(c0) as smallint)", makeRowVector({data}));
  auto expected = makeNullableFlatVector<int16_t>({1, 1, 1, 1, 1});
  assertEqualVectors(expected, result);
}

namespace {
// Wrap input in a dictionary that point to subset of rows of the inner
// vector.
class TestingDictionaryToFewerRowsFunction : public exec::VectorFunction {
 public:
  TestingDictionaryToFewerRowsFunction() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto size = rows.size();
    auto indices =
        makeIndices(size, [](auto /*row*/) { return 0; }, context.pool());

    result = BaseVector::wrapInDictionary(nullptr, indices, size, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

} // namespace

TEST_F(CastExprTest, dictionaryEncodedNestedInput) {
  // Cast ARRAY<ROW<BIGINT>> to ARRAY<ROW<VARCHAR>> where the outermost ARRAY
  // layer and innermost BIGINT layer are dictionary-encoded. This test case
  // ensures that when casting the ROW<BIGINT> vector, the result ROW vector
  // would not be longer than the result VARCHAR vector. In the test below,
  // the ARRAY vector has 2 rows, each containing 3 elements. The ARRAY vector
  // is wrapped in a dictionary layer that only references its first row,
  // hence only the first 3 out of 6 rows are evaluated for the ROW and BIGINT
  // vector. The BIGINT vector is also dictionary-encoded, so CastExpr
  // produces a result VARCHAR vector of length 3. If the casting of the ROW
  // vector produces a result ROW<VARCHAR> vector of the length of all rows,
  // i.e., 6, the subsequent call to Expr::addNull() would throw due to the
  // attempt of accessing the element VARCHAR vector at indices corresonding
  // to the non-existent ROW at indices 3--5.
  exec::registerVectorFunction(
      "add_dict",
      TestingDictionaryToFewerRowsFunction::signatures(),
      std::make_unique<TestingDictionaryToFewerRowsFunction>(),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());

  auto elements = makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6});
  auto elementsInDict = BaseVector::wrapInDictionary(
      nullptr, makeIndices(6, [](auto row) { return row; }), 6, elements);
  auto row = makeRowVector({elementsInDict}, [](auto row) { return row == 2; });
  auto array = makeArrayVector({0, 3}, row);

  auto result = evaluate(
      "cast(add_dict(c0) as STRUCT(i varchar)[])", makeRowVector({array}));

  auto expectedElements = makeNullableFlatVector<StringView>(
      {"1"_sv, "2"_sv, "n"_sv, "1"_sv, "2"_sv, "n"_sv});
  auto expectedRow =
      makeRowVector({expectedElements}, [](auto row) { return row % 3 == 2; });
  auto expectedArray = makeArrayVector({0, 3}, expectedRow);
  assertEqualVectors(expectedArray, result);
}

TEST_F(CastExprTest, smallerNonNullRowsSizeThanRows) {
  // Evaluating Cast in Coalesce as the second argument triggers the copy of
  // Cast localResult to the result vector. The localResult vector is of the
  // size nonNullRows which can be smaller than rows. This test ensures that
  // Cast doesn't attempt to access values out-of-bound and hit errors.
  exec::registerVectorFunction(
      "add_dict_with_2_trailing_nulls",
      TestingDictionaryFunction::signatures(),
      std::make_unique<TestingDictionaryFunction>(2));

  auto data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4}),
       makeNullableFlatVector<double>({std::nullopt, 6, 7, std::nullopt})});
  auto result = evaluate(
      "coalesce(c1, cast(add_dict_with_2_trailing_nulls(c0) as double))", data);
  auto expected = makeNullableFlatVector<double>({4, 6, 7, std::nullopt});
  assertEqualVectors(expected, result);
}

TEST_F(CastExprTest, tryCastDoesNotHideInputsAndExistingErrors) {
  auto test = [&](const std::string& castExprThatThrow,
                  const std::string& type,
                  const auto& data) {
    ASSERT_THROW(
        auto result = evaluate(
            fmt::format("try_cast({} as {})", castExprThatThrow, type), data),
        VeloxException);

    ASSERT_NO_THROW(evaluate(
        fmt::format("try (cast ({} as {}))", castExprThatThrow, type), data));
    ASSERT_NO_THROW(evaluate(fmt::format("try_{}", castExprThatThrow), data));
    ASSERT_NO_THROW(evaluate(fmt::format("try ({})", castExprThatThrow), data));
  };

  {
    auto data = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})});
    test("cast('' as int)", "int", data);
  }

  {
    auto data =
        makeRowVector({makeArrayVector<StringView>({{"1", "", "3", "4"}})});
    test("cast(c0 as integer[])", "integer[]", data);
    test("cast(map(c0, c0) as map(int, int))", "map(int, int)", data);
    test(
        "cast(row_constructor(c0, c0, c0) as struct(a int[], b bigint[], c float[]))",
        "struct(a int[], b bigint[], c float[])",
        data);
  }

  {
    auto data = makeRowVector(
        {makeFlatVector<bool>({true, false, true, false}),
         makeFlatVector<StringView>({{"1", "2", "3", "4"}})});

    ASSERT_THROW(
        evaluate("switch(c0, cast('' as int), cast(c1 as integer))", data),
        VeloxException);

    ASSERT_THROW(
        evaluate("switch(c0, cast('' as int), try_cast(c1 as integer))", data),
        VeloxException);
    {
      auto result = evaluate(
          "try(switch(c0, cast('' as int), cast(c1 as integer)))", data);
      ASSERT_TRUE(result->isNullAt(0));
      ASSERT_TRUE(result->isNullAt(2));
    }

    {
      auto result = evaluate(
          "try(switch(c0, try_cast('' as int), cast(c1 as integer)))", data);
      ASSERT_TRUE(result->isNullAt(0));
      ASSERT_TRUE(result->isNullAt(2));
    }
  }
}

TEST_F(CastExprTest, lazyInput) {
  auto lazy =
      vectorMaker_.lazyFlatVector<int64_t>(5, [](auto row) { return row; });
  auto indices = makeIndices({0, 1, 2, 3, 4});
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 5, lazy);
  dictionary->loadedVector();
  auto data = makeRowVector(
      {dictionary,
       makeNullableFlatVector<int64_t>(
           {std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt})});

  evaluate("cast(switch(gt(c0, c1), c1, c0) as double)", data);
}

TEST_F(CastExprTest, identicalTypes) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10, folly::identity),
  });
  auto result = evaluate("cast(c0 as bigint)", data);
  ASSERT_EQ(result.get(), data->childAt(0).get());
}

TEST_F(CastExprTest, skipCastEvaluation) {
  // Inputs to error_on_odd_else_unknown are even, odd.
  // Input to cast is an UNKNOWN vector which is not supported.
  // Verify that input rows marked as errors are skipped.
  {
    auto data = makeRowVector({
        makeFlatVector<int>(10, [&](auto row) { return row; }),
    });

    VELOX_ASSERT_THROW(
        evaluate("try(cast(error_on_odd_else_unknown(c0) as INTEGER))", data),
        "not a scalar type! kind: UNKNOWN");
  }

  // All inputs to error_on_odd_else_unknown are odd.
  // All inputs to cast are errors, we skip evaluation.
  {
    auto data = makeRowVector({
        makeFlatVector<int>(10, [&](auto row) { return row * 2 + 1; }),
    });

    auto result =
        evaluate("try(cast(error_on_odd_else_unknown(c0) as INTEGER))", data);
    ASSERT_EQ(BaseVector::countNulls(result->nulls(), result->size()), 10);
  }

  // Ensure trailing rows that are marked as error are handled correctly as they
  // can result in intermediate result vectors of smaller size.
  {
    auto data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 0}),
    });
    auto result = evaluate("try(cast((c0 / c0) as VARCHAR))", data);
    auto expected =
        makeNullableFlatVector<StringView>({"1", "1", "1", std::nullopt});
    assertEqualVectors(result, expected);
  }
}

TEST_F(CastExprTest, intervalDayTimeToVarchar) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          {kMillisInDay,
           kMillisInHour,
           kMillisInMinute,
           kMillisInSecond,
           5 * kMillisInDay + 14 * kMillisInHour + 20 * kMillisInMinute +
               52 * kMillisInSecond + 88},
          INTERVAL_DAY_TIME()),
  });

  auto result = evaluate("cast(c0 as varchar)", data);
  auto expected = makeFlatVector<std::string>({
      "1 00:00:00.000",
      "0 01:00:00.000",
      "0 00:01:00.000",
      "0 00:00:01.000",
      "5 14:20:52.088",
  });

  assertEqualVectors(result, expected);

  // Reverse cast is not supported.
  VELOX_ASSERT_THROW(
      evaluate("cast('5 14:20:52.088' as interval day to second)", data),
      "Cast from VARCHAR to INTERVAL DAY TO SECOND is not supported");
}

} // namespace
} // namespace facebook::velox::test
