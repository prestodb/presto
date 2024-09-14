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
#include "velox/type/Conversions.h"
#include "gtest/gtest.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/CppToType.h"

using namespace facebook::velox;

namespace facebook::velox::util {
namespace {

constexpr float kInf = std::numeric_limits<float>::infinity();
constexpr float kNan = std::numeric_limits<float>::quiet_NaN();

class ConversionsTest : public testing::Test {
 protected:
  template <typename TFrom, typename TTo>
  static void testConversion(
      std::vector<TFrom> input,
      std::vector<TTo> expectedResult,
      bool truncate = false,
      bool legacyCast = false,
      bool expectError = false) {
    if (!expectError) {
      VELOX_CHECK_EQ(input.size(), expectedResult.size());
    }
    const TypeKind toTypeKind = CppToType<TTo>::typeKind;

    auto cast = [&](TFrom input) -> TTo {
      Expected<TTo> result;
      if (truncate & legacyCast) {
        VELOX_NYI("No associated cast policy for truncate and legacy cast.");
      } else if (!truncate & legacyCast) {
        result = Converter<toTypeKind, void, LegacyCastPolicy>::tryCast(input);
      } else if (truncate & !legacyCast) {
        result = Converter<toTypeKind, void, SparkCastPolicy>::tryCast(input);
      } else {
        result = Converter<toTypeKind, void, PrestoCastPolicy>::tryCast(input);
      }

      return result.thenOrThrow(folly::identity, [](const Status& status) {
        VELOX_USER_FAIL(status.message());
      });
    };

    for (auto i = 0; i < input.size(); i++) {
      if (expectError) {
        ASSERT_ANY_THROW(cast(input[i])) << "Converting input at " << i;
      } else {
        TTo actual = cast(input[i]);
        TTo expected = expectedResult[i];
        if constexpr (std::is_floating_point_v<TTo>) {
          if (std::isnan(actual)) {
            ASSERT_TRUE(std::isnan(expected))
                << ", when actual is NaN, and converting input at " << i;
          } else if (std::isnan(expected)) {
            ASSERT_TRUE(std::isnan(actual))
                << ", when expected is NaN, and converting input at " << i;
          } else {
            ASSERT_EQ(actual, expected) << "Converting input at " << i;
          }
        } else {
          ASSERT_EQ(actual, expected) << "Converting input at " << i;
        }
      }
    }
  }
};

TEST_F(ConversionsTest, toBoolean) {
  // From integral types.
  {
    // When TRUNCATE = false.
    testConversion<int8_t, bool>(
        {
            1,
            0,
            12,
            -1,
        },
        {
            true,
            false,
            true,
            true,
        },
        /*truncate*/ false);

    // When TRUNCATE = true.
    testConversion<int8_t, bool>(
        {
            1,
            0,
            12,
            -1,
        },
        {
            true,
            false,
            true,
            true,
        },
        /*truncate*/ true);
  }

  // From double.
  {
    // When TRUNCATE = false.
    testConversion<double, bool>(
        {
            1.0,
            1.1,
            -1.0,
            0.0000000000001,
        },
        {
            true,
            true,
            true,
            true,
        },
        /*truncate*/ false);

    // When TRUNCATE = true.
    testConversion<double, bool>(
        {
            1.0,
            1.1,
            -1.0,
            0.0000000000001,
        },
        {
            true,
            true,
            true,
            true,
        },
        /*truncate*/ true);
  }

  // From float.
  {
    // When TRUNCATE = false.
    testConversion<float, bool>(
        {
            0.1,
            0.0,
            -0.1,
            kInf,
            kNan,
        },
        {
            true,
            false,
            true,
            true,
            true,
        },
        /*truncate*/ false);

    // When TRUNCATE = true.
    testConversion<float, bool>(
        {
            0.1,
            0.0,
            -0.1,
            kInf,
            kNan,
        },
        {
            true,
            false,
            true,
            true,
            false,
        },
        /*truncate*/ true);
  }

  // From boolean.
  {
    testConversion<bool, bool>(
        {
            true,
            false,
        },
        {
            true,
            false,
        });
  }

  // From string.
  {
    // When TRUNCATE = false.
    testConversion<std::string, bool>(
        {
            "1",
            "0",
            "t",
            "true",
            "f",
            "false",
        },
        {
            true,
            false,
            true,
            true,
            false,
            false,
        },
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<std::string, bool>(
        {
            "1.7E308",
            "nan",
            "infinity",
            "12",
            "-1",
            "tr",
            "tru",
        },
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<std::string, bool>(
        {
            "1",
            "0",
            "t",
            "true",
            "f",
            "false",
        },
        {
            true,
            false,
            true,
            true,
            false,
            false,
        },
        /*truncate*/ true);

    // When TRUNCATE = true, invalid cases.
    testConversion<std::string, bool>(
        {
            "1.7E308",
            "nan",
            "infinity",
            "12",
            "-1",
            "tr",
            "tru",
        },
        {},
        /*truncate*/ true,
        false,
        /*expectError*/ true);
  }

  // From timestamp.
  {
    // When TRUNCATE = false, invalid cases.
    testConversion<Timestamp, bool>(
        {Timestamp(946729316, 123)},
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);

    // When TRUNCATE = true, invalid cases.
    testConversion<Timestamp, bool>(
        {Timestamp(946729316, 123)},
        {},
        /*truncate*/ true,
        false,
        /*expectError*/ true);
  }
}

TEST_F(ConversionsTest, toIntegeralTypes) {
  // From double.
  {
    // When TRUNCATE = false.
    testConversion<double, int64_t>(
        {
            12345.12,
            12345.67,
        },
        {
            12345,
            12346,
        },
        /*truncate*/ false);
    testConversion<double, int32_t>(
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
        },
        {
            2,
            3,
            4,
            100,
            -100,
        },
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<double, int8_t>(
        {
            12345.67,
            -12345.67,
            127.8,
        },
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<double, int64_t>(
        {
            12345.12,
            12345.67,
        },
        {
            12345,
            12345,
        },
        /*truncate*/ true);
    testConversion<double, int32_t>(
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
        },
        {
            1,
            2,
            3,
            100,
            -100,
        },
        /*truncate*/ true);
    testConversion<double, int8_t>(
        {
            12345.67,
            -12345.67,
            127.8,
        },
        {
            57,
            -57,
            127,
        },
        /*truncate*/ true);
    testConversion<double, int8_t>(
        {
            1,
            256,
            257,
            2147483646,
            2147483647,
            2147483648,
            -2147483646,
            -2147483647,
            -2147483648,
            -2147483649,
        },
        {
            1,
            0,
            1,
            -2,
            -1,
            -1,
            2,
            1,
            0,
            0,
        },
        /*truncate*/ true);
  }

  // From float.
  {
    // When TRUNCATE = false, invalid cases.
    testConversion<float, int64_t>(
        {kInf, kNan}, {}, /*truncate*/ false, false, /*expectError*/ true);
    testConversion<float, int32_t>(
        {kNan}, {}, /*truncate*/ false, false, /*expectError*/ true);
    testConversion<float, int16_t>(
        {kNan}, {}, /*truncate*/ false, false, /*expectError*/ true);
    testConversion<float, int8_t>(
        {kNan}, {}, /*truncate*/ false, false, /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<float, int64_t>(
        {kInf, kNan}, {9223372036854775807, 0}, /*truncate*/ true);
    testConversion<float, int32_t>({kNan}, {0}, /*truncate*/ true);
    testConversion<float, int16_t>({kNan}, {0}, /*truncate*/ true);
    testConversion<float, int8_t>({kNan}, {0}, /*truncate*/ true);
  }

  // From string.
  {
    // When TRUNCATE = false.
    testConversion<std::string, int16_t>(
        {
            "1",
            "+1",
            "-100",
            "\u000f100",
            "\u000f100\u000f",
            // unicode space's interspaced with control chars
            " -100\u000f",
            " \u000f+100",
            "\u001f101\u000e",
            " \u001f-102\u000f ",
        },
        {1, 1, -100, 100, 100, -100, 100, 101, -102},
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<std::string, int8_t>(
        {
            "1.2",
            "1.23444",
            ".2355",
            "-1.8",
            "1.",
            "-1.",
            "0.",
            ".",
            "-.",
        },
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);
    testConversion<std::string, int8_t>(
        {"1234567"}, {}, /*truncate*/ false, false, /*expectError*/ true);
    testConversion<std::string, int64_t>(
        {"1a",
         "",
         "1'234'567",
         "1,234,567",
         "infinity",
         "nan",
         // Unicode spaces and control chars and unicode at the end
         // Should fail conversion since we do not support unicode digits
         " \u001f-103٢\u000f ",
         // All spaces
         "   \u000f "},
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<std::string, int8_t>(
        {
            "1.2",
            "1.23444",
            ".2355",
            "-1.8",
            "1.",
            "-1.",
            "0.",
            ".",
            "-.",
            "+1",
        },
        {
            1,
            1,
            0,
            -1,
            1,
            -1,
            0,
            0,
            0,
            1,
        },
        /*truncate*/ true);
    testConversion<std::string, int64_t>(
        {
            "1a",
            "",
            "1'234'567",
            "1,234,567",
            "infinity",
            "nan",
        },
        {},
        /*truncate*/ true,
        false,
        /*expectError*/ true);
  }

  // From integral types.
  {
    // When TRUNCATE = false.
    testConversion<int32_t, int8_t>(
        {
            2,
            3,
        },
        {
            2,
            3,
        },
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<int32_t, int8_t>(
        {
            1234567,
            -1234567,
            1111111,
            1000,
            -100101,
        },
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<int32_t, int8_t>(
        {
            1234567,
            -1234567,
            1111111,
            2,
            3,
            1000,
            -100101,
        },
        {
            -121,
            121,
            71,
            2,
            3,
            -24,
            -5,
        },
        /*truncate*/ true);
  }

  // From boolean
  {
    testConversion<bool, int8_t>(
        {
            true,
            false,
        },
        {
            1,
            0,
        });
  }
}

TEST_F(ConversionsTest, toString) {
  // From integral types.
  {
    testConversion<int32_t, std::string>(
        {
            1,
            2,
            3,
            100,
            -100,
        },
        {
            "1",
            "2",
            "3",
            "100",
            "-100",
        });
  }

  // From double.
  {
    // When LEGACY_CAST = false.
    testConversion<double, std::string>(
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
        },
        false,
        /*legacyCast*/ false);

    // When LEGACY_CAST = true.
    testConversion<double, std::string>(
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
        },
        false,
        /*legacyCast*/ true);
  }

  // From float.
  {
    // When LEGACY_CAST = false.
    testConversion<float, std::string>(
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
        },
        false,
        /*legacyCast*/ false);

    // When LEGACY_CAST = true.
    testConversion<float, std::string>(
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
        },
        false,
        /*legacyCast*/ true);
  }

  // From Timestamp.
  {
    // When LEGACY_CAST = false.
    testConversion<Timestamp, std::string>(
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
        },
        false,
        /*legacyCast*/ false);

    // When LEGACY_CAST = true.
    testConversion<Timestamp, std::string>(
        {
            Timestamp(946729316, 123),
            Timestamp(-50049331200, 0),
            Timestamp(253405036800, 0),
            Timestamp(-62480037600, 0),
        },
        {
            "2000-01-01T12:21:56.000",
            "384-01-01T08:00:00.000",
            "10000-02-01T16:00:00.000",
            "-10-02-01T10:00:00.000",
        },
        false,
        /*legacyCast*/ true);
  }
}

TEST_F(ConversionsTest, toRealAndDouble) {
  // From integral types.
  {
    testConversion<int8_t, float>({1}, {1.0}, /*truncate*/ false);
    testConversion<int32_t, double>(
        {
            1,
            2,
            3,
            100,
            -100,
        },
        {
            1.0,
            2.0,
            3.0,
            100.0,
            -100.0,
        });
  }

  // From double.
  {
    // When TRUNCATE = false.
    testConversion<double, float>(
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
            1.0,
            -2.0,
        },
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
            1.0,
            -2.0,
        },
        /*truncate*/ false);
    testConversion<double, double>(
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
            1.0,
            -2.0,
        },
        {
            1.888,
            2.5,
            3.6,
            100.44,
            -100.101,
            1.0,
            -2.0,
        },
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<double, float>(
        {1.7E308}, {}, /*truncate*/ false, false, /*expectError*/ true);

    // When TRUNCATE = true.
    testConversion<double, float>({1.7E308}, {kInf}, /*truncate*/ true);
  }

  // From string.
  {
    // When TRUNCATE = false.
    testConversion<std::string, float>(
        {
            "1.7E308",   "1.",           "1",
            ".1324",     "1.2345678E19", "1.2345678E8",
            "1.0E7",     "12345.0",      "0.001",
            "1.2E-4",    "0.0",          "-0.0",
            "-1.2E-4",   "-0.001",       "-12345.0",
            "-1.0E7",    "-1.2345678E8", "-1.2345678E19",
            " 123",      "123 ",         " 123 ",
            "infinity",  "-infinity",    "InfiNiTy",
            "-InfiNiTy", "Inf",          "-Inf",
            "nan",       "nAn",
        },
        {
            kInf,
            1.0,
            1.0,
            0.1324,
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
            123.0,
            123.0,
            123.0,
            kInf,
            -kInf,
            kInf,
            -kInf,
            kInf,
            -kInf,
            kNan,
            kNan,
        },
        /*truncate*/ false);

    // When TRUNCATE = false, invalid cases.
    testConversion<std::string, float>(
        {
            "1.2a",
            "1.2.3",
            "1.2EE4",
            "123.4f",
            "123.4F",
            "123.4d",
            "123.4D",
            "In",
            "Infx",
            "-Infx",
            "nanx",
            "na",
            "infinit",
            "infinityx",
            "-infinityx",
        },
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);
  }

  // From Timestamp.
  {
    // Invalid cases.
    testConversion<Timestamp, float>(
        {Timestamp(946729316, 123)},
        {},
        /*truncate*/ false,
        false,
        /*expectError*/ true);
  }
}

TEST_F(ConversionsTest, toTimestamp) {
  // From string.
  {
    testConversion<std::string, Timestamp>(
        {
            "1970-01-01",
            "2000-01-01",
            "1970-01-01 00:00:00",
            "2000-01-01 12:21:56",
            "1970-01-01 00:01",
        },
        {
            Timestamp(0, 0),
            Timestamp(946684800, 0),
            Timestamp(0, 0),
            Timestamp(946729316, 0),
            Timestamp(60, 0),
        });

    // Invalid case.
    testConversion<std::string, Timestamp>(
        {"2012-Oct-01"}, {}, false, false, /*expectError*/ true);
  }

  // From integral types, invalid cases.
  {
    testConversion<int8_t, Timestamp>(
        {123}, {}, false, false, /*expectError*/ true);
    testConversion<int16_t, Timestamp>(
        {12345}, {}, false, false, /*expectError*/ true);
    testConversion<int32_t, Timestamp>(
        {123456}, {}, false, false, /*expectError*/ true);
    testConversion<int64_t, Timestamp>(
        {123456}, {}, false, false, /*expectError*/ true);
  }

  // From floating-point types, invalid cases.
  {
    testConversion<float, Timestamp>(
        {123456.78}, {}, false, false, /*expectError*/ true);
    testConversion<double, Timestamp>(
        {123456.78}, {}, false, false, /*expectError*/ true);
  }
}
} // namespace
} // namespace facebook::velox::util
