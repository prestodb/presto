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

#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
namespace facebook::velox::test {
namespace {

class SparkCastExprTest : public functions::test::CastBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    functions::sparksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
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
             -2 /*-2.6 truncated to -2*/,
             -2 /*-2.3 truncated to -2*/,
             -2,
             -1,
             0,
             55,
             57 /*57.49 truncated to 57*/,
             57 /*57.55 truncated to 57*/,
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
             -2 /*-2.55 truncated to -2*/,
             -2 /*-2.45 truncated to -2*/,
             -2,
             -1,
             0,
             55,
             55 /* 55.49 truncated to 55*/,
             55 /* 55.99 truncated to 55*/,
             69,
             72,
             std::nullopt}));
  }

  template <typename T>
  void testIntegralToTimestampCast() {
    testCast(
        makeNullableFlatVector<T>({
            0,
            1,
            std::numeric_limits<T>::max(),
            std::numeric_limits<T>::min(),
            std::nullopt,
        }),
        makeNullableFlatVector<Timestamp>(
            {Timestamp(0, 0),
             Timestamp(1, 0),
             Timestamp(std::numeric_limits<T>::max(), 0),
             Timestamp(std::numeric_limits<T>::min(), 0),
             std::nullopt}));
  }

  template <typename T>
  void testTimestampToIntegralCast() {
    testCast(
        makeFlatVector<Timestamp>({
            Timestamp(0, 0),
            Timestamp(1, 0),
            Timestamp(std::numeric_limits<T>::max(), 0),
            Timestamp(std::numeric_limits<T>::min(), 0),
        }),
        makeFlatVector<T>({
            0,
            1,
            std::numeric_limits<T>::max(),
            std::numeric_limits<T>::min(),
        }));
  }

  template <typename T>
  void testTimestampToIntegralCastOverflow(std::vector<T> expected) {
    testCast(
        makeFlatVector<Timestamp>({
            Timestamp(1740470426, 0),
            Timestamp(2147483647, 0),
            Timestamp(9223372036854, 775'807'000),
            Timestamp(-9223372036855, 224'192'000),
        }),
        makeFlatVector<T>(expected));
  }
};

TEST_F(SparkCastExprTest, date) {
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
       0,
       std::nullopt},
      VARCHAR(),
      DATE());
  testCast<std::string, int32_t>(
      "date",
      {"12345",
       "2015",
       "2015-03",
       "2015-03-18T",
       "2015-03-18T123123",
       "2015-03-18 123142",
       "2015-03-18 (BC)"},
      {3789391, 16436, 16495, 16512, 16512, 16512, 16512},
      VARCHAR(),
      DATE());
}

TEST_F(SparkCastExprTest, decimalToIntegral) {
  testDecimalToIntegralCasts<int64_t>();
  testDecimalToIntegralCasts<int32_t>();
  testDecimalToIntegralCasts<int16_t>();
  testDecimalToIntegralCasts<int8_t>();
}

TEST_F(SparkCastExprTest, invalidDate) {
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
  testCast<std::string, int32_t>(
      "date", {"2012-Oct-23"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"2015-03-18X"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"2015/03/18"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"2015.03.18"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"20150318"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"2015-031-8"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"-1-1-1"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"-11-1-1"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"-111-1-1"}, {std::nullopt}, VARCHAR(), DATE());
  testCast<std::string, int32_t>(
      "date", {"- 1111-1-1"}, {std::nullopt}, VARCHAR(), DATE());
}

TEST_F(SparkCastExprTest, stringToTimestamp) {
  std::vector<std::optional<std::string>> input{
      "1970-01-01",
      "1970-01-01 00:00:00-02:00",
      "1970-01-01 00:00:00 +02:00",
      "2000-01-01",
      "1970-01-01 00:00:00",
      "2000-01-01 12:21:56",
      std::nullopt,
      "2015-03-18T12:03:17",
      "2015-03-18T12:03:17Z",
      "2015-03-18 12:03:17",
      "2015-03-18T12:03:17",
      "2015-03-18 12:03:17.123",
      "2015-03-18T12:03:17.123",
      "2015-03-18T12:03:17.456",
      "2015-03-18 12:03:17.456",
  };
  std::vector<std::optional<Timestamp>> expected{
      Timestamp(0, 0),
      Timestamp(7200, 0),
      Timestamp(-7200, 0),
      Timestamp(946684800, 0),
      Timestamp(0, 0),
      Timestamp(946729316, 0),
      std::nullopt,
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000),
  };
  testCast<std::string, Timestamp>("timestamp", input, expected);

  setTimezone("Asia/Shanghai");
  testCast<std::string, Timestamp>(
      "timestamp",
      {"1970-01-01 00:00:00",
       "1970-01-01 08:00:00",
       "1970-01-01 08:00:59",
       "1970"},
      {Timestamp(-8 * 3600, 0),
       Timestamp(0, 0),
       Timestamp(59, 0),
       Timestamp(-8 * 3600, 0)});
}

TEST_F(SparkCastExprTest, intToTimestamp) {
  // Cast bigint as timestamp.
  testCast(
      makeNullableFlatVector<int64_t>({
          0,
          1727181032,
          -1727181032,
          9223372036855,
          -9223372036856,
          std::numeric_limits<int64_t>::max(),
          std::numeric_limits<int64_t>::min(),
      }),
      makeNullableFlatVector<Timestamp>({
          Timestamp(0, 0),
          Timestamp(1727181032, 0),
          Timestamp(-1727181032, 0),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
      }));

  // Cast tinyint/smallint/integer as timestamp.
  testIntegralToTimestampCast<int8_t>();
  testIntegralToTimestampCast<int16_t>();
  testIntegralToTimestampCast<int32_t>();
}

TEST_F(SparkCastExprTest, timestampToInt) {
  // Cast timestamp as bigint.
  testCast(
      makeFlatVector<Timestamp>(
          {Timestamp(0, 0),
           Timestamp(1, 0),
           Timestamp(10, 0),
           Timestamp(-1, 0),
           Timestamp(-10, 0),
           Timestamp(-1, 500000),
           Timestamp(-2, 999999),
           Timestamp(-10, 999999),
           Timestamp(1, 999999),
           Timestamp(-1, 1),
           Timestamp(1234567, 500000),
           Timestamp(-9876543, 1234),
           Timestamp(1727181032, 0),
           Timestamp(-1727181032, 0),
           Timestamp(9223372036854, 775'807'000),
           Timestamp(-9223372036855, 224'192'000),
           Timestamp(9223372036856, 0)}),
      makeNullableFlatVector<int64_t>({
          0,
          1,
          10,
          -1,
          -10,
          -1,
          -2,
          -10,
          1,
          -1,
          1234567,
          -9876543,
          1727181032,
          -1727181032,
          9223372036854,
          -9223372036855,
          std::nullopt,
      }));

  // Cast timestamp as tinyint/smallint/integer.
  testTimestampToIntegralCast<int8_t>();
  testTimestampToIntegralCast<int16_t>();
  testTimestampToIntegralCast<int32_t>();

  // Cast overflowed timestamp as tinyint/smallint/integer.
  testTimestampToIntegralCastOverflow<int8_t>({
      -102,
      -1,
      -10,
      9,
  });
  testTimestampToIntegralCastOverflow<int16_t>({
      30874,
      -1,
      23286,
      -23287,
  });
  testTimestampToIntegralCastOverflow<int32_t>({
      1740470426,
      2147483647,
      2077252342,
      -2077252343,
  });
}

TEST_F(SparkCastExprTest, doubleToTimestamp) {
  testCast(
      makeFlatVector<double>({
          0.0,
          1727181032.0,
          -1727181032.0,
          9223372036855.999,
          -9223372036856.999,
          1.79769e+308,
          std::numeric_limits<double>::max(),
          -std::numeric_limits<double>::max(),
          std::numeric_limits<double>::min(),
          kInf,
          kNan,
          -kInf,
      }),
      makeNullableFlatVector<Timestamp>({
          Timestamp(0, 0),
          Timestamp(1727181032, 0),
          Timestamp(-1727181032, 0),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(-9223372036855, 224'192'000),
          Timestamp(0, 0),
          std::nullopt,
          std::nullopt,
          std::nullopt,
      }));
}

TEST_F(SparkCastExprTest, floatToTimestamp) {
  testCast(
      makeFlatVector<float>({
          0.0,
          1727181032.0,
          -1727181032.0,
          std::numeric_limits<float>::max(),
          std::numeric_limits<float>::min(),
          kInf,
          kNan,
          -kInf,
      }),
      makeNullableFlatVector<Timestamp>({
          Timestamp(0, 0),
          Timestamp(1727181056, 0),
          Timestamp(-1727181056, 0),
          Timestamp(9223372036854, 775'807'000),
          Timestamp(0, 0),
          std::nullopt,
          std::nullopt,
          std::nullopt,
      }));
}

TEST_F(SparkCastExprTest, primitiveInvalidCornerCases) {
  // To integer.
  {
    // Invalid strings.
    testCast<std::string, int8_t>("tinyint", {"1234567"}, {std::nullopt});
    testCast<std::string, int8_t>("tinyint", {"1a"}, {std::nullopt});
    testCast<std::string, int8_t>("tinyint", {""}, {std::nullopt});
    testCast<std::string, int32_t>("integer", {"1'234'567"}, {std::nullopt});
    testCast<std::string, int32_t>("integer", {"1,234,567"}, {std::nullopt});
    testCast<std::string, int64_t>("bigint", {"infinity"}, {std::nullopt});
    testCast<std::string, int64_t>("bigint", {"nan"}, {std::nullopt});
  }

  // To floating-point.
  {
    // Invalid strings.
    testCast<std::string, float>("real", {"1.2a"}, {std::nullopt});
    testCast<std::string, float>("real", {"1.2.3"}, {std::nullopt});
  }

  // To boolean.
  {
    testCast<std::string, bool>("boolean", {"1.7E308"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"nan"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"12"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"-1"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"tr"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"tru"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"on"}, {std::nullopt});
    testCast<std::string, bool>("boolean", {"off"}, {std::nullopt});
  }
}

TEST_F(SparkCastExprTest, primitiveValidCornerCases) {
  // To integer.
  {
    // Valid strings.
    testCast<std::string, int8_t>("tinyint", {"1.2"}, {1});
    testCast<std::string, int8_t>("tinyint", {"1.23444"}, {1});
    testCast<std::string, int8_t>("tinyint", {".2355"}, {0});
    testCast<std::string, int8_t>("tinyint", {"-1.8"}, {-1});
    testCast<std::string, int8_t>("tinyint", {"+1"}, {1});
    testCast<std::string, int8_t>("tinyint", {"1."}, {1});
    testCast<std::string, int8_t>("tinyint", {"-1"}, {-1});
    testCast<std::string, int8_t>("tinyint", {"-1."}, {-1});
    testCast<std::string, int8_t>("tinyint", {"0."}, {0});
    testCast<std::string, int8_t>("tinyint", {"."}, {0});
    testCast<std::string, int8_t>("tinyint", {"-."}, {0});

    testCast<int32_t, int8_t>("tinyint", {1234567}, {-121});
    testCast<int32_t, int8_t>("tinyint", {-1234567}, {121});
    testCast<double, int8_t>("tinyint", {12345.67}, {57});
    testCast<double, int8_t>("tinyint", {-12345.67}, {-57});
    testCast<double, int8_t>("tinyint", {127.1}, {127});
    testCast<float, int64_t>("bigint", {kInf}, {9223372036854775807});
    testCast<float, int64_t>("bigint", {kNan}, {0});
    testCast<float, int32_t>("integer", {kNan}, {0});
    testCast<float, int16_t>("smallint", {kNan}, {0});
    testCast<float, int8_t>("tinyint", {kNan}, {0});

    testCast<double, int64_t>("bigint", {12345.12}, {12345});
    testCast<double, int64_t>("bigint", {12345.67}, {12345});
  }

  // To floating-point.
  {
    testCast<double, float>("real", {1.7E308}, {kInf});

    testCast<std::string, float>("real", {"1.7E308"}, {kInf});
    testCast<std::string, float>("real", {"1."}, {1.0});
    testCast<std::string, float>("real", {"1"}, {1});
    testCast<std::string, float>("real", {"infinity"}, {kInf});
    testCast<std::string, float>("real", {"-infinity"}, {-kInf});
    testCast<std::string, float>("real", {"nan"}, {kNan});
    testCast<std::string, float>("real", {"InfiNiTy"}, {kInf});
    testCast<std::string, float>("real", {"-InfiNiTy"}, {-kInf});
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
    testCast<float, bool>("boolean", {kNan}, {false});
    testCast<float, bool>("boolean", {kInf}, {true});
    testCast<double, bool>("boolean", {0.0000000000001}, {true});

    testCast<std::string, bool>("boolean", {"1"}, {true});
    testCast<std::string, bool>("boolean", {"t"}, {true});
    testCast<std::string, bool>("boolean", {"y"}, {true});
    testCast<std::string, bool>("boolean", {"yes"}, {true});
    testCast<std::string, bool>("boolean", {"true"}, {true});

    testCast<std::string, bool>("boolean", {"0"}, {false});
    testCast<std::string, bool>("boolean", {"f"}, {false});
    testCast<std::string, bool>("boolean", {"n"}, {false});
    testCast<std::string, bool>("boolean", {"no"}, {false});
    testCast<std::string, bool>("boolean", {"false"}, {false});
  }

  // To string.
  {
    testCast<float, std::string>("varchar", {kInf}, {"Infinity"});
    testCast<float, std::string>("varchar", {kNan}, {"NaN"});
  }
}

TEST_F(SparkCastExprTest, truncate) {
  // Testing truncate cast from double to int.
  testCast<int32_t, int8_t>(
      "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5});
}

TEST_F(SparkCastExprTest, tryCast) {
  testTryCast<std::string, int8_t>(
      "tinyint",
      {"-",
       "-0",
       " @w 123",
       "123 ",
       "  122",
       "",
       "-12-3",
       "1234",
       "-129",
       "1.1.1",
       "1..",
       "1.abc",
       "..",
       "-..",
       "125.5",
       "127",
       "-128",
       "1.2"},
      {std::nullopt,
       0,
       std::nullopt,
       123,
       122,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       127,
       -128,
       std::nullopt});

  testTryCast<double, int32_t>(
      "integer",
      {1e12, 2.5, 3.6, 100.44, -100.101},
      {std::nullopt, 2, 3, 100, -100});
  testTryCast<int64_t, int8_t>("tinyint", {456}, {std::nullopt});
  testTryCast<int64_t, int16_t>("smallint", {1234567}, {std::nullopt});
  testTryCast<int64_t, int32_t>("integer", {2147483649}, {std::nullopt});

  testTryCast<std::string, int16_t>("smallint", {"52769"}, {std::nullopt});
  testTryCast<std::string, int32_t>("integer", {"17515055537"}, {std::nullopt});
  testTryCast<std::string, int32_t>(
      "integer", {"-17515055537"}, {std::nullopt});
  testTryCast<std::string, int64_t>(
      "bigint", {"9663372036854775809"}, {std::nullopt});
  testTryCast<int64_t, int8_t>("tinyint", {456}, {std::nullopt}, DECIMAL(6, 0));
}

TEST_F(SparkCastExprTest, overflow) {
  testCast<int16_t, int8_t>("tinyint", {456}, {-56});
  testCast<int32_t, int8_t>("tinyint", {266}, {10});
  testCast<int64_t, int8_t>("tinyint", {1234}, {-46});
  testCast<int64_t, int16_t>("smallint", {1234567}, {-10617});
  testCast<double, int8_t>("tinyint", {127.8}, {127});
  testCast<double, int8_t>("tinyint", {129.9}, {-127});
  testCast<double, int16_t>("smallint", {1234567.89}, {-10617});
  testCast<double, int64_t>(
      "bigint", {std::numeric_limits<double>::max()}, {9223372036854775807});
  testCast<double, int64_t>(
      "bigint", {std::numeric_limits<double>::quiet_NaN()}, {0});
  auto shortFlat = makeNullableFlatVector<int64_t>(
      {-3000,
       -2600,
       -2300,
       -2000,
       -1000,
       0,
       55000,
       57490,
       5755,
       6900,
       7200,
       std::nullopt},
      DECIMAL(5, 1));
  testCast(
      shortFlat,
      makeNullableFlatVector<int8_t>(
          {-44, -4, 26, 56, -100, 0, 124, 117, 63, -78, -48, std::nullopt}),
      false);
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int8_t>({0}),
      false);
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int32_t>({-2147483648}),
      false);
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int64_t>({2147483648}),
      false);

  testCast<std::string, int8_t>("tinyint", {"166"}, {std::nullopt});
  testCast<std::string, int16_t>("smallint", {"52769"}, {std::nullopt});
  testCast<std::string, int32_t>("integer", {"17515055537"}, {std::nullopt});
  testCast<std::string, int32_t>("integer", {"-17515055537"}, {std::nullopt});
  testCast<std::string, int64_t>(
      "bigint", {"9663372036854775809"}, {std::nullopt});
}

TEST_F(SparkCastExprTest, timestampToString) {
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
          Timestamp(946729316, 100000000),
          Timestamp(946729316, 129900000),
          Timestamp(946729316, 123456789),
          Timestamp(7266, 0),
          Timestamp(-50049331200, 0),
          Timestamp(253405036800, 0),
          Timestamp(-62480037600, 0),
          std::nullopt,
      },
      {
          "1940-01-02 00:00:00",
          "1969-12-31 21:58:54",
          "1970-01-01 00:00:00",
          "2000-01-01 00:00:00",
          "2269-12-29 00:00:00",
          "4969-12-04 00:00:00",
          "2000-01-01 12:21:56",
          "2000-01-01 12:21:56",
          "2000-01-01 12:21:56.1",
          "2000-01-01 12:21:56.1299",
          "2000-01-01 12:21:56.123456",
          "1970-01-01 02:01:06",
          "0384-01-01 08:00:00",
          "+10000-02-01 16:00:00",
          "-0010-02-01 10:00:00",
          std::nullopt,
      });

  std::vector<std::optional<Timestamp>> input = {
      Timestamp(-946684800, 0),
      Timestamp(-7266, 0),
      Timestamp(0, 0),
      Timestamp(61, 10),
      Timestamp(3600, 0),
      Timestamp(946684800, 0),

      Timestamp(946729316, 0),
      Timestamp(946729316, 123),
      Timestamp(946729316, 100000000),
      Timestamp(946729316, 129900000),
      Timestamp(946729316, 123456789),
      Timestamp(7266, 0),
      std::nullopt,
  };

  setTimezone("America/Los_Angeles");
  testCast<Timestamp, std::string>(
      "string",
      input,
      {
          "1940-01-01 16:00:00",
          "1969-12-31 13:58:54",
          "1969-12-31 16:00:00",
          "1969-12-31 16:01:01",
          "1969-12-31 17:00:00",
          "1999-12-31 16:00:00",
          "2000-01-01 04:21:56",
          "2000-01-01 04:21:56",
          "2000-01-01 04:21:56.1",
          "2000-01-01 04:21:56.1299",
          "2000-01-01 04:21:56.123456",
          "1969-12-31 18:01:06",
          std::nullopt,
      });
  setTimezone("Asia/Shanghai");
  testCast<Timestamp, std::string>(
      "string",
      input,
      {
          "1940-01-02 08:00:00",
          "1970-01-01 05:58:54",
          "1970-01-01 08:00:00",
          "1970-01-01 08:01:01",
          "1970-01-01 09:00:00",
          "2000-01-01 08:00:00",
          "2000-01-01 20:21:56",
          "2000-01-01 20:21:56",
          "2000-01-01 20:21:56.1",
          "2000-01-01 20:21:56.1299",
          "2000-01-01 20:21:56.123456",
          "1970-01-01 10:01:06",
          std::nullopt,
      });
}

TEST_F(SparkCastExprTest, fromString) {
  // String with leading and trailing whitespaces.
  testCast<std::string, int8_t>(
      "tinyint", {"\n\f\r\t\n\u001F 123\u000B\u001C\u001D\u001E"}, {123});
  testCast<std::string, int32_t>(
      "integer", {"\n\f\r\t\n\u001F 123\u000B\u001C\u001D\u001E"}, {123});
  testCast<std::string, int64_t>(
      "bigint", {"\n\f\r\t\n\u001F 123\u000B\u001C\u001D\u001E"}, {123});
  testCast<std::string, int32_t>(
      "date",
      {"\n\f\r\t\n\u001F 2015-03-18T\u000B\u001C\u001D\u001E"},
      {16512},
      VARCHAR(),
      DATE());
  testCast<std::string, float>(
      "real", {"\n\f\r\t\n\u001F 123.0\u000B\u001C\u001D\u001E"}, {123.0});
  testCast<std::string, double>(
      "double", {"\n\f\r\t\n\u001F 123.0\u000B\u001C\u001D\u001E"}, {123.0});
  testCast<std::string, Timestamp>(
      "timestamp",
      {"\n\f\r\t\n\u001F 2000-01-01 12:21:56\u000B\u001C\u001D\u001E"},
      {Timestamp(946729316, 0)});
  testCast(
      makeFlatVector<StringView>(
          {" 9999999999.99",
           "9999999999.99 ",
           "\n\f\r\t\n\u001F 9999999999.99\u000B\u001C\u001D\u001E",
           " -3E+2",
           "-3E+2 ",
           "\u000B\u001C\u001D-3E+2\u001E\n\f\r\t\n\u001F "}),
      makeFlatVector<int64_t>(
          {999'999'999'999,
           999'999'999'999,
           999'999'999'999,
           -30000,
           -30000,
           -30000},
          DECIMAL(12, 2)));
}

TEST_F(SparkCastExprTest, tinyintToBinary) {
  testCast<int8_t, std::string>(
      TINYINT(),
      VARBINARY(),
      {18,
       -26,
       0,
       110,
       std::numeric_limits<int8_t>::max(),
       std::numeric_limits<int8_t>::min()},
      {std::string("\x12", 1),
       std::string("\xE6", 1),
       std::string("\0", 1),
       std::string("\x6E", 1),
       std::string("\x7F", 1),
       std::string("\x80", 1)});
}

TEST_F(SparkCastExprTest, smallintToBinary) {
  testCast<int16_t, std::string>(
      SMALLINT(),
      VARBINARY(),
      {180,
       -199,
       0,
       12300,
       std::numeric_limits<int16_t>::max(),
       std::numeric_limits<int16_t>::min()},
      {std::string("\0\xB4", 2),
       std::string("\xFF\x39", 2),
       std::string("\0\0", 2),
       std::string("\x30\x0C", 2),
       std::string("\x7F\xFF", 2),
       std::string("\x80\00", 2)});
}

TEST_F(SparkCastExprTest, integerToBinary) {
  testCast<int32_t, std::string>(
      INTEGER(),
      VARBINARY(),
      {18,
       -26,
       0,
       180000,
       std::numeric_limits<int32_t>::max(),
       std::numeric_limits<int32_t>::min()},
      {std::string("\0\0\0\x12", 4),
       std::string("\xFF\xFF\xFF\xE6", 4),
       std::string("\0\0\0\0", 4),
       std::string("\0\x02\xBF\x20", 4),
       std::string("\x7F\xFF\xFF\xFF", 4),
       std::string("\x80\0\0\0", 4)});
}

TEST_F(SparkCastExprTest, bigintToBinary) {
  testCast<int64_t, std::string>(
      BIGINT(),
      VARBINARY(),
      {123456,
       -256789,
       0,
       180000,
       std::numeric_limits<int64_t>::max(),
       std::numeric_limits<int64_t>::min()},
      {std::string("\0\0\0\0\0\x01\xE2\x40", 8),
       std::string("\xFF\xFF\xFF\xFF\xFF\xFC\x14\xEB", 8),
       std::string("\0\0\0\0\0\0\0\0", 8),
       std::string("\0\0\0\0\0\x02\xBF\x20", 8),
       std::string("\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8),
       std::string("\x80\x00\x00\x00\x00\x00\x00\x00", 8)});
}

TEST_F(SparkCastExprTest, boolToTimestamp) {
  testCast(
      makeFlatVector<bool>({true, false}),
      makeFlatVector<Timestamp>({
          Timestamp(0, 1000),
          Timestamp(0, 0),
      }));
}

TEST_F(SparkCastExprTest, recursiveTryCast) {
  // Test array elements.
  testCast(
      makeArrayVector<StringView>({
          {"1", "2", "3"},
          {"4", "a", "6"},
          {"b", "c", "d"},
      }),
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {4, std::nullopt, 6},
          {std::nullopt, std::nullopt, std::nullopt},
      }));

  // Test map values (Spark doesn't allow casting if the map keys can become
  // null).
  testCast(
      makeMapVectorFromJson<int64_t, std::string>({
          R"( {1:"1", 2:"2", 3:"3"} )",
          R"( {1:"4", 2:"a", 3:"6"} )",
          R"( {1:"b", 2:"c", 3:"d"} )",
      }),
      makeMapVectorFromJson<int64_t, int64_t>({
          "{1:1, 2:2, 3:3}",
          "{1:4, 2:null, 3:6}",
          "{1:null, 2:null, 3:null}",
      }));

  // Test row fields.
  testCast(
      makeRowVector(
          {makeFlatVector<StringView>({"1", "4", "b"}),
           makeFlatVector<StringView>({"2", "a", "c"}),
           makeFlatVector<StringView>({"3", "6", "d"})}),
      makeRowVector(
          {makeNullableFlatVector<int64_t>({1, 4, std::nullopt}),
           makeNullableFlatVector<int64_t>({2, std::nullopt, std::nullopt}),
           makeNullableFlatVector<int64_t>({3, 6, std::nullopt})}));

  // Test nested arrays.
  testCast(
      makeNestedArrayVectorFromJson<std::string>({
          R"( [["1", "2", "3"], ["4", "a", "6"]] )",
          R"( [["b", "c", "d"], ["x", "7", "z"]] )",
      }),
      makeNestedArrayVectorFromJson<int64_t>({
          "[[1, 2, 3], [4, null, 6]]",
          "[[null, null, null], [null, 7, null]]",
      }));
}

} // namespace
} // namespace facebook::velox::test
