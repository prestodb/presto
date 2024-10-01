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
#include "velox/functions/sparksql/Register.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
namespace facebook::velox::test {
namespace {

class SparkCastExprTest : public functions::test::CastBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    functions::sparksql::registerFunctions("");
    memory::MemoryManager::testingSetInstance({});
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
      "date", {"-1-1-1"}, "Unable to parse date value: \"-1-1-1\"", VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"-11-1-1"},
      "Unable to parse date value: \"-11-1-1\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"-111-1-1"},
      "Unable to parse date value: \"-111-1-1\"",
      VARCHAR());
  testInvalidCast<std::string>(
      "date",
      {"- 1111-1-1"},
      "Unable to parse date value: \"- 1111-1-1\"",
      VARCHAR());
}

TEST_F(SparkCastExprTest, stringToTimestamp) {
  std::vector<std::optional<std::string>> input{
      "1970-01-01",
      "2000-01-01",
      "1970-01-01 00:00:00",
      "2000-01-01 12:21:56",
      std::nullopt,
      "2015-03-18T12:03:17",
      "2015-03-18 12:03:17",
      "2015-03-18T12:03:17",
      "2015-03-18 12:03:17.123",
      "2015-03-18T12:03:17.123",
      "2015-03-18T12:03:17.456",
      "2015-03-18 12:03:17.456",
  };
  std::vector<std::optional<Timestamp>> expected{
      Timestamp(0, 0),
      Timestamp(946684800, 0),
      Timestamp(0, 0),
      Timestamp(946729316, 0),
      std::nullopt,
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 0),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 123000000),
      Timestamp(1426680197, 456000000),
      Timestamp(1426680197, 456000000),
  };
  testCast<std::string, Timestamp>("timestamp", input, expected);
}

TEST_F(SparkCastExprTest, primitiveInvalidCornerCases) {
  // To integer.
  {
    // Invalid strings.
    testInvalidCast<std::string>(
        "tinyint",
        {"1234567"},
        "Cannot cast VARCHAR '1234567' to TINYINT. TINYINT overflow: 123 * 10");
    testInvalidCast<std::string>(
        "tinyint", {"1a"}, "Encountered a non-digit character");
    testInvalidCast<std::string>("tinyint", {""}, "Empty string");
    testInvalidCast<std::string>(
        "integer", {"1'234'567"}, "Encountered a non-digit character");
    testInvalidCast<std::string>(
        "integer", {"1,234,567"}, "Encountered a non-digit character");
    testInvalidCast<std::string>(
        "bigint", {"infinity"}, "Encountered a non-digit character");
    testInvalidCast<std::string>(
        "bigint", {"nan"}, "Encountered a non-digit character");
  }

  // To floating-point.
  {
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
        "boolean", {"1.7E308"}, "Cannot cast 1.7E308 to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"nan"}, "Cannot cast nan to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"infinity"}, "Cannot cast infinity to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"12"}, "Cannot cast 12 to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"-1"}, "Cannot cast -1 to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"tr"}, "Cannot cast tr to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"tru"}, "Cannot cast tru to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"on"}, "Cannot cast on to BOOLEAN");
    testInvalidCast<std::string>(
        "boolean", {"off"}, "Cannot cast off to BOOLEAN");
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

TEST_F(SparkCastExprTest, errorHandling) {
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
       "-128"},
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
       125,
       127,
       -128});

  testTryCast<double, int>(
      "integer",
      {1e12, 2.5, 3.6, 100.44, -100.101},
      {std::numeric_limits<int32_t>::max(), 2, 3, 100, -100});
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
          {-44, -4, 26, 56, -100, 0, 124, 117, 63, -78, -48, std::nullopt}));
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int8_t>({0}));
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int32_t>({-2147483648}));
  testCast(
      makeNullableFlatVector<int64_t>({214748364890}, DECIMAL(12, 2)),
      makeNullableFlatVector<int64_t>({2147483648}));

  testInvalidCast<std::string>(
      "tinyint",
      {"166"},
      "Cannot cast VARCHAR '166' to TINYINT. TINYINT overflow: 16 * 10");
  testInvalidCast<std::string>(
      "smallint",
      {"52769"},
      "Cannot cast VARCHAR '52769' to SMALLINT. SMALLINT overflow: 5276 * 10");
  testInvalidCast<std::string>(
      "integer",
      {"17515055537"},
      "Cannot cast VARCHAR '17515055537' to INTEGER. INTEGER overflow: 1751505553 * 10");
  testInvalidCast<std::string>(
      "integer",
      {"-17515055537"},
      "Cannot cast VARCHAR '-17515055537' to INTEGER. INTEGER overflow: -1751505553 * 10");
  testInvalidCast<std::string>(
      "bigint",
      {"9663372036854775809"},
      "Cannot cast VARCHAR '9663372036854775809' to BIGINT. BIGINT overflow: 966337203685477580 * 10");
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

} // namespace
} // namespace facebook::velox::test
