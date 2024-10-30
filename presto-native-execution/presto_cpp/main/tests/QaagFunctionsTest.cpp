#include "presto_cpp/main/governance/wkc_functions/QaagFunctionsRegistration.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class QaagFunctionsTest : public functions::test::FunctionBaseTest {
 public:
  void SetUp() override {
    facebook::presto::governance::registerQaagFunctions("");
  }

  int64_t testMaskBigint(
      std::optional<int64_t> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<int64_t>(
               "wkcgovernmask_bigint(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  int32_t testMaskInteger(
      std::optional<int32_t> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<int32_t>(
               "wkcgovernmask_integer(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  int16_t testMaskSmallint(
      std::optional<int16_t> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<int16_t>(
               "wkcgovernmask_smallint(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  double testMaskDouble(
      std::optional<double> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<double>(
               "wkcgovernmask_double(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  float testMaskReal(
      std::optional<float> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<float>(
               "wkcgovernmask_real(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  template <typename T>
  void testMaskDecimal(const RowVectorPtr& input, const VectorPtr& expected) {
    std::string maskingFunction;
    if constexpr (std::is_same_v<T, int128_t>) {
      maskingFunction = "wkcgovernmask_decimal_long";
    } else {
      maskingFunction = "wkcgovernmask_decimal_short";
    }
    auto result = evaluate(
        fmt::format("{}(c0, c1, c2, c3, c4, c5, c6, c7)", maskingFunction),
        input);
    test::assertEqualVectors(expected, result);
  };

  template <typename T>
  void testMaskStringType(
      const RowVectorPtr& input,
      const VectorPtr& expected) {
    std::string maskingFunctionCall;
    if constexpr (std::is_same_v<T, Varchar>) {
      maskingFunctionCall = "wkcgovernmask_varchar(c0";
    } else {
      maskingFunctionCall = "wkcgovernmask_varbinary(from_hex(c0)";
    }
    auto result = evaluate(
        fmt::format("{}, c1, c2, c3, c4, c5)", maskingFunctionCall), input);
    test::assertEqualVectors(expected, result);
  };

  Timestamp testMaskTimestamp(
      std::optional<Timestamp> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<Timestamp>(
               "wkcgovernmask_timestamp(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  int32_t testMaskDate(
      std::optional<int32_t> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<int32_t>(
               "wkcgovernmask_date(c0, c1, c2, c3, c4, c5)",
               {DATE(), INTEGER(), VARCHAR(), VARCHAR(), VARCHAR(), INTEGER()},
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  bool testMaskBool(
      std::optional<bool> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<bool>(
               "wkcgovernmask_boolean(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  Timestamp testMaskTimestampVarchar(
      std::optional<StringView> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<Timestamp>(
               "wkcgovernmask_timestamp(c0, c1, c2, c3, c4, c5)",
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };

  int32_t testMaskDateVarchar(
      std::optional<StringView> input,
      std::optional<int32_t> inputMask,
      std::optional<StringView> maskParams,
      std::optional<StringView> format,
      std::optional<StringView> seed,
      std::optional<int32_t> outLength) {
    return evaluateOnce<int32_t>(
               "wkcgovernmask_date(c0, c1, c2, c3, c4, c5)",
               {VARCHAR(),
                INTEGER(),
                VARCHAR(),
                VARCHAR(),
                VARCHAR(),
                INTEGER()},
               input,
               inputMask,
               maskParams,
               format,
               seed,
               outLength)
        .value();
  };
};

TEST_F(QaagFunctionsTest, maskDouble) {
  auto assertEqual = [&](double a, double b) {
    return variant(a).equalsWithEpsilon(variant(b));
  };

  ASSERT_TRUE(assertEqual(0.0, testMaskDouble(42.634, 0, "", "", "", 0)));
  ASSERT_TRUE(assertEqual(0.0, testMaskDouble(-3.14159, 1, "1", "", "", 0)));
  ASSERT_TRUE(assertEqual(0.0, testMaskDouble(1e-09, 0, "1", "", "", 0)));
  ASSERT_TRUE(assertEqual(
      -67130735623609331310137004302023122629068481956800529622610885131348855582352924987583807029033537618272795522949402602160501499729882591763875454192696625558684286545614661286169605994722535913721098822690275604158758235409673762432354739244488689101968351743949722745651548345220390434370415472741185486848.000000,
      testMaskDouble(125.21, 2, "", "", "0123456789abcdef", 0)));
  ASSERT_TRUE(assertEqual(
      -12666284541559788540306588201362919368359019518509312343939690900366530374988135453612597037467311458157586015249771959542751038057454579112556819927750464596369755196522785267443290433716647444287285903618654815634311854551592058754637627704768533443455206026264630853440556698241135627261666310803593101312.000000,
      testMaskDouble(
          4583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.0000,
          2,
          "",
          "",
          "0123456789abcdef",
          0)));
  ASSERT_TRUE(assertEqual(
      -71237344514421384799548392831898624087293210792910665081605987958800312342322742548787675709452401460396439575029098331272079209179404663234313517542431177414547250060065553825125932002900269690809206528107640045991254466482109985174137956718152790959282623677157913281037781223766444707630234510072238571520.000000,
      testMaskDouble(-125.21, 2, "", "", "0123456789abcdef", 0)));
}

TEST_F(QaagFunctionsTest, maskReal) {
  auto assertEqual = [&](float a, float b) {
    return variant(a).equalsWithEpsilon(variant(b));
  };

  ASSERT_TRUE(assertEqual(0.0, testMaskReal(42.634, 0, "", "", "", 0)));
  ASSERT_TRUE(assertEqual(0.0, testMaskReal(-3.14159, 1, "1", "", "", 0)));
  ASSERT_TRUE(assertEqual(0.0, testMaskReal(1e-09, 0, "1", "", "", 0)));
}

TEST_F(QaagFunctionsTest, maskDecimal) {
  auto maskType = makeFlatVector<int32_t>({1, 0, 1, 0, 1, 0, 1});
  auto maskParams = makeFlatVector<StringView>(
      {"abc", "def", "xyz", "abc", "def", "xyz", "abc"});
  auto formatName = makeFlatVector<StringView>(
      {"def", "xyz", "abc", "abc", "def", "xyz", "abc"});
  auto seed = makeFlatVector<StringView>(
      {"xyz", "abc", "def", "abc", "def", "xyz", "abc"});
  auto dbType = makeConstant<StringView>("OTHERS", 7);
  auto shortDecimalType = DECIMAL(3, 2);
  RowVectorPtr input = makeRowVector(
      {makeNullableFlatVector<int64_t>(
           {std::nullopt,
            456,
            789,
            -123,
            -678,
            DecimalUtil::kShortDecimalMin,
            DecimalUtil::kShortDecimalMax},
           shortDecimalType),
       maskType,
       maskParams,
       formatName,
       seed,
       makeConstant<int32_t>(3, 7),
       makeConstant<int32_t>(2, 7),
       dbType});
  auto expectedShortDecimal = makeNullableFlatVector<int64_t>(
      {std::nullopt, 0, 0, 0, 0, 0, 0}, shortDecimalType);
  testMaskDecimal<int64_t>(input, expectedShortDecimal);

  auto longDecimalType = DECIMAL(20, 2);
  input = makeRowVector(
      {makeNullableFlatVector<int128_t>(
           {std::nullopt,
            67890,
            45678,
            -12345,
            -56789,
            DecimalUtil::kLongDecimalMin,
            DecimalUtil::kLongDecimalMax},
           longDecimalType),
       maskType,
       maskParams,
       formatName,
       seed,
       makeConstant<int32_t>(20, 7),
       makeConstant<int32_t>(2, 7),
       dbType});
  auto expectedLongDecimal = makeNullableFlatVector<int128_t>(
      {std::nullopt, 0, 0, 0, 0, 0, 0}, longDecimalType);
  testMaskDecimal<int128_t>(input, expectedLongDecimal);
}

TEST_F(QaagFunctionsTest, maskSmallint) {
  EXPECT_EQ(5555, testMaskSmallint(234, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(10857, testMaskSmallint(31932, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(-3113, testMaskSmallint(32768, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(-26476, testMaskSmallint(32767, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(5555, testMaskSmallint(234, 2, "", "", "0123456789abcdef", 0));
}

TEST_F(QaagFunctionsTest, maskInt) {
  EXPECT_EQ(
      1068482221, testMaskInteger(42634, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      654798527, testMaskInteger(2147483647, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      -321468631, testMaskInteger(-214873, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      224941421, testMaskInteger(2141395, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      1068482221, testMaskInteger(42634, 2, "", "", "0123456789abcdef", 0));
}

TEST_F(QaagFunctionsTest, maskBigint) {
  EXPECT_EQ(
      7207139586282576853,
      testMaskBigint(-922337203685477580, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      -1068680321296455194,
      testMaskBigint(9223372036854775807, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      1391323895165191512,
      testMaskBigint(31932, 2, "", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      0,
      testMaskBigint(-922337203685477580, 0, "5", "", "0123456789abcdef", 0));
  EXPECT_EQ(
      0,
      testMaskBigint(9223372036854775807, 1, "6", "", "0123456789abcdef", 0));
  EXPECT_EQ(0, testMaskBigint(std::nullopt, 0, "5", "", "0123456789abcdef", 0));
}

TEST_F(QaagFunctionsTest, maskStringTypes) {
  auto varcharInput = makeNullableFlatVector<StringView>(
      {"someinput",
       "someinput",
       "someinput",
       "someinput",
       "someinput",
       "someinput",
       std::nullopt,
       "someinput",
       "someinput",
       "someinput",
       "123-45-6789",
       "123-45-6789",
       "123-45-6789",
       "someinputfoobar",
       std::nullopt},
      VARCHAR());
  auto fromHexInput = makeNullableFlatVector<StringView>(
      {"FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB6BE",
       std::nullopt,
       "FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB6BE",
       "FBB78EB658ABCDE9",
       "FBB78EB658ABCDE9",
       "FBB78EB658ABCDE9",
       "FEDCBA0123456789ABCDEF9876543210",
       std::nullopt},
      VARCHAR());
  auto inputMask =
      makeFlatVector<int32_t>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2, 1, 0});
  auto maskParams = makeFlatVector<StringView>(
      {"**********",
       "******************************",
       "**********",
       "$$$$$$$$$$$$",
       "AAAAAAAAA",
       "$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$$$$",
       "*",
       "B",
       "",
       "",
       "",
       "B",
       "$$$$$$$$$$$$$$$"});
  auto format = makeFlatVector<StringView>(
      {"", "", "", "", "", "", "", "", "", "", "", "", "", "", ""});
  auto seed = makeFlatVector<StringView>(
      {"",
       "",
       "",
       "",
       "",
       "",
       "",
       "",
       "",
       "",
       "0123456789abcdef",
       "0123456789abcdef",
       "0123456789abcdef",
       "",
       ""});
  auto outLength = makeFlatVector<int32_t>(
      {10,
       30,
       2,
       5,
       2,
       15,
       25,
       15,
       10,
       12,
       61,
       20,
       2147483647,
       2147483647,
       2147483647});
  auto varcharRowVector = makeRowVector(
      {varcharInput, inputMask, maskParams, format, seed, outLength});
  auto varbinaryRowVector = makeRowVector(
      {fromHexInput, inputMask, maskParams, format, seed, outLength});

  auto varcharExpected = makeNullableFlatVector<StringView>(
      {"**********",
       "******************************",
       "**",
       "$$$$$",
       "AA",
       "$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$",
       "*********",
       "BBBBBBBBB",
       "RGNGthH412z8b4vsne3uSbiQGDHTZbTjzaqg8me4yrI=",
       "RGNGthH412z8b4vsne3u",
       "RGNGthH412z8b4vsne3uSbiQGDHTZbTjzaqg8me4yrI=",
       "BBBBBBBBBBBBBBB",
       "$$$$$$$$$$$$$$$"},
      VARCHAR());
  auto varbinaryExpected = makeNullableFlatVector<StringView>(
      {"**********",
       "******************************",
       "**",
       "$$$$$",
       "AA",
       "$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$",
       "$$$$$$$$$$$$$$$",
       "*****",
       "BBBBB",
       "eL/LGgOMc/dkr9Y43dNgZoYC12weCpNduSsTBY3HRmA=",
       "eL/LGgOMc/dkr9Y43dNg",
       "eL/LGgOMc/dkr9Y43dNgZoYC12weCpNduSsTBY3HRmA=",
       "BBBBBBBBBBBBBB",
       "$$$$$$$$$$$$$$$"},
      VARBINARY());

  testMaskStringType<Varchar>(varcharRowVector, varcharExpected);
  testMaskStringType<Varbinary>(varbinaryRowVector, varbinaryExpected);
}

TEST_F(QaagFunctionsTest, maskTimestamp) {
  auto timestampValue = [](StringView timestamp) -> auto {
    return util::fromTimestampString(
               timestamp, util::TimestampParseMode::kPrestoCast)
        .thenOrThrow(folly::identity, [&](const Status& status) {
          VELOX_USER_FAIL("{}", status.message());
        });
  };

  EXPECT_EQ(
      timestampValue("1990-10-24 20:00:00").toString(),
      testMaskTimestamp(
          timestampValue("1985-09-02"), 0, "1990-10-24 20:00:00.00", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("1982-10-02 12:24:36.01").toString(),
      testMaskTimestamp(
          timestampValue("1996-05-12"), 0, "1982-10-02 12:24:36.01", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestamp(std::nullopt, 0, "1999-11-22 22:11:00.03", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestamp(
          timestampValue("2004-01-23"), 1, "2002-04-12 32:05:00", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestamp(
          timestampValue("2022-09-23 23:21:56"),
          1,
          "2022-09-23 23:21:56",
          "",
          "",
          25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestamp(std::nullopt, 1, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestamp(
          timestampValue("1968-01-01"), 1, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2022-09-23 23:21:56.000").toString(),
      testMaskTimestamp(
          timestampValue("9999-12-31"), 0, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("1990-10-24 20:00:00").toString(),
      testMaskTimestampVarchar(
          "1985-09-02", 0, "1990-10-24 20:00:00.00", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("1982-10-02 12:24:36.01").toString(),
      testMaskTimestampVarchar(
          "1996-05-12", 0, "1982-10-02 12:24:36.01", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestampVarchar(
          std::nullopt, 0, "1999-11-22 22:11:00.03", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestampVarchar(
          "2004-01-23", 1, "2002-04-12 32:05:00", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestampVarchar(
          "2022-09-23 23:21:56", 1, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestampVarchar(
          std::nullopt, 1, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2001-01-01 00:00:00.000").toString(),
      testMaskTimestampVarchar(
          "1968-01-01", 1, "2022-09-23 23:21:56", "", "", 25)
          .toString());
  EXPECT_EQ(
      timestampValue("2022-09-23 23:21:56.000").toString(),
      testMaskTimestampVarchar(
          "9999-12-31", 0, "2022-09-23 23:21:56", "", "", 25)
          .toString());
}

TEST_F(QaagFunctionsTest, maskDate) {
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDate(std::nullopt, 0, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("1999-11-22"),
      testMaskDate(DATE()->toDays("1815-04-22"), 0, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDate(DATE()->toDays("1815-04-22"), 2, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDate(
          DATE()->toDays("1815-04-22"), 0, "XXXXXXXXX", "", "", 2147483647));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDate(
          DATE()->toDays("1968-01-01"), 0, "XXXXXXXXX", "", "", 2147483647));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDate(
          DATE()->toDays("9999-12-31"), 0, "XXXXXXXXX", "", "", 2147483647));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDateVarchar(std::nullopt, 0, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("1999-11-22"),
      testMaskDateVarchar("1815-04-22", 0, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDateVarchar("1815-04-22", 2, "1999-11-22", "", "", 25));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDateVarchar("1968-01-01", 0, "XXXXXXXXX", "", "", 2147483647));
  EXPECT_EQ(
      DATE()->toDays("2001-01-01"),
      testMaskDateVarchar("9999-12-31", 0, "XXXXXXXXX", "", "", 2147483647));
}

TEST_F(QaagFunctionsTest, maskBoolean) {
  EXPECT_FALSE(testMaskBool(true, 0, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(true, 4, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(true, 7, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(true, 10, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(false, 0, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(false, 4, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(false, 7, "", "", "0123456789abcdef", 0));
  EXPECT_FALSE(testMaskBool(false, 10, "", "", "0123456789abcdef", 0));
}
