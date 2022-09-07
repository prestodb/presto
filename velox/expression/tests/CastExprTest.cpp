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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/TypeAliases.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace {
/// Wraps input in a dictionary that reverses the order of rows.
class TestingDictionaryFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK(rows.isAllSelected());
    const auto size = rows.size();
    auto indices = makeIndicesInReverse(size, context.pool());
    result = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, size, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T, integer -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};
} // namespace

class CastExprTest : public functions::test::FunctionBaseTest {
 protected:
  CastExprTest() {
    exec::registerVectorFunction(
        "testing_dictionary",
        TestingDictionaryFunction::signatures(),
        std::make_unique<TestingDictionaryFunction>());
  }

  void setCastIntByTruncate(bool value) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kCastIntByTruncate, std::to_string(value)},
    });
  }

  void setCastMatchStructByName(bool value) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kCastMatchStructByName, std::to_string(value)},
    });
  }

  void setTimezone(const std::string& value) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kSessionTimezone, value},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  std::shared_ptr<core::CastTypedExpr> makeCastExpr(
      const std::shared_ptr<const core::ITypedExpr>& input,
      const TypePtr& toType,
      bool nullOnFailure) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs = {input};
    return std::make_shared<core::CastTypedExpr>(toType, inputs, nullOnFailure);
  }

  void testComplexCast(
      const std::string& fromExpression,
      const VectorPtr& data,
      const VectorPtr& expected,
      bool nullOnFailure = false) {
    auto rowVector = makeRowVector({data});
    auto rowType = std::dynamic_pointer_cast<const RowType>(rowVector->type());
    auto castExpr = makeCastExpr(
        makeTypedExpr(fromExpression, rowType),
        expected->type(),
        nullOnFailure);
    exec::ExprSet exprSet({castExpr}, &execCtx_);

    const auto size = data->size();
    SelectivityVector rows(size);
    std::vector<VectorPtr> result(1);
    {
      exec::EvalCtx evalCtx(&execCtx_, &exprSet, rowVector.get());
      exprSet.eval(rows, evalCtx, result);

      assertEqualVectors(expected, result[0]);
    }

    // Test constant input.
    {
      // Use last element for constant.
      const auto index = size - 1;
      auto constantData = BaseVector::wrapInConstant(size, index, data);
      auto constantRow = makeRowVector({constantData});
      exec::EvalCtx evalCtx(&execCtx_, &exprSet, constantRow.get());
      exprSet.eval(rows, evalCtx, result);

      assertEqualVectors(
          BaseVector::wrapInConstant(size, index, expected), result[0]);
    }

    // Test dictionary input. It is not sufficient to wrap input in a dictionary
    // as it will be peeled off before calling "cast". Apply
    // testing_dictionary function to input to ensure that "cast" receives
    // dictionary input.
    {
      auto dictionaryCastExpr = makeCastExpr(
          makeTypedExpr(
              fmt::format("testing_dictionary({})", fromExpression), rowType),
          expected->type(),
          nullOnFailure);
      exec::ExprSet dictionaryExprSet({dictionaryCastExpr}, &execCtx_);
      exec::EvalCtx evalCtx(&execCtx_, &dictionaryExprSet, rowVector.get());
      dictionaryExprSet.eval(rows, evalCtx, result);

      auto indices = ::makeIndicesInReverse(size, pool());
      assertEqualVectors(wrapInDictionary(indices, size, expected), result[0]);
    }
  }

  /**
   * @tparam From Source type for cast
   * @tparam To Destination type for cast
   * @param typeString Cast type in string
   * @param input Input vector of type From
   * @param expectedResult Expected output vector of type To
   * @param inputNulls Input null indexes
   * @param expectedNulls Expected output null indexes
   */
  template <typename TFrom, typename TTo>
  void testCast(
      const std::string& typeString,
      std::vector<std::optional<TFrom>> input,
      std::vector<std::optional<TTo>> expectedResult,
      bool expectFailure = false,
      bool tryCast = false) {
    std::vector<TFrom> rawInput(input.size());
    for (auto index = 0; index < input.size(); index++) {
      if (input[index].has_value()) {
        rawInput[index] = input[index].value();
      }
    }
    // Create input vector using values and nulls
    auto inputVector = makeFlatVector(rawInput);

    for (auto index = 0; index < input.size(); index++) {
      if (!input[index].has_value()) {
        inputVector->setNull(index, true);
      }
    }
    auto rowVector = makeRowVector({inputVector});
    std::string castFunction = tryCast ? "try_cast" : "cast";
    if (expectFailure) {
      EXPECT_THROW(
          evaluate<FlatVector<typename CppToType<TTo>::NativeType>>(
              castFunction + "(c0 as " + typeString + ")", rowVector),
          VeloxException);
      return;
    }
    // run try cast and get the result vector
    auto result = evaluate<FlatVector<typename CppToType<TTo>::NativeType>>(
        castFunction + "(c0 as " + typeString + ")", rowVector);

    std::string msg;
    // Compare the values and nulls in the output with expected
    for (int index = 0; index < input.size(); index++) {
      if (expectedResult[index].has_value()) {
        EXPECT_TRUE(
            compareValues(result->valueAt(index), expectedResult[index], msg))
            << "values at index " << index << " do not match!" << msg;
      } else {
        EXPECT_TRUE(result->isNullAt(index)) << " at index " << index;
      }
    }
  }
};

TEST_F(CastExprTest, basics) {
  // Testing non-null or error cases
  const std::vector<std::optional<int32_t>> ii = {1, 2, 3, 100, -100};
  const std::vector<std::optional<double>> oo = {1.0, 2.0, 3.0, 100.0, -100.0};
  testCast<int32_t, double>(
      "double", {1, 2, 3, 100, -100}, {1.0, 2.0, 3.0, 100.0, -100.0});
  testCast<int32_t, std::string>(
      "string", {1, 2, 3, 100, -100}, {"1", "2", "3", "100", "-100"});
  testCast<std::string, int8_t>(
      "tinyint", {"1", "2", "3", "100", "-100"}, {1, 2, 3, 100, -100});
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});
  testCast<double, double>(
      "double",
      {1.888, 2.5, 3.6, 100.44, -100.101},
      {1.888, 2.5, 3.6, 100.44, -100.101});
  testCast<double, std::string>(
      "string",
      {1.888, 2.5, 3.6, 100.44, -100.101},
      {"1.888", "2.5", "3.6", "100.44", "-100.101"});
  testCast<double, double>(
      "double",
      {1.888, 2.5, 3.6, 100.44, -100.101},
      {1.888, 2.5, 3.6, 100.44, -100.101});
  testCast<double, float>(
      "float",
      {1.888, 2.5, 3.6, 100.44, -100.101},
      {1.888, 2.5, 3.6, 100.44, -100.101});
  testCast<bool, std::string>("string", {true, false}, {"true", "false"});
}

TEST_F(CastExprTest, timestamp) {
  testCast<std::string, Timestamp>(
      "timestamp",
      {
          "1970-01-01",
          "2000-01-01",
          "1970-01-01 00:00:00",
          "2000-01-01 12:21:56",
          "1970-01-01 00:00:00-02:00",
          std::nullopt,
      },
      {
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(0, 0),
          Timestamp(946729316, 0),
          Timestamp(7200, 0),
          std::nullopt,
      });
}

TEST_F(CastExprTest, dateToTimestamp) {
  testCast<Date, Timestamp>(
      "timestamp",
      {
          Date(0),
          Date(10957),
          Date(14557),
          std::nullopt,
      },
      {
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(1257724800, 0),
          std::nullopt,
      });
}

TEST_F(CastExprTest, timestampToDate) {
  testCast<Timestamp, Date>(
      "date",
      {
          Timestamp(0, 0),
          Timestamp(946684800, 0),
          Timestamp(1257724800, 0),
          std::nullopt,
      },
      {
          Date(0),
          Date(10957),
          Date(14557),
          std::nullopt,
      });
}

TEST_F(CastExprTest, timestampInvalid) {
  testCast<int8_t, Timestamp>("timestamp", {12}, {Timestamp(0, 0)}, true);
  testCast<int16_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);
  testCast<int32_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);
  testCast<int64_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);

  testCast<float, Timestamp>("timestamp", {12.99}, {Timestamp(0, 0)}, true);
  testCast<double, Timestamp>("timestamp", {12.99}, {Timestamp(0, 0)}, true);
}

TEST_F(CastExprTest, timestampAdjustToTimezone) {
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
          Timestamp(-21600, 0),
          std::nullopt,
          Timestamp(957164400, 0),
      });

  // Empty timezone is assumed to be GMT.
  setTimezone("");
  testCast<std::string, Timestamp>(
      "timestamp", {"1970-01-01"}, {Timestamp(0, 0)});
}

TEST_F(CastExprTest, timestampAdjustToTimezoneInvalid) {
  auto testFunc = [&]() {
    testCast<std::string, Timestamp>(
        "timestamp", {"1970-01-01"}, {Timestamp(1, 0)});
  };

  setTimezone("bla");
  EXPECT_THROW(testFunc(), std::runtime_error);
}

TEST_F(CastExprTest, date) {
  testCast<std::string, Date>(
      "date",
      {
          "1970-01-01",
          "2020-01-01",
          "2135-11-09",
          "1969-12-27",
          "1812-04-15",
          "1920-01-02",
          std::nullopt,
      },
      {
          Date(0),
          Date(18262),
          Date(60577),
          Date(-5),
          Date(-57604),
          Date(-18262),
          std::nullopt,
      });
}

TEST_F(CastExprTest, invalidDate) {
  testCast<int8_t, Date>("date", {12}, {Date(0)}, true);
  testCast<int16_t, Date>("date", {1234}, {Date(0)}, true);
  testCast<int32_t, Date>("date", {1234}, {Date(0)}, true);
  testCast<int64_t, Date>("date", {1234}, {Date(0)}, true);

  testCast<float, Date>("date", {12.99}, {Date(0)}, true);
  testCast<double, Date>("date", {12.99}, {Date(0)}, true);

  // Parsing an ill-formated date.
  testCast<std::string, Date>("date", {"2012-Oct-23"}, {Date(0)}, true);
}

TEST_F(CastExprTest, truncateVsRound) {
  // Testing truncate vs round cast from double to int.
  setCastIntByTruncate(true);
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {1, 2, 3, 100, -100});
  testCast<double, int8_t>(
      "tinyint",
      {1,
       256,
       257,
       2147483646,
       2147483647,
       2147483648,
       -2147483646,
       -2147483647,
       -2147483648,
       -2147483649},
      {1, 0, 1, -2, -1, -1, 2, 1, 0, 0});

  setCastIntByTruncate(false);
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});

  testCast<int8_t, int32_t>("int", {111, 2, 3, 10, -10}, {111, 2, 3, 10, -10});

  setCastIntByTruncate(true);
  testCast<int32_t, int8_t>(
      "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5});

  setCastIntByTruncate(false);
  EXPECT_THROW(
      (testCast<int32_t, int8_t>(
          "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5})),
      VeloxUserError);
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
  testCast<std::string, int8_t>(
      "tinyint",
      {"1abc", "2", "3", "100", std::nullopt},
      {std::nullopt, 2, 3, 100, std::nullopt},
      false,
      true);

  setCastIntByTruncate(true);
  testCast<std::string, int8_t>(
      "tinyint",
      {"-",
       "-0",
       " @w 123",
       "123 ",
       "  122",
       "",
       "-12-3",
       "125.5",
       "1234",
       "-129",
       "127",
       "-128"},
      {std::nullopt,
       0,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       std::nullopt,
       127,
       -128},
      false,
      true);

  testCast<double, int>(
      "integer",
      {1e12, 2.5, 3.6, 100.44, -100.101},
      {std::numeric_limits<int32_t>::max(), 2, 3, 100, -100},
      false,
      true);

  setCastIntByTruncate(false);
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});

  testCast<std::string, int8_t>(
      "tinyint", {"1abc", "2", "3", "100", "-100"}, {1, 2, 3, 100, -100}, true);

  testCast<std::string, int8_t>(
      "tinyint", {"1", "2", "3", "100", "-100.5"}, {1, 2, 3, 100, -100}, true);
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

    testComplexCast("c0", inputMap, expectedMap);
  }

  // Cast map<bigint, bigint> -> map<bigint, varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row) {
      return StringView(folly::to<std::string>(valueAt(row)));
    };

    auto expectedMap = makeMapVector<int64_t, StringView>(
        kVectorSize, sizeAt, keyAt, valueAtString, nullEvery(3));

    testComplexCast("c0", inputMap, expectedMap);
  }

  // Cast map<bigint, bigint> -> map<varchar, bigint>.
  {
    auto keyAtString = [&](vector_size_t row) {
      return StringView(folly::to<std::string>(keyAt(row)));
    };

    auto expectedMap = makeMapVector<StringView, int64_t>(
        kVectorSize, sizeAt, keyAtString, valueAt, nullEvery(3));

    testComplexCast("c0", inputMap, expectedMap);
  }

  // null values
  {
    auto inputWithNullValues = makeMapVector<int64_t, int64_t>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    auto expectedMap = makeMapVector<int32_t, double>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    testComplexCast("c0", inputWithNullValues, expectedMap);
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
    testComplexCast("c0", arrayVector, expected);
  }

  // Cast array<double> -> array<varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row, vector_size_t idx) {
      return StringView(folly::to<std::string>(valueAt(row, idx)));
    };
    auto expected = makeArrayVector<StringView>(
        kVectorSize, sizeAt, valueAtString, nullEvery(3));
    testComplexCast("c0", arrayVector, expected);
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
    testComplexCast("c0", rowVector, expectedRowVector);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(a: double, b:
  // bigint)
  {
    auto expectedRowVector = makeRowVector(
        {"a", "b"},
        {doubleVectorNullEvery11, intVectorNullEvery3},
        nullEvery(5));
    testComplexCast("c0", rowVector, expectedRowVector);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double)
  {
    auto expectedRowVector =
        makeRowVector({doubleVectorNullEvery11}, nullEvery(5));
    testComplexCast("c0", rowVector, expectedRowVector);
  }

  // Name-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double) dropping
  // b
  setCastMatchStructByName(true);
  {
    auto intVectorNullAll = makeFlatVector<int64_t>(
        kVectorSize, valueAtInt, [](vector_size_t /* row */) { return true; });
    auto expectedRowVector = makeRowVector(
        {"c0", "b"}, {doubleVectorNullEvery11, intVectorNullAll}, nullEvery(5));
    testComplexCast("c0", rowVector, expectedRowVector);
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
  testComplexCast("c0", input, expected, true);

  // nullOnFailure is false, so we should throw.
  EXPECT_THROW(testComplexCast("c0", input, expected, false), VeloxUserError);
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

TEST_F(CastExprTest, decimalToDecimal) {
  // short to short, scale up.
  auto shortFlat =
      makeShortDecimalFlatVector({-3, -2, -1, 0, 55, 69, 72}, DECIMAL(2, 2));
  testComplexCast(
      "c0",
      shortFlat,
      makeShortDecimalFlatVector(
          {-300, -200, -100, 0, 5'500, 6'900, 7'200}, DECIMAL(4, 4)));

  // short to short, scale down.
  testComplexCast(
      "c0",
      shortFlat,
      makeShortDecimalFlatVector({0, 0, 0, 0, 6, 7, 7}, DECIMAL(4, 1)));

  // long to short, scale up.
  auto longFlat =
      makeLongDecimalFlatVector({-201, -109, 0, 105, 208}, DECIMAL(20, 2));
  testComplexCast(
      "c0",
      longFlat,
      makeShortDecimalFlatVector(
          {-201'000, -109'000, 0, 105'000, 208'000}, DECIMAL(10, 5)));

  // long to short, scale down.
  testComplexCast(
      "c0",
      longFlat,
      makeShortDecimalFlatVector({-20, -11, 0, 11, 21}, DECIMAL(10, 1)));

  // long to long, scale up.
  testComplexCast(
      "c0",
      longFlat,
      makeLongDecimalFlatVector(
          {-20'100'000'000, -10'900'000'000, 0, 10'500'000'000, 20'800'000'000},
          DECIMAL(20, 10)));

  // long to long, scale down.
  testComplexCast(
      "c0",
      longFlat,
      makeLongDecimalFlatVector({-20, -11, 0, 11, 21}, DECIMAL(20, 1)));

  // short to long, scale up.
  testComplexCast(
      "c0",
      shortFlat,
      makeLongDecimalFlatVector(
          {-3'000'000'000,
           -2'000'000'000,
           -1'000'000'000,
           0,
           55'000'000'000,
           69'000'000'000,
           72'000'000'000},
          DECIMAL(20, 11)));

  // short to long, scale down.
  testComplexCast(
      "c0",
      makeShortDecimalFlatVector(
          {-20'500, -190, 12'345, 19'999}, DECIMAL(6, 4)),
      makeLongDecimalFlatVector({-21, 0, 12, 20}, DECIMAL(20, 1)));

  // NULLs and overflow.
  longFlat = makeNullableLongDecimalFlatVector(
      {-20'000, -1'000'000, 10'000, std::nullopt}, DECIMAL(20, 3));
  auto expectedShort = makeNullableShortDecimalFlatVector(
      {-200'000, std::nullopt, 100'000, std::nullopt}, DECIMAL(6, 4));

  // Throws exception if CAST fails.
  VELOX_ASSERT_THROW(
      testComplexCast("c0", longFlat, expectedShort),
      "Cannot cast DECIMAL '-1000.000' to DECIMAL(6,4)");

  // nullOnFailure is true.
  testComplexCast("c0", longFlat, expectedShort, true);

  // long to short, big numbers.
  testComplexCast(
      "c0",
      makeNullableLongDecimalFlatVector(
          {buildInt128(-2, 200),
           buildInt128(-1, 300),
           buildInt128(0, 400),
           buildInt128(1, 1),
           buildInt128(10, 100),
           std::nullopt},
          DECIMAL(23, 8)),
      makeNullableShortDecimalFlatVector(
          {-368934881474,
           -184467440737,
           0,
           184467440737,
           std::nullopt,
           std::nullopt},
          DECIMAL(12, 0)),
      true);
}
