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

#include "velox/expression/ControlExpr.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;

class CastExprTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      RowVectorPtr data) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    exec::ExprSet exprSet({makeTypedExpr(expression, rowType)}, &execCtx_);

    SelectivityVector rows(data->size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, &evalCtx, &result);
    return std::dynamic_pointer_cast<T>(result[0]);
  }

  template <typename T>
  std::shared_ptr<T> evaluateComplexCast(
      const std::string& fromExpression,
      RowVectorPtr data,
      const TypePtr& toType,
      bool nullOnFailure = false) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    auto fromTypedExpression = makeTypedExpr(fromExpression, rowType);
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>
        inputs = {fromTypedExpression};
    auto castExpr = std::make_shared<facebook::velox::core::CastTypedExpr>(
        toType, inputs, nullOnFailure);
    exec::ExprSet exprSet({castExpr}, &execCtx_);

    SelectivityVector rows(data->size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, &evalCtx, &result);
    return std::dynamic_pointer_cast<T>(result[0]);
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
      EXPECT_ANY_THROW(
          evaluate<FlatVector<typename CppToType<TTo>::NativeType>>(
              castFunction + "(c0 as " + typeString + ")", rowVector));
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
  std::cout << oo[2].has_value();
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

TEST_F(CastExprTest, timestampInvalid) {
  testCast<int8_t, Timestamp>("timestamp", {12}, {Timestamp(0, 0)}, true);
  testCast<int16_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);
  testCast<int32_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);
  testCast<int64_t, Timestamp>("timestamp", {1234}, {Timestamp(0, 0)}, true);

  testCast<float, Timestamp>("timestamp", {12.99}, {Timestamp(0, 0)}, true);
  testCast<double, Timestamp>("timestamp", {12.99}, {Timestamp(0, 0)}, true);
}

TEST_F(CastExprTest, timestampAdjustToTimezone) {
  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kSessionTimezone, "America/Los_Angeles"},
      {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
  });

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
  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kSessionTimezone, ""},
      {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
  });
  testCast<std::string, Timestamp>(
      "timestamp", {"1970-01-01"}, {Timestamp(0, 0)});
}

TEST_F(CastExprTest, timestampAdjustToTimezoneInvalid) {
  auto testFunc = [&]() {
    testCast<std::string, Timestamp>(
        "timestamp", {"1970-01-01"}, {Timestamp(1, 0)});
  };

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kSessionTimezone, "bla"},
      {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
  });
  EXPECT_THROW(testFunc(), std::runtime_error);
}

TEST_F(CastExprTest, truncateVsRound) {
  // Testing truncate vs round cast from double to int.
  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "true"},
  });
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {1, 2, 3, 100, -100});

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "false"},
  });
  testCast<double, int>(
      "int", {1.888, 2.5, 3.6, 100.44, -100.101}, {2, 3, 4, 100, -100});

  testCast<int8_t, int32_t>("int", {111, 2, 3, 10, -10}, {111, 2, 3, 10, -10});

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "true"},
  });
  testCast<int32_t, int8_t>(
      "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5});
  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "false"},
  });
  EXPECT_THROW(
      (testCast<int32_t, int8_t>(
          "tinyint", {1111111, 2, 3, 1000, -100101}, {71, 2, 3, -24, -5})),
      std::exception);
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

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "true"},
  });
  testCast<double, int>(
      "int",
      {1e12, 2.5, 3.6, 100.44, -100.101},
      {std::nullopt, 2, 3, 100, -100},
      false,
      true);

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastIntByTruncate, "false"},
  });
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

    auto result = evaluateComplexCast<MapVector>(
        "c0", makeRowVector({inputMap}), MAP(INTEGER(), DOUBLE()));
    assertEqualVectors(expectedMap, result);
  }

  // Cast map<bigint, bigint> -> map<bigint, varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row) {
      return StringView(folly::to<std::string>(valueAt(row)));
    };

    auto expectedMap = makeMapVector<int64_t, StringView>(
        kVectorSize, sizeAt, keyAt, valueAtString, nullEvery(3));

    auto result = evaluateComplexCast<MapVector>(
        "c0", makeRowVector({inputMap}), MAP(BIGINT(), VARCHAR()));
    assertEqualVectors(expectedMap, result);
  }

  // Cast map<bigint, bigint> -> map<varchar, bigint>.
  {
    auto keyAtString = [&](vector_size_t row) {
      return StringView(folly::to<std::string>(keyAt(row)));
    };

    auto expectedMap = makeMapVector<StringView, int64_t>(
        kVectorSize, sizeAt, keyAtString, valueAt, nullEvery(3));

    auto result = evaluateComplexCast<MapVector>(
        "c0", makeRowVector({inputMap}), MAP(VARCHAR(), BIGINT()));
    assertEqualVectors(expectedMap, result);
  }

  // null values
  {
    auto inputWithNullValues = makeMapVector<int64_t, int64_t>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    auto expectedMap = makeMapVector<int32_t, double>(
        kVectorSize, sizeAt, keyAt, valueAt, nullEvery(3), nullEvery(7));

    auto result = evaluateComplexCast<MapVector>(
        "c0", makeRowVector({inputWithNullValues}), MAP(INTEGER(), DOUBLE()));
    assertEqualVectors(expectedMap, result);
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
    auto expectedVector =
        makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt, nullEvery(3));
    auto result = evaluateComplexCast<ArrayVector>(
        "c0", makeRowVector({arrayVector}), ARRAY(BIGINT()));
    assertEqualVectors(expectedVector, result);
  }

  // Cast array<double> -> array<varchar>.
  {
    auto valueAtString = [valueAt](vector_size_t row, vector_size_t idx) {
      return StringView(folly::to<std::string>(valueAt(row, idx)));
    };
    auto expectedVector = makeArrayVector<StringView>(
        kVectorSize, sizeAt, valueAtString, nullEvery(3));
    auto result = evaluateComplexCast<ArrayVector>(
        "c0", makeRowVector({arrayVector}), ARRAY(VARCHAR()));
    assertEqualVectors(expectedVector, result);
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

  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastMatchStructByName, "false"},
  });
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double, c1:
  // bigint)
  {
    auto expectedRowVector = makeRowVector(
        {doubleVectorNullEvery11, intVectorNullEvery3}, nullEvery(5));
    auto result = evaluateComplexCast<RowVector>(
        "c0",
        makeRowVector({rowVector}),
        ROW({"c0", "c1"}, {DOUBLE(), BIGINT()}));
    assertEqualVectors(expectedRowVector, result);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(a: double, b:
  // bigint)
  {
    auto expectedRowVector = makeRowVector(
        {doubleVectorNullEvery11, intVectorNullEvery3}, nullEvery(5));
    auto result = evaluateComplexCast<RowVector>(
        "c0",
        makeRowVector({rowVector}),
        ROW({"a", "b"}, {DOUBLE(), BIGINT()}));
    assertEqualVectors(expectedRowVector, result);
  }
  // Position-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double)
  {
    auto expectedRowVector =
        makeRowVector({doubleVectorNullEvery11}, nullEvery(5));
    auto result = evaluateComplexCast<RowVector>(
        "c0", makeRowVector({rowVector}), ROW({"c0"}, {DOUBLE()}));
    assertEqualVectors(expectedRowVector, result);
  }
  // Name-based cast: ROW(c0: bigint, c1: double) -> ROW(c0: double) dropping b
  queryCtx_->setConfigOverridesUnsafe({
      {core::QueryConfig::kCastMatchStructByName, "true"},
  });
  {
    auto intVectorNullAll = makeFlatVector<int64_t>(
        kVectorSize, valueAtInt, [](vector_size_t /* row */) { return true; });
    auto expectedRowVector = makeRowVector(
        {doubleVectorNullEvery11, intVectorNullAll}, nullEvery(5));
    auto result = evaluateComplexCast<RowVector>(
        "c0",
        makeRowVector({rowVector}),
        ROW({"c0", "b"}, {DOUBLE(), BIGINT()}));
    assertEqualVectors(expectedRowVector, result);
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
  auto rowVector = makeRowVector({input});
  auto result = evaluateComplexCast<FlatVector<int32_t>>(
      "c0", rowVector, INTEGER(), true);

  // nullOnFailure is true, so we should return null instead of throwing
  assertEqualVectors(expected, result);

  // nullOnFailure is false, so we should throw
  EXPECT_THROW(
      evaluateComplexCast<FlatVector<int32_t>>(
          "c0", rowVector, INTEGER(), false),
      std::exception);
}
