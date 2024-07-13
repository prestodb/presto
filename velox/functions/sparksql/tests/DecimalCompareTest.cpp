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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DecimalCompareTest : public SparkFunctionBaseTest {
 protected:
  void testCompareExpr(
      const std::string& exprStr,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expectedResult,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    auto actual = evaluate(exprStr, makeRowVector(input), rows);
    if (rows.has_value()) {
      velox::test::assertEqualVectors(expectedResult, actual, rows.value());
    } else {
      velox::test::assertEqualVectors(expectedResult, actual);
    }
  }
};

TEST_F(DecimalCompareTest, gt) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, true, true, false}));
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, true, true, false}));
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({false, true, true, false}));
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, true, false}));
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, false, false}));

  // All rows selected.
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          70, [](auto row) { return row % 2 == 0 ? false : true; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_greaterthan(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          130, [](auto row) { return row % 2 == 0 ? false : true; }),
      row);
}

TEST_F(DecimalCompareTest, gte) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, true, true, false}));
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({true, true, true, false}));
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, true, false}));
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, false, false}));

  // All rows selected.
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(70, [](auto /*row*/) { return true; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_greaterthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(130, [](auto /*row*/) { return true; }),
      row);
}

TEST_F(DecimalCompareTest, eq) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, false, false, false}));
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, false, false, false}));
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({true, false, false, false}));
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({false, false, false, false}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, false, false}));
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, false, false}));

  // All rows selected.
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          70, [](auto row) { return row % 2 == 0 ? true : false; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_equalto(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          130, [](auto row) { return row % 2 == 0 ? true : false; }),
      row);
}

TEST_F(DecimalCompareTest, neq) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, true, true, true}));
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, true, true, true}));
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({false, true, true, true}));
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, true, true}));
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, true, true}));

  // All rows selected.
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          70, [](auto row) { return row % 2 == 0 ? false : true; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_notequalto(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          130, [](auto row) { return row % 2 == 0 ? false : true; }),
      row);
}

TEST_F(DecimalCompareTest, lt) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, false, false, true}));
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({false, false, false, true}));
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({false, false, false, true}));
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({false, std::nullopt, false, true}));
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, true, true}));

  // All rows selected.
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(70, [](auto /*row*/) { return false; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_lessthan(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(130, [](auto /*row*/) { return false; }),
      row);
}

TEST_F(DecimalCompareTest, lte) {
  // Fast path when c1 vector is constant.
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeConstant((int64_t)100, 4, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, false, false, true}));
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeConstant((int64_t)100000, 4, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Fast path when vectors are flat.
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1)),
      },
      makeFlatVector<bool>({true, false, false, true}));
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeFlatVector<int64_t>(
              {100000, 120000, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // General case when vectors are dictionary-encoded.
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>({100, 120, 130, 350}, DECIMAL(5, 1))),
      },
      makeFlatVector<bool>({true, false, false, true}));
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int128_t>(
                  {10'000'000'000'000'000,
                   12'000'000'000'000'000,
                   13'000'000'000'000'000,
                   35'000'000'000'000'000},
                  DECIMAL(38, 34))),
          wrapInDictionary(
              makeIndices({0, 1, 2, 3}),
              makeFlatVector<int64_t>(
                  {100000, 120000, 130000, 350000}, DECIMAL(6, 1))),
      },
      makeFlatVector<bool>({true, true, true, true}));

  // Decimal with nulls.
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>({1000, 2000, 3000, 400}, DECIMAL(6, 2)),
          makeNullableFlatVector<int64_t>(
              {100, std::nullopt, 130, 350}, DECIMAL(5, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, false, true}));
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int128_t>(
              {10'000'000'000'000'000,
               12'000'000'000'000'000,
               13'000'000'000'000'000,
               35'000'000'000'000'000},
              DECIMAL(38, 34)),
          makeNullableFlatVector<int64_t>(
              {100000, std::nullopt, 130000, 350000}, DECIMAL(6, 1)),
      },
      makeNullableFlatVector<bool>({true, std::nullopt, true, true}));

  // All rows selected.
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              70,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          70, [](auto row) { return row % 2 == 0 ? true : false; }));

  // 90% rows selected.
  SelectivityVector row(130);
  row.setValidRange(0, 13, false);
  testCompareExpr(
      "decimal_lessthanorequal(c0, c1)",
      {
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 1000 : 3000; },
              nullptr,
              DECIMAL(6, 2)),
          makeFlatVector<int64_t>(
              130,
              [](auto row) { return row % 2 == 0 ? 100 : 130; },
              nullptr,
              DECIMAL(5, 1)),
      },
      makeFlatVector<bool>(
          130, [](auto row) { return row % 2 == 0 ? true : false; }),
      row);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
