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

#include <gtest/gtest.h>
#include <array>
#include "velox/common/base/Status.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using facebook::velox::functions::test::FunctionBaseTest;

class GeometryFunctionsTest : public FunctionBaseTest {
 public:
  // A set of geometries such that:
  // 0, 1: Within (1, 0: Contains)
  // 0, 2: Touches
  // 1, 2: Overlaps
  // 0, 3: Touches
  // 1, 3: Crosses
  // 1, 4: Touches
  // 1, 5: Touches
  // 2, 3: Contains
  // 2, 4: Crosses
  // 2, 5: Crosses
  // 3, 4: Crosses
  // 3, 5: Touches
  // 4, 5: Contains
  // 1, 6: Contains
  // 2, 6: Contains
  // 1, 7: Touches
  // 2, 7: Contains
  // 3, 6: Contains
  // 3, 7: Contains
  // 4, 7: Contains
  // 5, 7: Touches
  static constexpr std::array<std::string_view, 8> kRelationGeometriesWKT = {
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", // 0
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))", // 1
      "POLYGON ((1 0, 1 1, 3 1, 3 0, 1 0))", // 2
      "LINESTRING (1 0.5, 2.5 0.5)", // 3
      "LINESTRING (2 0, 2 2)", // 4
      "LINESTRING (2 0.5, 2 2)", // 5
      "POINT (1.5 0.5)", // 6
      "POINT (2 0.5)" // 7
  };

  void assertRelation(
      std::string_view relation,
      std::optional<std::string_view> leftWkt,
      std::optional<std::string_view> rightWkt,
      bool expected) {
    std::optional<bool> actual = evaluateOnce<bool>(
        fmt::format(
            "{}(ST_GeometryFromText(c0), ST_GeometryFromText(c1))", relation),
        leftWkt,
        rightWkt);
    if (leftWkt.has_value() && rightWkt.has_value()) {
      EXPECT_TRUE(actual.has_value());
      EXPECT_EQ(actual.value(), expected);
    } else {
      EXPECT_FALSE(actual.has_value());
    }
  };

  void assertOverlay(
      std::string_view overlay,
      std::optional<std::string_view> leftWkt,
      std::optional<std::string_view> rightWkt,
      std::optional<std::string_view> expectedWkt) {
    // We are forced to make expectedWkt optional based on type signature, but
    // we always want to supply a value.
    std::optional<bool> actual = evaluateOnce<bool>(
        fmt::format(
            "ST_Equals({}(ST_GeometryFromText(c0), ST_GeometryFromText(c1)), ST_GeometryFromText(c2))",
            overlay),
        leftWkt,
        rightWkt,
        expectedWkt);
    if (leftWkt.has_value() && rightWkt.has_value()) {
      assert(expectedWkt.has_value());
      EXPECT_TRUE(actual.has_value());
      EXPECT_TRUE(actual.value());
    } else {
      EXPECT_FALSE(actual.has_value());
    }
  }
  facebook::velox::RowVectorPtr makeSingleStringInputRow(
      std::optional<std::string> input) {
    auto vec = makeNullableFlatVector<std::string>({input});
    return makeRowVector({vec});
  }
};

TEST_F(GeometryFunctionsTest, errorStGeometryFromTextAndParsing) {
  const auto assertGeomFromText = [&](const std::optional<std::string>& a) {
    return evaluateOnce<std::string>("ST_GeometryFromText(c0)", a);
  };
  const auto assertGeomFromBinary = [&](const std::optional<std::string>& wkt) {
    return evaluateOnce<std::string>(
        "to_hex(ST_AsBinary(ST_GeomFromBinary(from_hex(c0))))", wkt);
  };

  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("xyz"),
      "Failed to parse WKT: ParseException: Unknown type: 'XYZ'");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("LINESTRING (-71.3839245 42.3128124)"),
      "Failed to parse WKT: IllegalArgumentException: point array must contain 0 or >1 elements");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText(
          "POLYGON ((-13.637339 9.617113, -13.637339 9.617113))"),
      "Failed to parse WKT: IllegalArgumentException: Invalid number of points in LinearRing found 2 - must be 0 or >= 4");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("POLYGON(0 0)"),
      "Failed to parse WKT: ParseException: Expected word but encountered number: '0'");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("POLYGON((0 0))"),
      "Failed to parse WKT: IllegalArgumentException: point array must contain 0 or >1 elements");

  // WKT invalid cases
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText(""), "Expected word but encountered end of stream");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("RANDOM_TEXT"), "Unknown type: 'RANDOM_TEXT'");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("LINESTRING (1 1)"),
      "point array must contain 0 or >1 elements");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("LINESTRING ()"),
      "Expected number but encountered ')'");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("POLYGON ((0 0, 0 0))"),
      "Invalid number of points in LinearRing found 2 - must be 0 or >= 4");
  VELOX_ASSERT_USER_THROW(
      assertGeomFromText("POLYGON ((0 0, 0 1, 1 1, 1 0))"),
      "Points of LinearRing do not form a closed linestring");

  // WKB invalid cases
  // Empty
  VELOX_ASSERT_USER_THROW(
      assertGeomFromBinary(""), "Unexpected EOF parsing WKB");

  // Random bytes
  VELOX_ASSERT_USER_THROW(
      assertGeomFromBinary("ABCDEF"), "Unexpected EOF parsing WKB");

  // Unrecognized geometry type
  VELOX_ASSERT_USER_THROW(
      assertGeomFromBinary("0109000000000000000000F03F0000000000000040"),
      "Unknown WKB type 9");

  // Point with missing y
  VELOX_ASSERT_USER_THROW(
      assertGeomFromBinary("0101000000000000000000F03F"),
      "Unexpected EOF parsing WKB");

  // LineString with only one point
  VELOX_ASSERT_THROW(
      assertGeomFromBinary(
          "010200000001000000000000000000F03F000000000000F03F"),
      "point array must contain 0 or >1 elements");

  // Polygon with unclosed LinString
  VELOX_ASSERT_THROW(
      assertGeomFromBinary(
          "01030000000100000004000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F0000000000000000"),
      "Points of LinearRing do not form a closed linestring");

  VELOX_ASSERT_THROW(
      assertGeomFromBinary(
          "010300000001000000020000000000000000000000000000000000000000000000000000000000000000000000"),
      "Invalid number of points in LinearRing found 2 - must be 0 or >= 4");
}

TEST_F(GeometryFunctionsTest, wktAndWkb) {
  const auto wktRoundTrip = [&](const std::optional<std::string>& a) {
    return evaluateOnce<std::string>("ST_AsText(ST_GeometryFromText(c0))", a);
  };

  const auto wktToWkb = [&](const std::optional<std::string>& wkt) {
    return evaluateOnce<std::string>(
        "to_hex(ST_AsBinary(ST_GeometryFromText(c0)))", wkt);
  };

  const auto wkbToWkT = [&](const std::optional<std::string>& wkb) {
    return evaluateOnce<std::string>(
        "ST_AsText(ST_GeomFromBinary(from_hex(c0)))", wkb);
  };

  const auto wkbRoundTrip = [&](const std::optional<std::string>& wkt) {
    return evaluateOnce<std::string>(
        "to_hex(ST_AsBinary(ST_GeomFromBinary(from_hex(c0))))", wkt);
  };

  const std::vector<std::string> wkts = {
      "POINT (1 2)",
      "LINESTRING (0 0, 10 10)",
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 4 1, 4 4, 1 4, 1 1))",
      "MULTIPOINT (1 2, 3 4)",
      "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
      "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))",
      "MULTIPOLYGON (((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1)), ((5 5, 5 7, 7 7, 7 5, 5 5)))",
      "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))",
      "GEOMETRYCOLLECTION (POINT (1 1), GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), GEOMETRYCOLLECTION (POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2)))))",
      "GEOMETRYCOLLECTION (MULTILINESTRING ((0 0, 1 1), (2 2, 3 3)), MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2))))"};

  const std::vector<std::string> wkbs = {"0101000000000000000000F03F0000000000000040", "0102000000020000000000000000000000000000000000000000000000000024400000000000002440", "010300000001000000050000000000000000000000000000000000000000000000000000000000000000001440000000000000144000000000000014400000000000001440000000000000000000000000000000000000000000000000", "01030000000200000005000000000000000000000000000000000000000000000000000000000000000000144000000000000014400000000000001440000000000000144000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F0000000000001040000000000000F03F00000000000010400000000000001040000000000000F03F0000000000001040000000000000F03F000000000000F03F", "0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040", "01050000000200000001020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F0102000000020000000000000000000040000000000000004000000000000008400000000000000840", "01060000000200000001030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010300000001000000050000000000000000000040000000000000004000000000000000400000000000000840000000000000084000000000000008400000000000000840000000000000004000000000000000400000000000000040", "01060000000200000001030000000200000005000000000000000000000000000000000000000000000000000000000000000000104000000000000010400000000000001040000000000000104000000000000000000000000000000000000000000000000005000000000000000000F03F000000000000F03F0000000000000840000000000000F03F00000000000008400000000000000840000000000000F03F0000000000000840000000000000F03F000000000000F03F010300000001000000050000000000000000001440000000000000144000000000000014400000000000001C400000000000001C400000000000001C400000000000001C40000000000000144000000000000014400000000000001440", "0107000000020000000101000000000000000000F03F00000000000000400102000000020000000000000000000840000000000000104000000000000014400000000000001840", "0107000000020000000101000000000000000000F03F000000000000F03F01070000000200000001020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F010700000001000000010300000001000000050000000000000000000040000000000000004000000000000000400000000000000840000000000000084000000000000008400000000000000840000000000000004000000000000000400000000000000040", "01070000000200000001050000000200000001020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F010200000002000000000000000000004000000000000000400000000000000840000000000000084001060000000200000001030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000010300000001000000050000000000000000000040000000000000004000000000000000400000000000000840000000000000084000000000000008400000000000000840000000000000004000000000000000400000000000000040"};

  const std::vector<std::string> bigEndianWkbs = {
      "00000000013FF00000000000004000000000000000",
      "0000000002000000020000000000000000000000000000000040240000000000004024000000000000",
      "000000000300000001000000050000000000000000000000000000000000000000000000004014000000000000401400000000000040140000000000004014000000000000000000000000000000000000000000000000000000000000",
      "000000000300000002000000050000000000000000000000000000000000000000000000004014000000000000401400000000000040140000000000004014000000000000000000000000000000000000000000000000000000000000000000053FF00000000000003FF000000000000040100000000000003FF0000000000000401000000000000040100000000000003FF000000000000040100000000000003FF00000000000003FF0000000000000",
      "00000000040000000200000000013FF00000000000004000000000000000000000000140080000000000004010000000000000",
      "000000000500000002000000000200000002000000000000000000000000000000003FF00000000000003FF00000000000000000000002000000024000000000000000400000000000000040080000000000004008000000000000",
      "000000000600000002000000000300000001000000050000000000000000000000000000000000000000000000003FF00000000000003FF00000000000003FF00000000000003FF0000000000000000000000000000000000000000000000000000000000000000000000300000001000000054000000000000000400000000000000040000000000000004008000000000000400800000000000040080000000000004008000000000000400000000000000040000000000000004000000000000000",
      "000000000600000002000000000300000002000000050000000000000000000000000000000000000000000000004010000000000000401000000000000040100000000000004010000000000000000000000000000000000000000000000000000000000000000000053FF00000000000003FF00000000000003FF000000000000040080000000000004008000000000000400800000000000040080000000000003FF00000000000003FF00000000000003FF000000000000000000000030000000100000005401400000000000040140000000000004014000000000000401c000000000000401c000000000000401c000000000000401c000000000000401400000000000040140000000000004014000000000000",
      "00000000070000000200000000013FF000000000000040000000000000000000000002000000024008000000000000401000000000000040140000000000004018000000000000",
      "00000000070000000200000000013FF00000000000003FF0000000000000000000000700000002000000000200000002000000000000000000000000000000003FF00000000000003FF0000000000000000000000700000001000000000300000001000000054000000000000000400000000000000040080000000000004000000000000000400800000000000040080000000000004000000000000000400800000000000040000000000000004000000000000000",
      //      "000000000700000002000000000500000002000000000200000002000000000000000000000000000000003FF00000000000003FF0000000000000000000000200000002400000000000000040000000000000004008000000000000400800000000000000000000060000000200000000030000000100000005000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000010300000001000000050000000000000000000040000000000000004000000000000000400000000000000840000000000000084000000000000008400000000000000840000000000000004000000000000000400000000000000040"
      "000000000700000002000000000500000002000000000200000002000000000000000000000000000000003FF00000000000003FF00000000000000000000002000000024000000000000000400000000000000040080000000000004008000000000000000000000600000002000000000300000001000000050000000000000000000000000000000000000000000000003FF00000000000003FF00000000000003FF00000000000003FF0000000000000000000000000000000000000000000000000000000000000000000000300000001000000054000000000000000400000000000000040000000000000004008000000000000400800000000000040080000000000004008000000000000400000000000000040000000000000004000000000000000"};

  for (size_t i = 0; i < wkts.size(); i++) {
    assert(i < wkbs.size() && i < bigEndianWkbs.size());
    EXPECT_EQ(wkts[i], wktRoundTrip(wkts[i]));
    EXPECT_EQ(wkbs[i], wktToWkb(wkts[i]));
    EXPECT_EQ(wkts[i], wkbToWkT(wkbs[i]));
    EXPECT_EQ(wkbs[i], wkbRoundTrip(wkbs[i]));

    EXPECT_EQ(wkbs[i], wkbRoundTrip(bigEndianWkbs[i]));
    EXPECT_EQ(wkts[i], wkbToWkT(bigEndianWkbs[i]));
  }

  const std::vector<std::string> emptyGeometryWkts = {
      "POINT EMPTY",
      "LINESTRING EMPTY",
      "POLYGON EMPTY",
      "MULTIPOINT EMPTY",
      "MULTILINESTRING EMPTY",
      "MULTIPOLYGON EMPTY",
      "GEOMETRYCOLLECTION EMPTY"};

  const std::vector<std::string> emptyGeometryWkbs = {
      "0101000000000000000000F87F000000000000F87F",
      "010200000000000000",
      "010300000000000000",
      "010400000000000000",
      "010500000000000000",
      "010600000000000000",
      "010700000000000000"};

  for (size_t i = 0; i < emptyGeometryWkts.size(); i++) {
    assert(i < emptyGeometryWkbs.size());
    EXPECT_EQ(wktRoundTrip(emptyGeometryWkts[i]), emptyGeometryWkts[i]);
    EXPECT_EQ(emptyGeometryWkbs[i], wktToWkb(emptyGeometryWkts[i]));
    EXPECT_EQ(emptyGeometryWkts[i], wkbToWkT(emptyGeometryWkbs[i]));
    EXPECT_EQ(emptyGeometryWkbs[i], wkbRoundTrip(emptyGeometryWkbs[i]));
  }
}

// Constructors and accessors

TEST_F(GeometryFunctionsTest, testStPoint) {
  const auto assertPoint = [&](const std::optional<double>& x,
                               const std::optional<double> y) {
    std::optional<double> actualX =
        evaluateOnce<double>("ST_X(ST_Point(c0, c1))", x, y);
    std::optional<double> actualY =
        evaluateOnce<double>("ST_Y(ST_Point(c0, c1))", x, y);
    if (x.has_value() && y.has_value()) {
      EXPECT_TRUE(actualX.has_value());
      EXPECT_TRUE(actualY.has_value());
      EXPECT_EQ(x.value(), actualX.value());
      EXPECT_EQ(y.value(), actualY.value());
    } else {
      EXPECT_FALSE(actualX.has_value());
      EXPECT_FALSE(actualY.has_value());
    }
  };

  assertPoint(std::nullopt, 0.0);
  assertPoint(0.0, 0.0);
  assertPoint(1.0, -23.12344);
  VELOX_ASSERT_THROW(
      assertPoint(0.0, NAN),
      "ST_Point requires finite coordinates, got x=0 y=nan");
  VELOX_ASSERT_THROW(
      assertPoint(INFINITY, 0.0),
      "ST_Point requires finite coordinates, got x=inf y=0");

  std::optional<double> nullX =
      evaluateOnce<double>("ST_X(ST_GeometryFromText('POINT EMPTY'))");
  EXPECT_FALSE(nullX.has_value());
  std::optional<double> nullY =
      evaluateOnce<double>("ST_Y(ST_GeometryFromText('POINT EMPTY'))");
  EXPECT_FALSE(nullY.has_value());
}

// Relationship predicates

TEST_F(GeometryFunctionsTest, testStRelate) {
  const auto assertStRelate =
      [&](std::optional<std::string_view> leftWkt,
          std::optional<std::string_view> rightWkt,
          std::optional<std::string_view> relateCondition,
          bool expected) {
        std::optional<bool> actual = evaluateOnce<bool>(
            "ST_Relate(ST_GeometryFromText(c0), ST_GeometryFromText(c1), c2)",
            leftWkt,
            rightWkt,
            relateCondition);
        if (leftWkt.has_value() && rightWkt.has_value() &&
            relateCondition.has_value()) {
          EXPECT_TRUE(actual.has_value());
          EXPECT_EQ(actual.value(), expected);
        } else {
          EXPECT_FALSE(actual.has_value());
        }
      };

  assertStRelate(
      "LINESTRING (0 0, 3 3)", "LINESTRING (1 1, 4 1)", "****T****", false);
  assertStRelate(
      "POLYGON ((2 0, 2 1, 3 1, 2 0))",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "****T****",
      true);
  assertStRelate(
      "POLYGON ((2 0, 2 1, 3 1, 2 0))",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "T********",
      false);
  assertStRelate(std::nullopt, std::nullopt, std::nullopt, false);
}

TEST_F(GeometryFunctionsTest, testStContains) {
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[1],
      kRelationGeometriesWKT[0],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[2],
      kRelationGeometriesWKT[3],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[4],
      kRelationGeometriesWKT[5],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[1],
      kRelationGeometriesWKT[6],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[2],
      kRelationGeometriesWKT[6],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[2],
      kRelationGeometriesWKT[7],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[3],
      kRelationGeometriesWKT[6],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[3],
      kRelationGeometriesWKT[7],
      true);
  assertRelation(
      "ST_Contains",
      kRelationGeometriesWKT[4],
      kRelationGeometriesWKT[7],
      true);

  assertRelation("ST_Contains", std::nullopt, "POINT (25 25)", false);
  assertRelation("ST_Contains", "POINT (20 20)", "POINT (25 25)", false);
  assertRelation(
      "ST_Contains", "MULTIPOINT (20 20, 25 25)", "POINT (25 25)", true);
  assertRelation(
      "ST_Contains", "LINESTRING (20 20, 30 30)", "POINT (25 25)", true);
  assertRelation(
      "ST_Contains",
      "LINESTRING (20 20, 30 30)",
      "MULTIPOINT (25 25, 31 31)",
      false);
  assertRelation(
      "ST_Contains",
      "LINESTRING (20 20, 30 30)",
      "LINESTRING (25 25, 27 27)",
      true);
  assertRelation(
      "ST_Contains",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 4 4), (2 1, 6 1))",
      false);
  assertRelation(
      "ST_Contains",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))",
      true);
  assertRelation(
      "ST_Contains",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))",
      false);
  assertRelation(
      "ST_Contains",
      "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
      "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
      true);
  assertRelation(
      "ST_Contains",
      "LINESTRING (20 20, 30 30)",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      false);
  assertRelation(
      "ST_Contains",
      "LINESTRING EMPTY",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      false);
  assertRelation(
      "ST_Contains", "LINESTRING (20 20, 30 30)", "POLYGON EMPTY", false);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Contains",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStCrosses) {
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[1], kRelationGeometriesWKT[3], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[3], kRelationGeometriesWKT[1], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[2], kRelationGeometriesWKT[4], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[4], kRelationGeometriesWKT[2], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[2], kRelationGeometriesWKT[5], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[5], kRelationGeometriesWKT[2], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[3], kRelationGeometriesWKT[4], true);
  assertRelation(
      "ST_Crosses", kRelationGeometriesWKT[4], kRelationGeometriesWKT[3], true);

  assertRelation("ST_Crosses", std::nullopt, "POINT (25 25)", false);
  assertRelation("ST_Crosses", "POINT (20 20)", "POINT (25 25)", false);
  assertRelation(
      "ST_Crosses", "LINESTRING (20 20, 30 30)", "POINT (25 25)", false);
  assertRelation(
      "ST_Crosses",
      "LINESTRING (20 20, 30 30)",
      "MULTIPOINT (25 25, 31 31)",
      true);
  assertRelation(
      "ST_Crosses", "LINESTRING(0 0, 1 1)", "LINESTRING (1 0, 0 1)", true);
  assertRelation(
      "ST_Crosses",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))",
      false);
  assertRelation(
      "ST_Crosses",
      "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
      "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
      false);
  assertRelation(
      "ST_Crosses",
      "LINESTRING (-2 -2, 6 6)",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      true);
  assertRelation("ST_Crosses", "POINT (20 20)", "POINT (20 20)", false);
  assertRelation(
      "ST_Crosses",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      false);
  assertRelation(
      "ST_Crosses",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "LINESTRING (0 0, 0 4, 4 4, 4 0)",
      false);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Crosses",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStDisjoint) {
  assertRelation("ST_Disjoint", std::nullopt, "POINT (150 150)", true);
  assertRelation("ST_Disjoint", "POINT (50 100)", "POINT (150 150)", true);
  assertRelation(
      "ST_Disjoint", "MULTIPOINT (50 100, 50 200)", "POINT (50 100)", false);
  assertRelation(
      "ST_Disjoint", "LINESTRING (0 0, 0 1)", "LINESTRING (1 1, 1 0)", true);
  assertRelation(
      "ST_Disjoint", "LINESTRING (2 1, 1 2)", "LINESTRING (3 1, 1 3)", true);
  assertRelation(
      "ST_Disjoint", "LINESTRING (1 1, 3 3)", "LINESTRING (3 1, 1 3)", false);
  assertRelation(
      "ST_Disjoint",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (20 150, 100 150)",
      false);
  assertRelation(
      "ST_Disjoint",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      false);
  assertRelation(
      "ST_Disjoint",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      true);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Disjoint",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStEquals) {
  for (const auto& leftWkt : kRelationGeometriesWKT) {
    for (const auto& rightWkt : kRelationGeometriesWKT) {
      assertRelation("ST_Equals", leftWkt, rightWkt, leftWkt == rightWkt);
    }
  }

  assertRelation("ST_Equals", std::nullopt, "POINT (150 150)", false);
  assertRelation("ST_Equals", "POINT (50 100)", "POINT (150 150)", false);
  assertRelation(
      "ST_Equals", "MULTIPOINT (50 100, 50 200)", "POINT (50 100)", false);
  assertRelation(
      "ST_Equals", "LINESTRING (0 0, 0 1)", "LINESTRING (1 1, 1 0)", false);
  assertRelation(
      "ST_Equals", "LINESTRING (0 0, 2 2)", "LINESTRING (0 0, 2 2)", true);
  assertRelation(
      "ST_Equals",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      false);
  assertRelation(
      "ST_Equals",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((3 3, 3 1, 1 1, 1 3, 3 3))",
      true);
  assertRelation(
      "ST_Equals",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))",
      false);
  // Invalid geometries.  This test might have to change when upgrading GEOS.
  assertRelation(
      "ST_Equals",
      "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
      "LINESTRING (0 0, 1 1, 1 0, 0 1)",
      false);
}

TEST_F(GeometryFunctionsTest, testStIntersects) {
  assertRelation("ST_Intersects", std::nullopt, "POINT (150 150)", false);
  assertRelation("ST_Intersects", "POINT (50 100)", "POINT (150 150)", false);
  assertRelation(
      "ST_Intersects", "MULTIPOINT (50 100, 50 200)", "POINT (50 100)", true);
  assertRelation(
      "ST_Intersects", "LINESTRING (0 0, 0 1)", "LINESTRING (1 1, 1 0)", false);
  assertRelation(
      "ST_Intersects",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (20 150, 100 150)",
      true);
  assertRelation(
      "ST_Intersects",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      true);
  assertRelation(
      "ST_Intersects",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      false);
  assertRelation(
      "ST_Intersects",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))",
      true);
  assertRelation(
      "ST_Intersects",
      "POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))",
      "LINESTRING (16.6 53, 16.6 56)",
      true);
  assertRelation(
      "ST_Intersects",
      "POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))",
      "LINESTRING (16.6667 54.05, 16.8667 54.05)",
      false);
  assertRelation(
      "ST_Intersects",
      "POLYGON ((16.5 54, 16.5 54.1, 16.51 54.1, 16.8 54, 16.5 54))",
      "LINESTRING (16.6667 54.25, 16.8667 54.25)",
      false);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Intersects",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStOverlaps) {
  assertRelation(
      "ST_Overlaps",
      kRelationGeometriesWKT[1],
      kRelationGeometriesWKT[2],
      true);
  assertRelation(
      "ST_Overlaps",
      kRelationGeometriesWKT[2],
      kRelationGeometriesWKT[1],
      true);

  assertRelation("ST_Overlaps", std::nullopt, "POINT (150 150)", false);
  assertRelation("ST_Overlaps", "POINT (50 100)", "POINT (150 150)", false);
  assertRelation("ST_Overlaps", "POINT (50 100)", "POINT (50 100)", false);
  assertRelation(
      "ST_Overlaps", "MULTIPOINT (50 100, 50 200)", "POINT (50 100)", false);
  assertRelation(
      "ST_Overlaps", "LINESTRING (0 0, 0 1)", "LINESTRING (1 1, 1 0)", false);
  assertRelation(
      "ST_Overlaps",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      true);
  assertRelation(
      "ST_Overlaps",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))",
      true);
  assertRelation(
      "ST_Overlaps",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      false);
  assertRelation(
      "ST_Overlaps",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "LINESTRING (1 1, 4 4)",
      false);
  assertRelation(
      "ST_Overlaps",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      false);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Overlaps",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStTouches) {
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[0], kRelationGeometriesWKT[2], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[2], kRelationGeometriesWKT[0], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[0], kRelationGeometriesWKT[3], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[3], kRelationGeometriesWKT[0], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[1], kRelationGeometriesWKT[4], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[4], kRelationGeometriesWKT[1], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[1], kRelationGeometriesWKT[5], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[5], kRelationGeometriesWKT[1], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[3], kRelationGeometriesWKT[5], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[5], kRelationGeometriesWKT[3], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[1], kRelationGeometriesWKT[7], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[7], kRelationGeometriesWKT[1], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[5], kRelationGeometriesWKT[7], true);
  assertRelation(
      "ST_Touches", kRelationGeometriesWKT[7], kRelationGeometriesWKT[5], true);

  assertRelation("ST_Touches", std::nullopt, "POINT (150 150)", false);
  assertRelation("ST_Touches", "POINT (50 100)", "POINT (150 150)", false);
  assertRelation(
      "ST_Touches", "MULTIPOINT (50 100, 50 200)", "POINT (50 100)", false);
  assertRelation(
      "ST_Touches",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (20 150, 100 150)",
      false);
  assertRelation(
      "ST_Touches",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      false);
  assertRelation(
      "ST_Touches", "POINT (1 2)", "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", true);
  assertRelation(
      "ST_Touches",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      false);
  assertRelation(
      "ST_Touches",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "LINESTRING (0 0, 1 1)",
      true);
  assertRelation(
      "ST_Touches",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((3 3, 3 5, 5 5, 5 3, 3 3))",
      true);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Touches",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT (1 1)",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

TEST_F(GeometryFunctionsTest, testStWithin) {
  // 0, 1: Within (1, 0: Contains)
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[0], kRelationGeometriesWKT[1], true);
  // 2, 3: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[3], kRelationGeometriesWKT[2], true);
  // 4, 5: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[5], kRelationGeometriesWKT[4], true);
  // 1, 6: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[6], kRelationGeometriesWKT[1], true);
  // 2, 6: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[6], kRelationGeometriesWKT[2], true);
  // 2, 7: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[7], kRelationGeometriesWKT[2], true);
  // 3, 6: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[6], kRelationGeometriesWKT[3], true);
  // 3, 7: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[7], kRelationGeometriesWKT[3], true);
  // 4, 7: Contains
  assertRelation(
      "ST_Within", kRelationGeometriesWKT[7], kRelationGeometriesWKT[4], true);

  assertRelation("ST_Within", std::nullopt, "POINT (150 150)", false);
  assertRelation("ST_Within", "POINT (50 100)", "POINT (150 150)", false);
  assertRelation(
      "ST_Within", "POINT (50 100)", "MULTIPOINT (50 100, 50 200)", true);
  assertRelation(
      "ST_Within",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (50 50, 50 250)",
      true);
  assertRelation(
      "ST_Within",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      false);
  assertRelation(
      "ST_Within", "POINT (3 2)", "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", true);
  assertRelation(
      "ST_Within",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      true);
  assertRelation(
      "ST_Within",
      "LINESTRING (1 1, 3 3)",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      true);
  assertRelation(
      "ST_Within",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))",
      false);
  assertRelation(
      "ST_Within",
      "POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      false);

  VELOX_ASSERT_USER_THROW(
      assertRelation(
          "ST_Within",
          "POINT (0 0)",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          false),
      "TopologyException: side location conflict at 1 2. This can occur if the input geometry is invalid.");
}

// Overlay operations

TEST_F(GeometryFunctionsTest, testStDifference) {
  assertOverlay("ST_Difference", std::nullopt, std::nullopt, std::nullopt);
  assertOverlay(
      "ST_Difference", "POINT (50 100)", "POINT (150 150)", "POINT (50 100)");
  assertOverlay(
      "ST_Difference",
      "MULTIPOINT (50 100, 50 200)",
      "POINT (50 100)",
      "POINT (50 200)");
  assertOverlay(
      "ST_Difference",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (50 50, 50 150)",
      "LINESTRING (50 150, 50 200)");
  assertOverlay(
      "ST_Difference",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((2 1, 4 1), (3 3, 7 3))",
      "MULTILINESTRING ((1 1, 2 1), (4 1, 5 1), (2 4, 4 4))");
  assertOverlay(
      "ST_Difference",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))",
      "POLYGON ((1 4, 2 4, 2 2, 4 2, 4 1, 1 1, 1 4))");
  assertOverlay(
      "ST_Difference",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))",
      "POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))",
      "POLYGON ((0 1, 1 1, 1 0, 0 0, 0 1))");

  ASSERT_THROW(
      assertOverlay(
          "ST_Difference",
          "LINESTRING (0 0, 1 1, 1 0, 0 1)",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT EMPTY"),
      facebook::velox::VeloxUserError);
}

TEST_F(GeometryFunctionsTest, testStIntersection) {
  assertOverlay("ST_Intersection", std::nullopt, std::nullopt, std::nullopt);
  assertOverlay(
      "ST_Intersection", "POINT (50 100)", "POINT (150 150)", "POINT EMPTY");
  assertOverlay(
      "ST_Intersection",
      "MULTIPOINT (50 100, 50 200)",
      "POINT (50 100)",
      "POINT (50 100)");
  assertOverlay(
      "ST_Intersection",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (20 150, 100 150)",
      "POINT (50 150)");
  assertOverlay(
      "ST_Intersection",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      "GEOMETRYCOLLECTION (LINESTRING (3 4, 4 4), POINT (5 1))");
  assertOverlay(
      "ST_Intersection",
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      "POLYGON EMPTY");
  assertOverlay(
      "ST_Intersection",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))",
      "POLYGON ((0 1, 3 1, 3 3, 0 3, 0 1))",
      "GEOMETRYCOLLECTION (POLYGON ((1 3, 3 3, 3 1, 1 1, 1 3)), LINESTRING (0 1, 1 1))");
  assertOverlay(
      "ST_Intersection",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "LINESTRING (2 0, 2 3)",
      "LINESTRING (2 1, 2 3)");
  assertOverlay(
      "ST_Intersection",
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
      "LINESTRING (0 0, 1 -1, 1 2)",
      "GEOMETRYCOLLECTION (LINESTRING (1 1, 1 0), POINT (0 0))");

  ASSERT_THROW(
      assertOverlay(
          "ST_Intersection",
          "LINESTRING (0 0, 1 1, 1 0, 0 1)",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT EMPTY"),
      facebook::velox::VeloxUserError);
}

TEST_F(GeometryFunctionsTest, testStSymDifference) {
  assertOverlay("ST_SymDifference", std::nullopt, std::nullopt, std::nullopt);
  assertOverlay(
      "ST_SymDifference",
      "POINT (50 100)",
      "POINT (50 150)",
      "MULTIPOINT (50 100, 50 150)");
  assertOverlay(
      "ST_SymDifference",
      "MULTIPOINT (50 100, 60 200)",
      "MULTIPOINT (60 200, 70 150)",
      "MULTIPOINT (50 100, 70 150)");
  assertOverlay(
      "ST_SymDifference",
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (50 50, 50 150)",
      "MULTILINESTRING ((50 150, 50 200), (50 50, 50 100))");
  assertOverlay(
      "ST_SymDifference",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTILINESTRING ((3 4, 6 4), (5 0, 5 4))",
      "MULTILINESTRING ((1 1, 5 1), (2 4, 3 4), (4 4, 5 4), (5 4, 6 4), (5 0, 5 1), (5 1, 5 4))");
  assertOverlay(
      "ST_SymDifference",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))",
      "POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))",
      "MULTIPOLYGON (((1 4, 2 4, 2 2, 4 2, 4 1, 1 1, 1 4)), ((4 4, 2 4, 2 5, 5 5, 5 2, 4 2, 4 4)))");
  assertOverlay(
      "ST_SymDifference",
      "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
      "POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))",
      "MULTIPOLYGON (((0 2, 0 3, 2 3, 2 2, 0 2)), ((2 2, 3 2, 3 0, 2 0, 2 2)), ((2 4, 4 4, 4 2, 3 2, 3 3, 2 3, 2 4)))");

  ASSERT_THROW(
      assertOverlay(
          "ST_SymDifference",
          "LINESTRING (0 0, 1 1, 1 0, 0 1)",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT EMPTY"),
      facebook::velox::VeloxUserError);
}

TEST_F(GeometryFunctionsTest, testStUnion) {
  std::array<std::string_view, 7> emptyWkts = {
      "POINT EMPTY",
      "MULTIPOINT EMPTY",
      "LINESTRING EMPTY",
      "MULTILINESTRING EMPTY",
      "POLYGON EMPTY",
      "MULTIPOLYGON EMPTY",
      "GEOMETRYCOLLECTION EMPTY"};
  std::array<std::string_view, 7> simpleWkts = {
      "POINT (1 2)",
      "MULTIPOINT (1 2, 3 4)",
      "LINESTRING (0 0, 2 2, 4 4)",
      "MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9))",
      "POLYGON ((0 1, 1 1, 1 0, 0 0, 0 1))",
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      "GEOMETRYCOLLECTION (LINESTRING (0 5, 5 5), POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1)))"};

  // empty geometry
  for (std::string_view emptyWkt : emptyWkts) {
    for (std::string_view simpleWkt : simpleWkts) {
      assertOverlay("ST_Union", emptyWkt, simpleWkt, simpleWkt);
    }
  }

  // self union
  for (std::string_view simpleWkt : simpleWkts) {
    assertOverlay("ST_Union", simpleWkt, simpleWkt, simpleWkt);
  }

  assertOverlay("ST_Union", std::nullopt, std::nullopt, std::nullopt);

  // touching union
  assertOverlay(
      "ST_Union",
      "POINT (1 2)",
      "MULTIPOINT (1 2, 3 4)",
      "MULTIPOINT (1 2, 3 4)");
  assertOverlay(
      "ST_Union",
      "MULTIPOINT (1 2)",
      "MULTIPOINT (1 2, 3 4)",
      "MULTIPOINT (1 2, 3 4)");
  assertOverlay(
      "ST_Union",
      "LINESTRING (0 1, 1 2)",
      "LINESTRING (1 2, 3 4)",
      "LINESTRING (0 1, 1 2, 3 4)");
  assertOverlay(
      "ST_Union",
      "MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9))",
      "MULTILINESTRING ((5 5, 7 7, 9 9), (11 11, 13 13, 15 15))",
      "MULTILINESTRING ((0 0, 2 2, 4 4), (5 5, 7 7, 9 9), (11 11, 13 13, 15 15))");
  assertOverlay(
      "ST_Union",
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
      "POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))",
      "POLYGON ((0 0, 0 1, 1 1, 2 1, 2 0, 1 0, 0 0))");
  assertOverlay(
      "ST_Union",
      "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)))",
      "MULTIPOLYGON (((1 0, 2 0, 2 1, 1 1, 1 0)))",
      "POLYGON ((0 0, 0 1, 1 1, 2 1, 2 0, 1 0, 0 0))");
  assertOverlay(
      "ST_Union",
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)), POINT (1 2))",
      "GEOMETRYCOLLECTION (POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0)), MULTIPOINT ((1 2), (3 4)))",
      "GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4), POLYGON ((0 0, 0 1, 1 1, 2 1, 2 0, 1 0, 0 0)))");

  // within union
  assertOverlay(
      "ST_Union",
      "MULTIPOINT (20 20, 25 25)",
      "POINT (25 25)",
      "MULTIPOINT (20 20, 25 25)");
  assertOverlay(
      "ST_Union",
      "LINESTRING (20 20, 30 30)",
      "POINT (25 25)",
      "LINESTRING (20 20, 30 30)");
  assertOverlay(
      "ST_Union",
      "LINESTRING (20 20, 30 30)",
      "LINESTRING (25 25, 27 27)",
      "LINESTRING (20 20, 25 25, 27 27, 30 30)");
  assertOverlay(
      "ST_Union",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
      "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))",
      "POLYGON ((0 4, 4 4, 4 0, 0 0, 0 4))");
  assertOverlay(
      "ST_Union",
      "MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0)), ((2 2, 2 4, 4 4, 4 2, 2 2)))",
      "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
      "MULTIPOLYGON (((2 2, 2 3, 2 4, 4 4, 4 2, 3 2, 2 2)), ((0 0, 0 2, 2 2, 2 0, 0 0)))");
  assertOverlay(
      "ST_Union",
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0)), MULTIPOINT (20 20, 25 25))",
      "GEOMETRYCOLLECTION (POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1)), POINT (25 25))",
      "GEOMETRYCOLLECTION (MULTIPOINT (20 20, 25 25), POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0)))");

  // overlap union
  assertOverlay(
      "ST_Union",
      "LINESTRING (1 1, 3 1)",
      "LINESTRING (2 1, 4 1)",
      "LINESTRING (1 1, 2 1, 3 1, 4 1)");
  assertOverlay(
      "ST_Union",
      "MULTILINESTRING ((1 1, 3 1))",
      "MULTILINESTRING ((2 1, 4 1))",
      "LINESTRING (1 1, 2 1, 3 1, 4 1)");
  assertOverlay(
      "ST_Union",
      "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))",
      "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))",
      "POLYGON ((1 1, 1 3, 2 3, 2 4, 4 4, 4 2, 3 2, 3 1, 1 1))");
  assertOverlay(
      "ST_Union",
      "MULTIPOLYGON (((1 1, 3 1, 3 3, 1 3, 1 1)))",
      "MULTIPOLYGON (((2 2, 4 2, 4 4, 2 4, 2 2)))",
      "POLYGON ((1 1, 1 3, 2 3, 2 4, 4 4, 4 2, 3 2, 3 1, 1 1))");
  assertOverlay(
      "ST_Union",
      "GEOMETRYCOLLECTION (POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), LINESTRING (1 1, 3 1))",
      "GEOMETRYCOLLECTION (POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2)), LINESTRING (2 1, 4 1))",
      "GEOMETRYCOLLECTION (LINESTRING (3 1, 4 1), POLYGON ((1 1, 1 3, 2 3, 2 4, 4 4, 4 2, 3 2, 3 1, 2 1, 1 1)))");

  ASSERT_THROW(
      assertOverlay(
          "ST_Union",
          "LINESTRING (0 0, 1 1, 1 0, 0 1)",
          "MULTIPOLYGON ( ((0 0, 0 2, 2 2, 2 0, 0 0)), ((1 1, 1 3, 3 3, 3 1, 1 1)) )",
          "POINT EMPTY"),
      facebook::velox::VeloxUserError);
}

// Accessors

TEST_F(GeometryFunctionsTest, testStIsSimpleValid) {
  const auto assertStIsValidSimpleFunc = [&](std::optional<std::string> wkt,
                                             bool expectedValid,
                                             bool expectedSimple) {
    std::optional<bool> validResult =
        evaluateOnce<bool>("ST_IsValid(ST_GeometryFromText(c0))", wkt);
    std::optional<bool> simpleResult =
        evaluateOnce<bool>("ST_IsSimple(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(validResult.has_value());
      ASSERT_TRUE(simpleResult.has_value());
      ASSERT_EQ(validResult.value(), expectedValid)
          << " from WKT: " << wkt.value();
      ASSERT_EQ(simpleResult.value(), expectedSimple)
          << " from WKT: " << wkt.value();
    } else {
      ASSERT_FALSE(validResult.has_value());
      ASSERT_FALSE(simpleResult.has_value());
    }
  };

  assertStIsValidSimpleFunc(std::nullopt, true, true);
  assertStIsValidSimpleFunc("POINT EMPTY", true, true);
  assertStIsValidSimpleFunc("MULTIPOINT EMPTY", true, true);
  assertStIsValidSimpleFunc("LINESTRING EMPTY", true, true);
  assertStIsValidSimpleFunc("MULTILINESTRING EMPTY", true, true);
  assertStIsValidSimpleFunc("POLYGON EMPTY", true, true);
  assertStIsValidSimpleFunc("MULTIPOLYGON EMPTY", true, true);
  assertStIsValidSimpleFunc("GEOMETRYCOLLECTION EMPTY", true, true);

  // valid geometries
  assertStIsValidSimpleFunc("POINT (1.5 2.5)", true, true);

  assertStIsValidSimpleFunc("MULTIPOINT (1 2, 3 4)", true, true);
  assertStIsValidSimpleFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", true, true);
  // Repeated point
  assertStIsValidSimpleFunc(
      "MULTIPOINT ((0 0), (0 1), (0 1), (1 1))", true, false);
  // Duplicate point
  assertStIsValidSimpleFunc("MULTIPOINT (1 2, 2 4, 3 6, 1 2)", true, false);

  assertStIsValidSimpleFunc("LINESTRING (0 0, 1 2, 3 4)", true, true);
  // Geos/JTS considers LineStrings with repeated points valid/simple (it drops
  // the dupes), even though ISO says they should be invalid.
  assertStIsValidSimpleFunc(
      "LINESTRING (0 0, 0 1, 0 1, 1 1, 1 0, 0 0)", true, true);
  // Valid but not simple: Self-intersection at (0, 1) (vertex)
  assertStIsValidSimpleFunc(
      "LINESTRING (0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0)", true, false);
  assertStIsValidSimpleFunc("LINESTRING (8 4, 5 7)", true, true);
  assertStIsValidSimpleFunc("LINESTRING (1 1, 2 2, 1 3, 1 1)", true, true);
  // Valid but not simple: Self-intersection at (0.5, 0.5) (in segment)
  assertStIsValidSimpleFunc("LINESTRING (0 0, 1 1, 1 0, 0 1)", true, false);

  assertStIsValidSimpleFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", true, true);
  // Valid but not simple: Self-intersection at (2, 1)
  assertStIsValidSimpleFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 0))", true, false);

  assertStIsValidSimpleFunc("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", true, true);
  // Hole outside of shell
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, 0 1, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))",
      false,
      true);
  // Hole outside of shell
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))",
      false,
      true);
  // Backtrack from (2, 1) to (1, 1)
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0))", false, false);
  // Hole segment (0 1, 1 1) overlaps shell segment
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 1, 1 1, 0.5 0.5, 0 1))",
      false,
      true);
  // Hole intersects shell at two points (0, 0) and (1, 1)
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 0, 0.5 0.7, 1 1, 0.5 0.4, 0 0))",
      false,
      true);
  // Shell intersects self at (0, 1)
  assertStIsValidSimpleFunc(
      "POLYGON ((0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0))", false, false);
  assertStIsValidSimpleFunc("POLYGON ((2 0, 2 1, 3 1, 2 0))", true, true);

  assertStIsValidSimpleFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      true,
      true);
  // Overlapping rectangles.  This is invalid but simple because multipolygon
  // rules are weird.
  assertStIsValidSimpleFunc(
      "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((0.5 0.5, 0.5 2, 2 2, 2 0.5, 0.5 0.5)))",
      false,
      true);

  assertStIsValidSimpleFunc(
      "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))) ",
      true,
      true);
  // Invalid Polygon
  assertStIsValidSimpleFunc(
      "GEOMETRYCOLLECTION (POINT (1 2), POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0)))",
      false,
      false);
}

TEST_F(GeometryFunctionsTest, testStArea) {
  const auto testStAreaFunc = [&](std::optional<std::string> wkt,
                                  std::optional<double> expectedArea) {
    std::optional<double> result =
        evaluateOnce<double>("ST_Area(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_TRUE(expectedArea.has_value());
      ASSERT_EQ(result.value(), expectedArea.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStAreaFunc("POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))", 16.0);
  testStAreaFunc("POLYGON EMPTY", 0.0);
  testStAreaFunc("LINESTRING (1 4, 2 5)", 0.0);
  testStAreaFunc("LINESTRING EMPTY", 0.0);
  testStAreaFunc("POINT (1 4)", 0.0);
  testStAreaFunc("POINT EMPTY", 0.0);
  testStAreaFunc("GEOMETRYCOLLECTION EMPTY", 0.0);

  // Test basic geometry collection. Area is the area of the polygon.
  testStAreaFunc(
      "GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1)))",
      6.0);

  // Test overlapping geometries. Area is the sum of the individual elements
  testStAreaFunc(
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)))",
      8.0);

  // Test nested geometry collection
  testStAreaFunc(
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))",
      14.0);
}

TEST_F(GeometryFunctionsTest, testGeometryInvalidReason) {
  const auto assertInvalidReason =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expectedMessage) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "geometry_invalid_reason(ST_GeometryFromText(c0))", wkt);

        if (wkt.has_value() && expectedMessage.has_value()) {
          ASSERT_TRUE(result.has_value()) << " from WKT: " << wkt.value();
          ASSERT_EQ(result.value(), expectedMessage.value())
              << " from WKT: " << wkt.value();
        } else {
          ASSERT_FALSE(result.has_value()) << " from WKT: " << wkt.value();
        }
      };

  // Invalid geometries
  assertInvalidReason(
      "POLYGON ((0 0, 0 1, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))",
      "Invalid Polygon: Hole lies outside shell");
  assertInvalidReason(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (2 2, 2 3, 3 3, 3 2, 2 2))",
      "Invalid Polygon: Hole lies outside shell");
  assertInvalidReason(
      "POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0))",
      "Invalid Polygon: Ring Self-intersection");
  assertInvalidReason(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 1, 1 1, 0.5 0.5, 0 1))",
      "Invalid Polygon: Self-intersection");
  assertInvalidReason(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0 0, 0.5 0.7, 1 1, 0.5 0.4, 0 0))",
      "Invalid Polygon: Interior is disconnected");
  assertInvalidReason(
      "POLYGON ((0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0))",
      "Invalid Polygon: Ring Self-intersection");
  assertInvalidReason(
      "MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((0.5 0.5, 0.5 2, 2 2, 2 0.5, 0.5 0.5)))",
      "Invalid MultiPolygon: Self-intersection");
  assertInvalidReason(
      "GEOMETRYCOLLECTION (POINT (1 2), POLYGON ((0 0, 0 1, 2 1, 1 1, 1 0, 0 0)))",
      "Invalid GeometryCollection: Ring Self-intersection");

  // non-simple geometries
  assertInvalidReason(
      "LINESTRING (0 0, -1 0.5, 0 1, 1 1, 1 0, 0 1, 0 0)",
      "Non-simple LineString: Self-intersection at or near (0 1)");
  assertInvalidReason(
      "MULTIPOINT (1 2, 2 4, 3 6, 1 2)",
      "Non-simple MultiPoint: Repeated point (1 2)");
  assertInvalidReason(
      "LINESTRING (0 0, 1 1, 1 0, 0 1)",
      "Non-simple LineString: Self-intersection at or near (0.5 0.5)");
  assertInvalidReason(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 0))",
      "Non-simple MultiLineString: Self-intersection at or near (3.5 1)");

  // valid geometries
  assertInvalidReason(std::nullopt, std::nullopt);
  assertInvalidReason("LINESTRING EMPTY", std::nullopt);
  assertInvalidReason("POINT (1 2)", std::nullopt);
  assertInvalidReason("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", std::nullopt);
  assertInvalidReason(
      "GEOMETRYCOLLECTION (MULTIPOINT (1 0, 1 1, 0 1, 0 0))", std::nullopt);
}

TEST_F(GeometryFunctionsTest, testSimplifyGeometry) {
  const auto assertSimplifyGeometry = [&](const std::optional<std::string>& wkt,
                                          std::optional<double> tolerance,
                                          const std::optional<std::string>&
                                              expectedWkt) {
    std::optional<bool> result = evaluateOnce<bool>(
        "ST_Equals(simplify_geometry(ST_GeometryFromText(c0), c1), ST_GeometryFromText(c2))",
        wkt,
        tolerance,
        expectedWkt);

    if (wkt.has_value() && tolerance.has_value() && expectedWkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_TRUE(result.value()) << " from WKT: " << wkt.value();
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  assertSimplifyGeometry("POLYGON EMPTY", 1.0, "POLYGON EMPTY");
  assertSimplifyGeometry(
      "POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))",
      1.5,
      "POLYGON ((1 0, 2 1, 4 1, 1 0))");
  // Simplifying by 0.0 leaves the geometry unchanged
  assertSimplifyGeometry(
      "POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))",
      0.0,
      "POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))");

  // Check different tolerance produce different answers
  assertSimplifyGeometry(
      "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))",
      1.0,
      "POLYGON ((1 0, 2 3, 3 3, 4 0, 1 0))");
  assertSimplifyGeometry(
      "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))",
      0.5,
      "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))");

  assertSimplifyGeometry(
      "POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))",
      std::nullopt,
      "POLYGON ((1 0, 2 1, 4 1, 1 0))");
  assertSimplifyGeometry(std::nullopt, 1.0, "POLYGON ((1 0, 2 1, 4 1, 1 0))");

  VELOX_ASSERT_USER_THROW(
      assertSimplifyGeometry(
          "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))",
          -0.5,
          "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))"),
      "simplification tolerance must be a non-negative finite number");

  VELOX_ASSERT_USER_THROW(
      assertSimplifyGeometry(
          "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))",
          std::nan("1"),
          "POLYGON ((1 0, 1 1, 2 1, 2 3, 3 3, 3 1, 4 1, 4 0, 1 0))"),
      "simplification tolerance must be a non-negative finite number");
}

TEST_F(GeometryFunctionsTest, testStBoundary) {
  const auto testStBoundaryFunc = [&](const std::optional<std::string>& wkt,
                                      const std::optional<std::string>&
                                          expected) {
    std::optional<bool> result = evaluateOnce<bool>(
        "ST_Equals(ST_Boundary(ST_GeometryFromText(c0)), ST_GeometryFromText(c1))",
        wkt,
        expected);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_TRUE(expected.has_value());
      ASSERT_TRUE(result.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStBoundaryFunc("POINT (1 2)", "GEOMETRYCOLLECTION EMPTY");
  testStBoundaryFunc(
      "MULTIPOINT (1 2, 2 4, 3 6, 4 8)", "GEOMETRYCOLLECTION EMPTY");
  testStBoundaryFunc("LINESTRING EMPTY", "MULTIPOINT EMPTY");
  testStBoundaryFunc("LINESTRING (8 4, 5 7)", "MULTIPOINT (8 4, 5 7)");
  testStBoundaryFunc(
      "LINESTRING (100 150, 50 60, 70 80, 160 170)",
      "MULTIPOINT (100 150, 160 170)");
  testStBoundaryFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "MULTIPOINT (1 1, 2 4, 4 4, 5 1)");
  testStBoundaryFunc(
      "POLYGON ((1 1, 4 1, 1 4, 1 1))", "LINESTRING (1 1, 1 4, 4 1, 1 1)");
  testStBoundaryFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "MULTILINESTRING ((1 1, 1 3, 3 3, 3 1, 1 1), (0 0, 0 2, 2 2, 2 0, 0 0))");
}

TEST_F(GeometryFunctionsTest, testStCentroid) {
  const auto testStCentroidFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_Centroid(ST_GeometryFromText(c0)))", wkt);

        if (wkt.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_TRUE(expected.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStCentroidFunc("LINESTRING EMPTY", "POINT EMPTY");
  testStCentroidFunc("POINT (3 5)", "POINT (3 5)");
  testStCentroidFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", "POINT (2.5 5)");
  testStCentroidFunc("LINESTRING (1 1, 2 2, 3 3)", "POINT (2 2)");
  testStCentroidFunc("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "POINT (3 2)");
  testStCentroidFunc("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", "POINT (2.5 2.5)");
  testStCentroidFunc("POLYGON ((1 1, 5 1, 3 4, 1 1))", "POINT (3 2)");
  testStCentroidFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      "POINT (3.3333333333333335 4)");
  testStCentroidFunc(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
      "POINT (2.5416666666666665 2.5416666666666665)");

  VELOX_ASSERT_USER_THROW(
      testStCentroidFunc(
          "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))",
          std::nullopt),
      "ST_Centroid only applies to Point or MultiPoint or LineString or MultiLineString or Polygon or MultiPolygon. Input type is: GeometryCollection");
}

TEST_F(GeometryFunctionsTest, testSTMin) {
  const auto assertPointMin = [&](const std::optional<std::string>& wkt,
                                  const std::optional<double> expectedXMin,
                                  const std::optional<double> expectedYMin) {
    std::optional<double> xMin =
        evaluateOnce<double>("ST_XMin(ST_GeometryFromText(c0))", wkt);
    std::optional<double> yMin =
        evaluateOnce<double>("ST_YMin(ST_GeometryFromText(c0))", wkt);
    if (expectedXMin.has_value() && expectedYMin.has_value()) {
      EXPECT_TRUE(xMin.has_value());
      EXPECT_TRUE(yMin.has_value());
      EXPECT_EQ(expectedXMin.value(), xMin.value());
      EXPECT_EQ(expectedYMin.value(), yMin.value());
    } else {
      EXPECT_FALSE(xMin.has_value());
      EXPECT_FALSE(yMin.has_value());
    }
  };

  assertPointMin("POINT (1.5 2.5)", 1.5, 2.5);
  assertPointMin("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 1.0, 2.0);
  assertPointMin("LINESTRING (8 4, 5 7)", 5.0, 4.0);
  assertPointMin("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 1.0, 1.0);
  assertPointMin("POLYGON ((2 0, 2 1, 3 1, 2 0))", 2.0, 0.0);
  assertPointMin(
      "MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10, 1 10)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      1.0,
      3.0);
  assertPointMin(
      "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))", 3.0, 1.0);
  assertPointMin(std::nullopt, std::nullopt, std::nullopt);
  assertPointMin("POLYGON EMPTY", std::nullopt, std::nullopt);
}

TEST_F(GeometryFunctionsTest, testSTMax) {
  const auto assertPointMax = [&](const std::optional<std::string>& wkt,
                                  const std::optional<double> expectedXMax,
                                  const std::optional<double> expectedYMax) {
    std::optional<double> xMax =
        evaluateOnce<double>("ST_XMax(ST_GeometryFromText(c0))", wkt);
    std::optional<double> yMax =
        evaluateOnce<double>("ST_YMax(ST_GeometryFromText(c0))", wkt);
    if (expectedXMax.has_value() && expectedYMax.has_value()) {
      EXPECT_TRUE(xMax.has_value());
      EXPECT_TRUE(yMax.has_value());
      EXPECT_EQ(expectedXMax.value(), xMax.value());
      EXPECT_EQ(expectedYMax.value(), yMax.value());
    } else {
      EXPECT_FALSE(xMax.has_value());
      EXPECT_FALSE(yMax.has_value());
    }
  };

  assertPointMax("POINT (1.5 2.5)", 1.5, 2.5);
  assertPointMax("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4.0, 8.0);
  assertPointMax("LINESTRING (8 4, 5 7)", 8.0, 7.0);
  assertPointMax("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 5.0, 4.0);
  assertPointMax("POLYGON ((2 0, 2 1, 3 1, 2 0))", 3.0, 1.0);
  assertPointMax(
      "MULTIPOLYGON (((1 10, 1 3, 3 3, 3 10, 1 10)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      6.0,
      10.0);
  assertPointMax(
      "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))", 5.0, 4.0);
  assertPointMax(std::nullopt, std::nullopt, std::nullopt);
  assertPointMax("POLYGON EMPTY", std::nullopt, std::nullopt);
}

TEST_F(GeometryFunctionsTest, testStGeometryType) {
  const auto testStGeometryTypeFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_GeometryType(ST_GeometryFromText(c0))", wkt);

        if (wkt.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_TRUE(expected.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStGeometryTypeFunc("POINT EMPTY", "Point");
  testStGeometryTypeFunc("POINT (3 5)", "Point");
  testStGeometryTypeFunc("LINESTRING EMPTY", "LineString");
  testStGeometryTypeFunc("LINESTRING (1 1, 2 2, 3 3)", "LineString");
  testStGeometryTypeFunc("LINEARRING EMPTY", "LineString");
  testStGeometryTypeFunc("POLYGON EMPTY", "Polygon");
  testStGeometryTypeFunc("POLYGON ((1 1, 4 1, 1 4, 1 1))", "Polygon");
  testStGeometryTypeFunc("MULTIPOINT EMPTY", "MultiPoint");
  testStGeometryTypeFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", "MultiPoint");
  testStGeometryTypeFunc("MULTILINESTRING EMPTY", "MultiLineString");
  testStGeometryTypeFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", "MultiLineString");
  testStGeometryTypeFunc("MULTIPOLYGON EMPTY", "MultiPolygon");
  testStGeometryTypeFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      "MultiPolygon");
  testStGeometryTypeFunc("GEOMETRYCOLLECTION EMPTY", "GeometryCollection");
  testStGeometryTypeFunc(
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1)), GEOMETRYCOLLECTION (POINT (8 8), LINESTRING (5 5, 6 6), POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))))",
      "GeometryCollection");
}

TEST_F(GeometryFunctionsTest, testStDistance) {
  const auto testStDistanceFunc = [&](const std::optional<std::string>& wkt1,
                                      const std::optional<std::string>& wkt2,
                                      const std::optional<double>& expected =
                                          std::nullopt) {
    std::optional<double> result = evaluateOnce<double>(
        "ST_Distance(ST_GeometryFromText(c0), ST_GeometryFromText(c1))",
        wkt1,
        wkt2);

    if (wkt1.has_value() && wkt2.has_value()) {
      if (expected.has_value()) {
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value(), expected.value());
      } else {
        ASSERT_FALSE(result.has_value());
      }
    } else {
      ASSERT_FALSE(expected.has_value());
      ASSERT_FALSE(result.has_value());
    }
  };

  testStDistanceFunc("POINT (50 100)", "POINT (150 150)", 111.80339887498948);
  testStDistanceFunc("MULTIPOINT (50 100, 50 200)", "POINT (50 100)", 0.0);
  testStDistanceFunc(
      "LINESTRING (50 100, 50 200)",
      "LINESTRING (10 10, 20 20)",
      85.44003745317531);
  testStDistanceFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "LINESTRING (10 20, 20 50)'))",
      17.08800749063506);
  testStDistanceFunc(
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))",
      "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))",
      1.4142135623730951);
  testStDistanceFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "POLYGON ((10 100, 30 10, 30 100, 10 100))",
      27.892651361962706);

  testStDistanceFunc("POINT EMPTY", "POINT (150 150)");
  testStDistanceFunc("MULTIPOINT EMPTY", "POINT (50 100)");
  testStDistanceFunc("LINESTRING EMPTY", "LINESTRING (10 10, 20 20)");
  testStDistanceFunc("MULTILINESTRING EMPTY", "LINESTRING (10 20, 20 50)'))");
  testStDistanceFunc("POLYGON EMPTY", "POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))");
  testStDistanceFunc(
      "MULTIPOLYGON EMPTY", "POLYGON ((10 100, 30 10, 30 100, 10 100))");
  testStDistanceFunc(std::nullopt, "POINT (50 100)");
}

TEST_F(GeometryFunctionsTest, testStXY) {
  const auto testStX = [&](const std::optional<std::string>& wkt,
                           const std::optional<double>& expectedX =
                               std::nullopt) {
    std::optional<double> resultX =
        evaluateOnce<double>("ST_X(ST_GeometryFromText(c0))", wkt);

    if (expectedX.has_value()) {
      ASSERT_TRUE(resultX.has_value());
      ASSERT_EQ(expectedX.value(), resultX.value());
    } else {
      ASSERT_FALSE(resultX.has_value());
    }
  };
  const auto testStY = [&](const std::optional<std::string>& wkt,
                           const std::optional<double>& expectedY =
                               std::nullopt) {
    std::optional<double> resultY =
        evaluateOnce<double>("ST_Y(ST_GeometryFromText(c0))", wkt);

    if (expectedY.has_value()) {
      ASSERT_TRUE(resultY.has_value());
      ASSERT_EQ(expectedY.value(), resultY.value());
    } else {
      ASSERT_FALSE(resultY.has_value());
    }
  };

  testStX("POINT (1 2)", 1.0);
  testStY("POINT (1 2)", 2.0);
  testStX("POINT EMPTY", std::nullopt);
  testStY("POINT EMPTY", std::nullopt);
  VELOX_ASSERT_USER_THROW(
      testStX("GEOMETRYCOLLECTION EMPTY"),
      "ST_X requires a Point geometry, found GeometryCollection");
  VELOX_ASSERT_USER_THROW(
      testStY("GEOMETRYCOLLECTION EMPTY"),
      "ST_Y requires a Point geometry, found GeometryCollection");
  VELOX_ASSERT_USER_THROW(
      testStX("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))"),
      "ST_X requires a Point geometry, found Polygon");
  VELOX_ASSERT_USER_THROW(
      testStY("POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))"),
      "ST_Y requires a Point geometry, found Polygon");
}

TEST_F(GeometryFunctionsTest, testStPolygon) {
  const auto testStPolygonFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result =
            evaluateOnce<std::string>("ST_AsText(ST_Polygon(c0))", wkt);

        if (wkt.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStPolygonFunc("POLYGON EMPTY", "POLYGON EMPTY");
  testStPolygonFunc(
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'))",
      "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))");

  VELOX_ASSERT_USER_THROW(
      testStPolygonFunc("LINESTRING (1 1, 2 2, 1 3)", std::nullopt),
      "ST_Polygon only applies to Polygon. Input type is: LineString");

  VELOX_ASSERT_USER_THROW(
      testStPolygonFunc("POLYGON((-1 1, 1 -1))", std::nullopt),
      "Failed to parse WKT: IllegalArgumentException: Points of LinearRing do not form a closed linestring");
}

TEST_F(GeometryFunctionsTest, testStIsClosed) {
  const auto testStIsClosedFunc = [&](const std::optional<std::string>& wkt,
                                      const std::optional<bool>& expected) {
    std::optional<bool> result =
        evaluateOnce<bool>("ST_IsClosed(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStIsClosedFunc("LINESTRING (1 1, 2 2, 1 3, 1 1)", true);
  testStIsClosedFunc("LINESTRING (1 1, 2 2, 1 3)", false);
  testStIsClosedFunc(
      "MULTILINESTRING ((1 1, 2 2, 1 3, 1 1), (4 4, 5 5))", false);
  testStIsClosedFunc(
      "MULTILINESTRING ((1 1, 2 2, 1 3, 1 1), (4 4, 5 4, 5 5, 4 5, 4 4))",
      true);

  VELOX_ASSERT_USER_THROW(
      testStIsClosedFunc("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", std::nullopt),
      "ST_IsClosed only applies to LineString or MultiLineString. Input type is: Polygon");
}

TEST_F(GeometryFunctionsTest, testStIsEmpty) {
  const auto testStIsClosedFunc = [&](const std::optional<std::string>& wkt,
                                      const std::optional<bool>& expected) {
    std::optional<bool> result =
        evaluateOnce<bool>("ST_IsEmpty(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStIsClosedFunc("POINT (1.5 2.5)", false);
  testStIsClosedFunc("POLYGON EMPTY", true);
}

TEST_F(GeometryFunctionsTest, testStIsRing) {
  const auto testStIsRingFunc = [&](const std::optional<std::string>& wkt,
                                    const std::optional<bool>& expected) {
    std::optional<bool> result =
        evaluateOnce<bool>("ST_IsRing(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStIsRingFunc("LINESTRING (8 4, 4 8)", false);
  testStIsRingFunc("LINESTRING (0 0, 1 1, 0 2, 0 0)", true);

  VELOX_ASSERT_USER_THROW(
      testStIsRingFunc("POLYGON ((2 0, 2 1, 3 1, 2 0))", true),
      "ST_IsRing only applies to LineString. Input type is: Polygon");
}

TEST_F(GeometryFunctionsTest, testStLength) {
  const auto testStLengthFunc = [&](const std::optional<std::string>& wkt,
                                    const std::optional<double>& expected) {
    std::optional<double> result =
        evaluateOnce<double>("ST_Length(ST_GeometryFromText(c0))", wkt);

    if (wkt.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStLengthFunc("LINESTRING EMPTY", 0.0);
  testStLengthFunc("LINESTRING (0 0, 2 2)", 2.8284271247461903);
  testStLengthFunc("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 6.0);

  VELOX_ASSERT_USER_THROW(
      testStLengthFunc("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", std::nullopt),
      "ST_Length only applies to LineString or MultiLineString. Input type is: Polygon");
}

TEST_F(GeometryFunctionsTest, testStPointN) {
  const auto testStPointNFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<int32_t>& index,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_PointN(ST_GeometryFromText(c0), c1))", wkt, index);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStPointNFunc("LINESTRING(1 2, 3 4, 5 6, 7 8)", 1, "POINT (1 2)");
  testStPointNFunc("LINESTRING(1 2, 3 4, 5 6, 7 8)", 3, "POINT (5 6)");
  testStPointNFunc("LINESTRING(1 2, 3 4, 5 6, 7 8)", 10, std::nullopt);
  testStPointNFunc("LINESTRING(1 2, 3 4, 5 6, 7 8)", 0, std::nullopt);
  testStPointNFunc("LINESTRING(1 2, 3 4, 5 6, 7 8)", -1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testStPointNFunc("POINT (1 2)", -1, std::nullopt),
      "ST_PointN only applies to LineString. Input type is: Point");
}

TEST_F(GeometryFunctionsTest, testStStartPoint) {
  const auto testStStartPointFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_StartPoint(ST_GeometryFromText(c0)))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStStartPointFunc("LINESTRING (8 4, 4 8, 5 6)", "POINT (8 4)");
  testStStartPointFunc("LINESTRING (8 2, 4 12, 0 0)", "POINT (8 2)");
  testStStartPointFunc("LINESTRING (0 0, 4 12, 2 2)", "POINT (0 0)");
  testStStartPointFunc("LINESTRING EMPTY", std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testStStartPointFunc("POINT (1 2)", std::nullopt),
      "ST_StartPoint only applies to LineString. Input type is: Point");
}

TEST_F(GeometryFunctionsTest, testStEndPoint) {
  const auto testStEndPointFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_EndPoint(ST_GeometryFromText(c0)))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStEndPointFunc("LINESTRING (8 4, 4 8, 5 6)", "POINT (5 6)");
  testStEndPointFunc("LINESTRING (8 2, 4 12, 0 0)", "POINT (0 0)");
  testStEndPointFunc("LINESTRING (0 0, 4 12, 2 2)", "POINT (2 2)");
  testStEndPointFunc("LINESTRING EMPTY", std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testStEndPointFunc("POINT (1 2)", std::nullopt),
      "ST_EndPoint only applies to LineString. Input type is: Point");
}

TEST_F(GeometryFunctionsTest, testStGeometryN) {
  const auto testStGeometryNFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<int32_t>& index,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_GeometryN(ST_GeometryFromText(c0), c1))", wkt, index);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStGeometryNFunc("POINT EMPTY", 1, std::nullopt);
  testStGeometryNFunc("LINESTRING EMPTY", 1, std::nullopt);
  testStGeometryNFunc(
      "LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)",
      1,
      "LINESTRING (77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)");

  testStGeometryNFunc(
      "LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)",
      2,
      std::nullopt);
  testStGeometryNFunc(
      "LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)",
      -1,
      std::nullopt);
  testStGeometryNFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
      1,
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
  testStGeometryNFunc("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 2, std::nullopt);
  testStGeometryNFunc("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", -1, std::nullopt);
  testStGeometryNFunc("POLYGON EMPTY", 0, std::nullopt);
  testStGeometryNFunc("POLYGON EMPTY", 2, std::nullopt);
  testStGeometryNFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 1, "POINT (1 2)");
  testStGeometryNFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 2, "POINT (2 4)");
  testStGeometryNFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 0, std::nullopt);
  testStGeometryNFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 5, std::nullopt);
  testStGeometryNFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", -1, std::nullopt);
  testStGeometryNFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 1, "LINESTRING (1 1, 5 1)");
  testStGeometryNFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 2, "LINESTRING (2 4, 4 4)");
  testStGeometryNFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 0, std::nullopt);
  testStGeometryNFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 3, std::nullopt);
  testStGeometryNFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", -1, std::nullopt);
  testStGeometryNFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      1,
      "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))");
  testStGeometryNFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      2,
      "POLYGON ((2 4, 2 6, 6 6, 6 4, 2 4))");
  testStGeometryNFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      0,
      std::nullopt);
  testStGeometryNFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      3,
      std::nullopt);
  testStGeometryNFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      -1,
      std::nullopt);
  testStGeometryNFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))",
      1,
      "POINT (2 3)");
  testStGeometryNFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))",
      2,
      "LINESTRING (2 3, 3 4)");
  testStGeometryNFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 3, std::nullopt);
  testStGeometryNFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 0, std::nullopt);
  testStGeometryNFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))",
      -1,
      std::nullopt);
}

TEST_F(GeometryFunctionsTest, testStInteriorRingN) {
  const auto testStInteriorRingNFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<int32_t>& index,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_InteriorRingN(ST_GeometryFromText(c0), c1))",
            wkt,
            index);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 1, std::nullopt);

  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 2, std::nullopt);
  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", -1, std::nullopt);
  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 0, std::nullopt);
  testStInteriorRingNFunc("POLYGON EMPTY", 1, std::nullopt);
  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
      1,
      "LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)");
  testStInteriorRingNFunc(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (3 3, 4 3, 4 4, 3 4, 3 3))",
      2,
      "LINESTRING (3 3, 4 3, 4 4, 3 4, 3 3)");

  VELOX_ASSERT_USER_THROW(
      testStInteriorRingNFunc("POINT EMPTY", 0, std::nullopt),
      "ST_InteriorRingN only applies to Polygon. Input type is: Point");
}

TEST_F(GeometryFunctionsTest, testStNumInteriorRing) {
  const auto testStNumInteriorRingFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<int32_t>& expected) {
        std::optional<int32_t> result = evaluateOnce<int32_t>(
            "ST_NumInteriorRing(ST_GeometryFromText(c0))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStNumInteriorRingFunc("POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))", 0);
  testStNumInteriorRingFunc(
      "POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))", 1);
  testStNumInteriorRingFunc("POLYGON EMPTY", std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testStNumInteriorRingFunc("LINESTRING (8 4, 5 7)", std::nullopt),
      "ST_NumInteriorRing only applies to Polygon. Input type is: LineString");
}

TEST_F(GeometryFunctionsTest, testStNumGeometries) {
  const auto testStNumGeometriesFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<int32_t>& expected) {
        std::optional<int32_t> result = evaluateOnce<int32_t>(
            "ST_NumGeometries(ST_GeometryFromText(c0))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  // GeometryCollections with only empty geometries always return 0
  testStNumGeometriesFunc("GEOMETRYCOLLECTION (POINT EMPTY, POINT EMPTY)", 0);
  testStNumGeometriesFunc("GEOMETRYCOLLECTION (POINT EMPTY)", 0);
  // GeometryCollections with at least 1 non-empty geometry return the number of
  // geometries
  testStNumGeometriesFunc("GEOMETRYCOLLECTION(POINT EMPTY, POINT (1 2))", 2);

  testStNumGeometriesFunc("POINT EMPTY", 0);
  testStNumGeometriesFunc("GEOMETRYCOLLECTION EMPTY", 0);
  testStNumGeometriesFunc("MULTIPOLYGON EMPTY", 0);
  testStNumGeometriesFunc("POINT (1 2)", 1);
  testStNumGeometriesFunc(
      "LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)", 1);
  testStNumGeometriesFunc("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 1);
  testStNumGeometriesFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
  testStNumGeometriesFunc("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 2);
  testStNumGeometriesFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      2);
  testStNumGeometriesFunc(
      "GEOMETRYCOLLECTION(POINT(2 3), LINESTRING (2 3, 3 4))", 2);
}

TEST_F(GeometryFunctionsTest, testStConvexHull) {
  const auto testStConvexHullFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_ConvexHull(ST_GeometryFromText(c0)))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  // test empty geometry
  testStConvexHullFunc("POINT EMPTY", "POINT EMPTY");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (POINT (1 1), POINT EMPTY)", "POINT (1 1)");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 1), GEOMETRYCOLLECTION (POINT (1 5), POINT (4 5), GEOMETRYCOLLECTION (POINT (3 4), POINT EMPTY))))",
      "POLYGON ((1 1, 1 5, 4 5, 1 1))");

  // test single geometry
  testStConvexHullFunc("POINT (1 1)", "POINT (1 1)");
  testStConvexHullFunc(
      "LINESTRING (1 1, 1 9, 2 2)", "POLYGON ((1 1, 1 9, 2 2, 1 1))");

  // convex single geometry
  testStConvexHullFunc(
      "LINESTRING (1 1, 1 9, 2 2, 1 1)", "POLYGON ((1 1, 1 9, 2 2, 1 1))");
  testStConvexHullFunc(
      "POLYGON ((0 0, 0 3, 2 4, 4 2, 3 0, 0 0))",
      "POLYGON ((0 0, 0 3, 2 4, 4 2, 3 0, 0 0))");

  // non-convex geometry
  testStConvexHullFunc(
      "LINESTRING (1 1, 1 9, 2 2, 1 1, 4 0)", "POLYGON ((4 0, 1 1, 1 9, 4 0))");
  testStConvexHullFunc(
      "POLYGON ((0 0, 0 3, 4 4, 1 1, 3 0, 0 0))",
      "POLYGON ((0 0, 0 3, 4 4, 3 0, 0 0))");

  // all points are on the same line
  testStConvexHullFunc(
      "LINESTRING (20 20, 30 30)", "LINESTRING (20 20, 30 30)");
  testStConvexHullFunc(
      "MULTILINESTRING ((0 0, 3 3), (1 1, 2 2), (2 2, 4 4), (5 5, 8 8))",
      "LINESTRING (0 0, 8 8)");
  testStConvexHullFunc(
      "MULTIPOINT (0 1, 1 2, 2 3, 3 4, 4 5, 5 6)", "LINESTRING (0 1, 5 6)");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 2), POINT (1 1)), POINT(3 3))",
      "LINESTRING (1 1, 3 3)");

  // not all points are on the same line
  testStConvexHullFunc(
      "MULTILINESTRING ((1 1, 5 1, 6 6), (2 4, 4 0), (2 -4, 4 4), (3 -2, 4 -3))",
      "POLYGON ((2 -4, 1 1, 2 4, 6 6, 5 1, 4 -3, 2 -4))");
  testStConvexHullFunc(
      "MULTIPOINT (0 2, 1 0, 3 0, 4 0, 4 2, 2 2, 2 4)",
      "POLYGON ((1 0, 0 2, 2 4, 4 2, 4 0, 1 0))");
  testStConvexHullFunc(
      "MULTIPOLYGON (((0 3, 2 0, 3 6, 0 3), (2 1, 2 3, 5 3, 5 1, 2 1), (1 7, 2 4, 4 2, 5 6, 3 8, 1 7)))",
      "POLYGON ((2 0, 0 3, 1 7, 3 8, 5 6, 5 1, 2 0))");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (POINT (2 3), LINESTRING (2 8, 7 10), POINT (8 10), POLYGON ((4 4, 4 8, 9 8, 6 6, 6 4, 8 3, 6 1, 4 4)), POINT (4 2), LINESTRING (3 6, 5 5), POLYGON ((7 5, 7 6, 8 6, 8 5, 7 5)))",
      "POLYGON ((6 1, 2 3, 2 8, 7 10, 8 10, 9 8, 8 3, 6 1))");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 3), LINESTRING (2 8, 7 10), GEOMETRYCOLLECTION (POINT (8 10))), POLYGON ((4 4, 4 8, 9 8, 6 6, 6 4, 8 3, 6 1, 4 4)), POINT (4 2), LINESTRING (3 6, 5 5), POLYGON ((7 5, 7 6, 8 6, 8 5, 7 5)))",
      "POLYGON ((6 1, 2 3, 2 8, 7 10, 8 10, 9 8, 8 3, 6 1))");

  // single-element multi-geometries and geometry collections
  testStConvexHullFunc(
      "MULTILINESTRING ((1 1, 5 1, 6 6))", "POLYGON ((1 1, 6 6, 5 1, 1 1))");
  testStConvexHullFunc(
      "MULTILINESTRING ((1 1, 5 1, 1 4, 5 4))",
      "POLYGON ((1 1, 1 4, 5 4, 5 1, 1 1))");
  testStConvexHullFunc("MULTIPOINT (0 2)", "POINT (0 2)");
  testStConvexHullFunc(
      "MULTIPOLYGON (((0 3, 3 6, 2 0, 0 3)))",
      "POLYGON ((2 0, 0 3, 3 6, 2 0))");
  testStConvexHullFunc(
      "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 2 2, 0 0)))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))");
  testStConvexHullFunc("GEOMETRYCOLLECTION (POINT (2 3))", "POINT (2 3)");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (LINESTRING (1 1, 5 1, 6 6))",
      "POLYGON ((1 1, 6 6, 5 1, 1 1))");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (LINESTRING (1 1, 5 1, 1 4, 5 4))",
      "POLYGON ((1 1, 1 4, 5 4, 5 1, 1 1))");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (POLYGON ((0 3, 3 6, 2 0, 0 3)))",
      "POLYGON ((2 0, 0 3, 3 6, 2 0))");
  testStConvexHullFunc(
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 4 0, 4 4, 0 4, 2 2, 0 0)))",
      "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))");
}

TEST_F(GeometryFunctionsTest, testStCoordDim) {
  const auto testStCoordDimFunc = [&](const std::optional<std::string>& wkt,
                                      const std::optional<int32_t>& expected) {
    std::optional<int32_t> result =
        evaluateOnce<int32_t>("ST_CoordDim(ST_GeometryFromText(c0))", wkt);

    if (expected.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStCoordDimFunc("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", 2);
  testStCoordDimFunc("POLYGON EMPTY))", 2);
  testStCoordDimFunc("LINESTRING (1 1, 1 2)", 2);
  testStCoordDimFunc("POINT (1 4)", 2);

  testStCoordDimFunc("LINESTRING EMPTY", 2);
}

TEST_F(GeometryFunctionsTest, testStDimension) {
  const auto testStDimensionFunc = [&](const std::optional<std::string>& wkt,
                                       const std::optional<int8_t>& expected) {
    std::optional<int8_t> result =
        evaluateOnce<int8_t>("ST_Dimension(ST_GeometryFromText(c0))", wkt);

    if (expected.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStDimensionFunc("POLYGON EMPTY", 2);
  testStDimensionFunc("POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))", 2);
  testStDimensionFunc("LINESTRING EMPTY", 1);
  testStDimensionFunc("POINT (1 4))", 0);
}

TEST_F(GeometryFunctionsTest, testStBuffer) {
  const auto testStBufferFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<double>& distance,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_Buffer(ST_GeometryFromText(c0), c1))", wkt, distance);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStBufferFunc(
      "POINT (0 0)",
      0.5,
      "POLYGON ((0.5 0, 0.4903926402016152 -0.0975451610080641, 0.4619397662556434 -0.1913417161825449, 0.4157348061512726 -0.2777851165098011, 0.3535533905932738 -0.3535533905932737, 0.2777851165098011 -0.4157348061512726, 0.1913417161825449 -0.4619397662556434, 0.0975451610080642 -0.4903926402016152, 0 -0.5, -0.0975451610080641 -0.4903926402016152, -0.1913417161825449 -0.4619397662556434, -0.277785116509801 -0.4157348061512727, -0.3535533905932737 -0.3535533905932738, -0.4157348061512727 -0.2777851165098011, -0.4619397662556434 -0.191341716182545, -0.4903926402016152 -0.0975451610080643, -0.5 -0.0000000000000001, -0.4903926402016152 0.0975451610080642, -0.4619397662556434 0.1913417161825448, -0.4157348061512727 0.277785116509801, -0.3535533905932738 0.3535533905932737, -0.2777851165098011 0.4157348061512726, -0.1913417161825452 0.4619397662556433, -0.0975451610080643 0.4903926402016152, -0.0000000000000001 0.5, 0.0975451610080642 0.4903926402016152, 0.191341716182545 0.4619397662556433, 0.2777851165098009 0.4157348061512727, 0.3535533905932737 0.3535533905932738, 0.4157348061512726 0.2777851165098011, 0.4619397662556433 0.1913417161825452, 0.4903926402016152 0.0975451610080644, 0.5 0))");
  testStBufferFunc(
      "LINESTRING (0 0, 1 1, 2 0.5)",
      .2,
      "POLYGON ((0.8585786437626906 1.1414213562373094, 0.8908600605480863 1.167596162296255, 0.9278541681368628 1.1865341227356967, 0.9679635513986066 1.1974174915274993, 1.0094562767938988 1.1997763219933664, 1.050540677712335 1.1935087592239118, 1.0894427190999916 1.1788854381999831, 2.0894427190999916 0.6788854381999831, 2.1226229200749436 0.6579987957938098, 2.1510907909991412 0.6310403482720258, 2.173752327557934 0.5990460936544217, 2.189736659610103 0.5632455532033676, 2.198429518239 0.5250145216112229, 2.1994968417625285 0.4858221959818642, 2.192897613536241 0.4471747154099183, 2.178885438199983 0.4105572809000084, 2.1579987957938096 0.3773770799250564, 2.131040348272026 0.3489092090008588, 2.099046093654422 0.3262476724420662, 2.0632455532033678 0.3102633403898972, 2.0250145216112228 0.3015704817609999, 1.985822195981864 0.3005031582374715, 1.9471747154099184 0.3071023864637593, 1.9105572809000084 0.3211145618000168, 1.0394906098164267 0.7566478973418077, 0.1414213562373095 -0.1414213562373095, 0.1111140466039205 -0.1662939224605091, 0.076536686473018 -0.1847759065022574, 0.0390180644032257 -0.1961570560806461, 0 -0.2, -0.0390180644032256 -0.1961570560806461, -0.076536686473018 -0.1847759065022574, -0.1111140466039204 -0.1662939224605091, -0.1414213562373095 -0.1414213562373095, -0.1662939224605091 -0.1111140466039204, -0.1847759065022574 -0.076536686473018, -0.1961570560806461 -0.0390180644032257, -0.2 0, -0.1961570560806461 0.0390180644032257, -0.1847759065022574 0.0765366864730179, -0.1662939224605091 0.1111140466039204, -0.1414213562373095 0.1414213562373095, 0.8585786437626906 1.1414213562373094))");
  testStBufferFunc(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
      1.2,
      "POLYGON ((0 -1.2, -0.2341083864193544 -1.1769423364838763, -0.4592201188381084 -1.1086554390135437, -0.6666842796235226 -0.9977635347630542, -0.8485281374238572 -0.8485281374238569, -0.9977635347630545 -0.6666842796235223, -1.1086554390135441 -0.4592201188381076, -1.1769423364838765 -0.234108386419354, -1.2 0, -1.2 5, -1.1769423364838765 5.234108386419354, -1.1086554390135441 5.4592201188381075, -0.9977635347630543 5.666684279623523, -0.8485281374238569 5.848528137423857, -0.6666842796235223 5.997763534763054, -0.4592201188381076 6.108655439013544, -0.2341083864193538 6.176942336483877, 0 6.2, 5 6.2, 5.234108386419354 6.176942336483877, 5.4592201188381075 6.108655439013544, 5.666684279623523 5.997763534763054, 5.848528137423857 5.848528137423857, 5.997763534763054 5.666684279623523, 6.108655439013544 5.4592201188381075, 6.176942336483877 5.234108386419354, 6.2 5, 6.2 0, 6.176942336483877 -0.2341083864193539, 6.108655439013544 -0.4592201188381077, 5.997763534763054 -0.6666842796235226, 5.848528137423857 -0.8485281374238569, 5.666684279623523 -0.9977635347630542, 5.4592201188381075 -1.1086554390135441, 5.234108386419354 -1.1769423364838765, 5 -1.2, 0 -1.2))");

  // Zero distance
  testStBufferFunc("POINT (0 0)", 0, "POINT (0 0)");
  testStBufferFunc(
      "LINESTRING (0 0, 1 1, 2 0.5)", 0, "LINESTRING (0 0, 1 1, 2 0.5)");
  testStBufferFunc(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))",
      0,
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))");

  // GeometryCollection
  testStBufferFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      .2,
      "MULTIPOLYGON (((5 1.2, 5.039018064403225 1.196157056080646, 5.076536686473018 1.1847759065022574, 5.11111404660392 1.1662939224605091, 5.141421356237309 1.1414213562373094, 5.166293922460509 1.1111140466039204, 5.184775906502257 1.076536686473018, 5.196157056080646 1.0390180644032256, 5.2 1, 5.196157056080646 0.9609819355967744, 5.184775906502257 0.9234633135269821, 5.166293922460509 0.8888859533960796, 5.141421356237309 0.8585786437626906, 5.11111404660392 0.8337060775394909, 5.076536686473018 0.8152240934977426, 5.039018064403225 0.803842943919354, 5 0.8, 1 0.8, 0.9609819355967743 0.803842943919354, 0.923463313526982 0.8152240934977427, 0.8888859533960796 0.8337060775394909, 0.8585786437626904 0.8585786437626906, 0.8337060775394909 0.8888859533960796, 0.8152240934977426 0.9234633135269821, 0.803842943919354 0.9609819355967744, 0.8 1, 0.803842943919354 1.0390180644032256, 0.8152240934977426 1.076536686473018, 0.8337060775394909 1.1111140466039204, 0.8585786437626906 1.1414213562373094, 0.8888859533960796 1.1662939224605091, 0.9234633135269821 1.1847759065022574, 0.9609819355967744 1.196157056080646, 1 1.2, 5 1.2)), ((4 4.2, 4.039018064403225 4.196157056080646, 4.076536686473018 4.184775906502257, 4.11111404660392 4.166293922460509, 4.141421356237309 4.141421356237309, 4.166293922460509 4.11111404660392, 4.184775906502257 4.076536686473018, 4.196157056080646 4.039018064403225, 4.2 4, 4.196157056080646 3.960981935596774, 4.184775906502257 3.923463313526982, 4.166293922460509 3.8888859533960796, 4.141421356237309 3.8585786437626903, 4.11111404660392 3.833706077539491, 4.076536686473018 3.8152240934977426, 4.039018064403225 3.8038429439193537, 4 3.8, 2 3.8, 1.9609819355967744 3.8038429439193537, 1.9234633135269819 3.8152240934977426, 1.8888859533960796 3.833706077539491, 1.8585786437626906 3.8585786437626903, 1.8337060775394909 3.8888859533960796, 1.8152240934977426 3.923463313526982, 1.803842943919354 3.960981935596774, 1.8 4, 1.803842943919354 4.039018064403225, 1.8152240934977426 4.076536686473018, 1.8337060775394909 4.11111404660392, 1.8585786437626906 4.141421356237309, 1.8888859533960796 4.166293922460509, 1.923463313526982 4.184775906502257, 1.9609819355967744 4.196157056080646, 2 4.2, 4 4.2)))");

  // Empty
  testStBufferFunc("POINT EMPTY", .2, std::nullopt);

  // Negative distance
  VELOX_ASSERT_USER_THROW(
      testStBufferFunc("POINT (0 0)", -1.2, "POINT (0 0)"),
      "Provided distance must not be negative. Provided distance: -1.2");
}

TEST_F(GeometryFunctionsTest, testStExteriorRing) {
  const auto testStExteriorRingFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_ExteriorRing(ST_GeometryFromText(c0)))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStExteriorRingFunc("POLYGON EMPTY", std::nullopt);
  testStExteriorRingFunc(
      "POLYGON ((1 1, 1 4, 4 1, 1 1))", "LINESTRING (1 1, 1 4, 4 1, 1 1)");

  testStExteriorRingFunc(
      "POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
      "LINESTRING (0 0, 0 5, 5 5, 5 0, 0 0)");

  VELOX_ASSERT_USER_THROW(
      testStExteriorRingFunc("LINESTRING (1 1, 2 2, 1 3)", std::nullopt),
      "ST_ExteriorRing only applies to Polygon. Input type is: LineString");
  VELOX_ASSERT_USER_THROW(
      testStExteriorRingFunc(
          "MULTIPOLYGON (((1 1, 2 2, 1 3, 1 1)), ((4 4, 5 5, 4 6, 4 4)))",
          std::nullopt),
      "ST_ExteriorRing only applies to Polygon. Input type is: MultiPolygon");
}

TEST_F(GeometryFunctionsTest, testStEnvelope) {
  const auto testStEnvelopeFunc =
      [&](const std::optional<std::string>& wkt,
          const std::optional<std::string>& expected) {
        std::optional<std::string> result = evaluateOnce<std::string>(
            "ST_AsText(ST_Envelope(ST_GeometryFromText(c0)))", wkt);

        if (expected.has_value()) {
          ASSERT_TRUE(result.has_value());
          ASSERT_EQ(result.value(), expected.value());
        } else {
          ASSERT_FALSE(result.has_value());
        }
      };

  testStEnvelopeFunc(
      "MULTIPOINT (1 2, 2 4, 3 6, 4 8)", "POLYGON ((1 2, 1 8, 4 8, 4 2, 1 2))");
  testStEnvelopeFunc("LINESTRING EMPTY", "POLYGON EMPTY");
  testStEnvelopeFunc(
      "LINESTRING (1 1, 2 2, 1 3)", "POLYGON ((1 1, 1 3, 2 3, 2 1, 1 1))");
  testStEnvelopeFunc(
      "LINESTRING (8 4, 5 7)", "POLYGON ((5 4, 5 7, 8 7, 8 4, 5 4))");
  testStEnvelopeFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))",
      "POLYGON ((1 1, 1 4, 5 4, 5 1, 1 1))");
  testStEnvelopeFunc(
      "POLYGON ((1 1, 4 1, 1 4, 1 1))", "POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))");
  testStEnvelopeFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      "POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))");
  testStEnvelopeFunc(
      "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))",
      "POLYGON ((3 1, 3 4, 5 4, 5 1, 3 1))");
}

TEST_F(GeometryFunctionsTest, testStPoints) {
  const auto testStPointsFunc = [&](const std::optional<std::string>& wkt,
                                    const std::optional<std::vector<
                                        std::optional<std::string>>>&
                                        expectedPoints) {
    auto input = makeSingleStringInputRow(wkt);

    facebook::velox::VectorPtr output = evaluate(
        "transform(ST_Points(ST_GeometryFromText(c0)), x -> substr(ST_AsText(x), 7))",
        input);

    auto arrayVector =
        std::dynamic_pointer_cast<facebook::velox::ArrayVector>(output);

    ASSERT_TRUE(arrayVector != nullptr);

    auto expected = makeNullableArrayVector<std::string>({{expectedPoints}});
    facebook::velox::test::assertEqualVectors(expected, output);
  };

  testStPointsFunc("LINESTRING (0 0, 0 0)", {{"(0 0)", "(0 0)"}});
  testStPointsFunc("LINESTRING (8 4, 3 9, 8 4)", {{"(8 4)", "(3 9)", "(8 4)"}});
  testStPointsFunc("LINESTRING (8 4, 3 9, 5 6)", {{"(8 4)", "(3 9)", "(5 6)"}});
  testStPointsFunc(
      "LINESTRING (8 4, 3 9, 5 6, 3 9, 8 4)",
      {{"(8 4)", "(3 9)", "(5 6)", "(3 9)", "(8 4)"}});

  testStPointsFunc(
      "POLYGON ((8 4, 3 9, 5 6, 8 4))", {{"(8 4)", "(5 6)", "(3 9)", "(8 4)"}});
  testStPointsFunc(
      "POLYGON ((8 4, 3 9, 5 6, 7 2, 8 4))",
      {{"(8 4)", "(7 2)", "(5 6)", "(3 9)", "(8 4)"}});

  testStPointsFunc("POINT (0 0)", {{"(0 0)"}});
  testStPointsFunc("POINT (0 1)", {{"(0 1)"}});

  testStPointsFunc("MULTIPOINT (0 0)", {{"(0 0)"}});
  testStPointsFunc("MULTIPOINT (0 0, 1 2)", {{"(0 0)", "(1 2)"}});

  testStPointsFunc(
      "MULTILINESTRING ((0 0, 1 1), (2 3, 3 2))",
      {{"(0 0)", "(1 1)", "(2 3)", "(3 2)"}});
  testStPointsFunc(
      "MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))",
      {{"(0 0)", "(1 1)", "(1 2)", "(2 3)", "(3 2)", "(5 4)"}});
  testStPointsFunc(
      "MULTILINESTRING ((0 0, 1 1, 1 2), (1 2, 3 2, 5 4))",
      {{"(0 0)", "(1 1)", "(1 2)", "(1 2)", "(3 2)", "(5 4)"}});

  testStPointsFunc(
      "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))",
      {{
          "(0 0)",
          "(0 4)",
          "(4 4)",
          "(4 0)",
          "(0 0)",
          "(1 1)",
          "(2 1)",
          "(2 2)",
          "(1 2)",
          "(1 1)",
          "(-1 -1)",
          "(-1 -2)",
          "(-2 -2)",
          "(-2 -1)",
          "(-1 -1)",
      }});

  testStPointsFunc(
      "GEOMETRYCOLLECTION(POINT(0 1),LINESTRING(0 3,3 4),POLYGON((2 0,2 3,0 2,2 0)),POLYGON((3 0,3 3,6 3,6 0,3 0),(5 1,4 2,5 2,5 1)),MULTIPOLYGON(((0 5,0 8,4 8,4 5,0 5),(1 6,3 6,2 7,1 6)),((5 4,5 8,6 7,5 4))))",
      {{"(0 1)", "(0 3)", "(3 4)", "(2 0)", "(0 2)", "(2 3)", "(2 0)", "(3 0)",
        "(3 3)", "(6 3)", "(6 0)", "(3 0)", "(5 1)", "(5 2)", "(4 2)", "(5 1)",
        "(0 5)", "(0 8)", "(4 8)", "(4 5)", "(0 5)", "(1 6)", "(3 6)", "(2 7)",
        "(1 6)", "(5 4)", "(5 8)", "(6 7)", "(5 4)"}});

  const auto testStPointsNullAndEmptyFunc =
      [&](const std::optional<std::string>& wkt) {
        std::optional<bool> result = evaluateOnce<bool>(
            "ST_Points(ST_GeometryFromText(c0)) IS NULL", wkt);

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value());
      };

  testStPointsNullAndEmptyFunc("POINT EMPTY");
  testStPointsNullAndEmptyFunc("LINESTRING EMPTY");
  testStPointsNullAndEmptyFunc("POLYGON EMPTY");
  testStPointsNullAndEmptyFunc("MULTIPOINT EMPTY");
  testStPointsNullAndEmptyFunc("MULTILINESTRING EMPTY");
  testStPointsNullAndEmptyFunc("MULTIPOLYGON EMPTY");
  testStPointsNullAndEmptyFunc("GEOMETRYCOLLECTION EMPTY");
  testStPointsNullAndEmptyFunc(std::nullopt);
}

TEST_F(GeometryFunctionsTest, testStEnvelopeAsPts) {
  const auto testStEnvelopeAsPtsFunc = [&](const std::optional<std::string>&
                                               wkt,
                                           const std::optional<std::vector<
                                               std::optional<std::string>>>&
                                               expectedPoints) {
    auto input = makeSingleStringInputRow(wkt);

    facebook::velox::VectorPtr output = evaluate(
        "transform(ST_EnvelopeAsPts(ST_GeometryFromText(c0)), x -> substr(ST_AsText(x), 7))",
        input);

    auto arrayVector =
        std::dynamic_pointer_cast<facebook::velox::ArrayVector>(output);

    ASSERT_TRUE(arrayVector != nullptr);

    std::vector<std::vector<std::optional<std::string>>> vec = {
        expectedPoints.value()};
    auto expected = makeNullableArrayVector<std::string>(vec);
    facebook::velox::test::assertEqualVectors(expected, output);
  };

  testStEnvelopeAsPtsFunc(
      "MULTIPOINT (1 2, 2 4, 3 6, 4 8)", {{"(1 2)", "(4 8)"}});
  testStEnvelopeAsPtsFunc("LINESTRING (1 1, 2 2, 1 3)", {{"(1 1)", "(2 3)"}});
  testStEnvelopeAsPtsFunc("LINESTRING (8 4, 5 7)", {{"(5 4)", "(8 7)"}});
  testStEnvelopeAsPtsFunc(
      "MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", {{"(1 1)", "(5 4)"}});
  testStEnvelopeAsPtsFunc(
      "POLYGON ((1 1, 4 1, 1 4, 1 1))", {{"(1 1)", "(4 4)"}});
  testStEnvelopeAsPtsFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((0 0, 0 2, 2 2, 2 0, 0 0)))",
      {{"(0 0)", "(3 3)"}});
  testStEnvelopeAsPtsFunc(
      "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (3 4, 4 4))",
      {{"(3 1)", "(5 4)"}});
  testStEnvelopeAsPtsFunc("POINT (1 2)", {{"(1 2)", "(1 2)"}});

  const auto testStEnvelopeAsPtsNullAndEmptyFunc =
      [&](const std::optional<std::string>& wkt) {
        std::optional<bool> result = evaluateOnce<bool>(
            "ST_EnvelopeAsPts(ST_GeometryFromText(c0)) IS NULL", wkt);

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value());
      };

  testStEnvelopeAsPtsNullAndEmptyFunc("POINT EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("LINESTRING EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("POLYGON EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("MULTIPOINT EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("MULTILINESTRING EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("MULTIPOLYGON EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc("GEOMETRYCOLLECTION EMPTY");
  testStEnvelopeAsPtsNullAndEmptyFunc(std::nullopt);
}

TEST_F(GeometryFunctionsTest, testStNumPoints) {
  const auto testStNumPointsFunc = [&](const std::optional<std::string>& wkt,
                                       const std::optional<int32_t>& expected) {
    std::optional<int32_t> result =
        evaluateOnce<int32_t>("ST_NumPoints(ST_GeometryFromText(c0))", wkt);

    if (expected.has_value()) {
      ASSERT_TRUE(result.has_value());
      ASSERT_EQ(result.value(), expected.value());
    } else {
      ASSERT_FALSE(result.has_value());
    }
  };

  testStNumPointsFunc("POINT EMPTY", 0);
  testStNumPointsFunc("MULTIPOINT EMPTY", 0);
  testStNumPointsFunc("LINESTRING EMPTY", 0);
  testStNumPointsFunc("MULTILINESTRING EMPTY", 0);
  testStNumPointsFunc("POLYGON EMPTY", 0);
  testStNumPointsFunc("MULTIPOLYGON EMPTY", 0);
  testStNumPointsFunc("GEOMETRYCOLLECTION EMPTY", 0);

  testStNumPointsFunc("POINT (1 2)", 1);
  testStNumPointsFunc("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
  testStNumPointsFunc("LINESTRING (8 4, 5 7)", 2);
  testStNumPointsFunc("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", 4);
  testStNumPointsFunc("POLYGON ((0 0, 8 0, 0 8, 0 0))", 3);
  testStNumPointsFunc(
      "POLYGON ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))", 6);
  testStNumPointsFunc(
      "MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 6 4, 2 4)))",
      8);
  testStNumPointsFunc(
      "GEOMETRYCOLLECTION (POINT (1 1), GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), GEOMETRYCOLLECTION (POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2)))))",
      7);
}
