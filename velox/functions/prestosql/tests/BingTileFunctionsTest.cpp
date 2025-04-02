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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/BingTileType.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class BingTileFunctionsTest : public functions::test::FunctionBaseTest {
 public:
  RowVectorPtr makeSingleXYZoomRow(
      std::optional<uint32_t> x,
      std::optional<uint32_t> y,
      std::optional<uint8_t> zoom) {
    std::vector<std::optional<int32_t>> xVec = {x};
    auto inputX = makeNullableFlatVector<int32_t>(xVec);
    std::vector<std::optional<int32_t>> yVec = {y};
    auto inputY = makeNullableFlatVector<int32_t>(yVec);
    std::vector<std::optional<int8_t>> zVec = {zoom};
    auto inputZ = makeNullableFlatVector<int8_t>(zVec);
    return makeRowVector({inputX, inputY, inputZ});
  }

  RowVectorPtr makeSingleXYZoomZoomRow(
      std::optional<uint32_t> x,
      std::optional<uint32_t> y,
      std::optional<uint8_t> zoom,
      std::optional<uint8_t> zoom2) {
    std::vector<std::optional<int32_t>> xVec = {x};
    auto inputX = makeNullableFlatVector<int32_t>(xVec);
    std::vector<std::optional<int32_t>> yVec = {y};
    auto inputY = makeNullableFlatVector<int32_t>(yVec);
    std::vector<std::optional<int8_t>> zVec = {zoom};
    auto inputZ = makeNullableFlatVector<int8_t>(zVec);
    std::vector<std::optional<int8_t>> z2Vec = {zoom2};
    auto inputZ2 = makeNullableFlatVector<int8_t>(z2Vec);
    return makeRowVector({inputX, inputY, inputZ, inputZ2});
  }

  folly::Expected<std::vector<int64_t>, std::string> makeExpectedChildren(
      int32_t x,
      int32_t y,
      int8_t tileZoom,
      int8_t childZoom) {
    uint64_t tileInt = BingTileType::bingTileCoordsToInt(x, y, tileZoom);
    // Making children is tested in BingTileTypeTest; this test is for whether
    // we call it correctly.
    auto childrenRes = BingTileType::bingTileChildren(tileInt, childZoom);
    if (childrenRes.hasError()) {
      return folly::makeUnexpected(childrenRes.error());
    }
    std::vector<int64_t> children;
    for (uint64_t child : childrenRes.value()) {
      children.push_back(static_cast<int64_t>(child));
    }
    std::sort(children.begin(), children.end());
    return children;
  }
};

TEST_F(BingTileFunctionsTest, toBingTileSignatures) {
  auto signatures = getSignatureStrings("bing_tile");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(integer,integer,tinyint) -> bingtile"));
  ASSERT_EQ(1, signatures.count("(varchar) -> bingtile"));
}

TEST_F(BingTileFunctionsTest, toBingTileCoordinates) {
  const auto testToBingTile = [&](std::optional<int32_t> x,
                                  std::optional<int32_t> y,
                                  std::optional<int8_t> zoom) {
    std::optional<int64_t> tile = evaluateOnce<int64_t>(
        "CAST(bing_tile(c0, c1, c2) AS BIGINT)", x, y, zoom);
    if (x.has_value() && y.has_value() && zoom.has_value()) {
      ASSERT_TRUE(tile.has_value());
      ASSERT_EQ(
          BingTileType::bingTileCoordsToInt(x.value(), y.value(), zoom.value()),
          tile.value());
    } else {
      ASSERT_FALSE(tile.has_value());
    }
  };

  testToBingTile(0, 0, 0);
  testToBingTile(1, 1, 1);
  testToBingTile(127, 11, 8);
  testToBingTile(0, 3000, 20);
  testToBingTile(std::nullopt, 1, 1);
  testToBingTile(1, std::nullopt, 1);
  testToBingTile(1, 1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testToBingTile(0, 1, 0),
      "Bing tile Y coordinate 1 is greater than max coordinate 0 at zoom 0");
  VELOX_ASSERT_USER_THROW(
      testToBingTile(256, 1, 8),
      "Bing tile X coordinate 256 is greater than max coordinate 255 at zoom 8");
  VELOX_ASSERT_USER_THROW(
      testToBingTile(0, 0, 24),
      "Bing tile zoom 24 is greater than max zoom 23");
  VELOX_ASSERT_USER_THROW(
      testToBingTile(-1, 1, 2), "Bing tile X coordinate -1 cannot be negative");
  VELOX_ASSERT_USER_THROW(
      testToBingTile(1, -1, 2), "Bing tile Y coordinate -1 cannot be negative");
  VELOX_ASSERT_USER_THROW(
      testToBingTile(1, 1, -1), "Bing tile zoom -1 cannot be negative");
}

TEST_F(BingTileFunctionsTest, quadKeyToBingTile) {
  const auto testToBingTile = [&](std::optional<std::string_view> quadKey) {
    std::optional<int64_t> tile =
        evaluateOnce<int64_t>("CAST(bing_tile(c0) AS BIGINT)", quadKey);
    if (quadKey.has_value()) {
      ASSERT_TRUE(tile.has_value());
      ASSERT_EQ(
          static_cast<int64_t>(
              BingTileType::bingTileFromQuadKey(quadKey.value()).value()),
          tile.value());
    } else {
      ASSERT_FALSE(tile.has_value());
    }
  };

  testToBingTile("000");
  testToBingTile("001");
  testToBingTile("123123123123123123123"); // 21 digits, valid quadKey
  testToBingTile(std::nullopt);
  testToBingTile("");

  VELOX_ASSERT_USER_THROW(
      testToBingTile("fourty-two"),
      "Invalid QuadKey digit sequence: fourty-two");
  VELOX_ASSERT_USER_THROW(
      testToBingTile("-1"), "Invalid QuadKey digit sequence: -1");
  VELOX_ASSERT_USER_THROW(
      testToBingTile("125"), "Invalid QuadKey digit sequence: 125");
  VELOX_ASSERT_USER_THROW(
      testToBingTile("123123123123123123123123"),
      "Zoom level 24 is greater than max zoom 23"); // 24 digits, invalid
                                                    // quadkey
}

TEST_F(BingTileFunctionsTest, bingTileToQuadKey) {
  const auto testQuadKeySymmetry = [&](std::optional<std::string> quadKey) {
    std::optional<std::string> quadKeyRes =
        evaluateOnce<std::string>("bing_tile_quadkey(bing_tile(c0))", quadKey);
    if (quadKey.has_value()) {
      ASSERT_TRUE(quadKeyRes.has_value());
      ASSERT_EQ(quadKey.value(), quadKeyRes.value());
    } else {
      ASSERT_FALSE(quadKeyRes.has_value());
    }
  };

  testQuadKeySymmetry("");
  testQuadKeySymmetry("213");
  testQuadKeySymmetry("123030123010121");

  const auto getQuadKeyResultFromCoordinates = [&](std::optional<int32_t> x,
                                                   std::optional<int32_t> y,
                                                   std::optional<int8_t> zoom) {
    std::optional<std::string> quadKey = evaluateOnce<std::string>(
        "bing_tile_quadkey(bing_tile(c0, c1, c2))", x, y, zoom);
    return quadKey;
  };

  ASSERT_EQ("", getQuadKeyResultFromCoordinates(0, 0, 0));
  ASSERT_EQ("213", getQuadKeyResultFromCoordinates(3, 5, 3));
  ASSERT_EQ(
      "123030123010121", getQuadKeyResultFromCoordinates(21845, 13506, 15));
}

TEST_F(BingTileFunctionsTest, bingTileZoomLevelSignatures) {
  auto signatures = getSignatureStrings("bing_tile_zoom_level");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> tinyint"));
}

TEST_F(BingTileFunctionsTest, bingTileZoomLevel) {
  const auto testBingTileZoom = [&](std::optional<int32_t> x,
                                    std::optional<int32_t> y,
                                    std::optional<int8_t> zoom) {
    std::optional<int8_t> output = evaluateOnce<int8_t>(
        "bing_tile_zoom_level(bing_tile(c0, c1, c2))", x, y, zoom);
    if (x.has_value() && y.has_value() && zoom.has_value()) {
      ASSERT_TRUE(output.has_value());
      ASSERT_EQ(zoom.value(), output.value());
    } else {
      ASSERT_FALSE(output.has_value());
    }
  };

  testBingTileZoom(0, 0, 0);
  testBingTileZoom(1, 1, 1);
  testBingTileZoom(127, 11, 8);
  testBingTileZoom(0, 3000, 20);
  testBingTileZoom(std::nullopt, 1, 1);
  testBingTileZoom(1, std::nullopt, 1);
  testBingTileZoom(1, 1, std::nullopt);
}

TEST_F(BingTileFunctionsTest, bingTileCoordinatesSignatures) {
  auto signatures = getSignatureStrings("bing_tile_coordinates");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> row(integer,integer)"));
}

TEST_F(BingTileFunctionsTest, bingTileCoordinates) {
  const auto testBingTileCoordinates = [&](std::optional<int32_t> x,
                                           std::optional<int32_t> y,
                                           std::optional<int8_t> zoom) {
    auto input = makeSingleXYZoomRow(x, y, zoom);

    VectorPtr output =
        evaluate("bing_tile_coordinates(bing_tile(c0, c1, c2))", input);

    if (x.has_value() && y.has_value() && zoom.has_value()) {
      auto rowVector = output->asChecked<RowVector>();
      ASSERT_EQ(
          rowVector->childAt(0)->asChecked<SimpleVector<int32_t>>()->valueAt(0),
          x);
      ASSERT_EQ(
          rowVector->childAt(1)->asChecked<SimpleVector<int32_t>>()->valueAt(0),
          y);
    } else {
      // Null inputs mean null output
      ASSERT_TRUE(output->isNullAt(0));
    }
  };

  testBingTileCoordinates(0, 0, 0);
  testBingTileCoordinates(1, 1, 1);
  testBingTileCoordinates(127, 11, 8);
  testBingTileCoordinates(0, 3000, 20);
  testBingTileCoordinates(std::nullopt, 1, 1);
  testBingTileCoordinates(1, std::nullopt, 1);
  testBingTileCoordinates(1, 1, std::nullopt);
}

TEST_F(BingTileFunctionsTest, bingTileParentSignatures) {
  auto signatures = getSignatureStrings("bing_tile_parent");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> bingtile"));
  ASSERT_EQ(1, signatures.count("(bingtile,tinyint) -> bingtile"));
}

TEST_F(BingTileFunctionsTest, bingTileParentNoZoom) {
  const auto testBingTileParent = [&](std::optional<int32_t> x,
                                      std::optional<int32_t> y,
                                      std::optional<int8_t> zoom) {
    std::optional<int64_t> tile = evaluateOnce<int64_t>(
        "CAST(bing_tile_parent(bing_tile(c0, c1, c2)) AS BIGINT)", x, y, zoom);
    if (x.has_value() && y.has_value() && zoom.has_value()) {
      ASSERT_TRUE(tile.has_value());
      ASSERT_EQ(
          BingTileType::bingTileCoordsToInt(
              static_cast<uint32_t>(*x) >> 1,
              static_cast<uint32_t>(*y) >> 1,
              *zoom - 1),
          *tile);
    } else {
      ASSERT_FALSE(tile.has_value());
    }
  };

  testBingTileParent(0, 0, 1);
  testBingTileParent(1, 1, 1);
  testBingTileParent(0, 127, 8);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23);
  testBingTileParent(std::nullopt, 1, 1);
  testBingTileParent(1, std::nullopt, 1);
  testBingTileParent(1, 1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testBingTileParent(0, 0, 0),
      "Cannot call bing_tile_parent on zoom 0 tile");
}

TEST_F(BingTileFunctionsTest, bingTileParentZoom) {
  const auto testBingTileParent = [&](std::optional<int32_t> x,
                                      std::optional<int32_t> y,
                                      std::optional<int8_t> zoom,
                                      std::optional<int8_t> parentZoom) {
    std::optional<int64_t> tile = evaluateOnce<int64_t>(
        "CAST(bing_tile_parent(bing_tile(c0, c1, c2), c3) AS BIGINT)",
        x,
        y,
        zoom,
        parentZoom);
    if (x.has_value() && y.has_value() && zoom.has_value() &&
        parentZoom.has_value()) {
      ASSERT_TRUE(tile.has_value());
      int32_t shift = *zoom - *parentZoom;
      ASSERT_EQ(
          BingTileType::bingTileCoordsToInt(
              static_cast<uint32_t>(*x) >> shift,
              static_cast<uint32_t>(*y) >> shift,
              *parentZoom),
          *tile);
    } else {
      ASSERT_FALSE(tile.has_value());
    }
  };

  testBingTileParent(0, 0, 0, 0);
  testBingTileParent(0, 0, 1, 0);
  testBingTileParent(1, 1, 1, 0);
  testBingTileParent(0, 127, 8, 6);
  testBingTileParent(0, 127, 8, 8);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 22);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 8);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 0);
  testBingTileParent(std::nullopt, 1, 1, 0);
  testBingTileParent(1, std::nullopt, 1, 0);
  testBingTileParent(1, 1, std::nullopt, 0);
  testBingTileParent(1, 1, 1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testBingTileParent(0, 0, 2, -1),
      "Cannot call bing_tile_parent with negative zoom");
  VELOX_ASSERT_USER_THROW(
      testBingTileParent(0, 0, 0, 1), "Parent zoom 1 must be <= tile zoom 0");
  VELOX_ASSERT_USER_THROW(
      testBingTileParent(5, 17, 5, 8), "Parent zoom 8 must be <= tile zoom 5");
}

TEST_F(BingTileFunctionsTest, bingTileChildrenSignatures) {
  auto signatures = getSignatureStrings("bing_tile_children");
  ASSERT_EQ(2, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> array(bingtile)"));
  ASSERT_EQ(1, signatures.count("(bingtile,tinyint) -> array(bingtile)"));
}

TEST_F(BingTileFunctionsTest, bingTileChildren) {
  const auto testBingTileChildren = [&](std::optional<int32_t> x,
                                        std::optional<int32_t> y,
                                        std::optional<int8_t> zoom) {
    RowVectorPtr input = makeSingleXYZoomRow(x, y, zoom);

    VectorPtr output = evaluate(
        "ARRAY_SORT(TRANSFORM(bing_tile_children(bing_tile(c0, c1, c2)) , x -> CAST(x AS BIGINT)))",
        input);

    if (x.has_value() && y.has_value() && zoom.has_value()) {
      ASSERT_EQ(output->getNullCount().value_or(0), 0);
      auto expectedRes =
          makeExpectedChildren(*x, *y, *zoom, static_cast<int8_t>(*zoom + 1));
      ASSERT_TRUE(expectedRes.hasValue());
      std::vector<int64_t> expected = expectedRes.value();

      VectorPtr elements = output->asChecked<ArrayVector>()->elements();
      ASSERT_EQ(output->getNullCount().value_or(0), 0);
      FlatVectorPtr<int64_t> expectedVector = makeFlatVector<int64_t>(expected);
      test::assertEqualVectors(elements, expectedVector);
    } else {
      // Null inputs mean null output
      ASSERT_TRUE(output->isNullAt(0));
    }
  };

  testBingTileChildren(0, 0, 0);
  testBingTileChildren(0, 1, 1);
  testBingTileChildren(7, 127, 8);
  testBingTileChildren((1 << 22) - 1, 1 << 20, 22);
  testBingTileChildren(std::nullopt, 1, 1);
  testBingTileChildren(1, std::nullopt, 1);
  testBingTileChildren(1, 1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testBingTileChildren(0, 0, 23),
      "Cannot call bing_tile_children on zoom 23 tile");
}

TEST_F(BingTileFunctionsTest, bingTileChildrenZoom) {
  const auto testBingTileChildren = [&](std::optional<int32_t> x,
                                        std::optional<int32_t> y,
                                        std::optional<int8_t> zoom,
                                        std::optional<int8_t> childZoom) {
    RowVectorPtr input = makeSingleXYZoomZoomRow(x, y, zoom, childZoom);

    VectorPtr output = evaluate(
        "ARRAY_SORT(TRANSFORM(bing_tile_children(bing_tile(c0, c1, c2), c3) , x -> CAST(x AS BIGINT)))",
        input);

    if (x.has_value() && y.has_value() && zoom.has_value() &&
        childZoom.has_value()) {
      auto expectedRes = makeExpectedChildren(*x, *y, *zoom, *childZoom);
      ASSERT_TRUE(expectedRes.hasValue());
      std::vector<int64_t> expected = expectedRes.value();

      VectorPtr elements = output->asChecked<ArrayVector>()->elements();
      ASSERT_EQ(output->getNullCount().value_or(0), 0);
      FlatVectorPtr<int64_t> expectedVector = makeFlatVector<int64_t>(expected);
      test::assertEqualVectors(elements, expectedVector);
    } else {
      // Null inputs mean null output
      ASSERT_TRUE(output->isNullAt(0));
    }
  };

  testBingTileChildren(0, 0, 0, 0);
  testBingTileChildren(0, 0, 0, 1);
  testBingTileChildren(0, 0, 0, 2);
  testBingTileChildren(7, 127, 8, 12);
  testBingTileChildren((1 << 18) - 1, 1 << 10, 18, 22);
  testBingTileChildren(std::nullopt, 1, 1, 2);
  testBingTileChildren(1, std::nullopt, 1, 2);
  testBingTileChildren(1, 1, std::nullopt, 2);
  testBingTileChildren(1, 1, 1, std::nullopt);

  VELOX_ASSERT_USER_THROW(
      testBingTileChildren(0, 0, 1, -1),
      "Cannot call bing_tile_children with negative zoom");
  VELOX_ASSERT_USER_THROW(
      testBingTileChildren(0, 0, 2, 1), "Child zoom 1 must be >= tile zoom 2");
}
