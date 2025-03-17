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

class GeospatialFunctionsTest : public functions::test::FunctionBaseTest {};

TEST_F(GeospatialFunctionsTest, toBingTileSignatures) {
  auto signatures = getSignatureStrings("bing_tile");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(integer,integer,tinyint) -> bingtile"));
}

TEST_F(GeospatialFunctionsTest, toBingTileCoordinates) {
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

TEST_F(GeospatialFunctionsTest, bingTileZoomLevelSignatures) {
  auto signatures = getSignatureStrings("bing_tile_zoom_level");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> tinyint"));
}

TEST_F(GeospatialFunctionsTest, bingTileZoomLevel) {
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

TEST_F(GeospatialFunctionsTest, bingTileCoordinatesSignatures) {
  auto signatures = getSignatureStrings("bing_tile_coordinates");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(1, signatures.count("(bingtile) -> row(integer,integer)"));
}

TEST_F(GeospatialFunctionsTest, bingTileCoordinates) {
  const auto testBingTileCoordinates = [&](std::optional<int32_t> x,
                                           std::optional<int32_t> y,
                                           std::optional<int8_t> zoom) {
    std::vector<std::optional<int32_t>> xVec = {x};
    auto inputX = makeNullableFlatVector<int32_t>(xVec);
    std::vector<std::optional<int32_t>> yVec = {y};
    auto inputY = makeNullableFlatVector<int32_t>(yVec);
    std::vector<std::optional<int8_t>> zVec = {zoom};
    auto inputZ = makeNullableFlatVector<int8_t>(zVec);
    auto input = makeRowVector({inputX, inputY, inputZ});

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
