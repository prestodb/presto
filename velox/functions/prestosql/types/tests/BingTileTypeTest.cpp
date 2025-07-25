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
#include "velox/functions/prestosql/types/BingTileType.h"
#include "velox/functions/prestosql/types/BingTileRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class BingTileTypeTest : public testing::Test, public TypeTestBase {
 public:
  BingTileTypeTest() {
    registerBingTileType();
  }
};

TEST_F(BingTileTypeTest, basic) {
  ASSERT_STREQ(BINGTILE()->name(), "BINGTILE");
  ASSERT_STREQ(BINGTILE()->kindName(), "BIGINT");
  ASSERT_TRUE(BINGTILE()->parameters().empty());
  ASSERT_EQ(BINGTILE()->toString(), "BINGTILE");

  ASSERT_TRUE(hasType("BINGTILE"));
  ASSERT_EQ(*getType("BINGTILE", {}), *BINGTILE());

  ASSERT_FALSE(BINGTILE()->isOrderable());
}

TEST_F(BingTileTypeTest, serde) {
  testTypeSerde(BINGTILE());
}

TEST_F(BingTileTypeTest, packing) {
  uint64_t tileX123Y111Z7 = BingTileType::bingTileCoordsToInt(123, 111, 7);
  ASSERT_TRUE(BingTileType::isBingTileIntValid(tileX123Y111Z7));
  ASSERT_EQ(BingTileType::bingTileVersion(tileX123Y111Z7), 0);
  ASSERT_EQ(BingTileType::bingTileZoom(tileX123Y111Z7), 7);
  ASSERT_EQ(BingTileType::bingTileX(tileX123Y111Z7), 123);
  ASSERT_EQ(BingTileType::bingTileY(tileX123Y111Z7), 111);

  uint64_t tileZ0 = BingTileType::bingTileCoordsToInt(0, 0, 0);
  ASSERT_TRUE(BingTileType::isBingTileIntValid(tileZ0));
  ASSERT_EQ(BingTileType::bingTileVersion(tileZ0), 0);
  ASSERT_EQ(BingTileType::bingTileZoom(tileZ0), 0);
  ASSERT_EQ(BingTileType::bingTileX(tileZ0), 0);
  ASSERT_EQ(BingTileType::bingTileY(tileZ0), 0);

  // Error cases
  uint64_t tileV1 = (uint64_t)1 << (63 - 5);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileV1));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileV1), "Version 1 not supported");

  uint64_t tileZ24 = BingTileType::bingTileCoordsToInt(0, 0, 24);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileZ24));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileZ24),
      "Bing tile zoom 24 is greater than max zoom 23");

  uint64_t tileX0Y1Z0 = BingTileType::bingTileCoordsToInt(0, 1, 0);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileX0Y1Z0));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileX0Y1Z0),
      "Bing tile Y coordinate 1 is greater than max coordinate 0 at zoom 0");

  uint64_t tileX256Y1Z8 = BingTileType::bingTileCoordsToInt(256, 1, 8);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileX256Y1Z8));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileX256Y1Z8),
      "Bing tile X coordinate 256 is greater than max coordinate 255 at zoom 8");
}

TEST_F(BingTileTypeTest, coordinateRoundtrip) {
  const auto testBingTileCoordinates =
      [&](uint32_t x, uint32_t y, uint8_t zoom) {
        uint64_t tile = BingTileType::bingTileCoordsToInt(x, y, zoom);
        ASSERT_EQ(x, BingTileType::bingTileX(tile));
        ASSERT_EQ(y, BingTileType::bingTileY(tile));
        ASSERT_EQ(zoom, BingTileType::bingTileZoom(tile));
      };

  testBingTileCoordinates(0, 0, 0);
  testBingTileCoordinates(1, 1, 1);
  testBingTileCoordinates(127, 11, 8);
  testBingTileCoordinates(0, 3000, 20);
}

TEST_F(BingTileTypeTest, bingTileParent) {
  const auto testBingTileParent = [&](uint32_t x,
                                      uint32_t y,
                                      uint8_t zoom,
                                      uint8_t parentZoom,
                                      std::optional<std::string> errorMsg) {
    uint64_t tile = BingTileType::bingTileCoordsToInt(x, y, zoom);
    int32_t shift = zoom - parentZoom;
    auto parentResult = BingTileType::bingTileParent(tile, parentZoom);
    if (errorMsg.has_value()) {
      ASSERT_TRUE(parentResult.hasError());
      ASSERT_EQ(errorMsg.value(), parentResult.error());
    } else {
      ASSERT_TRUE(parentResult.hasValue());
      uint64_t parent = parentResult.value();
      ASSERT_EQ(x >> shift, BingTileType::bingTileX(parent));
      ASSERT_EQ(y >> shift, BingTileType::bingTileY(parent));
      ASSERT_EQ(parentZoom, BingTileType::bingTileZoom(parent));
    }
  };

  testBingTileParent(0, 0, 0, 0, std::nullopt);
  testBingTileParent(0, 0, 1, 0, std::nullopt);
  testBingTileParent(1, 1, 1, 0, std::nullopt);
  testBingTileParent(0, 127, 8, 6, std::nullopt);
  testBingTileParent(0, 127, 8, 8, std::nullopt);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 22, std::nullopt);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 8, std::nullopt);
  testBingTileParent((1 << 21) - 1, 1 << 20, 23, 0, std::nullopt);

  testBingTileParent(0, 0, 0, 1, "Parent zoom 1 must be <= tile zoom 0");
  testBingTileParent(5, 17, 5, 8, "Parent zoom 8 must be <= tile zoom 5");
}

TEST_F(BingTileTypeTest, bingTileChildren) {
  const auto testBingTileChildren = [&](uint32_t x,
                                        uint32_t y,
                                        uint8_t zoom,
                                        uint8_t childZoom,
                                        uint8_t maxZoomShift,
                                        std::optional<std::string> errorMsg) {
    uint64_t tile = BingTileType::bingTileCoordsToInt(x, y, zoom);
    int32_t shift = childZoom - zoom;
    auto childrenResult =
        BingTileType::bingTileChildren(tile, childZoom, maxZoomShift);
    if (errorMsg.has_value()) {
      ASSERT_TRUE(childrenResult.hasError());
      ASSERT_EQ(errorMsg.value(), childrenResult.error());
    } else {
      ASSERT_TRUE(childrenResult.hasValue());
      std::vector<uint64_t> children = childrenResult.value();
      uint32_t numChildrenPerOrdinate = 1 << shift;
      uint32_t numChildren = numChildrenPerOrdinate * numChildrenPerOrdinate;
      ASSERT_EQ(children.size(), numChildren);

      std::set<uint32_t> deltaXs;
      std::set<uint32_t> deltaYs;
      for (uint64_t child : children) {
        deltaXs.insert(BingTileType::bingTileX(child) - (x << shift));
        deltaYs.insert(BingTileType::bingTileY(child) - (y << shift));
        ASSERT_EQ(x, BingTileType::bingTileX(child) >> shift);
        ASSERT_EQ(y, BingTileType::bingTileY(child) >> shift);
        ASSERT_EQ(childZoom, BingTileType::bingTileZoom(child));
      }

      std::set<uint32_t> expectedDeltas;
      for (uint32_t i = 0; i < numChildrenPerOrdinate; i++) {
        expectedDeltas.insert(i);
      }
      ASSERT_EQ(deltaXs, expectedDeltas);
      ASSERT_EQ(deltaYs, expectedDeltas);
    }
  };

  testBingTileChildren(0, 0, 0, 0, 23, std::nullopt);
  testBingTileChildren(0, 0, 0, 1, 23, std::nullopt);
  testBingTileChildren(0, 0, 0, 2, 23, std::nullopt);
  testBingTileChildren(1, 1, 1, 2, 23, std::nullopt);
  testBingTileChildren(0, 127, 8, 12, 23, std::nullopt);
  testBingTileChildren(0, 127, 8, 8, 23, std::nullopt);
  testBingTileChildren((1 << 21) - 1, 1 << 20, 21, 23, 23, std::nullopt);
  testBingTileChildren((1 << 18) - 1, 1 << 17, 18, 23, 23, std::nullopt);

  testBingTileChildren(0, 0, 2, 1, 23, "Child zoom 1 must be >= tile zoom 2");
  testBingTileChildren(
      0, 0, 23, 24, 23, "Child zoom 24 must be <= max zoom 23");
  testBingTileChildren(
      0,
      0,
      0,
      6,
      5,
      "Difference between parent zoom (0) and child zoom (6) must be <= 5");
}

TEST_F(BingTileTypeTest, bingTileToQuadKey) {
  ASSERT_EQ(
      "123123123123123123", BingTileType::bingTileToQuadKey(804212359411419));
  ASSERT_EQ("000", BingTileType::bingTileToQuadKey(201326592));
  ASSERT_EQ("0", BingTileType::bingTileToQuadKey(67108864));
  ASSERT_EQ("123", BingTileType::bingTileToQuadKey(21676163075));
  ASSERT_EQ("", BingTileType::bingTileToQuadKey(0));
}

TEST_F(BingTileTypeTest, bingTileFromQuadKey) {
  const auto testBingTileFromQuadKey =
      [&](const std::string& quadKey,
          std::optional<uint64_t> expectedResult,
          std::optional<std::string> errorMsg = std::nullopt) {
        auto tileResult = BingTileType::bingTileFromQuadKey(quadKey);
        if (errorMsg.has_value()) {
          ASSERT_TRUE(tileResult.hasError());
          ASSERT_EQ(errorMsg.value(), tileResult.error());
        } else {
          ASSERT_TRUE(tileResult.hasValue());
          uint64_t tile = tileResult.value();
          ASSERT_EQ(tile, expectedResult.value());
          ASSERT_EQ(quadKey, BingTileType::bingTileToQuadKey(tile));
        }
      };

  testBingTileFromQuadKey("123123123123123123", 804212359411419);
  testBingTileFromQuadKey("000", 201326592);
  testBingTileFromQuadKey("0", 67108864);
  testBingTileFromQuadKey("123", 21676163075);
  testBingTileFromQuadKey("", 0);
  testBingTileFromQuadKey(
      "123123123123123123123123123123123123",
      std::nullopt,
      "Zoom level 36 is greater than max zoom 23");
  testBingTileFromQuadKey(
      "blah", std::nullopt, "Invalid QuadKey digit sequence: blah");
  testBingTileFromQuadKey(
      "1231234", std::nullopt, "Invalid QuadKey digit sequence: 1231234");
}

TEST_F(BingTileTypeTest, latitudeLongitudeToTile) {
  const auto testLatitudeLongitudeToTile =
      [&](double latitude,
          double longitude,
          uint8_t zoomLevel,
          std::optional<uint32_t> expectedY = std::nullopt,
          std::optional<uint32_t> expectedX = std::nullopt,
          std::optional<std::string> errorMsg = std::nullopt) {
        auto result = BingTileType::latitudeLongitudeToTile(
            latitude, longitude, zoomLevel);
        if (errorMsg.has_value()) {
          ASSERT_TRUE(result.hasError());
          ASSERT_EQ(errorMsg.value(), result.error());
        } else {
          ASSERT_TRUE(result.hasValue());
          ASSERT_EQ(zoomLevel, BingTileType::bingTileZoom(result.value()));
          if (expectedX.has_value()) {
            ASSERT_EQ(
                expectedX.value(), BingTileType::bingTileX(result.value()));
          }
          if (expectedY.has_value()) {
            ASSERT_EQ(
                expectedY.value(), BingTileType::bingTileY(result.value()));
          }
        }
      };

  testLatitudeLongitudeToTile(0, 50, 2, 2, 2);
  testLatitudeLongitudeToTile(0, 50, 4, 8, 10);
  testLatitudeLongitudeToTile(50, 0, 8, 86, 128);
  testLatitudeLongitudeToTile(85, 180, 8, 0, 255);
  testLatitudeLongitudeToTile(-85, -180, 8, 255, 0);
  testLatitudeLongitudeToTile(-85, -180, 23, 8374868, 0);
  testLatitudeLongitudeToTile(-85, -180, 0, 0, 0);
  testLatitudeLongitudeToTile(0, 0, 0, 0, 0);

  // Failure cases
  testLatitudeLongitudeToTile(
      0,
      180.1,
      20,
      std::nullopt,
      std::nullopt,
      "Longitude 180.1 is outside of valid range [-180, 180]");
  testLatitudeLongitudeToTile(
      0,
      -180.1,
      20,
      std::nullopt,
      std::nullopt,
      "Longitude -180.1 is outside of valid range [-180, 180]");
  testLatitudeLongitudeToTile(
      85.1,
      0,
      20,
      std::nullopt,
      std::nullopt,
      "Latitude 85.1 is outside of valid range [-85.05112878, 85.05112878]");
  testLatitudeLongitudeToTile(
      -85.1,
      0,
      20,
      std::nullopt,
      std::nullopt,
      "Latitude -85.1 is outside of valid range [-85.05112878, 85.05112878]");
  testLatitudeLongitudeToTile(
      0,
      50.0,
      24,
      std::nullopt,
      std::nullopt,
      "Zoom level 24 is greater than max zoom 23");
}

TEST_F(BingTileTypeTest, bingTilesAround) {
  const auto testBingTilesAround =
      [&](double latitude,
          double longitude,
          uint8_t zoom,
          std::optional<std::vector<std::string>> expectedTileQuadKeys,
          std::optional<std::string> errorMsg = std::nullopt) {
        auto tilesAroundResult =
            BingTileType::bingTilesAround(latitude, longitude, zoom);
        if (errorMsg.has_value()) {
          ASSERT_TRUE(tilesAroundResult.hasError());
          ASSERT_EQ(errorMsg.value(), tilesAroundResult.error());
        } else {
          ASSERT_TRUE(tilesAroundResult.hasValue());
          std::vector<uint64_t> tiles = tilesAroundResult.value();
          ASSERT_LE(tiles.size(), 9);
          std::vector<std::string> tileQuadKeys;
          // Compare quadkeys instead of integer values for brevity and
          // readability
          tileQuadKeys.reserve(tiles.size());
          for (uint64_t tile : tiles) {
            tileQuadKeys.push_back(BingTileType::bingTileToQuadKey(tile));
          }
          ASSERT_EQ(expectedTileQuadKeys.value(), tileQuadKeys);
        }
      };

  // General tests
  testBingTilesAround(30.12, 60, 1, {{"0", "2", "1", "3"}});
  testBingTilesAround(
      30.12,
      60,
      15,
      {{"123030123010102",
        "123030123010120",
        "123030123010122",
        "123030123010103",
        "123030123010121",
        "123030123010123",
        "123030123010112",
        "123030123010130",
        "123030123010132"}});
  testBingTilesAround(
      30.12,
      60,
      23,
      {{"12303012301012121210122",
        "12303012301012121210300",
        "12303012301012121210302",
        "12303012301012121210123",
        "12303012301012121210301",
        "12303012301012121210303",
        "12303012301012121210132",
        "12303012301012121210310",
        "12303012301012121210312"}});

  // Test around corner
  testBingTilesAround(-85.05112878, -180, 1, {{"0", "2", "1", "3"}});
  testBingTilesAround(-85.05112878, -180, 3, {{"220", "222", "221", "223"}});
  testBingTilesAround(
      -85.05112878,
      -180,
      15,
      {{"222222222222220",
        "222222222222222",
        "222222222222221",
        "222222222222223"}});

  testBingTilesAround(-85.05112878, -180, 2, {{"20", "22", "21", "23"}});
  testBingTilesAround(-85.05112878, 180, 2, {{"30", "32", "31", "33"}});
  testBingTilesAround(85.05112878, -180, 2, {{"00", "02", "01", "03"}});
  testBingTilesAround(85.05112878, 180, 2, {{"10", "12", "11", "13"}});

  // Test around edge
  testBingTilesAround(-85.05112878, 0, 1, {{"0", "2", "1", "3"}});
  testBingTilesAround(
      -85.05112878, 0, 3, {{"231", "233", "320", "322", "321", "323"}});
  testBingTilesAround(
      -85.05112878,
      0,
      15,
      {{"233333333333331",
        "233333333333333",
        "322222222222220",
        "322222222222222",
        "322222222222221",
        "322222222222223"}});

  testBingTilesAround(
      -85.05112878, 0, 2, {{"21", "23", "30", "32", "31", "33"}});
  testBingTilesAround(
      85.05112878, 0, 2, {{"01", "03", "10", "12", "11", "13"}});
  testBingTilesAround(0, 180, 2, {{"12", "30", "32", "13", "31", "33"}});
  testBingTilesAround(0, -180, 2, {{"02", "20", "22", "03", "21", "23"}});

  // Test failure cases
  testBingTilesAround(
      0, 0, 25, std::nullopt, "Zoom level 25 is greater than max zoom 23");
  testBingTilesAround(
      -86,
      -180,
      5,
      std::nullopt,
      "Latitude -86 is outside of valid range [-85.05112878, 85.05112878]");
  testBingTilesAround(
      -85,
      -181,
      5,
      std::nullopt,
      "Longitude -181 is outside of valid range [-180, 180]");
}

TEST_F(BingTileTypeTest, bingTilesAroundWithRadius) {
  const auto testBingTilesAroundWithRadius =
      [&](double latitude,
          double longitude,
          uint8_t zoom,
          double radiusInKm,
          std::optional<std::vector<std::string>> expectedTileQuadKeys,
          std::optional<std::string> errorMsg = std::nullopt) {
        auto tilesAroundResult = BingTileType::bingTilesAround(
            latitude, longitude, zoom, radiusInKm);
        if (errorMsg.has_value()) {
          ASSERT_TRUE(tilesAroundResult.hasError());
          ASSERT_EQ(errorMsg.value(), tilesAroundResult.error());
        } else {
          ASSERT_TRUE(tilesAroundResult.hasValue());
          std::vector<uint64_t> tiles = tilesAroundResult.value();
          std::vector<std::string> tileQuadKeys;
          tileQuadKeys.reserve(tiles.size());
          for (uint64_t tile : tiles) {
            tileQuadKeys.push_back(BingTileType::bingTileToQuadKey(tile));
          }
          ASSERT_EQ(expectedTileQuadKeys.value(), tileQuadKeys);
        }
      };

  // General cases
  testBingTilesAroundWithRadius(0, 0, 3, 10, {{"211", "300", "122", "033"}});
  testBingTilesAroundWithRadius(30.12, 60, 1, 1000, {{"1"}});
  testBingTilesAroundWithRadius(
      30.12,
      60,
      15,
      .5,
      {{"123030123010120", "123030123010121", "123030123010123"}});

  testBingTilesAroundWithRadius(
      30.12,
      60,
      19,
      .05,
      {{"1230301230101212120",
        "1230301230101212121",
        "1230301230101212130",
        "1230301230101212103",
        "1230301230101212123",
        "1230301230101212112",
        "1230301230101212102"}});

  // Corner cases. Literally! These test bing tiles around corners.
  testBingTilesAroundWithRadius(-85.05112878, -180, 1, 500, {{"3", "2"}});
  testBingTilesAroundWithRadius(
      -85.05112878,
      -180,
      5,
      200,
      {{"33332",
        "33333",
        "22222",
        "22223",
        "22220",
        "22221",
        "33330",
        "33331"}});
  testBingTilesAroundWithRadius(
      -85.05112878,
      -180,
      15,
      .2,
      {{"333333333333332",
        "333333333333333",
        "222222222222222",
        "222222222222223",
        "222222222222220",
        "222222222222221",
        "333333333333330",
        "333333333333331"}});
  testBingTilesAroundWithRadius(
      -85.05112878,
      -180,
      4,
      500,
      {{"3323",
        "3332",
        "3333",
        "2222",
        "2223",
        "2232",
        "2220",
        "2221",
        "3330",
        "3331"}});
  testBingTilesAroundWithRadius(
      -85.05112878,
      180,
      4,
      500,
      {{"3323",
        "3332",
        "3333",
        "2222",
        "2223",
        "2232",
        "3331",
        "2221",
        "2220",
        "3330"}});
  testBingTilesAroundWithRadius(
      85.05112878,
      -180,
      4,
      500,
      {{"1101",
        "1110",
        "1111",
        "0000",
        "0001",
        "0010",
        "0002",
        "0003",
        "1112",
        "1113"}});
  testBingTilesAroundWithRadius(
      85.05112878,
      180,
      4,
      500,
      {{"1101",
        "1110",
        "1111",
        "0000",
        "0001",
        "0010",
        "1113",
        "0003",
        "0002",
        "1112"}});

  // Edge cases- these test bing tiles around edges.
  testBingTilesAroundWithRadius(-85.05112878, 0, 3, 300, {{"233", "322"}});
  testBingTilesAroundWithRadius(
      -85.05112878,
      0,
      12,
      1,
      {{"233333333332",
        "233333333333",
        "322222222222",
        "322222222223",
        "322222222220",
        "233333333331"}});
  // Different edges, starting at 2,3

  testBingTilesAroundWithRadius(-85.05112878, 0, 4, 100, {{"2333", "3222"}});
  testBingTilesAroundWithRadius(85.05112878, 0, 4, 100, {{"0111", "1000"}});
  testBingTilesAroundWithRadius(
      0, 180, 4, 100, {{"3111", "2000", "1333", "0222"}});
  testBingTilesAroundWithRadius(
      0, -180, 4, 100, {{"3111", "2000", "0222", "1333"}});

  // Failure cases
  // Invalid radius
  testBingTilesAroundWithRadius(
      30.12,
      60,
      1,
      -1,
      std::nullopt,
      "Radius in km must between 0 and 1000, got -1");
  testBingTilesAroundWithRadius(
      30.12,
      60,
      1,
      2000,
      std::nullopt,
      "Radius in km must between 0 and 1000, got 2000");
  // Too many tiles
  testBingTilesAroundWithRadius(
      30.12,
      60,
      20,
      100,
      std::nullopt,
      "The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: 36699364.");
}

} // namespace facebook::velox::test
