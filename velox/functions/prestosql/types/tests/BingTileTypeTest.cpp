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
                                        std::optional<std::string> errorMsg) {
    uint64_t tile = BingTileType::bingTileCoordsToInt(x, y, zoom);
    int32_t shift = childZoom - zoom;
    auto childrenResult = BingTileType::bingTileChildren(tile, childZoom);
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

  testBingTileChildren(0, 0, 0, 0, std::nullopt);
  testBingTileChildren(0, 0, 0, 1, std::nullopt);
  testBingTileChildren(0, 0, 0, 2, std::nullopt);
  testBingTileChildren(1, 1, 1, 2, std::nullopt);
  testBingTileChildren(0, 127, 8, 12, std::nullopt);
  testBingTileChildren(0, 127, 8, 8, std::nullopt);
  testBingTileChildren((1 << 21) - 1, 1 << 20, 21, 23, std::nullopt);
  testBingTileChildren((1 << 18) - 1, 1 << 17, 18, 23, std::nullopt);

  testBingTileChildren(0, 0, 2, 1, "Child zoom 1 must be >= tile zoom 2");
  testBingTileChildren(0, 0, 23, 24, "Child zoom 24 must be <= max zoom 23");
}

TEST_F(BingTileTypeTest, bingTileToQuadKey) {
  ASSERT_EQ(
      "123123123123123123", BingTileType::bingTileToQuadKey(804212359411419));
  ASSERT_EQ("000", BingTileType::bingTileToQuadKey(201326592));
  ASSERT_EQ("0", BingTileType::bingTileToQuadKey(67108864));
  ASSERT_EQ("123", BingTileType::bingTileToQuadKey(21676163075));
  ASSERT_EQ("", BingTileType::bingTileToQuadKey(0));
}

} // namespace facebook::velox::test
