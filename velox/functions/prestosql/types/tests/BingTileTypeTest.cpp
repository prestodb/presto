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
      "Zoom 24 is greater than max zoom 23");

  uint64_t tileX0Y1Z0 = BingTileType::bingTileCoordsToInt(0, 1, 0);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileX0Y1Z0));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileX0Y1Z0),
      "Y coordinate 1 is greater than max coordinate 0 at zoom 0");

  uint64_t tileX256Y1Z8 = BingTileType::bingTileCoordsToInt(256, 1, 8);
  ASSERT_FALSE(BingTileType::isBingTileIntValid(tileX256Y1Z8));
  ASSERT_EQ(
      BingTileType::bingTileInvalidReason(tileX256Y1Z8),
      "X coordinate 256 is greater than max coordinate 255 at zoom 8");
}

} // namespace facebook::velox::test
