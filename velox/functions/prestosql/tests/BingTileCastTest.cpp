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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/BingTileType.h"

namespace facebook::velox::functions::prestosql {

namespace {

class BingTileCastTest : public functions::test::FunctionBaseTest {
 protected:
  std::optional<int64_t> castToBigintRoundtrip(std::optional<int64_t> input) {
    std::optional<int64_t> result =
        evaluateOnce<int64_t>("cast(cast(c0 as bingtile) as bigint)", input);
    return result;
  }
};

TEST_F(BingTileCastTest, roundtrip) {
  auto assertRoundtrip = [this](int32_t x, int32_t y, int8_t zoom) {
    std::optional<int64_t> result_bigint = castToBigintRoundtrip(
        (int64_t)BingTileType::bingTileCoordsToInt(x, y, zoom));
    ASSERT_TRUE(result_bigint.has_value());
    uint64_t tile = static_cast<uint64_t>(result_bigint.value());
    ASSERT_TRUE(BingTileType::isBingTileIntValid(tile));
    ASSERT_EQ(BingTileType::bingTileZoom(tile), zoom);
    ASSERT_EQ(BingTileType::bingTileX(tile), x);
    ASSERT_EQ(BingTileType::bingTileY(tile), y);
  };

  assertRoundtrip(0, 0, 0);
  assertRoundtrip(123, 111, 7);
}

TEST_F(BingTileCastTest, errors) {
  VELOX_ASSERT_USER_THROW(
      evaluateOnce<int64_t>(
          "cast(c0 as bingtile)",
          std::optional((int64_t)BingTileType::bingTileCoordsToInt(0, 1, 0))),
      "Y coordinate 1 is greater than max coordinate 0 at zoom 0");
}

} // namespace

} // namespace facebook::velox::functions::prestosql
