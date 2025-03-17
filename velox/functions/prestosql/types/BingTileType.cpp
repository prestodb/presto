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
#include <folly/Expected.h>
#include <optional>
#include <string>

namespace facebook::velox {

std::optional<std::string> BingTileType::bingTileInvalidReason(uint64_t tile) {
  // TODO?: We are duplicating some logic in isBingTileIntValid; maybe we
  // should extract?

  uint8_t version = BingTileType::bingTileVersion(tile);
  if (version != BingTileType::kBingTileVersion) {
    return fmt::format("Version {} not supported", version);
  }

  uint8_t zoom = BingTileType::bingTileZoom(tile);
  if (zoom > BingTileType::kBingTileMaxZoomLevel) {
    return fmt::format(
        "Bing tile zoom {} is greater than max zoom {}",
        zoom,
        BingTileType::kBingTileMaxZoomLevel);
  }

  uint64_t coordinateBound = 1 << zoom;

  if (BingTileType::bingTileX(tile) >= coordinateBound) {
    return fmt::format(
        "Bing tile X coordinate {} is greater than max coordinate {} at zoom {}",
        BingTileType::bingTileX(tile),
        coordinateBound - 1,
        zoom);
  }
  if (BingTileType::bingTileY(tile) >= coordinateBound) {
    return fmt::format(
        "Bing tile Y coordinate {} is greater than max coordinate {} at zoom {}",
        BingTileType::bingTileY(tile),
        coordinateBound - 1,
        zoom);
  }

  return std::nullopt;
}

} // namespace facebook::velox
