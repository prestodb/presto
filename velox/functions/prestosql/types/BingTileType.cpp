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

folly::Expected<uint64_t, std::string> BingTileType::bingTileParent(
    uint64_t tile,
    uint8_t parentZoom) {
  uint8_t tileZoom = bingTileZoom(tile);
  if (FOLLY_UNLIKELY(tileZoom == parentZoom)) {
    return tile;
  }
  uint32_t x = bingTileX(tile);
  uint32_t y = bingTileY(tile);

  if (FOLLY_UNLIKELY(tileZoom < parentZoom)) {
    return folly::makeUnexpected(fmt::format(
        "Parent zoom {} must be <= tile zoom {}", parentZoom, tileZoom));
  }
  uint8_t shift = tileZoom - parentZoom;
  return bingTileCoordsToInt((x >> shift), (y >> shift), parentZoom);
}

folly::Expected<std::vector<uint64_t>, std::string>
BingTileType::bingTileChildren(uint64_t tile, uint8_t childZoom) {
  uint8_t tileZoom = bingTileZoom(tile);
  if (FOLLY_UNLIKELY(tileZoom == childZoom)) {
    return std::vector<uint64_t>{tile};
  }
  uint32_t x = bingTileX(tile);
  uint32_t y = bingTileY(tile);

  if (FOLLY_UNLIKELY(childZoom < tileZoom)) {
    return folly::makeUnexpected(fmt::format(
        "Child zoom {} must be >= tile zoom {}", childZoom, tileZoom));
  }
  if (FOLLY_UNLIKELY(childZoom > kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Child zoom {} must be <= max zoom {}",
        childZoom,
        kBingTileMaxZoomLevel));
  }

  uint8_t shift = childZoom - tileZoom;
  uint32_t xBase = (x << shift);
  uint32_t yBase = (y << shift);
  uint32_t numChildrenPerOrdinate = 1 << shift;
  std::vector<uint64_t> children;
  children.reserve(numChildrenPerOrdinate * numChildrenPerOrdinate);
  for (uint32_t deltaX = 0; deltaX < numChildrenPerOrdinate; ++deltaX) {
    for (uint32_t deltaY = 0; deltaY < numChildrenPerOrdinate; ++deltaY) {
      children.push_back(
          bingTileCoordsToInt(xBase + deltaX, yBase + deltaY, childZoom));
    }
  }
  return children;
}

folly::Expected<uint64_t, std::string> BingTileType::bingTileFromQuadKey(
    const std::string_view& quadKey) {
  size_t zoomLevelInt32 = quadKey.size();
  if (FOLLY_UNLIKELY(zoomLevelInt32 > kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Zoom level {} is greater than max zoom {}",
        zoomLevelInt32,
        kBingTileMaxZoomLevel));
  }
  uint8_t zoomLevel = static_cast<uint8_t>(zoomLevelInt32);
  uint32_t tileX = 0;
  uint32_t tileY = 0;
  for (uint8_t i = zoomLevel; i > 0; i--) {
    int mask = 1 << (i - 1);
    switch (quadKey.at(zoomLevel - i)) {
      case '0':
        break;
      case '1':
        tileX |= mask;
        break;
      case '2':
        tileY |= mask;
        break;
      case '3':
        tileX |= mask;
        tileY |= mask;
        break;
      default:
        return folly::makeUnexpected(
            fmt::format("Invalid QuadKey digit sequence: {}", quadKey));
    }
  }
  return BingTileType::bingTileCoordsToInt(tileX, tileY, zoomLevel);
}

std::string BingTileType::bingTileToQuadKey(uint64_t tile) {
  uint8_t zoomLevel = bingTileZoom(tile);
  uint32_t tileX = bingTileX(tile);
  uint32_t tileY = bingTileY(tile);

  std::string quadKey;
  quadKey.resize(zoomLevel);

  for (uint8_t i = zoomLevel; i > 0; i--) {
    char digit = '0';
    int mask = 1 << (i - 1);
    if ((tileX & mask) != 0) {
      digit++;
    }
    if ((tileY & mask) != 0) {
      digit += 2;
    }
    quadKey[zoomLevel - i] = digit;
  }
  return quadKey;
}

} // namespace facebook::velox
