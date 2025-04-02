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
#pragma once

#include <folly/Expected.h>
#include <cstdint>
#include "velox/type/SimpleFunctionApi.h"

namespace facebook::velox {

/// Bing Tile represented by a 64-bit int.
///
/// Bing tiles are a quad-tree representation of the earth's surface, allowing
/// geospatial data to be represented in a hierarchical manner to the desired
/// granularity. Zoom level 0 is a single square, zoom 1 divides zoom 0 into 4
/// equal-area squares, and so on recursively. Each point of the Earth's surface
/// (aside from near the poles) is contained within a single BingTile at each
/// zoom level (with rules for boundaries). Bing tiles can be referenced by
/// (zoom, x, y), with (zoom=2, x=0, y=3) being the southwest-most square of 16
/// squares at zoom level 2. Bing tiles can also be referenced by quadkey, a
/// string of ['0', '1', '2', '3'] for each level.  Quadkey "" is the only zoom
/// 0 square, "22" is the southwest-most square at zoom 2, and so on.  Quadkeys
/// have the nice property that geometrical containment is equivalent to a
/// string prefix check. Definition and more details:
/// https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
///
/// In Velox, a Bing Tile is a packed 64 bits.  The packing is:
/// 0-4: Version (5 bits)
/// 5-8: 0 (4 bits)
/// 9-31: x-coordinate (23 bits)
/// 32-35: Zoom (5 bits)
/// 36-40: 0 (4 bits)
/// 41-63: y-coordinate (23 bits)
/// (high bits first, low bits last). This peculiar arrangement maximizes
/// low-bit entropy for the Java long hash function.
class BingTileType : public BigintType {
  BingTileType() : BigintType() {}

 public:
  static const std::shared_ptr<const BingTileType>& get() {
    static const std::shared_ptr<const BingTileType> instance =
        std::shared_ptr<BingTileType>(new BingTileType());

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "BINGTILE";
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  static constexpr uint8_t kBingTileVersion = 0;
  static constexpr uint8_t kBingTileMaxZoomLevel = 23;
  static constexpr uint8_t kBingTileZoomBitWidth = 5;
  static constexpr uint8_t kBingTileVersionOffset = 63 - kBingTileZoomBitWidth;
  static constexpr uint8_t kBingTileZoomOffset = 31 - kBingTileZoomBitWidth;
  static constexpr uint64_t kBits23Mask = (1 << 24) - 1;
  static constexpr uint64_t kBits5Mask = (1 << 6) - 1;

  static inline uint64_t
  bingTileCoordsToInt(uint32_t x, uint32_t y, uint8_t zoom) {
    uint64_t tile = (static_cast<uint64_t>(zoom) << kBingTileZoomOffset) |
        (static_cast<uint64_t>(x) << 32) | static_cast<uint64_t>(y);
    return tile;
  }

  /// Returns the version of the tile (as uint64)
  static inline uint8_t bingTileVersion(uint64_t tile) {
    return (tile >> kBingTileVersionOffset) & kBits5Mask;
  }

  /// Returns the zoom of the tile (as uint64)
  static inline uint8_t bingTileZoom(uint64_t tile) {
    return (tile >> kBingTileZoomOffset) & kBits5Mask;
  }

  /// Returns the x-coordinate of the tile (as uint64)
  static inline uint32_t bingTileX(uint64_t tile) {
    return (tile >> 32) & kBits23Mask;
  }

  /// Returns the y-coordinate of the tile (as uint64)
  static inline uint32_t bingTileY(uint64_t tile) {
    return tile & kBits23Mask;
  }

  /// Returns true if the tile (as uint64) is valid
  static inline bool isBingTileIntValid(uint64_t tile) {
    uint8_t zoom = bingTileZoom(tile);
    uint64_t coordinateBound = 1 << zoom;
    // Using bitwise & so that it's branchless and the data
    // can be prefetched and the ops pipelined.
    // Linter wants the bools cast to uint8 for bitwise ops.
    return (uint8_t)(bingTileVersion(tile) == kBingTileVersion) &
        (uint8_t)(zoom <= kBingTileMaxZoomLevel) &
        (uint8_t)(bingTileX(tile) < coordinateBound) &
        (uint8_t)(bingTileY(tile) < coordinateBound);
  }

  static std::optional<std::string> bingTileInvalidReason(uint64_t tile);

  static folly::Expected<uint64_t, std::string> bingTileParent(
      uint64_t tile,
      uint8_t parentZoom);

  static folly::Expected<std::vector<uint64_t>, std::string> bingTileChildren(
      uint64_t tile,
      uint8_t childZoom);

  static folly::Expected<uint64_t, std::string> bingTileFromQuadKey(
      const std::string_view& quadKey);

  static std::string bingTileToQuadKey(uint64_t tile);
};

inline bool isBingTileType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return BingTileType::get() == type;
}

inline std::shared_ptr<const BingTileType> BINGTILE() {
  return BingTileType::get();
}

// Type used for function registration.
struct BingTileT {
  using type = int64_t;
  static constexpr const char* typeName = "bingtile";
};

using BingTile = CustomType<BingTileT, false>;

} // namespace facebook::velox
