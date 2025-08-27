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
#include <algorithm>
#include <optional>
#include <string>

namespace facebook::velox {

namespace {
folly::Expected<int64_t, std::string> mapSize(uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(zoomLevel > BingTileType::kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Zoom level {} is greater than max zoom {}",
        zoomLevel,
        BingTileType::kBingTileMaxZoomLevel));
  }
  return 256L << zoomLevel;
}

int32_t axisToCoordinates(double axis, long mapSize) {
  int32_t tileAxis = std::clamp<int32_t>(
      static_cast<int32_t>(axis * mapSize),
      0,
      static_cast<int32_t>(mapSize - 1));
  return tileAxis / BingTileType::kTilePixels;
}

/**
 * Given longitude in degrees, and the level of detail, the tile X coordinate
 * can be calculated as follows: pixelX = ((longitude + 180) / 360) * 2**level
 * The latitude and longitude are assumed to be on the WGS 84 datum. Even
 * though Bing Maps uses a spherical projection, it’s important to convert all
 * geographic coordinates into a common datum, and WGS 84 was chosen to be
 * that datum. The longitude is assumed to range from -180 to +180 degrees.
 * <p> reference: https://msdn.microsoft.com/en-us/library/bb259689.aspx
 */
folly::Expected<uint32_t, std::string> longitudeToTileX(
    double longitude,
    uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(
          longitude > BingTileType::kMaxLongitude ||
          longitude < BingTileType::kMinLongitude)) {
    return folly::makeUnexpected(fmt::format(
        "Longitude {} is outside of valid range [{}, {}]",
        longitude,
        BingTileType::kMinLongitude,
        BingTileType::kMaxLongitude));
  }
  double x = (longitude + 180) / 360;

  folly::Expected<int64_t, std::string> mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }

  return axisToCoordinates(x, mpSize.value());
}

/**
 * Given latitude in degrees, and the level of detail, the tile Y coordinate
 * can be calculated as follows: sinLatitude = sin(latitude * pi/180) pixelY =
 * (0.5 – log((1 + sinLatitude) / (1 – sinLatitude)) / (4 * pi)) * 2**level
 * The latitude and longitude are assumed to be on the WGS 84 datum. Even
 * though Bing Maps uses a spherical projection, it’s important to convert all
 * geographic coordinates into a common datum, and WGS 84 was chosen to be
 * that datum. The latitude must be clipped to range from -85.05112878
 * to 85.05112878. This avoids a singularity at the poles, and it causes the
 * projected map to be square. <p> reference:
 * https://msdn.microsoft.com/en-us/library/bb259689.aspx
 */
folly::Expected<uint32_t, std::string> latitudeToTileY(
    double latitude,
    uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(
          latitude > BingTileType::kMaxLatitude ||
          latitude < BingTileType::kMinLatitude)) {
    return folly::makeUnexpected(fmt::format(
        "Latitude {} is outside of valid range [{}, {}]",
        latitude,
        BingTileType::kMinLatitude,
        BingTileType::kMaxLatitude));
  }
  double sinLatitude = sin(latitude * M_PI / 180);
  double y = 0.5 - log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * M_PI);
  folly::Expected<int64_t, std::string> mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }

  return axisToCoordinates(y, mpSize.value());
}

double toRadians(double degrees) {
  return degrees * M_PI / 180.0;
}
double toDegrees(double radians) {
  return radians * 180.0 / M_PI;
}

double addDistanceToLongitude(
    double latitude,
    double longitude,
    double radiusInKm,
    double bearing) {
  double latitudeInRadians = toRadians(latitude);
  double longitudeInRadians = toRadians(longitude);
  double bearingInRadians = toRadians(bearing);
  double radiusRatio = radiusInKm / BingTileType::kEarthRadiusKm;

  // Haversine formula
  double newLongitude = toDegrees(
      longitudeInRadians +
      atan2(
          sin(bearingInRadians) * sin(radiusRatio) * cos(latitudeInRadians),
          cos(radiusRatio) - sin(latitudeInRadians) * sin(latitudeInRadians)));

  if (newLongitude > BingTileType::kMaxLongitude) {
    return BingTileType::kMinLongitude +
        (newLongitude - BingTileType::kMaxLongitude);
  }

  if (newLongitude < BingTileType::kMinLongitude) {
    return BingTileType::kMaxLongitude +
        (newLongitude - BingTileType::kMinLongitude);
  }

  return newLongitude;
}

double
addDistanceToLatitude(double latitude, double radiusInKm, double bearing) {
  double latitudeInRadians = toRadians(latitude);
  double bearingInRadians = toRadians(bearing);
  double radiusRatio = radiusInKm / BingTileType::kEarthRadiusKm;
  // Haversine formula
  double newLatitude = toDegrees(asin(
      sin(latitudeInRadians) * cos(radiusRatio) +
      cos(latitudeInRadians) * sin(radiusRatio) * cos(bearingInRadians)));
  if (newLatitude > BingTileType::kMaxLatitude) {
    return BingTileType::kMaxLatitude;
  }
  if (newLatitude < BingTileType::kMinLatitude) {
    return BingTileType::kMinLatitude;
  }
  return newLatitude;
}

struct GreatCircleDistanceToPoint {
 public:
  GreatCircleDistanceToPoint(double latitude, double longitude) {
    double radianLatitude = toRadians(latitude);

    sinLatitude = sin(radianLatitude);
    cosLatitude = cos(radianLatitude);

    radianLongitude = toRadians(longitude);
  }

  double sinLatitude;
  double cosLatitude;
  double radianLongitude;

  double distance(double latitude2, double longitude2) {
    double radianLatitude2 = toRadians(latitude2);
    double sin2 = sin(radianLatitude2);
    double cos2 = cos(radianLatitude2);

    double deltaLongitude = radianLongitude - toRadians(longitude2);
    double cosDeltaLongitude = cos(deltaLongitude);

    double t1 = cos2 * sin(deltaLongitude);
    double t2 = cosLatitude * sin2 - sinLatitude * cos2 * cosDeltaLongitude;
    double t3 = sinLatitude * sin2 + cosLatitude * cos2 * cosDeltaLongitude;
    return atan2(std::sqrt(t1 * t1 + t2 * t2), t3) *
        BingTileType::kEarthRadiusKm;
  }

  bool withinDistance(double latitude, double longitude, double maxDistance) {
    return distance(latitude, longitude) <= maxDistance;
  }
};
} // namespace

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

  uint64_t coordinateBound = 1ul << zoom;

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
BingTileType::bingTileChildren(
    uint64_t tile,
    uint8_t childZoom,
    uint8_t maxZoomShift) {
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
  if (shift > maxZoomShift) {
    return folly::makeUnexpected(fmt::format(
        "Difference between parent zoom ({}) and child zoom ({}) must be <= {}",
        tileZoom,
        childZoom,
        maxZoomShift));
  }
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

/**
 * Returns a Bing tile at a given zoom level containing a point at a given
 * latitude and longitude. Latitude must be within [-85.05112878, 85.05112878]
 * range. Longitude must be within [-180, 180] range. Zoom levels from 1 to 23
 * are supported. For latitude/longitude values on the border of multiple tiles,
 * the southeastern tile is returned. For example, bing_tile_at(0,0,3) will
 * return a Bing tile with coordinates (4,4)
 */
folly::Expected<uint64_t, std::string> BingTileType::latitudeLongitudeToTile(
    double latitude,
    double longitude,
    uint8_t zoomLevel) {
  auto tileX = longitudeToTileX(longitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileX.hasError())) {
    return folly::makeUnexpected(tileX.error());
  }

  auto tileY = latitudeToTileY(latitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileY.hasError())) {
    return folly::makeUnexpected(tileY.error());
  }

  return bingTileCoordsToInt(tileX.value(), tileY.value(), zoomLevel);
}

double BingTileType::tileXToLongitude(uint32_t tileX, uint8_t zoomLevel) {
  int32_t mapTileSize = 1 << zoomLevel;
  double x = (std::clamp<double>(tileX, 0, mapTileSize) / mapTileSize) - 0.5;
  return 360 * x;
}

double BingTileType::tileYToLatitude(uint32_t tileY, uint8_t zoomLevel) {
  int32_t mapTileSize = 1 << zoomLevel;
  double y = 0.5 - (std::clamp<double>(tileY, 0, mapTileSize) / mapTileSize);
  return 90 - 360 * atan(exp(-y * 2 * M_PI)) / M_PI;
}

// Given a (longitude, latitude) point, returns the surrounding Bing tiles at
// the specified zoom level
folly::Expected<std::vector<uint64_t>, std::string>
BingTileType::bingTilesAround(
    double latitude,
    double longitude,
    uint8_t zoomLevel) {
  auto tileX = longitudeToTileX(longitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileX.hasError())) {
    return folly::makeUnexpected(tileX.error());
  }
  auto tileY = latitudeToTileY(latitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileY.hasError())) {
    return folly::makeUnexpected(tileY.error());
  }
  auto mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }

  int64_t maxTileIndex = (mpSize.value() / kTilePixels) - 1;
  std::vector<uint64_t> tiles;
  tiles.reserve(9);
  for (int32_t i = -1; i <= 1; ++i) {
    for (int32_t j = -1; j <= 1; ++j) {
      int32_t x = static_cast<int32_t>(tileX.value()) + i;
      int32_t y = static_cast<int32_t>(tileY.value()) + j;
      if (x >= 0 && x <= maxTileIndex && y >= 0 && y <= maxTileIndex) {
        tiles.push_back(bingTileCoordsToInt(x, y, zoomLevel));
      }
    }
  }
  return tiles;
}

folly::Expected<std::vector<uint64_t>, std::string>
BingTileType::bingTilesAround(
    double latitude,
    double longitude,
    uint8_t zoomLevel,
    double radiusInKm) {
  if (FOLLY_UNLIKELY(radiusInKm < 0 || radiusInKm > 1000)) {
    return folly::makeUnexpected(fmt::format(
        "Radius in km must between 0 and 1000, got {}", radiusInKm));
  }
  auto tileX = longitudeToTileX(longitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileX.hasError())) {
    return folly::makeUnexpected(tileX.error());
  }
  auto tileY = latitudeToTileY(latitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileY.hasError())) {
    return folly::makeUnexpected(tileY.error());
  }
  auto mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }
  uint32_t maxTileIndex =
      static_cast<uint32_t>((mpSize.value() / kTilePixels) - 1);

  // Find top, bottom, left and right tiles from center of circle
  double topLatitude = addDistanceToLatitude(latitude, radiusInKm, 0);
  auto topTileResult =
      latitudeLongitudeToTile(topLatitude, longitude, zoomLevel);
  if (FOLLY_UNLIKELY(topTileResult.hasError())) {
    return folly::makeUnexpected(topTileResult.error());
  }
  uint64_t topTile = topTileResult.value();

  double bottomLatitude = addDistanceToLatitude(latitude, radiusInKm, 180);
  auto bottomTileResult =
      latitudeLongitudeToTile(bottomLatitude, longitude, zoomLevel);
  if (FOLLY_UNLIKELY(bottomTileResult.hasError())) {
    return folly::makeUnexpected(bottomTileResult.error());
  }
  uint64_t bottomTile = bottomTileResult.value();

  double leftLongitude =
      addDistanceToLongitude(latitude, longitude, radiusInKm, 270);
  auto leftTileResult =
      latitudeLongitudeToTile(latitude, leftLongitude, zoomLevel);
  if (FOLLY_UNLIKELY(leftTileResult.hasError())) {
    return folly::makeUnexpected(leftTileResult.error());
  }
  uint64_t leftTile = leftTileResult.value();

  double rightLongitude =
      addDistanceToLongitude(latitude, longitude, radiusInKm, 90);
  auto rightTileResult =
      latitudeLongitudeToTile(latitude, rightLongitude, zoomLevel);
  if (FOLLY_UNLIKELY(rightTileResult.hasError())) {
    return folly::makeUnexpected(rightTileResult.error());
  }
  uint64_t rightTile = rightTileResult.value();

  bool wrapAroundX =
      BingTileType::bingTileX(rightTile) < BingTileType::bingTileX(leftTile);

  uint32_t leftX = BingTileType::bingTileX(leftTile);
  uint32_t rightX = BingTileType::bingTileX(rightTile);
  uint32_t bottomY = BingTileType::bingTileY(bottomTile);
  uint32_t topY = BingTileType::bingTileY(topTile);

  uint32_t tileCountX =
      wrapAroundX ? (rightX + maxTileIndex - leftX + 2) : (rightX - leftX + 1);

  uint32_t tileCountY = bottomY - topY + 1;

  uint32_t totalTileCount = tileCountX * tileCountY;
  if (totalTileCount > 1000000) {
    return folly::makeUnexpected(fmt::format(
        "The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: {}.",
        totalTileCount));
  }

  std::vector<uint64_t> result;
  result.reserve(totalTileCount);

  for (int32_t i = 0; i < tileCountX; i++) {
    uint32_t x = (leftX + i) % (maxTileIndex + 1);
    result.push_back(bingTileCoordsToInt(x, tileY.value(), zoomLevel));
  }

  for (uint32_t y = topY; y <= bottomY; y++) {
    if (y != tileY.value()) {
      result.push_back(bingTileCoordsToInt(tileX.value(), y, zoomLevel));
    }
  }

  GreatCircleDistanceToPoint distanceToCenter(latitude, longitude);

  // Remove tiles from each corner if they are outside the radius

  // Righthand corners
  for (uint32_t x = BingTileType::bingTileX(rightTile); x != tileX.value();
       x = (x == 0) ? maxTileIndex : x - 1) {
    // Top right corner
    bool include = false;
    for (uint32_t y = BingTileType::bingTileY(topTile); y < tileY.value();
         y++) {
      uint64_t tile = bingTileCoordsToInt(x, y, zoomLevel);
      if (include) {
        result.push_back(tile);
      } else {
        double tileLatitude = tileYToLatitude(y + 1, zoomLevel);
        double tileLongitude = tileXToLongitude(x, zoomLevel);
        if (distanceToCenter.withinDistance(
                tileLatitude, tileLongitude, radiusInKm)) {
          include = true;
          result.push_back(tile);
        }
      }
    }
    // Bottom right corner
    include = false;
    for (uint32_t y = BingTileType::bingTileY(bottomTile); y > tileY.value();
         y--) {
      uint64_t tile = bingTileCoordsToInt(x, y, zoomLevel);
      if (include) {
        result.push_back(tile);
      } else {
        double tileLatitude = tileYToLatitude(y, zoomLevel);
        double tileLongitude = tileXToLongitude(x, zoomLevel);
        if (distanceToCenter.withinDistance(
                tileLatitude, tileLongitude, radiusInKm)) {
          include = true;
          result.push_back(tile);
        }
      }
    }
  }

  // Lefthand corners
  for (uint32_t x = BingTileType::bingTileX(leftTile); x != tileX.value();
       x = (x + 1) % (maxTileIndex + 1)) {
    // Top left corner
    bool include = false;
    for (uint32_t y = BingTileType::bingTileY(topTile); y < tileY.value();
         y++) {
      uint64_t tile = bingTileCoordsToInt(x, y, zoomLevel);
      if (include) {
        result.push_back(tile);
      } else {
        double tileLatitude = tileYToLatitude(y + 1, zoomLevel);
        double tileLongitude = tileXToLongitude(x + 1, zoomLevel);
        if (distanceToCenter.withinDistance(
                tileLatitude, tileLongitude, radiusInKm)) {
          include = true;
          result.push_back(tile);
        }
      }
    }
    // Bottom left corner
    include = false;
    for (uint32_t y = BingTileType::bingTileY(bottomTile); y > tileY.value();
         y--) {
      uint64_t tile = bingTileCoordsToInt(x, y, zoomLevel);
      if (include) {
        result.push_back(tile);
      } else {
        double tileLatitude = tileYToLatitude(y, zoomLevel);
        double tileLongitude = tileXToLongitude(x + 1, zoomLevel);
        if (distanceToCenter.withinDistance(
                tileLatitude, tileLongitude, radiusInKm)) {
          include = true;
          result.push_back(tile);
        }
      }
    }
  }

  return result;
}

double BingTileType::greatCircleDistance(
    double latitude1,
    double longitude1,
    double latitude2,
    double longitude2) {
  return GreatCircleDistanceToPoint(latitude1, longitude1)
      .distance(latitude2, longitude2);
}

} // namespace facebook::velox
