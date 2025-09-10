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

#include "velox/functions/prestosql/geospatial/GeometryUtils.h"
#include <geos/geom/prep/PreparedGeometryFactory.h>
#include <geos/operation/valid/IsSimpleOp.h>
#include <geos/operation/valid/IsValidOp.h>
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/BingTileType.h"

using geos::operation::valid::IsSimpleOp;
using geos::operation::valid::IsValidOp;

namespace facebook::velox::functions::geospatial {

static constexpr double kRealMinLatitude = -90;
static constexpr double kRealMaxLatitude = 90;
static constexpr int32_t kMaxCoveringCount = 1'000'000;

GeometryCollectionIterator::GeometryCollectionIterator(
    const geos::geom::Geometry* geometry) {
  if (!geometry) {
    VELOX_USER_FAIL("geometry is null");
  }
  geometriesDeque.push_back(geometry);
}
// Returns true if there is a next geometry to iterate over
bool GeometryCollectionIterator::hasNext() {
  while (!geometriesDeque.empty()) {
    const geos::geom::Geometry* top = geometriesDeque.back();
    // Check if top is a GeometryCollection
    if (top->getGeometryTypeId() == geos::geom::GEOS_GEOMETRYCOLLECTION) {
      geometriesDeque.pop_back();
      const geos::geom::GeometryCollection* collection =
          dynamic_cast<const geos::geom::GeometryCollection*>(top);
      if (collection) {
        // Push children in reverse order so that the first child is on top of
        // the stack
        for (int i = static_cast<int>(collection->getNumGeometries()) - 1;
             i >= 0;
             --i) {
          geometriesDeque.push_back(collection->getGeometryN(i));
        }
        continue; // Check again with new top
      } else {
        VELOX_FAIL("Failed to cast to GeometryCollection");
      }
    } else {
      // Top is not a collection, so we have a next geometry
      return true;
    }
  }
  return false;
}

const geos::geom::Geometry* GeometryCollectionIterator::next() {
  if (!hasNext()) {
    throw std::out_of_range("No more geometries");
  }
  const geos::geom::Geometry* nextGeometry = geometriesDeque.back(); // NOLINT
  geometriesDeque.pop_back();
  return nextGeometry;
}

std::vector<const geos::geom::Geometry*> flattenCollection(
    const geos::geom::Geometry* geometry) {
  std::vector<const geos::geom::Geometry*> result;
  GeometryCollectionIterator it(geometry);
  while (it.hasNext()) {
    result.push_back(it.next());
  }
  return result;
}

geos::geom::GeometryFactory* getGeometryFactory() {
  thread_local static geos::geom::GeometryFactory::Ptr geometryFactory =
      geos::geom::GeometryFactory::create();
  return geometryFactory.get();
}

std::optional<std::string> geometryInvalidReason(
    const geos::geom::Geometry* geometry) {
  if (geometry == nullptr) {
    // Null geometries are a problem, but they not invalid or non-simple.
    return std::nullopt;
  }

  IsValidOp isValidOp(geometry);
  const geos::operation::valid::TopologyValidationError*
      topologyValidationError = isValidOp.getValidationError();
  if (topologyValidationError != nullptr) {
    return fmt::format(
        "Invalid {}: {}",
        geometry->getGeometryType(),
        topologyValidationError->getMessage());
  }

  IsSimpleOp isSimpleOp(geometry);
  if (isSimpleOp.isSimple()) {
    return std::nullopt;
  }

  std::string_view description;
  switch (geometry->getGeometryTypeId()) {
    case geos::geom::GeometryTypeId::GEOS_POINT:
      description = "Invalid point";
      break;
    case geos::geom::GeometryTypeId::GEOS_MULTIPOINT:
      description = "Repeated point";
      break;
    case geos::geom::GeometryTypeId::GEOS_LINESTRING:
    case geos::geom::GeometryTypeId::GEOS_LINEARRING:
    case geos::geom::GeometryTypeId::GEOS_MULTILINESTRING:
      description = "Self-intersection at or near";
      break;
    case geos::geom::GeometryTypeId::GEOS_POLYGON:
    case geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON:
    case geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION:
      // In OGC (which GEOS follows): Polygons, MultiPolygons, Geometry
      // Collections are simple.
      // This shouldn't happen, but in case it does, return a reasonable
      // generic message.
      description = "Topology exception at or near";
      break;
    default:
      return fmt::format(
          "Unknown Geometry type: {}", geometry->getGeometryType());
  }
  geos::geom::Coordinate nonSimpleLocation = isSimpleOp.getNonSimpleLocation();
  return fmt::format(
      "Non-simple {}: {} ({} {})",
      geometry->getGeometryType(),
      description,
      nonSimpleLocation.x,
      nonSimpleLocation.y);
}

Status validateLatitudeLongitude(double latitude, double longitude) {
  if (FOLLY_UNLIKELY(
          latitude < kRealMinLatitude || latitude > kRealMaxLatitude ||
          longitude < BingTileType::kMinLongitude ||
          longitude > BingTileType::kMaxLongitude || std::isnan(latitude) ||
          std::isnan(longitude))) {
    return Status::UserError(fmt::format(
        "Latitude must be in range [{}, {}] and longitude must be in range [{}, {}]. Got latitude: {} and longitude: {}",
        kRealMinLatitude,
        kRealMaxLatitude,
        BingTileType::kMinLongitude,
        BingTileType::kMaxLongitude,
        latitude,
        longitude));
  }
  return Status::OK();
}

namespace {

FOLLY_ALWAYS_INLINE void checkLatitudeLongitudeBounds(
    double latitude,
    double longitude) {
  if (FOLLY_UNLIKELY(
          latitude > BingTileType::kMaxLatitude ||
          latitude < BingTileType::kMinLatitude)) {
    VELOX_USER_FAIL(fmt::format(
        "Latitude span for the geometry must be in [{:.2f}, {:.2f}] range",
        BingTileType::kMinLatitude,
        BingTileType::kMaxLatitude));
  }
  if (FOLLY_UNLIKELY(
          longitude > BingTileType::kMaxLongitude ||
          longitude < BingTileType::kMinLongitude)) {
    VELOX_USER_FAIL(fmt::format(
        "Longitude span for the geometry must be in [{:.2f}, {:.2f}] range",
        BingTileType::kMinLongitude,
        BingTileType::kMaxLongitude));
  }
}

std::optional<std::vector<int64_t>> handleTrivialMinimalTileCoveringCases(
    const geos::geom::Envelope& envelope,
    int32_t zoom) {
  if (envelope.isNull()) {
    return std::vector<int64_t>{};
  }

  checkLatitudeLongitudeBounds(envelope.getMinY(), envelope.getMinX());
  checkLatitudeLongitudeBounds(envelope.getMaxY(), envelope.getMaxX());

  if (FOLLY_UNLIKELY(zoom == 0)) {
    return std::vector<int64_t>{
        static_cast<int64_t>(BingTileType::bingTileCoordsToInt(0, 0, 0))};
  }

  if (FOLLY_UNLIKELY(
          envelope.getMaxX() == envelope.getMinX() &&
          envelope.getMaxY() == envelope.getMinY())) {
    auto res = BingTileType::latitudeLongitudeToTile(
        envelope.getMaxY(), envelope.getMaxX(), zoom);
    if (res.hasError()) {
      VELOX_FAIL(res.error());
    }
    return std::vector<int64_t>{static_cast<int64_t>(res.value())};
  }
  return std::nullopt;
}

std::unique_ptr<geos::geom::Envelope> tileToEnvelope(int64_t tile) {
  uint8_t zoom = BingTileType::bingTileZoom(tile);
  uint32_t x = BingTileType::bingTileX(tile);
  uint32_t y = BingTileType::bingTileY(tile);

  double minX = BingTileType::tileXToLongitude(x, zoom);
  double maxX = BingTileType::tileXToLongitude(x + 1, zoom);
  double minY = BingTileType::tileYToLatitude(y, zoom);
  double maxY = BingTileType::tileYToLatitude(y + 1, zoom);

  return std::make_unique<geos::geom::Envelope>(minX, maxX, minY, maxY);
}

struct TilingEntry {
  TilingEntry(int64_t tile, geos::geom::GeometryFactory* factory)
      : tile{tile},
        envelope{tileToEnvelope(tile)},
        geometry{factory->toGeometry(envelope.get())} {}

  int64_t tile;
  std::unique_ptr<geos::geom::Envelope> envelope;
  std::unique_ptr<geos::geom::Geometry> geometry;
};

bool satisfiesTileEdgeCondition(
    const geos::geom::Envelope& query,
    const TilingEntry& tilingEntry) {
  int64_t tile = tilingEntry.tile;

  int64_t maxXy = (1 << BingTileType::bingTileZoom(tile)) - 1;
  if (BingTileType::bingTileY(tile) < maxXy &&
      query.getMaxY() == tilingEntry.envelope->getMinY()) {
    return false;
  }
  if (BingTileType::bingTileX(tile) < maxXy &&
      query.getMinX() == tilingEntry.envelope->getMaxX()) {
    return false;
  }
  return true;
}

std::vector<int64_t> getRawTilesCoveringGeometry(
    const geos::geom::Geometry& geometry,
    int32_t maxZoom) {
  const geos::geom::Envelope* envelope = geometry.getEnvelopeInternal();
  geos::geom::GeometryFactory::Ptr factory =
      geos::geom::GeometryFactory::create();

  std::optional<std::vector<int64_t>> trivialCases =
      handleTrivialMinimalTileCoveringCases(*envelope, maxZoom);
  if (trivialCases.has_value()) {
    return trivialCases.value();
  }

  auto preparedGeometry =
      geos::geom::prep::PreparedGeometryFactory::prepare(&geometry);
  std::stack<TilingEntry> stack;

  auto addIntersecting = [&](std::int64_t tile) {
    auto tilingEntry = TilingEntry(tile, factory.get());
    if (satisfiesTileEdgeCondition(*envelope, tilingEntry) &&
        preparedGeometry->intersects(tilingEntry.geometry.get())) {
      stack.push(std::move(tilingEntry));
    }
  };

  std::vector<uint64_t> baseTiles = {
      BingTileType::bingTileCoordsToInt(0, 0, 1),
      BingTileType::bingTileCoordsToInt(0, 1, 1),
      BingTileType::bingTileCoordsToInt(1, 0, 1),
      BingTileType::bingTileCoordsToInt(1, 1, 1)};
  std::for_each(baseTiles.begin(), baseTiles.end(), addIntersecting);

  std::vector<int64_t> outputTiles;

  while (!stack.empty()) {
    TilingEntry entry = std::move(stack.top());
    stack.pop();
    if (BingTileType::bingTileZoom(entry.tile) == maxZoom ||
        preparedGeometry->contains(entry.geometry.get())) {
      outputTiles.push_back(entry.tile);
    } else {
      auto children = BingTileType::bingTileChildren(
          entry.tile, BingTileType::bingTileZoom(entry.tile) + 1, 1);
      if (FOLLY_UNLIKELY(children.hasError())) {
        VELOX_FAIL(children.error());
      }
      std::for_each(
          children.value().begin(), children.value().end(), addIntersecting);
      VELOX_CHECK(
          outputTiles.size() + stack.size() <= kMaxCoveringCount,
          "The zoom level is too high or the geometry is too large to compute a set of covering Bing tiles. Please use a lower zoom level, or tile only a section of the geometry.");
    }
  }
  return outputTiles;
}
} // namespace

std::vector<int64_t> getMinimalTilesCoveringGeometry(
    const geos::geom::Geometry& geometry,
    int32_t zoom,
    uint8_t maxZoomShift) {
  std::vector<int64_t> outputTiles;

  std::stack<int64_t, std::vector<int64_t>> stack(
      getRawTilesCoveringGeometry(geometry, zoom));

  while (!stack.empty()) {
    int64_t thisTile = stack.top();
    stack.pop();
    auto expectedChildren =
        BingTileType::bingTileChildren(thisTile, zoom, maxZoomShift);
    if (FOLLY_UNLIKELY(expectedChildren.hasError())) {
      VELOX_FAIL(expectedChildren.error());
    }
    outputTiles.insert(
        outputTiles.end(),
        expectedChildren.value().begin(),
        expectedChildren.value().end());
    if (FOLLY_UNLIKELY(outputTiles.size() + stack.size() > kMaxCoveringCount)) {
      VELOX_USER_FAIL(
          "The zoom level is too high or the geometry is too large to compute a set of covering Bing tiles. Please use a lower zoom level, or tile only a section of the geometry.");
    }
  }

  return outputTiles;
}

std::vector<int64_t> getMinimalTilesCoveringGeometry(
    const geos::geom::Envelope& envelope,
    int32_t zoom) {
  auto trivialCases = handleTrivialMinimalTileCoveringCases(envelope, zoom);
  if (trivialCases.has_value()) {
    return trivialCases.value();
  }

  // envelope x,y (longitude,latitude) goes NE as they increase.
  // tile x,y goes SE as they increase
  auto seRes = BingTileType::latitudeLongitudeToTile(
      envelope.getMinY(), envelope.getMaxX(), zoom);
  if (FOLLY_UNLIKELY(seRes.hasError())) {
    VELOX_FAIL(seRes.error());
  }

  auto nwRes = BingTileType::latitudeLongitudeToTile(
      envelope.getMaxY(), envelope.getMinX(), zoom);
  if (FOLLY_UNLIKELY(nwRes.hasError())) {
    VELOX_FAIL(nwRes.error());
  }

  uint64_t seTile = seRes.value();
  uint64_t nwTile = nwRes.value();

  uint32_t minY = BingTileType::bingTileY(nwTile);
  uint32_t minX = BingTileType::bingTileX(nwTile);
  uint32_t maxY = BingTileType::bingTileY(seTile);
  uint32_t maxX = BingTileType::bingTileX(seTile);

  uint32_t numTiles = (maxX - minX + 1) * (maxY - minY + 1);
  if (numTiles > kMaxCoveringCount) {
    VELOX_USER_FAIL(
        "The zoom level is too high or the geometry is too large to compute a set of covering Bing tiles. Please use a lower zoom level, or tile only a section of the geometry.");
  }

  std::vector<int64_t> results;
  results.reserve((maxX - minX + 1) * (maxY - minY + 1));

  for (uint32_t y = minY; y <= maxY; ++y) {
    for (uint32_t x = minX; x <= maxX; ++x) {
      results.push_back(
          static_cast<int64_t>(BingTileType::bingTileCoordsToInt(x, y, zoom)));
    }
  }
  return results;
}

bool isPointOrRectangle(const geos::geom::Geometry& geometry) {
  if (geometry.getGeometryTypeId() == geos::geom::GeometryTypeId::GEOS_POINT) {
    return true;
  }
  if (geometry.getGeometryTypeId() !=
      geos::geom::GeometryTypeId::GEOS_POLYGON) {
    return false;
  }
  const geos::geom::Polygon& polygon =
      static_cast<const geos::geom::Polygon&>(geometry);

  if (polygon.getNumPoints() != 5) {
    return false;
  }

  auto envelope = geometry.getEnvelopeInternal();

  std::vector<std::tuple<int32_t, int32_t>> coords;
  coords.emplace_back(envelope->getMinX(), envelope->getMinY());
  coords.emplace_back(envelope->getMinX(), envelope->getMaxY());
  coords.emplace_back(envelope->getMaxX(), envelope->getMinY());
  coords.emplace_back(envelope->getMaxX(), envelope->getMaxY());

  for (int i = 0; i < 4; i++) {
    const geos::geom::Point* point =
        static_cast<const geos::geom::Point*>(polygon.getGeometryN(i));
    auto query = std::tuple<int32_t, int32_t>(point->getX(), point->getY());
    if (std::find(coords.begin(), coords.end(), query) == coords.end()) {
      return false;
    }
  }
  return true;
}

} // namespace facebook::velox::functions::geospatial
