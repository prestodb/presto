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
#include <geos/operation/valid/IsSimpleOp.h>
#include <geos/operation/valid/IsValidOp.h>
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/BingTileType.h"

using geos::operation::valid::IsSimpleOp;
using geos::operation::valid::IsValidOp;

namespace facebook::velox::functions::geospatial {

static constexpr double kRealMinLatitude = -90;
static constexpr double kRealMaxLatitude = 90;

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

} // namespace facebook::velox::functions::geospatial
