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

using geos::operation::valid::IsSimpleOp;
using geos::operation::valid::IsValidOp;

namespace facebook::velox::functions::geospatial {

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

} // namespace facebook::velox::functions::geospatial
