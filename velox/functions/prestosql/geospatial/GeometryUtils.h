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

#include <geos/geom/Geometry.h>
#include <geos/io/WKTReader.h>

#include <geos/util/AssertionFailedException.h>
#include <geos/util/UnsupportedOperationException.h>
#include <optional>

#include "velox/common/base/Status.h"

namespace facebook::velox::functions::geospatial {

/// Utility macro used to wrap GEOS library calls in a try-catch block,
/// returning a velox::Status with error message if an exception is caught.
#define GEOS_TRY(func, user_error_message)                       \
  try {                                                          \
    func                                                         \
  } catch (const geos::util::UnsupportedOperationException& e) { \
    return Status::UnknownError(                                 \
        fmt::format("Internal geometry error: {}", e.what()));   \
  } catch (const geos::util::AssertionFailedException& e) {      \
    return Status::UnknownError(                                 \
        fmt::format("Internal geometry error: {}", e.what()));   \
  } catch (const geos::util::GEOSException& e) {                 \
    return Status::UserError(                                    \
        fmt::format("{}: {}", user_error_message, e.what()));    \
  }

/// Utility macro used to wrap GEOS library calls in a try-catch block,
/// throwing a velox::Status with error message if an exception is caught.
#define GEOS_RETHROW(func, user_error_message)                             \
  try {                                                                    \
    func                                                                   \
  } catch (const geos::util::UnsupportedOperationException& e) {           \
    VELOX_USER_FAIL(fmt::format("Internal geometry error: {}", e.what())); \
  } catch (const geos::util::AssertionFailedException& e) {                \
    VELOX_FAIL(fmt::format("Internal geometry error: {}", e.what()));      \
  } catch (const geos::util::GEOSException& e) {                           \
    VELOX_FAIL(fmt::format("{}: {}", user_error_message, e.what()));       \
  }

class GeometryCollectionIterator {
 public:
  explicit GeometryCollectionIterator(const geos::geom::Geometry* geometry);
  bool hasNext();
  const geos::geom::Geometry* next();

 private:
  std::deque<const geos::geom::Geometry*> geometriesDeque;
};

geos::geom::GeometryFactory* getGeometryFactory();

FOLLY_ALWAYS_INLINE const
    std::unordered_map<geos::geom::GeometryTypeId, std::string>&
    getGeosTypeToStringIdentifier() {
  static const geos::geom::GeometryFactory::Ptr factory =
      geos::geom::GeometryFactory::create();

  static const std::unordered_map<geos::geom::GeometryTypeId, std::string>
      geosTypeToStringIdentifier{
          {geos::geom::GeometryTypeId::GEOS_POINT,
           factory->createPoint()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_LINESTRING,
           factory->createLineString()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_LINEARRING,
           factory->createLinearRing()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_POLYGON,
           factory->createPolygon()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_MULTIPOINT,
           factory->createMultiPoint()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_MULTILINESTRING,
           factory->createMultiLineString()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON,
           factory->createMultiPolygon()->getGeometryType()},
          {geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION,
           factory->createGeometryCollection()->getGeometryType()}};
  return geosTypeToStringIdentifier;
};

FOLLY_ALWAYS_INLINE std::vector<std::string> getGeosTypeNames(
    const std::vector<geos::geom::GeometryTypeId>& geometryTypeIds) {
  std::vector<std::string> geometryTypeNames;
  geometryTypeNames.reserve(geometryTypeIds.size());
  for (auto geometryTypeId : geometryTypeIds) {
    geometryTypeNames.push_back(
        getGeosTypeToStringIdentifier().at(geometryTypeId));
  }
  return geometryTypeNames;
}

FOLLY_ALWAYS_INLINE Status validateType(
    const geos::geom::Geometry& geometry,
    const std::vector<geos::geom::GeometryTypeId>& validTypes,
    std::string callerFunctionName) {
  geos::geom::GeometryTypeId type = geometry.getGeometryTypeId();
  if (!std::count(validTypes.begin(), validTypes.end(), type)) {
    return Status::UserError(fmt::format(
        "{} only applies to {}. Input type is: {}",
        callerFunctionName,
        fmt::join(getGeosTypeNames(validTypes), " or "),
        getGeosTypeToStringIdentifier().at(type)));
  }
  return Status::OK();
}

FOLLY_ALWAYS_INLINE bool isMultiType(const geos::geom::Geometry& geometry) {
  geos::geom::GeometryTypeId type = geometry.getGeometryTypeId();

  static const std::vector<geos::geom::GeometryTypeId> multiTypes{
      geos::geom::GeometryTypeId::GEOS_MULTILINESTRING,
      geos::geom::GeometryTypeId::GEOS_MULTIPOINT,
      geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON,
      geos::geom::GeometryTypeId::GEOS_GEOMETRYCOLLECTION};

  return std::count(multiTypes.begin(), multiTypes.end(), type);
}

FOLLY_ALWAYS_INLINE bool isAtomicType(const geos::geom::Geometry& geometry) {
  geos::geom::GeometryTypeId type = geometry.getGeometryTypeId();

  static const std::vector<geos::geom::GeometryTypeId> atomicTypes{
      geos::geom::GeometryTypeId::GEOS_LINESTRING,
      geos::geom::GeometryTypeId::GEOS_POLYGON,
      geos::geom::GeometryTypeId::GEOS_POINT};

  return std::count(atomicTypes.begin(), atomicTypes.end(), type);
}

std::optional<std::string> geometryInvalidReason(
    const geos::geom::Geometry* geometry);

/// Determines if a ring of coordinates (from `start` to `end`) is oriented
/// clockwise.
FOLLY_ALWAYS_INLINE bool isClockwise(
    const std::unique_ptr<geos::geom::CoordinateSequence>& coordinates,
    size_t start,
    size_t end) {
  double sum = 0.0;
  for (size_t i = start; i < end - 1; i++) {
    const auto& p1 = coordinates->getAt(i);
    const auto& p2 = coordinates->getAt(i + 1);
    sum += (p2.x - p1.x) * (p2.y + p1.y);
  }
  return sum > 0.0;
}

/// Reverses the order of coordinates in the sequence between `start` and `end`
FOLLY_ALWAYS_INLINE void reverse(
    const std::unique_ptr<geos::geom::CoordinateSequence>& coordinates,
    size_t start,
    size_t end) {
  for (size_t i = 0; i < (end - start) / 2; ++i) {
    auto temp = coordinates->getAt(start + i);
    coordinates->setAt(coordinates->getAt(end - 1 - i), start + i);
    coordinates->setAt(temp, end - 1 - i);
  }
}

/// Ensures that a polygon ring has the canonical orientation:
/// - Exterior rings (shells) must be clockwise.
/// - Interior rings (holes) must be counter-clockwise.
FOLLY_ALWAYS_INLINE void canonicalizePolygonCoordinates(
    const std::unique_ptr<geos::geom::CoordinateSequence>& coordinates,
    size_t start,
    size_t end,
    bool isShell) {
  bool isClockwiseFlag = isClockwise(coordinates, start, end);

  if ((isShell && !isClockwiseFlag) || (!isShell && isClockwiseFlag)) {
    reverse(coordinates, start, end);
  }
}

/// Applies `canonicalizePolygonCoordinates` to all rings in a polygon.
FOLLY_ALWAYS_INLINE void canonicalizePolygonCoordinates(
    const std::unique_ptr<geos::geom::CoordinateSequence>& coordinates,
    const std::vector<size_t>& partIndexes,
    const std::vector<bool>& shellPart) {
  for (size_t part = 0; part < partIndexes.size() - 1; part++) {
    canonicalizePolygonCoordinates(
        coordinates, partIndexes[part], partIndexes[part + 1], shellPart[part]);
  }
  if (!partIndexes.empty()) {
    canonicalizePolygonCoordinates(
        coordinates, partIndexes.back(), coordinates->size(), shellPart.back());
  }
}

Status validateLatitudeLongitude(double latitude, double longitude);

std::vector<const geos::geom::Geometry*> flattenCollection(
    const geos::geom::Geometry* geometry);

std::vector<int64_t> getMinimalTilesCoveringGeometry(
    const geos::geom::Envelope& envelope,
    int32_t zoom);

std::vector<int64_t> getMinimalTilesCoveringGeometry(
    const geos::geom::Geometry& geometry,
    int32_t zoom,
    uint8_t maxZoomShift);

std::vector<int64_t> getDissolvedTilesCoveringGeometry(
    const geos::geom::Geometry& geometry,
    int32_t zoom);

bool isPointOrRectangle(const geos::geom::Geometry& geometry);

} // namespace facebook::velox::functions::geospatial
