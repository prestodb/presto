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

std::optional<std::string> geometryInvalidReason(
    const geos::geom::Geometry* geometry);

} // namespace facebook::velox::functions::geospatial
