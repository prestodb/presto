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

#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/LineString.h>
#include <geos/geom/Point.h>

#include "velox/common/base/IOUtils.h"
#include "velox/functions/prestosql/geospatial/GeometrySerde.h"

using facebook::velox::common::InputByteStream;

namespace facebook::velox::functions::geospatial {
std::unique_ptr<geos::geom::Geometry> GeometryDeserializer::deserialize(
    velox::common::InputByteStream& stream,
    size_t size) {
  auto geometryType = static_cast<GeometrySerializationType>(
      stream.read<GeometrySerializationType>());
  switch (geometryType) {
    case GeometrySerializationType::POINT:
      return readPoint(stream);
    case GeometrySerializationType::MULTI_POINT:
      return readMultiPoint(stream);
    case GeometrySerializationType::LINE_STRING:
      return readPolyline(stream, false);
    case GeometrySerializationType::MULTI_LINE_STRING:
      return readPolyline(stream, true);
    case GeometrySerializationType::POLYGON:
      return readPolygon(stream, false);
    case GeometrySerializationType::MULTI_POLYGON:
      return readPolygon(stream, true);
    case GeometrySerializationType::ENVELOPE:
      return readEnvelope(stream);
    case GeometrySerializationType::GEOMETRY_COLLECTION:
      return readGeometryCollection(stream, size);
    default:
      VELOX_FAIL(
          "Unrecognized geometry type: {}", static_cast<uint8_t>(geometryType));
  }
}

const std::unique_ptr<geos::geom::Envelope>
GeometryDeserializer::deserializeEnvelope(const StringView& geometry) {
  velox::common::InputByteStream inputStream(geometry.data());
  auto geometryType = inputStream.read<GeometrySerializationType>();

  switch (geometryType) {
    case GeometrySerializationType::POINT:
      return std::make_unique<geos::geom::Envelope>(
          *readPoint(inputStream)->getEnvelopeInternal());
    case GeometrySerializationType::MULTI_POINT:
    case GeometrySerializationType::LINE_STRING:
    case GeometrySerializationType::MULTI_LINE_STRING:
    case GeometrySerializationType::POLYGON:
    case GeometrySerializationType::MULTI_POLYGON:
      skipEsriType(inputStream);
      return deserializeEnvelope(inputStream);
    case GeometrySerializationType::ENVELOPE:
      return deserializeEnvelope(inputStream);
    case GeometrySerializationType::GEOMETRY_COLLECTION:
      return std::make_unique<geos::geom::Envelope>(
          *readGeometryCollection(inputStream, geometry.size())
               ->getEnvelopeInternal());
    default:
      VELOX_FAIL(
          "Unrecognized geometry type: {}", static_cast<uint8_t>(geometryType));
  }
}

std::unique_ptr<geos::geom::Envelope> GeometryDeserializer::deserializeEnvelope(
    velox::common::InputByteStream& input) {
  auto xMin = input.read<double>();
  auto yMin = input.read<double>();
  auto xMax = input.read<double>();
  auto yMax = input.read<double>();

  if (FOLLY_UNLIKELY(
          isEsriNaN(xMin) || isEsriNaN(yMin) || isEsriNaN(xMax) ||
          isEsriNaN(yMax))) {
    return std::make_unique<geos::geom::Envelope>();
  }

  return std::make_unique<geos::geom::Envelope>(xMin, xMax, yMin, yMax);
}

geos::geom::Coordinate GeometryDeserializer::readCoordinate(
    velox::common::InputByteStream& input) {
  auto x = input.read<double>();
  auto y = input.read<double>();
  return {x, y};
}

std::unique_ptr<geos::geom::CoordinateSequence>
GeometryDeserializer::readCoordinates(
    velox::common::InputByteStream& input,
    size_t count) {
  auto coords = std::make_unique<geos::geom::CoordinateArraySequence>(count, 2);
  for (size_t i = 0; i < count; ++i) {
    // TODO: Consider using setOrdinate if there's a performance issue.
    coords->setAt(readCoordinate(input), i);
  }
  return coords;
}

std::unique_ptr<geos::geom::Point> GeometryDeserializer::readPoint(
    velox::common::InputByteStream& input) {
  geos::geom::Coordinate coordinate = readCoordinate(input);
  if (std::isnan(coordinate.x) || std::isnan(coordinate.y)) {
    return getGeometryFactory()->createPoint();
  }
  return std::unique_ptr<geos::geom::Point>(
      getGeometryFactory()->createPoint(coordinate));
}

std::unique_ptr<geos::geom::Geometry> GeometryDeserializer::readMultiPoint(
    velox::common::InputByteStream& input) {
  skipEsriType(input);
  skipEnvelope(input);
  size_t pointCount = input.read<int32_t>();
  auto coords = readCoordinates(input, pointCount);
  std::vector<std::unique_ptr<geos::geom::Point>> points;
  points.reserve(coords->size());
  for (size_t i = 0; i < coords->size(); ++i) {
    points.push_back(
        std::unique_ptr<geos::geom::Point>(getGeometryFactory()->createPoint(
            geos::geom::Coordinate(coords->getX(i), coords->getY(i)))));
  }
  return getGeometryFactory()->createMultiPoint(std::move(points));
}

std::unique_ptr<geos::geom::Geometry> GeometryDeserializer::readPolyline(
    velox::common::InputByteStream& input,
    bool multiType) {
  skipEsriType(input);
  skipEnvelope(input);
  size_t partCount = input.read<int32_t>();

  if (partCount == 0) {
    if (multiType) {
      return getGeometryFactory()->createMultiLineString();
    }
    return getGeometryFactory()->createLineString();
  }

  size_t pointCount = input.read<int32_t>();
  std::vector<size_t> startIndexes(partCount);

  for (size_t i = 0; i < partCount; ++i) {
    startIndexes[i] = input.read<int32_t>();
  }

  std::vector<size_t> partLengths(partCount);
  if (partCount > 1) {
    partLengths[0] = startIndexes[1];
    for (size_t i = 1; i < partCount - 1; ++i) {
      partLengths[i] = startIndexes[i + 1] - startIndexes[i];
    }
  }
  partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

  std::vector<std::unique_ptr<geos::geom::LineString>> lineStrings;
  lineStrings.reserve(partCount);
  for (size_t i = 0; i < partCount; ++i) {
    lineStrings.push_back(getGeometryFactory()->createLineString(
        readCoordinates(input, partLengths[i])));
  }

  if (multiType) {
    return getGeometryFactory()->createMultiLineString(std::move(lineStrings));
  }

  if (lineStrings.size() != 1) {
    VELOX_FAIL("Expected a single LineString for non-multiType polyline.");
  }

  return std::move(lineStrings[0]);
}

std::unique_ptr<geos::geom::Geometry> GeometryDeserializer::readPolygon(
    velox::common::InputByteStream& input,
    bool multiType) {
  skipEsriType(input);
  skipEnvelope(input);

  size_t partCount = input.read<int32_t>();
  if (partCount == 0) {
    if (multiType) {
      return getGeometryFactory()->createMultiPolygon();
    }
    return getGeometryFactory()->createPolygon();
  }

  size_t pointCount = input.read<int32_t>();
  std::vector<size_t> startIndexes(partCount);
  for (size_t i = 0; i < partCount; i++) {
    startIndexes[i] = input.read<int32_t>();
  }

  std::vector<size_t> partLengths(partCount);
  if (partCount > 1) {
    partLengths[0] = startIndexes[1];
    for (size_t i = 1; i < partCount - 1; i++) {
      partLengths[i] = startIndexes[i + 1] - startIndexes[i];
    }
  }
  partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

  std::unique_ptr<geos::geom::LinearRing> shell = nullptr;
  std::vector<std::unique_ptr<geos::geom::LinearRing>> holes;
  std::vector<std::unique_ptr<geos::geom::Polygon>> polygons;

  for (size_t i = 0; i < partCount; i++) {
    auto coordinates = readCoordinates(input, partLengths[i]);

    if (isClockwise(coordinates, 0, coordinates->size())) {
      // next polygon has started
      if (shell) {
        polygons.push_back(getGeometryFactory()->createPolygon(
            std::move(shell), std::move(holes)));
        holes.clear();
      }
      shell = getGeometryFactory()->createLinearRing(std::move(coordinates));
    } else {
      holes.push_back(
          getGeometryFactory()->createLinearRing(std::move(coordinates)));
    }
  }
  polygons.push_back(
      getGeometryFactory()->createPolygon(std::move(shell), std::move(holes)));

  if (multiType) {
    return getGeometryFactory()->createMultiPolygon(std::move(polygons));
  }

  if (polygons.size() != 1) {
    VELOX_FAIL("Expected exactly one polygon, but found multiple.");
  }
  return std::move(polygons[0]);
}

std::unique_ptr<geos::geom::Geometry> GeometryDeserializer::readEnvelope(
    velox::common::InputByteStream& input) {
  auto xMin = input.read<double>();
  auto yMin = input.read<double>();
  auto xMax = input.read<double>();
  auto yMax = input.read<double>();

  if (isEsriNaN(xMin) || isEsriNaN(yMin) || isEsriNaN(xMax) ||
      isEsriNaN(yMax)) {
    return getGeometryFactory()->createPolygon();
  }

  auto coordinates = std::make_unique<geos::geom::CoordinateArraySequence>();
  coordinates->add(geos::geom::Coordinate(xMin, yMin));
  coordinates->add(geos::geom::Coordinate(xMin, yMax));
  coordinates->add(geos::geom::Coordinate(xMax, yMax));
  coordinates->add(geos::geom::Coordinate(xMax, yMin));
  coordinates->add(geos::geom::Coordinate(xMin, yMin)); // Close the ring

  auto shell = getGeometryFactory()->createLinearRing(std::move(coordinates));
  return getGeometryFactory()->createPolygon(std::move(shell), {});
}

std::unique_ptr<geos::geom::Geometry>
GeometryDeserializer::readGeometryCollection(
    velox::common::InputByteStream& input,
    size_t size) {
  std::vector<std::unique_ptr<geos::geom::Geometry>> geometries;

  auto offset = input.offset();
  while (size - offset > 0) {
    // Skip the length field
    input.read<int32_t>();
    geometries.push_back(deserialize(input, size));
    offset = input.offset();
  }
  std::vector<const geos::geom::Geometry*> rawGeometries;
  rawGeometries.reserve(geometries.size());
  for (const auto& geometry : geometries) {
    rawGeometries.push_back(geometry.get());
  }

  return std::unique_ptr<geos::geom::GeometryCollection>(
      getGeometryFactory()->createGeometryCollection(rawGeometries));
}

} // namespace facebook::velox::functions::geospatial
