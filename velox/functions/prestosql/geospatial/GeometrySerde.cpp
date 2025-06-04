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

#include "velox/functions/prestosql/geospatial/GeometrySerde.h"

#include <geos/io/WKBReader.h>
#include <geos/io/WKBWriter.h>

namespace facebook::velox::functions::geospatial {

std::unique_ptr<geos::geom::Geometry> deserializeGeometry(
    const StringView& geometryString) {
  std::istringstream inputString(geometryString);
  geos::io::WKBReader reader;
  std::unique_ptr<geos::geom::Geometry> geosGeometry = reader.read(inputString);
  VELOX_DCHECK_NOT_NULL(geosGeometry);
  return geosGeometry;
}

std::string serializeGeometry(const geos::geom::Geometry& geosGeometry) {
  std::ostringstream outputStream;
  geos::io::WKBWriter writer;
  writer.write(geosGeometry, outputStream);
  const std::string outputString = outputStream.str();
  return outputString;
}

const std::unique_ptr<geos::geom::Envelope> getEnvelopeFromGeometry(
    const StringView& geometry) {
  std::unique_ptr<geos::geom::Geometry> geosGeometry =
      geospatial::deserializeGeometry(geometry);
  return std::make_unique<geos::geom::Envelope>(
      *geosGeometry->getEnvelopeInternal());
}

} // namespace facebook::velox::functions::geospatial
