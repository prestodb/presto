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
#include <geos/geom/Geometry.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <gtest/gtest.h>
#include "velox/type/StringView.h"

using namespace ::testing;

using namespace facebook::velox::functions::geospatial;

void assertRoundtrip(const std::string& wkt) {
  geos::io::WKTReader reader;
  geos::io::WKTWriter writer;
  std::unique_ptr<geos::geom::Geometry> geometry = reader.read(wkt);

  std::string buffer;
  GeometrySerializer::serialize(*geometry, buffer);
  facebook::velox::StringView readBuffer(buffer);
  auto deserialized = GeometryDeserializer::deserialize(readBuffer);

  EXPECT_TRUE(geometry->equals(deserialized.get()))
      << std::endl
      << "Input:" << std::endl
      << wkt << std::endl
      << "Output:" << std::endl
      << writer.write(deserialized.get());
}

TEST(GeometrySerdeTest, testBasicSerde) {
  assertRoundtrip("POINT EMPTY");
  assertRoundtrip("POINT (1 2)");
  assertRoundtrip("MULTIPOINT EMPTY");
  assertRoundtrip("MULTIPOINT (1 2)");
  assertRoundtrip("MULTIPOINT (1 2, 1 0)");

  assertRoundtrip("LINESTRING EMPTY");
  assertRoundtrip("LINESTRING (1 2, 1 0)");
  assertRoundtrip("LINESTRING (1 0, 2 0, 2 1, 1 1, 1 0)");
  assertRoundtrip("MULTILINESTRING EMPTY");
  assertRoundtrip("MULTILINESTRING ((1 2, 1 0))");
  assertRoundtrip("MULTILINESTRING ((1 2, 1 0), (10 11, 12 13))");

  assertRoundtrip("POLYGON EMPTY");
  assertRoundtrip("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))");
  assertRoundtrip(
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
  assertRoundtrip("MULTIPOLYGON EMPTY");
  assertRoundtrip("MULTIPOLYGON (((1 0, 2 0, 2 1, 1 1, 1 0)))");
  assertRoundtrip(
      "MULTIPOLYGON ( ((10 0, 20 0, 20 10, 10 10, 10 0)),  ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)) )");
}

TEST(GeometrySerdeTest, testGeometryCollectionSerde) {
  assertRoundtrip("GEOMETRYCOLLECTION EMPTY");
  assertRoundtrip("GEOMETRYCOLLECTION (POINT EMPTY)");
  assertRoundtrip("GEOMETRYCOLLECTION (POINT (0 0))");
  assertRoundtrip("GEOMETRYCOLLECTION (POINT (0 0), POINT EMPTY)");
  assertRoundtrip("GEOMETRYCOLLECTION (POINT (0 0), POINT (0 0))");
  assertRoundtrip("GEOMETRYCOLLECTION (POINT (0 0), POINT (1 1))");
  assertRoundtrip("GEOMETRYCOLLECTION (MULTIPOINT EMPTY)");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (MULTIPOINT (0 0, 1 2), POINT (1 1), MULTIPOINT EMPTY)");

  assertRoundtrip("GEOMETRYCOLLECTION (LINESTRING EMPTY)");
  assertRoundtrip("GEOMETRYCOLLECTION (MULTILINESTRING EMPTY)");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (MULTILINESTRING ((0 1, 2 3, 0 3, 0 1), (10 10, 10 12, 12 10)), POINT EMPTY, LINESTRING (0 0, -1 -1, 2 0))");

  assertRoundtrip("GEOMETRYCOLLECTION (POLYGON EMPTY)");
  assertRoundtrip("GEOMETRYCOLLECTION (MULTIPOLYGON EMPTY)");
  assertRoundtrip("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (8 4, 5 7), POLYGON EMPTY)");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION ( MULTIPOINT (1 2) ))");
}

TEST(GeometrySerdeTest, testComplexSerde) {
  assertRoundtrip("GEOMETRYCOLLECTION ( MULTIPOINT EMPTY, MULTIPOINT (1 1) )");
  assertRoundtrip("GEOMETRYCOLLECTION (POLYGON EMPTY, POINT (1 2))");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (POLYGON EMPTY, MULTIPOINT (1 2), GEOMETRYCOLLECTION ( MULTIPOINT (3 4) ))");
  assertRoundtrip(
      "GEOMETRYCOLLECTION (POLYGON EMPTY, GEOMETRYCOLLECTION ( POINT (1 2), POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), GEOMETRYCOLLECTION EMPTY, MULTIPOLYGON ( ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 12 11, 12 12, 11 12, 11 11)), ((-1 -1, -2 -2, -1 -2, -1 -1)) ) ))");
}
