/*
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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.geospatial.GeometrySerde.deserialize;
import static com.facebook.presto.geospatial.GeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.GeometrySerde.serialize;
import static org.testng.Assert.assertEquals;

public class TestGeometrySerde
{
    @Test
    public void testPoint()
    {
        testSerialization("POINT (1 2)");
        testSerialization("POINT (-1 -2)");
        testSerialization("POINT (0 0)");
        testSerialization("POINT (-2e3 -4e33)");
        testSerialization("POINT EMPTY");
    }

    @Test
    public void testMultiPoint()
    {
        testSerialization("MULTIPOINT (0 0)");
        testSerialization("MULTIPOINT (0 0, 1 1, 2 3)");
        testSerialization("MULTIPOINT EMPTY");
    }

    @Test
    public void testLineString()
    {
        testSerialization("LINESTRING (0 1)");
        testSerialization("LINESTRING (0 1, 2 3)");
        testSerialization("LINESTRING (0 1, 2 3, 4 5)");
        testSerialization("LINESTRING EMPTY");
    }

    @Test
    public void testMultiLineString()
    {
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))");
        testSerialization("MULTILINESTRING EMPTY");
    }

    @Test
    public void testPolygon()
    {
        testSerialization("POLYGON ((30 10, 40 40, 20 40))");
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
        testSerialization("POLYGON EMPTY");
    }

    @Test
    public void testMultiPolygon()
    {
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))");
        testSerialization("MULTIPOLYGON EMPTY");
    }

    @Test
    public void testGeometryCollection()
    {
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2))");
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2), POINT (2 1), POINT EMPTY)");
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5))))");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5))), POINT (1 2))");
        testSerialization("GEOMETRYCOLLECTION (POINT EMPTY)");
        testSerialization("GEOMETRYCOLLECTION EMPTY");
        testSerialization("GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20))), GEOMETRYCOLLECTION (MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))))");
    }

    @Test
    public void testDeserializeEnvelope()
    {
        assertDeserializeEnvelope("MULTIPOINT (20 20, 25 25)", new Envelope(20, 20, 25, 25));
        assertDeserializeEnvelope("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", new Envelope(1, 1, 5, 4));
        assertDeserializeEnvelope("POLYGON ((0 0, 0 4, 4 0))", new Envelope(0, 0, 4, 4));
        assertDeserializeEnvelope("MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))", new Envelope(0, 0, 4, 4));
        assertDeserializeEnvelope("GEOMETRYCOLLECTION (POINT (3 7), LINESTRING (4 6, 7 10))", new Envelope(3, 6, 7, 10));
        assertDeserializeEnvelope("POLYGON EMPTY", null);
        assertDeserializeEnvelope("POINT (1 2)", new Envelope(1, 2, 1, 2));
        assertDeserializeEnvelope("POINT EMPTY", null);
    }

    private static void testSerialization(String wkt)
    {
        OGCGeometry geometry = OGCGeometry.fromText(wkt);
        OGCGeometry deserializedGeometry = deserialize(serialize(geometry));
        assertGeometryEquals(geometry, deserializedGeometry);
    }

    private static void assertDeserializeEnvelope(String geometry, Envelope expectedEnvelope)
    {
        assertEquals(deserializeEnvelope(geometryFromText(geometry)), expectedEnvelope);
    }

    private static Slice geometryFromText(String wkt)
    {
        return serialize(OGCGeometry.fromText(wkt));
    }

    private static void assertGeometryEquals(OGCGeometry actual, OGCGeometry expected)
    {
        actual.setSpatialReference(null);
        expected.setSpatialReference(null);
        ensureEnvelopeLoaded(actual);
        ensureEnvelopeLoaded(expected);
        assertEquals(actual, expected);
    }

    /**
     * There is a weird bug in geometry comparison. If a geometry envelope is not loaded it may return
     * false for two empty line strings or multiline strings
     */
    private static void ensureEnvelopeLoaded(OGCGeometry geometry)
    {
        geometry.envelope();
    }
}
