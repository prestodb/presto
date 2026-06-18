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
package com.facebook.presto.geospatial.serde;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.slice.Slice;
import org.locationtech.jts.geom.Geometry;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static com.facebook.presto.geospatial.GeometryUtils.jtsGeometryFromWkt;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.createFromEsriGeometry;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserialize;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeType;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.serialize;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.ENVELOPE;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.GEOMETRY_COLLECTION;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.LINE_STRING;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.MULTI_LINE_STRING;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.MULTI_POINT;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.MULTI_POLYGON;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.POINT;
import static com.facebook.presto.geospatial.serde.GeometrySerializationType.POLYGON;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;

public class TestGeometrySerialization
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
        testSerialization("MULTIPOINT (0 0, 0 0)");
        testSerialization("MULTIPOINT (0 0, 1 1, 2 3)");
        testSerialization("MULTIPOINT EMPTY");
    }

    @Test
    public void testLineString()
    {
        testSerialization("LINESTRING (0 1, 2 3)");
        testSerialization("LINESTRING (0 1, 2 3, 4 5)");
        testSerialization("LINESTRING (0 1, 2 3, 4 5, 0 1)");
        testSerialization("LINESTRING EMPTY");
    }

    @Test
    public void testMultiLineString()
    {
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 5))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0 1, 2 3, 4 7, 0 1))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (0 1, 2 3, 4 6), (0 1, 2 3, 4 7), (0.333 0.74, 0.1 0.2, 2e3 4e-3), (0.333 0.74, 2e3 4e-3))");
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))");
        testSerialization("MULTILINESTRING EMPTY");
    }

    @Test
    public void testPolygon()
    {
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 30 10))");
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        testSerialization("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25), (0.25 0.25, 0.75 0.75, 0.25 0.75, 0.75 0.25, 0.25 0.25))");
        testSerialization("POLYGON EMPTY");
        testSerialization("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25))");
    }

    @Test
    public void testMultiPolygon()
    {
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((30 20, 45 40, 10 40, 30 20)))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 15 5))), ((0 0, 0 1, 1 1, 1 0.5, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), ((15 5, 40 10, 10 20, 5 10, 15 5))), ((0 0, 0 1, 1 1, 1 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25))");
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)))");
        testSerialization("MULTIPOLYGON (" +
                "((30 20, 45 40, 10 40, 30 20)), " +
                // clockwise, counter clockwise
                "((0 0, 0 1, 1 1, 1 0, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
                // clockwise, clockwise
                "((0 0, 0 1, 1 1, 1 0, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
                // counter clockwise, clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)), " +
                // counter clockwise, counter clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25)), " +
                // counter clockwise, counter clockwise, clockwise
                "((0 0, 1 0, 1 1, 0 1, 0 0), (0.75 0.25, 0.75 0.75, 0.25 0.75, 0.25 0.25, 0.75 0.25), (0.25 0.25, 0.25 0.75, 0.75 0.75, 0.75 0.25, 0.25 0.25)))");
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
    public void testEnvelope()
    {
        testEnvelopeSerialization(new Envelope());
        testEnvelopeSerialization(new Envelope(0, 0, 1, 1));
        testEnvelopeSerialization(new Envelope(1, 2, 3, 4));
        testEnvelopeSerialization(new Envelope(10101, -2.05, -3e5, 0));
    }

    private void testEnvelopeSerialization(Envelope envelope)
    {
        assertEquals(deserialize(serialize(envelope)), createFromEsriGeometry(envelope, false));
        assertEquals(deserializeEnvelope(serialize(envelope)), envelope);
        assertEquals(JtsGeometrySerde.serialize(JtsGeometrySerde.deserialize(serialize(envelope))), serialize(createFromEsriGeometry(envelope, false)));
    }

    @Test
    public void testDeserializeEnvelope()
    {
        assertDeserializeEnvelope("MULTIPOINT (20 20, 25 25)", new Envelope(20, 20, 25, 25));
        assertDeserializeEnvelope("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", new Envelope(1, 1, 5, 4));
        assertDeserializeEnvelope("POLYGON ((0 0, 0 4, 4 0))", new Envelope(0, 0, 4, 4));
        assertDeserializeEnvelope("MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))", new Envelope(0, 0, 4, 4));
        assertDeserializeEnvelope("GEOMETRYCOLLECTION (POINT (3 7), LINESTRING (4 6, 7 10))", new Envelope(3, 6, 7, 10));
        assertDeserializeEnvelope("POLYGON EMPTY", new Envelope());
        assertDeserializeEnvelope("POINT (1 2)", new Envelope(1, 2, 1, 2));
        assertDeserializeEnvelope("POINT EMPTY", new Envelope());
        assertDeserializeEnvelope("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (2 7), LINESTRING (4 6, 7 10)), POINT (3 7), LINESTRING (4 6, 7 10))", new Envelope(2, 6, 7, 10));
    }

    @Test
    public void testDeserializeType()
    {
        assertDeserializeType("POINT (1 2)", POINT);
        assertDeserializeType("POINT EMPTY", POINT);
        assertDeserializeType("MULTIPOINT (20 20, 25 25)", MULTI_POINT);
        assertDeserializeType("MULTIPOINT EMPTY", MULTI_POINT);
        assertDeserializeType("LINESTRING (1 1, 5 1, 6 2))", LINE_STRING);
        assertDeserializeType("LINESTRING EMPTY", LINE_STRING);
        assertDeserializeType("MULTILINESTRING ((1 1, 5 1), (2 4, 4 4))", MULTI_LINE_STRING);
        assertDeserializeType("MULTILINESTRING EMPTY", MULTI_LINE_STRING);
        assertDeserializeType("POLYGON ((0 0, 0 4, 4 0))", POLYGON);
        assertDeserializeType("POLYGON EMPTY", POLYGON);
        assertDeserializeType("MULTIPOLYGON (((0 0 , 0 2, 2 2, 2 0)), ((2 2, 2 4, 4 4, 4 2)))", MULTI_POLYGON);
        assertDeserializeType("MULTIPOLYGON EMPTY", MULTI_POLYGON);
        assertDeserializeType("GEOMETRYCOLLECTION (POINT (3 7), LINESTRING (4 6, 7 10))", GEOMETRY_COLLECTION);
        assertDeserializeType("GEOMETRYCOLLECTION EMPTY", GEOMETRY_COLLECTION);

        assertEquals(deserializeType(serialize(new Envelope(1, 2, 3, 4))), ENVELOPE);
    }

    @Test
    public void testInvalidSerializations()
    {
        String wkt = "LINESTRING (0 0)";
        testEsriSerialization(wkt);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::testJtsSerialization, INVALID_FUNCTION_ARGUMENT);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::tryDeserializeEsriFromJts, INVALID_FUNCTION_ARGUMENT);
        tryDeserializeJtsFromEsri(wkt);

        wkt = "POLYGON ((0 0, 1 1))";
        testEsriSerialization(wkt);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::testJtsSerialization, INVALID_FUNCTION_ARGUMENT);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::tryDeserializeEsriFromJts, INVALID_FUNCTION_ARGUMENT);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::tryDeserializeJtsFromEsri, INVALID_FUNCTION_ARGUMENT);

        wkt = "POLYGON ((0 0, 1 1, 0 1, 1 0, 0 0))";
        testEsriSerialization(wkt);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::testJtsSerialization, INVALID_FUNCTION_ARGUMENT);
        tryDeserializeEsriFromJts(wkt);
        assertThrowsPrestoException(wkt, TestGeometrySerialization::tryDeserializeJtsFromEsri, INVALID_FUNCTION_ARGUMENT);
    }

    private static void testSerialization(String wkt)
    {
        testEsriSerialization(wkt);
        testJtsSerialization(wkt);
        testCrossSerialization(wkt);
    }

    private static void testEsriSerialization(String wkt)
    {
        OGCGeometry expected = OGCGeometry.fromText(wkt);
        OGCGeometry actual = deserialize(serialize(expected));
        assertGeometryEquals(actual, expected);
    }

    private static void testJtsSerialization(String wkt)
    {
        Geometry expected = jtsGeometryFromWkt(wkt);
        Geometry actual = JtsGeometrySerde.deserialize(JtsGeometrySerde.serialize(expected));
        assertGeometryEquals(actual, expected);
    }

    private static void testCrossSerialization(String wkt)
    {
        Geometry jtsGeometry = jtsGeometryFromWkt(wkt);
        OGCGeometry esriGeometry = OGCGeometry.fromText(wkt);

        Slice jtsSerialized = JtsGeometrySerde.serialize(jtsGeometry);
        Slice esriSerialized = EsriGeometrySerde.serialize(esriGeometry);

        OGCGeometry esriFromJts = EsriGeometrySerde.deserialize(jtsSerialized);
        Geometry jtsFromEsri = JtsGeometrySerde.deserialize(esriSerialized);
        assertGeometryEquals(esriFromJts, esriGeometry);
        assertGeometryEquals(jtsFromEsri, jtsGeometry);
    }

    private static void tryDeserializeEsriFromJts(String wkt)
    {
        Geometry jtsGeometry = jtsGeometryFromWkt(wkt);
        EsriGeometrySerde.deserialize(JtsGeometrySerde.serialize(jtsGeometry));
    }

    private static void tryDeserializeJtsFromEsri(String wkt)
    {
        OGCGeometry esriGeometry = OGCGeometry.fromText(wkt);
        JtsGeometrySerde.deserialize(EsriGeometrySerde.serialize(esriGeometry));
    }

    private static Slice geometryFromText(String wkt)
    {
        return serialize(OGCGeometry.fromText(wkt));
    }

    private static void assertGeometryEquals(Geometry actual, Geometry expected)
    {
        assertEquals(actual.norm(), expected.norm());
    }

    private static void assertDeserializeEnvelope(String geometry, Envelope expectedEnvelope)
    {
        assertEquals(deserializeEnvelope(geometryFromText(geometry)), expectedEnvelope);
    }

    private static void assertDeserializeType(String wkt, GeometrySerializationType expectedType)
    {
        assertEquals(deserializeType(geometryFromText(wkt)), expectedType);
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

    private static void assertThrowsPrestoException(String argument, Consumer<String> function, StandardErrorCode errorCode)
    {
        try {
            function.accept(argument);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), errorCode.toErrorCode());
        }
    }
}
