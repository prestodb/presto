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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.geospatial.JtsGeometryUtils;
import io.airlift.slice.Slice;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertTrue;

public class TestJtsGeometryUtils
{
    private static final WKTReader WKT_READER = new WKTReader();

    @Test
    public void testPoint()
            throws Exception
    {
        assertGeometry("POINT EMPTY");
        assertGeometry("POINT (1 2)");
    }

    @Test
    public void testMultiPoint()
            throws Exception
    {
        assertGeometry("MULTIPOINT EMPTY");
        assertGeometry("MULTIPOINT ((1 2), (3 4))");
    }

    @Test
    public void testLineString()
            throws Exception
    {
        assertGeometry("LINESTRING EMPTY");
        assertGeometry("LINESTRING (1 2, 3 4, 5 6)");
    }

    @Test
    public void testMultiLineString()
            throws Exception
    {
        assertGeometry("MULTILINESTRING EMPTY");
        assertGeometry("MULTILINESTRING ((1 2, 3 4, 5 6), (10.1 11.2, 12.3 13.4))");
    }

    @Test
    public void testPolygon()
            throws Exception
    {
        assertGeometry("POLYGON EMPTY");
        assertGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))");
        assertGeometry("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1))");
    }

    @Test
    public void testMultiPolygon()
            throws Exception
    {
        assertGeometry("MULTIPOLYGON EMPTY");
        assertGeometry("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 2 4)))");
        assertGeometry("MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((2 4, 2 6, 6 6, 2 4)), ((0 0, 0 1, 1 1, 1 0, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.2, 0.1 0.1)))");
    }

    @Test
    public void testGeometryCollection()
            throws Exception
    {
        assertGeometry("GEOMETRYCOLLECTION EMPTY");
        assertGeometry("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");
    }

    private static void assertGeometry(String wkt)
            throws ParseException
    {
        Slice geometry = stGeometryFromText(utf8Slice(wkt));
        Geometry expected = WKT_READER.read(wkt);
        Geometry actual = JtsGeometryUtils.deserialize(geometry);

        // ESRI shape serialization format doesn't contain enough information
        // to distinguish between empty LineString and MultiLineString or
        // empty Polygon and MultiPolygon.
        if (expected.isEmpty()) {
            assertTrue(actual.isEmpty());
        }
        else {
            assertEquals(actual, expected);
        }
    }
}
