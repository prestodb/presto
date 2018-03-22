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

import com.esri.core.geometry.ogc.OGCGeometry;
import org.testng.annotations.Test;

import static com.facebook.presto.geospatial.GeometryUtils.deserialize;
import static com.facebook.presto.geospatial.GeometryUtils.serialize;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGeometrySerialization
{
    @Test
    public void testPoint()
    {
        testSerialization("POINT (1 2)");
        testSerialization("POINT EMPTY");
    }

    @Test
    public void testMultiPoint()
    {
        testSerialization("MULTIPOINT (0 0, 1 1, 2 3)");
        testSerialization("MULTIPOINT EMPTY");
    }

    @Test
    public void testLineString()
    {
        testSerialization("LINESTRING (0 1, 2 3, 4 5)");
        testSerialization("LINESTRING EMPTY");
    }

    @Test
    public void testMultiLineString()
    {
        testSerialization("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))");
        testSerialization("MULTILINESTRING EMPTY");
    }

    @Test
    public void testPolygon()
    {
        testSerialization("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
        testSerialization("POLYGON EMPTY");
    }

    @Test
    public void testMultiPolygon()
    {
        testSerialization("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))");
        testSerialization("MULTIPOLYGON EMPTY");
    }

    @Test
    public void testGeometryCollection()
    {
        testSerialization("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 2, 3 4), POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0)))");
    }

    private static void testSerialization(String wkt)
    {
        OGCGeometry geometry = OGCGeometry.fromText(wkt);
        geometry.setSpatialReference(null);
        OGCGeometry deserializedGeometry = deserialize(serialize(geometry));
        if (geometry.isEmpty()) {
            assertTrue(deserializedGeometry.isEmpty());
        }
        else {
            assertEquals(geometry, deserializedGeometry);
        }
    }
}
