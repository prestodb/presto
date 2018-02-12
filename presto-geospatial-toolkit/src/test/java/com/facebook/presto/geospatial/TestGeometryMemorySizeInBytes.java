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

import static com.facebook.presto.geospatial.GeometryUtils.getEstimatedMemorySizeInBytes;
import static org.testng.Assert.assertTrue;

public class TestGeometryMemorySizeInBytes
{
    @Test
    public void testPoint()
    {
        testGeometry(parseWkt("POINT (1 2)"));
    }

    @Test
    public void testMultiPoint()
    {
        testGeometry(parseWkt("MULTIPOINT (0 0, 1 1, 2 3)"));
    }

    @Test
    public void testLineString()
    {
        testGeometry(parseWkt("LINESTRING (0 1, 2 3, 4 5)"));
    }

    @Test
    public void testMultiLineString()
    {
        testGeometry(parseWkt("MULTILINESTRING ((0 1, 2 3, 4 5), (1 1, 2 2))"));
    }

    @Test
    public void testPolygon()
    {
        testGeometry(parseWkt("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"));
    }

    @Test
    public void testMultiPolygon()
    {
        testGeometry(parseWkt("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"));
    }

    @Test
    public void testGeometryCollection()
    {
        testGeometry(parseWkt("GEOMETRYCOLLECTION (POINT(4 6), LINESTRING(4 6,7 10))"));
    }

    private void testGeometry(OGCGeometry geometry)
    {
        assertTrue(getEstimatedMemorySizeInBytes(geometry) > 0);
    }

    private static OGCGeometry parseWkt(String wkt)
    {
        OGCGeometry geometry = OGCGeometry.fromText(wkt);
        geometry.setSpatialReference(null);
        return geometry;
    }
}
