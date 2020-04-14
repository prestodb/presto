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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.locationtech.jts.geom.Envelope;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.flattenCollection;
import static com.facebook.presto.geospatial.GeometryUtils.getExtent;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestGeometryUtils
{
    @Test
    public void testGetJtsEnvelope()
    {
        assertJtsEnvelope(
                "MULTIPOLYGON EMPTY",
                new Envelope());
        assertJtsEnvelope(
                "POINT (-23.4 12.2)",
                new Envelope(-23.4, -23.4, 12.2, 12.2));
        assertJtsEnvelope(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Envelope(-75.9375, -75.9375, 23.6359, 23.6364));
        assertJtsEnvelope(
                "GEOMETRYCOLLECTION (" +
                        "  LINESTRING (-75.9375 23.6359, -75.9375 23.6364)," +
                        "  MULTIPOLYGON (((-75.9375 23.45520, -75.9371 23.4554, -75.9375 23.46023325, -75.9375 23.45520)))" +
                        ")",
                new Envelope(-75.9375, -75.9371, 23.4552, 23.6364));
    }

    private void assertJtsEnvelope(String wkt, Envelope expected)
    {
        Envelope calculated = GeometryUtils.getJtsEnvelope(OGCGeometry.fromText(wkt));
        assertEquals(calculated, expected);
    }

    @Test
    public void testGetExtent()
    {
        assertGetExtent(
                "POINT (-23.4 12.2)",
                new Rectangle(-23.4, 12.2, -23.4, 12.2));
        assertGetExtent(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Rectangle(-75.9375, 23.6359, -75.9375, 23.6364));
        assertGetExtent(
                "GEOMETRYCOLLECTION (" +
                        "  LINESTRING (-75.9375 23.6359, -75.9375 23.6364)," +
                        "  MULTIPOLYGON (((-75.9375 23.45520, -75.9371 23.4554, -75.9375 23.46023325, -75.9375 23.45520)))" +
                        ")",
                new Rectangle(-75.9375, 23.4552, -75.9371, 23.6364));
    }

    private void assertGetExtent(String wkt, Rectangle expected)
    {
        assertEquals(getExtent(OGCGeometry.fromText(wkt)), expected);
    }

    @Test
    public void testFlattenCollection()
    {
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POINT EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POINT (1 2)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT (1 2)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOINT (1 2, 3 4)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("LINESTRING EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("LINESTRING (1 2, 3 4)"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING ((1 2, 3 4))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POLYGON EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("POLYGON ((0 0, 0 1, 1 1, 0 0))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON EMPTY"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)))"));
        assertFlattenLeavesUnchanged(OGCGeometry.fromText("MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)), ((10 10, 10 11, 11 11, 10 10)))"));

        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION EMPTY"), ImmutableList.of());
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (POINT EMPTY)"), OGCGeometry.fromText("POINT EMPTY"));
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (POINT (0 1))"), OGCGeometry.fromText("POINT (0 1)"));
        assertFlattens(OGCGeometry.fromText("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION EMPTY)"), ImmutableList.of());
        assertFlattens(
                OGCGeometry.fromText("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 1), POINT (1 2)))"),
                ImmutableList.of(OGCGeometry.fromText("POINT (0 1)"), OGCGeometry.fromText("POINT (1 2)")));
    }

    private void assertFlattenLeavesUnchanged(OGCGeometry original)
    {
        assertFlattens(original, original);
    }

    private void assertFlattens(OGCGeometry original, OGCGeometry expected)
    {
        assertFlattens(original, ImmutableList.of(expected));
    }

    private void assertFlattens(OGCGeometry original, List<OGCGeometry> expected)
    {
        List<String> expectedWkts = expected.stream().map(g -> g.toString()).sorted().collect(toImmutableList());
        List<String> result = Streams.stream(flattenCollection(original)).map(g -> g.toString()).sorted().collect(toImmutableList());
        assertEquals(result, expectedWkts);
    }
}
