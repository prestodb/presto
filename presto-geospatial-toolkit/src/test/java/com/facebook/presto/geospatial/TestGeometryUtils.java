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
import org.locationtech.jts.geom.Envelope;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestGeometryUtils
{
    @Test
    public void testGetJtsEnvelopeEmpty()
    {
        assertJtsEnvelope(
                "MULTIPOLYGON EMPTY",
                new Envelope());
    }

    @Test
    public void testGetJtsEnvelopePoint()
    {
        assertJtsEnvelope(
                "POINT (-23.4 12.2)",
                new Envelope(-23.4, -23.4, 12.2, 12.2));
    }

    @Test
    public void testGetJtsEnvelopeLineString()
    {
        assertJtsEnvelope(
                "LINESTRING (-75.9375 23.6359, -75.9375 23.6364)",
                new Envelope(-75.9375, -75.9375, 23.6359, 23.6364));
    }

    @Test
    public void testGetJtsEnvelopeGeometryCollection()
    {
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
}
