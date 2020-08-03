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

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.plugin.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.plugin.geospatial.BingTile.fromCoordinates;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LONGITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LONGITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.findDissolvedTileCovering;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.tileXToLongitude;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.tileYToLatitude;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestBingTile
        extends AbstractTestFunctions
{
    @Test
    public void testSerialization()
            throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();
        BingTile tile = fromCoordinates(1, 2, 3);
        String json = objectMapper.writeValueAsString(tile);
        assertEquals("{\"x\":1,\"y\":2,\"zoom\":3}", json);
        assertEquals(tile, objectMapper.readerFor(BingTile.class).readValue(json));
    }

    @Test
    public void testBingTileEncoding()
    {
        for (int zoom = 0; zoom <= MAX_ZOOM_LEVEL; zoom++) {
            int maxValue = (1 << zoom) - 1;
            testEncodingRoundTrip(0, 0, zoom);
            testEncodingRoundTrip(0, maxValue, zoom);
            testEncodingRoundTrip(maxValue, 0, zoom);
            testEncodingRoundTrip(maxValue, maxValue, zoom);
        }
    }

    private void testEncodingRoundTrip(int x, int y, int zoom)
    {
        BingTile expected = BingTile.fromCoordinates(x, y, zoom);
        BingTile actual = BingTile.decode(expected.encode());
        assertEquals(actual, expected);
    }

    @Test
    public void testTileXToLongitude()
    {
        assertEquals(tileXToLongitude(0, 0), MIN_LONGITUDE);
        assertEquals(tileXToLongitude(1, 0), MAX_LONGITUDE);
        assertEquals(tileXToLongitude(0, 1), MIN_LONGITUDE);
        assertEquals(tileXToLongitude(1, 1), 0.0);
        assertEquals(tileXToLongitude(2, 1), MAX_LONGITUDE);
        for (int zoom = 2; zoom <= MAX_ZOOM_LEVEL; zoom++) {
            assertEquals(tileXToLongitude(0, zoom), MIN_LONGITUDE);
            assertEquals(tileXToLongitude(1 << (zoom - 1), zoom), 0.0);
            assertEquals(tileXToLongitude(1 << zoom, zoom), MAX_LONGITUDE);
        }
    }

    @Test
    public void testTileYToLatitude()
    {
        double delta = 1e-8;
        assertEquals(tileYToLatitude(0, 0), MAX_LATITUDE, delta);
        assertEquals(tileYToLatitude(1, 0), MIN_LATITUDE, delta);
        assertEquals(tileYToLatitude(0, 1), MAX_LATITUDE, delta);
        assertEquals(tileYToLatitude(1, 1), 0.0);
        assertEquals(tileYToLatitude(2, 1), MIN_LATITUDE, delta);
        for (int zoom = 2; zoom <= MAX_ZOOM_LEVEL; zoom++) {
            assertEquals(tileYToLatitude(0, zoom), MAX_LATITUDE, delta);
            assertEquals(tileYToLatitude(1 << (zoom - 1), zoom), 0.0);
            assertEquals(tileYToLatitude(1 << zoom, zoom), MIN_LATITUDE, delta);
        }
    }

    @Test
    public void testFindChildren()
    {
        assertEquals(
                toSortedQuadkeys(BingTile.fromQuadKey("").findChildren()),
                ImmutableList.of("0", "1", "2", "3"));

        assertEquals(
                toSortedQuadkeys(BingTile.fromQuadKey("0123").findChildren()),
                ImmutableList.of("01230", "01231", "01232", "01233"));

        assertEquals(
                toSortedQuadkeys(BingTile.fromQuadKey("").findChildren(2)),
                ImmutableList.of("00", "01", "02", "03", "10", "11", "12", "13", "20", "21", "22", "23", "30", "31", "32", "33"));

        assertThatThrownBy(() -> BingTile.fromCoordinates(0, 0, MAX_ZOOM_LEVEL).findChildren())
                .hasMessage(format("newZoom must be less than or equal to %s: %s", MAX_ZOOM_LEVEL, MAX_ZOOM_LEVEL + 1));

        assertThatThrownBy(() -> BingTile.fromCoordinates(0, 0, 13).findChildren(MAX_ZOOM_LEVEL + 1))
                .hasMessage(format("newZoom must be less than or equal to %s: %s", MAX_ZOOM_LEVEL, MAX_ZOOM_LEVEL + 1));

        assertThatThrownBy(() -> BingTile.fromCoordinates(0, 0, 13).findChildren(12))
                .hasMessage(format("newZoom must be greater than or equal to current zoom %s: %s", 13, 12));
    }

    private List<String> toSortedQuadkeys(List<BingTile> tiles)
    {
        return tiles.stream()
                .map(BingTile::toQuadKey)
                .sorted()
                .collect(toImmutableList());
    }

    @Test
    public void testFindParent()
    {
        assertEquals(BingTile.fromQuadKey("0123").findParent().toQuadKey(), "012");
        assertEquals(BingTile.fromQuadKey("1").findParent().toQuadKey(), "");
        assertEquals(BingTile.fromQuadKey("0123").findParent(1).toQuadKey(), "0");
        assertEquals(BingTile.fromQuadKey("0123").findParent(4).toQuadKey(), "0123");

        assertThatThrownBy(() -> BingTile.fromQuadKey("0123").findParent(5))
                .hasMessage(format("newZoom must be less than or equal to current zoom %s: %s", 4, 5));

        assertThatThrownBy(() -> BingTile.fromQuadKey("").findParent())
                .hasMessage(format("newZoom must be greater than or equal to 0: %s", -1));

        assertThatThrownBy(() -> BingTile.fromQuadKey("12").findParent(-1))
                .hasMessage(format("newZoom must be greater than or equal to 0: %s", -1));
    }

    @Test
    public void testFindDissolvedTileCovering()
    {
        assertTileCovering("POINT EMPTY", 0, ImmutableList.of());
        assertTileCovering("POINT EMPTY", 10, ImmutableList.of());
        assertTileCovering("POINT EMPTY", 20, ImmutableList.of());

        assertSmallSquareCovering(2);
        assertSmallSquareCovering(5);
        assertSmallSquareCovering(11);
        assertSmallSquareCovering(MAX_ZOOM_LEVEL);

        // Geometries at tile borders
        assertTileCovering("POINT (0 0)", 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s 0)", MIN_LONGITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s 0)", MAX_LONGITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (0 %s)", MIN_LATITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (0 %s)", MAX_LATITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s %s)", MIN_LONGITUDE, MIN_LATITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s %s)", MIN_LONGITUDE, MAX_LATITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s %s)", MAX_LONGITUDE, MAX_LATITUDE), 0, ImmutableList.of(""));
        assertTileCovering(format("POINT (%s %s)", MAX_LONGITUDE, MIN_LATITUDE), 0, ImmutableList.of(""));

        assertTileCovering("POINT (0 0)", 1, ImmutableList.of("3"));
        assertTileCovering(format("POINT (%s 0)", MIN_LONGITUDE), 1, ImmutableList.of("2"));
        assertTileCovering(format("POINT (%s 0)", MAX_LONGITUDE), 1, ImmutableList.of("3"));
        assertTileCovering(format("POINT (0 %s)", MIN_LATITUDE), 1, ImmutableList.of("3"));
        assertTileCovering(format("POINT (0 %s)", MAX_LATITUDE), 1, ImmutableList.of("1"));
        assertTileCovering(format("POINT (%s %s)", MIN_LONGITUDE, MIN_LATITUDE), 1, ImmutableList.of("2"));
        assertTileCovering(format("POINT (%s %s)", MIN_LONGITUDE, MAX_LATITUDE), 1, ImmutableList.of("0"));
        assertTileCovering(format("POINT (%s %s)", MAX_LONGITUDE, MAX_LATITUDE), 1, ImmutableList.of("1"));
        assertTileCovering(format("POINT (%s %s)", MAX_LONGITUDE, MIN_LATITUDE), 1, ImmutableList.of("3"));

        assertTileCovering("LINESTRING (-1 0, -2 0)", 1, ImmutableList.of("2"));
        assertTileCovering("LINESTRING (1 0, 2 0)", 1, ImmutableList.of("3"));
        assertTileCovering("LINESTRING (0 -1, 0 -2)", 1, ImmutableList.of("3"));
        assertTileCovering("LINESTRING (0 1, 0 2)", 1, ImmutableList.of("1"));

        assertTileCovering(format("LINESTRING (%s 1, %s 2)", MIN_LONGITUDE, MIN_LONGITUDE), 1, ImmutableList.of("0"));
        assertTileCovering(format("LINESTRING (%s -1, %s -2)", MIN_LONGITUDE, MIN_LONGITUDE), 1, ImmutableList.of("2"));
        assertTileCovering(format("LINESTRING (%s 1, %s 2)", MAX_LONGITUDE, MAX_LONGITUDE), 1, ImmutableList.of("1"));
        assertTileCovering(format("LINESTRING (%s -1, %s -2)", MAX_LONGITUDE, MAX_LONGITUDE), 1, ImmutableList.of("3"));

        assertTileCovering("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", 6, ImmutableList.of("12222", "300000", "300001"));
        assertTileCovering("POLYGON ((0 0, 0 10, 10 10, 0 0))", 6, ImmutableList.of("122220", "122222", "122221", "300000"));
        assertTileCovering("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 3, ImmutableList.of("033", "211", "122"));
        assertTileCovering("POLYGON ((10 10, -10 10, -20 -15, 10 10))", 6, ImmutableList.of("211102", "211120", "033321", "033323", "211101", "211103", "211121", "03333", "211110", "211112", "211111", "122220", "122222", "122221"));

        assertTileCovering("GEOMETRYCOLLECTION (POINT (60 30.12))", 10, ImmutableList.of("1230301230"));
        assertTileCovering("GEOMETRYCOLLECTION (POINT (60 30.12))", 15, ImmutableList.of("123030123010121"));
        assertTileCovering("GEOMETRYCOLLECTION (POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122"));
        assertTileCovering("GEOMETRYCOLLECTION (POINT (60 30.12), POLYGON ((10 10, -10 10, -20 -15, 10 10)))", 3, ImmutableList.of("033", "211", "122", "123"));
        assertTileCovering("GEOMETRYCOLLECTION (POINT (60 30.12), LINESTRING (61 31, 61.01 31.01), POLYGON EMPTY)", 15, ImmutableList.of("123030123010121", "123030112310200", "123030112310202", "123030112310201"));
        assertTileCovering("GEOMETRYCOLLECTION (POINT (0.1 0.1), POINT(0.1 -0.1), POINT(-0.1 -0.1), POINT(-0.1 0.1))", 3,
                ImmutableList.of("033", "122", "211", "300"));
    }

    private void assertTileCovering(String wkt, int zoom, List<String> quadkeys)
    {
        OGCGeometry geometry = OGCGeometry.fromText(wkt);
        List<String> actual = findDissolvedTileCovering(geometry, zoom).stream().map(BingTile::toQuadKey).sorted().collect(toImmutableList());
        List<String> expected = ImmutableList.sortedCopyOf(quadkeys);
        assertEquals(actual, expected, format("Actual:\n%s\nExpected:\n%s", actual, expected));
    }

    private void assertSmallSquareCovering(int zoom)
    {
        int halfway = 1 << (zoom - 1);
        assertTileCovering(
                "POLYGON ((0.00001 0.00001, 0.00001 -0.00001, -0.00001 -0.00001, -0.00001 0.00001, 0.00001 0.00001))",
                zoom,
                Stream.of(
                        BingTile.fromCoordinates(halfway, halfway, zoom),
                        BingTile.fromCoordinates(halfway, halfway - 1, zoom),
                        BingTile.fromCoordinates(halfway - 1, halfway, zoom),
                        BingTile.fromCoordinates(halfway - 1, halfway - 1, zoom)
                ).map(BingTile::toQuadKey)
                        .collect(Collectors.toList()));
    }
}
