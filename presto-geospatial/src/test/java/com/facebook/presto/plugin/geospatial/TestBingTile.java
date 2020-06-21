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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.plugin.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.plugin.geospatial.BingTile.fromCoordinates;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LONGITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LONGITUDE;
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
}
