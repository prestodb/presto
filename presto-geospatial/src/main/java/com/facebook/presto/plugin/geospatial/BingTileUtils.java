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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.geospatial.GeometryUtils.accelerateGeometry;
import static com.facebook.presto.geospatial.GeometryUtils.getEnvelope;
import static com.facebook.presto.plugin.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class BingTileUtils
{
    static final int TILE_PIXELS = 256;
    static final double EARTH_RADIUS_KM = 6371.01;
    static final double MAX_LATITUDE = 85.05112878;
    static final double MIN_LATITUDE = -85.05112878;
    static final double MIN_LONGITUDE = -180;
    static final double MAX_LONGITUDE = 180;
    static final String LATITUDE_SPAN_OUT_OF_RANGE = format("Latitude span for the geometry must be in [%.2f, %.2f] range", MIN_LATITUDE, MAX_LATITUDE);
    static final String LATITUDE_OUT_OF_RANGE = "Latitude must be between " + MIN_LATITUDE + " and " + MAX_LATITUDE;
    static final String LONGITUDE_SPAN_OUT_OF_RANGE = format("Longitude span for the geometry must be in [%.2f, %.2f] range", MIN_LONGITUDE, MAX_LONGITUDE);
    static final String LONGITUDE_OUT_OF_RANGE = "Longitude must be between " + MIN_LONGITUDE + " and " + MAX_LONGITUDE;
    private static final String QUAD_KEY_TOO_LONG = "QuadKey must be " + MAX_ZOOM_LEVEL + " characters or less";
    private static final String ZOOM_LEVEL_TOO_SMALL = "Zoom level must be >= 0";
    private static final String ZOOM_LEVEL_TOO_LARGE = "Zoom level must be <= " + MAX_ZOOM_LEVEL;
    private static final int MAX_COVERING_COUNT = 1_000_000;

    private BingTileUtils() {}

    static void checkZoomLevel(long zoomLevel)
    {
        checkCondition(zoomLevel >= 0, ZOOM_LEVEL_TOO_SMALL);
        checkCondition(zoomLevel <= MAX_ZOOM_LEVEL, ZOOM_LEVEL_TOO_LARGE);
    }

    static void checkCoordinate(long coordinate, long zoomLevel)
    {
        checkCondition(coordinate >= 0 && coordinate < (1 << zoomLevel), "XY coordinates for a Bing tile at zoom level %s must be within [0, %s) range", zoomLevel, 1 << zoomLevel);
    }

    static void checkQuadKey(@SqlType(StandardTypes.VARCHAR) Slice quadkey)
    {
        checkCondition(quadkey.length() <= MAX_ZOOM_LEVEL, QUAD_KEY_TOO_LONG);
    }

    static void checkLatitude(double latitude, String errorMessage)
    {
        checkCondition(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE, errorMessage);
    }

    static void checkLongitude(double longitude, String errorMessage)
    {
        checkCondition(longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE, errorMessage);
    }

    static void checkCondition(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format(formatString, args));
        }
    }

    /**
     * Return the longitude (in degrees) of the west edge of the tile.
     */
    public static double tileXToLongitude(int tileX, int zoomLevel)
    {
        int mapTileSize = 1 << zoomLevel;
        double x = (clip(tileX, 0, mapTileSize) / mapTileSize) - 0.5;
        return 360 * x;
    }

    /**
     * Return the latitude (in degrees) of the north edge of the tile.
     */
    public static double tileYToLatitude(int tileY, int zoomLevel)
    {
        int mapTileSize = 1 << zoomLevel;
        double y = 0.5 - (clip(tileY, 0, mapTileSize) / mapTileSize);
        return 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
    }

    /**
     * Return the Point in the Northwest corner of the tile.
     */
    static Point tileXYToLatitudeLongitude(int tileX, int tileY, int zoomLevel)
    {
        return new Point(tileXToLongitude(tileX, zoomLevel), tileYToLatitude(tileY, zoomLevel));
    }

    static Envelope tileToEnvelope(BingTile tile)
    {
        double minX = tileXToLongitude(tile.getX(), tile.getZoomLevel());
        double maxX = tileXToLongitude(tile.getX() + 1, tile.getZoomLevel());
        double minY = tileYToLatitude(tile.getY(), tile.getZoomLevel());
        double maxY = tileYToLatitude(tile.getY() + 1, tile.getZoomLevel());
        return new Envelope(minX, minY, maxX, maxY);
    }

    static double clip(double n, double minValue, double maxValue)
    {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    static long mapSize(int zoomLevel)
    {
        return 256L << zoomLevel;
    }

    /**
     * Returns a Bing tile at a given zoom level containing a point at a given latitude and longitude.
     * Latitude must be within [-85.05112878, 85.05112878] range. Longitude must be within [-180, 180] range.
     * Zoom levels from 1 to 23 are supported.
     */
    static BingTile latitudeLongitudeToTile(double latitude, double longitude, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        int tileX = longitudeToTileX(longitude, mapSize);
        int tileY = latitudeToTileY(latitude, mapSize);
        return BingTile.fromCoordinates(tileX, tileY, zoomLevel);
    }

    /**
     * Given longitude in degrees, and the level of detail, the tile X coordinate can be calculated as follows:
     * pixelX = ((longitude + 180) / 360) * 2**level
     * The latitude and longitude are assumed to be on the WGS 84 datum. Even though Bing Maps uses a spherical projection,
     * it’s important to convert all geographic coordinates into a common datum, and WGS 84 was chosen to be that datum.
     * The longitude is assumed to range from -180 to +180 degrees.
     * <p>
     * reference: https://msdn.microsoft.com/en-us/library/bb259689.aspx
     */
    static int longitudeToTileX(double longitude, long mapSize)
    {
        double x = (longitude + 180) / 360;
        return axisToCoordinates(x, mapSize);
    }

    /**
     * Given latitude in degrees, and the level of detail, the tile Y coordinate can be calculated as follows:
     * sinLatitude = sin(latitude * pi/180)
     * pixelY = (0.5 – log((1 + sinLatitude) / (1 – sinLatitude)) / (4 * pi)) * 2**level
     * The latitude and longitude are assumed to be on the WGS 84 datum. Even though Bing Maps uses a spherical projection,
     * it’s important to convert all geographic coordinates into a common datum, and WGS 84 was chosen to be that datum.
     * The latitude must be clipped to range from -85.05112878 to 85.05112878.
     * This avoids a singularity at the poles, and it causes the projected map to be square.
     * <p>
     * reference: https://msdn.microsoft.com/en-us/library/bb259689.aspx
     */
    static int latitudeToTileY(double latitude, long mapSize)
    {
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);
        return axisToCoordinates(y, mapSize);
    }

    /**
     * Take axis and convert it to Tile coordinates
     */
    private static int axisToCoordinates(double axis, long mapSize)
    {
        int tileAxis = (int) clip(axis * mapSize, 0, mapSize - 1);
        return tileAxis / TILE_PIXELS;
    }

    private static List<BingTile> findRawTileCovering(OGCGeometry ogcGeometry, int maxZoom)
    {
        Envelope envelope = getEnvelope(ogcGeometry);
        Optional<List<BingTile>> trivialResult = handleTrivialCases(envelope, maxZoom);
        if (trivialResult.isPresent()) {
            return trivialResult.get();
        }

        accelerateGeometry(
                ogcGeometry, OperatorIntersects.local(), Geometry.GeometryAccelerationDegree.enumMedium);

        Deque<TilingEntry> stack = new ArrayDeque<>();
        Consumer<BingTile> addIntersecting = tile -> {
            TilingEntry tilingEntry = new TilingEntry(tile);
            if (satisfiesTileEdgeCondition(envelope, tilingEntry)
                    && ogcGeometry.intersects(tilingEntry.polygon)) {
                stack.push(tilingEntry);
            }
        };

        // Populate with initial tiles.  Since there aren't many low zoom tiles,
        // and throwing away totally disjoint ones is cheap (envelope check),
        // we might as well start comprehensively.
        ImmutableList.of(
                BingTile.fromCoordinates(0, 0, 1),
                BingTile.fromCoordinates(0, 1, 1),
                BingTile.fromCoordinates(1, 0, 1),
                BingTile.fromCoordinates(1, 1, 1)
        ).forEach(addIntersecting);

        List<BingTile> outputTiles = new ArrayList<>();
        while (!stack.isEmpty()) {
            TilingEntry entry = stack.pop();
            if (entry.tile.getZoomLevel() == maxZoom || ogcGeometry.contains(entry.polygon)) {
                outputTiles.add(entry.tile);
            }
            else {
                entry.tile.findChildren().forEach(addIntersecting);
                checkCondition(
                        outputTiles.size() + stack.size() <= MAX_COVERING_COUNT,
                        "The zoom level is too high or the geometry is too large to compute a set " +
                                "of covering Bing tiles. Please use a lower zoom level, or tile only a section " +
                                "of the geometry.");
            }
        }
        return outputTiles;
    }

    private static Optional<List<BingTile>> handleTrivialCases(Envelope envelope, int zoom)
    {
        checkZoomLevel(zoom);

        if (envelope.isEmpty()) {
            return Optional.of(ImmutableList.of());
        }
        checkLatitude(envelope.getYMin(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLatitude(envelope.getYMax(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMin(), LONGITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMax(), LONGITUDE_SPAN_OUT_OF_RANGE);

        if (zoom == 0) {
            return Optional.of(ImmutableList.of(BingTile.fromCoordinates(0, 0, 0)));
        }
        if (envelope.getXMax() == envelope.getXMin() && envelope.getYMax() == envelope.getYMin()) {
            return Optional.of(ImmutableList.of(latitudeLongitudeToTile(envelope.getYMax(), envelope.getXMax(), zoom)));
        }

        return Optional.empty();
    }

    /*
     * BingTiles don't contain their eastern/southern edges, so that each point lies
     * on a unique tile.  However, the easternmost and southernmost tiles must contain
     * their eastern and southern bounds (respectively), because they are the only
     * tiles that can.
     */
    private static boolean satisfiesTileEdgeCondition(Envelope query, TilingEntry entry)
    {
        BingTile tile = entry.tile;
        int maxXY = (1 << tile.getZoomLevel()) - 1;
        if (tile.getY() < maxXY && query.getYMax() == entry.envelope.getYMin()) {
            return false;
        }
        if (tile.getX() < maxXY && query.getXMin() == entry.envelope.getXMax()) {
            return false;
        }
        return true;
    }

    /**
     * Find a minimal set of BingTiles (at different zooms), covering the geometry.
     * If a larger tile fits within the geometry, do not split it into smaller
     * tiles.  Do not split a tile past maxZoom.
     */
    public static List<BingTile> findDissolvedTileCovering(OGCGeometry ogcGeometry, int maxZoom)
    {
        /*
         * Define "full siblings" to be found distinct ties with the same parent -- ie, the parent's children.
         * If we order a set of tiles first by zoom (descending) and then by quadkey, any siblings will adjacent
         * to each other.  If we put the tiles on a min-heap (so head is the lowest zoom, first quadkey), then if
         * the next four elements are full siblings, we can coalesce them into a parent which we put back on the
         * heap.  If the first items in the heap are not full siblings, there is no possibility to make full siblings
         * (since we are starting with highest zoom first), so we can add them to the results.
         *
         * In the end, we will have a fully dissolved set of tiles.
         */

        List<BingTile> rawTiles = findRawTileCovering(ogcGeometry, maxZoom);
        if (rawTiles.isEmpty()) {
            return rawTiles;
        }
        List<BingTile> results = new ArrayList<>(rawTiles.size());

        Set<BingTile> candidates = new HashSet<>(4);
        Comparator<BingTile> tileComparator = Comparator
                .comparing(BingTile::getZoomLevel)
                .thenComparing(BingTile::toQuadKey)
                .reversed();
        PriorityQueue<BingTile> queue = new PriorityQueue<>(rawTiles.size(), tileComparator);
        queue.addAll(rawTiles);
        while (!queue.isEmpty()) {
            BingTile candidate = queue.poll();
            if (candidate.getZoomLevel() == 0) {
                results.add(candidate);
                continue;
            }

            BingTile parent = candidate.findParent();
            candidates.add(candidate);
            while (!queue.isEmpty() && queue.peek().findParent().equals(parent)) {
                candidates.add(queue.poll());
            }
            if (candidates.size() == 4) {
                // We have a complete set!
                queue.add(parent);
            }
            else {
                results.addAll(candidates);
            }
            candidates.clear();
        }

        return results;
    }

    /**
     * Find a minimal set of BingTiles (at zoom), covering the geometry.
     */
    public static List<BingTile> findMinimalTileCovering(OGCGeometry ogcGeometry, int zoom)
    {
        List<BingTile> outputTiles = new ArrayList<>();
        Deque<BingTile> stack = new ArrayDeque<>(findRawTileCovering(ogcGeometry, zoom));
        while (!stack.isEmpty()) {
            BingTile thisTile = stack.pop();
            outputTiles.addAll(thisTile.findChildren(zoom));
            checkCondition(
                    outputTiles.size() + stack.size() <= MAX_COVERING_COUNT,
                    "The zoom level is too high or the geometry is too large to compute a set " +
                            "of covering Bing tiles. Please use a lower zoom level, or tile only a section " +
                            "of the geometry.");
        }
        return outputTiles;
    }

    public static List<BingTile> findMinimalTileCovering(Envelope envelope, int zoom)
    {
        Optional<List<BingTile>> maybeResult = handleTrivialCases(envelope, zoom);
        if (maybeResult.isPresent()) {
            return maybeResult.get();
        }

        // envelope x,y (longitude,latitude) goes NE as they increase.
        // tile x,y goes SE as they increase
        BingTile seTile = BingTileUtils.latitudeLongitudeToTile(envelope.getYMin(), envelope.getXMax(), zoom);
        BingTile nwTile = BingTileUtils.latitudeLongitudeToTile(envelope.getYMax(), envelope.getXMin(), zoom);
        int minY = nwTile.getY();
        int minX = nwTile.getX();
        int maxY = seTile.getY();
        int maxX = seTile.getX();

        int numTiles = (maxX - minX + 1) * (maxY - minY + 1);
        checkCondition(
                numTiles <= MAX_COVERING_COUNT,
                "The zoom level is too high or the geometry is too large to compute a set " +
                        "of covering Bing tiles. Please use a lower zoom level, or tile only a section " +
                        "of the geometry.");

        List<BingTile> results = new ArrayList<>(numTiles);
        for (int y = minY; y <= maxY; ++y) {
            for (int x = minX; x <= maxX; ++x) {
                results.add(BingTile.fromCoordinates(x, y, zoom));
            }
        }

        return results;
    }

    private static class TilingEntry
    {
        private final BingTile tile;
        private final Envelope envelope;
        private final OGCGeometry polygon;

        public TilingEntry(BingTile tile)
        {
            this.tile = tile;
            this.envelope = tileToEnvelope(tile);
            this.polygon = OGCGeometry.createFromEsriGeometry(envelope, null);
        }
    }
}
