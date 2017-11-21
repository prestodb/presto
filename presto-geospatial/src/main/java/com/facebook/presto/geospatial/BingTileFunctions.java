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
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.base.Verify;
import io.airlift.slice.Slice;

import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.geospatial.GeometryUtils.deserialize;
import static com.facebook.presto.geospatial.GeometryUtils.serialize;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * A set of functions to convert between geometries and Bing tiles.
 *
 * @see <a href="https://msdn.microsoft.com/en-us/library/bb259689.aspx">https://msdn.microsoft.com/en-us/library/bb259689.aspx</a>
 * for the description of the Bing tiles.
 */
public class BingTileFunctions
{
    private static final int TILE_PIXELS = 256;
    private static final double MAX_LATITUDE = 85.05112878;
    private static final double MIN_LATITUDE = -85.05112878;
    private static final double MIN_LONGITUDE = -180;
    private static final double MAX_LONGITUDE = 180;
    private static final int OPTIMIZED_TILING_MIN_ZOOM_LEVEL = 10;

    private static final String LATITUDE_OUT_OF_RANGE = "Latitude must be between " + MIN_LATITUDE + " and " + MAX_LATITUDE;
    private static final String LATITUDE_SPAN_OUT_OF_RANGE = String.format("Latitude span for the geometry must be in [%.2f, %.2f] range", MIN_LATITUDE, MAX_LATITUDE);
    private static final String LONGITUDE_OUT_OF_RANGE = "Longitude must be between " + MIN_LONGITUDE + " and " + MAX_LONGITUDE;
    private static final String LONGITUDE_SPAN_OUT_OF_RANGE = String.format("Longitude span for the geometry must be in [%.2f, %.2f] range", MIN_LONGITUDE, MAX_LONGITUDE);
    private static final String QUAD_KEY_EMPTY = "QuadKey must not be empty string";
    private static final String QUAD_KEY_TOO_LONG = "QuadKey must be " + MAX_ZOOM_LEVEL + " characters or less";
    private static final String ZOOM_LEVEL_TOO_SMALL = "Zoom level must be > 0";
    private static final String ZOOM_LEVEL_TOO_LARGE = "Zoom level must be <= " + MAX_ZOOM_LEVEL;

    private BingTileFunctions() {}

    @Description("Creates a Bing tile from XY coordinates and zoom level")
    @ScalarFunction("bing_tile")
    @SqlType(BingTileType.NAME)
    public static long toBingTile(@SqlType(StandardTypes.INTEGER) long tileX, @SqlType(StandardTypes.INTEGER) long tileY, @SqlType(StandardTypes.INTEGER) long zoomLevel)
    {
        checkZoomLevel(zoomLevel);
        checkCoordinate(tileX, zoomLevel);
        checkCoordinate(tileY, zoomLevel);

        return BingTile.fromCoordinates(toIntExact(tileX), toIntExact(tileY), toIntExact(zoomLevel)).encode();
    }

    @Description("Given a Bing tile, returns its QuadKey")
    @ScalarFunction("bing_tile_quadkey")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toQuadKey(@SqlType(BingTileType.NAME) long input)
    {
        return utf8Slice(BingTile.decode(input).toQuadKey());
    }

    @Description("Given a Bing tile, returns XY coordinates of the tile")
    @ScalarFunction("bing_tile_coordinates")
    @SqlType("row(x integer,y integer)")
    public static Block bingTileCoordinates(@SqlType(BingTileType.NAME) long input)
    {
        BingTile tile = BingTile.decode(input);

        BlockBuilder tileBlockBuilder = INTEGER.createBlockBuilder(new BlockBuilderStatus(), 2);
        INTEGER.writeLong(tileBlockBuilder, tile.getX());
        INTEGER.writeLong(tileBlockBuilder, tile.getY());

        return tileBlockBuilder.build();
    }

    @Description("Given a Bing tile, returns zoom level of the tile")
    @ScalarFunction("bing_tile_zoom_level")
    @SqlType(StandardTypes.TINYINT)
    public static long bingTileZoomLevel(@SqlType(BingTileType.NAME) long input)
    {
        return BingTile.decode(input).getZoomLevel();
    }

    @Description("Creates a Bing tile from a QuadKey")
    @ScalarFunction("bing_tile")
    @SqlType(BingTileType.NAME)
    public static long toBingTile(@SqlType(StandardTypes.VARCHAR) Slice quadKey)
    {
        checkQuadKey(quadKey);
        return BingTile.fromQuadKey(quadKey.toStringUtf8()).encode();
    }

    @Description("Given a (longitude, latitude) point, returns the containing Bing tile at the specified zoom level")
    @ScalarFunction("bing_tile_at")
    @SqlType(BingTileType.NAME)
    public static long bingTileAt(
            @SqlType(StandardTypes.DOUBLE) double latitude,
            @SqlType(StandardTypes.DOUBLE) double longitude,
            @SqlType(StandardTypes.INTEGER) long zoomLevel)
    {
        checkLatitude(latitude, LATITUDE_OUT_OF_RANGE);
        checkLongitude(longitude, LONGITUDE_OUT_OF_RANGE);
        checkZoomLevel(zoomLevel);

        return latitudeLongitudeToTile(latitude, longitude, toIntExact(zoomLevel)).encode();
    }

    @Description("Given a Bing tile, returns the polygon representation of the tile")
    @ScalarFunction("bing_tile_polygon")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice bingTilePolygon(@SqlType(BingTileType.NAME) long input)
    {
        BingTile tile = BingTile.decode(input);

        return serialize(tileToPolygon(tile));
    }

    @Description("Given a geometry and a zoom level, returns the minimum set of Bing tiles that fully covers that geometry")
    @ScalarFunction("geometry_to_bing_tiles")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block geometryToBingTiles(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(StandardTypes.INTEGER) long zoomLevelInput)
    {
        checkZoomLevel(zoomLevelInput);

        int zoomLevel = toIntExact(zoomLevelInput);

        OGCGeometry geometry = deserialize(input);
        checkCondition(!geometry.isEmpty(), "Input geometry must not be empty");

        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);

        checkLatitude(envelope.getYMin(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLatitude(envelope.getYMax(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMin(), LONGITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMax(), LONGITUDE_SPAN_OUT_OF_RANGE);

        boolean pointOrRectangle = isPointOrRectangle(geometry, envelope);

        BingTile leftUpperTile = latitudeLongitudeToTile(envelope.getYMax(), envelope.getXMin(), zoomLevel);
        BingTile rightLowerTile = latitudeLongitudeToTile(envelope.getYMin(), envelope.getXMax(), zoomLevel);

        // XY coordinates start at (0,0) in the left upper corner and increase left to right and top to bottom
        int tileCount = toIntExact((rightLowerTile.getX() - leftUpperTile.getX() + 1) * (rightLowerTile.getY() - leftUpperTile.getY() + 1));

        checkGeometryToBingTilesLimits(geometry, pointOrRectangle, tileCount);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, tileCount);
        if (pointOrRectangle || zoomLevel <= OPTIMIZED_TILING_MIN_ZOOM_LEVEL) {
            // Collect tiles covering the bounding box and check each tile for intersection with the geometry.
            // Skip intersection check if geometry is a point or rectangle. In these cases, by definition,
            // all tiles covering the bounding box intersect the geometry.
            for (int x = leftUpperTile.getX(); x <= rightLowerTile.getX(); x++) {
                for (int y = leftUpperTile.getY(); y <= rightLowerTile.getY(); y++) {
                    BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                    if (pointOrRectangle || !tileToPolygon(tile).disjoint(geometry)) {
                        BIGINT.writeLong(blockBuilder, tile.encode());
                    }
                }
            }
        }
        else {
            // Intersection checks above are expensive. The logic below attempts to reduce the number
            // of these checks. The idea is to identify large tiles which are fully covered by the
            // geometry. For each such tile, we can cheaply compute all the containing tiles at
            // the right zoom level and append them to results in bulk. This way we perform a single
            // containment check instead of 2 to the power of level delta intersection checks, where
            // level delta is the difference between the desired zoom level and level of the large
            // tile covered by the geometry.
            BingTile[] tiles = getTilesInBetween(leftUpperTile, rightLowerTile, OPTIMIZED_TILING_MIN_ZOOM_LEVEL);
            for (BingTile tile : tiles) {
                writeTilesToBlockBuilder(geometry, zoomLevel, tile, blockBuilder);
            }
        }

        return blockBuilder.build();
    }

    private static void checkGeometryToBingTilesLimits(OGCGeometry geometry, boolean pointOrRectangle, int tileCount)
    {
        if (pointOrRectangle) {
            checkCondition(tileCount <= 1_000_000, "The number of input tiles is too large (more than 1M) to compute a set of covering bing tiles.");
        }
        else {
            long complexity = tileCount * getPointCount(geometry.getEsriGeometry());
            checkCondition(complexity <= 25_000_000, "The zoom level is too high or the geometry is too complex to compute a set of covering bing tiles. " +
                    "Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
        }
    }

    private static BingTile[] getTilesInBetween(BingTile leftUpperTile, BingTile rightLowerTile, int zoomLevel)
    {
        checkArgument(leftUpperTile.getZoomLevel() == rightLowerTile.getZoomLevel());
        checkArgument(leftUpperTile.getZoomLevel() > zoomLevel);

        int divisor = 1 << (leftUpperTile.getZoomLevel() - zoomLevel);
        int minX = (int) Math.floor(leftUpperTile.getX() / divisor);
        int maxX = (int) Math.floor(rightLowerTile.getX() / divisor);
        int minY = (int) Math.floor(leftUpperTile.getY() / divisor);
        int maxY = (int) Math.floor(rightLowerTile.getY() / divisor);

        BingTile[] tiles = new BingTile[(maxX - minX + 1) * (maxY - minY + 1)];
        int index = 0;
        for (int x = minX; x <= maxX; x++) {
            for (int y = minY; y <= maxY; y++) {
                tiles[index] = BingTile.fromCoordinates(x, y, OPTIMIZED_TILING_MIN_ZOOM_LEVEL);
                index++;
            }
        }

        return tiles;
    }

    /**
     * Identifies a minimum set of tiles at specified zoom level that cover intersection of the
     * specified geometry and a specified tile of the same or lower level. Adds tiles to provided
     * BlockBuilder.
     */
    private static void writeTilesToBlockBuilder(
            OGCGeometry geometry,
            int zoomLevel,
            BingTile tile,
            BlockBuilder blockBuilder)
    {
        int tileZoomLevel = tile.getZoomLevel();
        checkArgument(tile.getZoomLevel() <= zoomLevel);

        OGCGeometry polygon = tileToPolygon(tile);
        if (tileZoomLevel == zoomLevel) {
            if (!geometry.disjoint(polygon)) {
                BIGINT.writeLong(blockBuilder, tile.encode());
            }
            return;
        }

        if (geometry.contains(polygon)) {
            int subTileCount = 1 << (zoomLevel - tileZoomLevel);
            int minX = subTileCount * tile.getX();
            int minY = subTileCount * tile.getY();
            for (int x = minX; x < minX + subTileCount; x++) {
                for (int y = minY; y < minY + subTileCount; y++) {
                    BIGINT.writeLong(blockBuilder, BingTile.fromCoordinates(x, y, zoomLevel).encode());
                }
            }
            return;
        }

        if (geometry.disjoint(polygon)) {
            return;
        }

        int minX = 2 * tile.getX();
        int minY = 2 * tile.getY();
        int nextZoomLevel = tileZoomLevel + 1;
        Verify.verify(nextZoomLevel <= MAX_ZOOM_LEVEL);
        for (int x = minX; x < minX + 2; x++) {
            for (int y = minY; y < minY + 2; y++) {
                writeTilesToBlockBuilder(
                        geometry,
                        zoomLevel,
                        BingTile.fromCoordinates(x, y, nextZoomLevel),
                        blockBuilder);
            }
        }
    }

    private static int getPointCount(Geometry geometry)
    {
        if (geometry instanceof Point) {
            return 1;
        }

        return ((MultiVertexGeometry) geometry).getPointCount();
    }

    private static Point tileXYToLatitudeLongitude(int tileX, int tileY, int zoomLevel)
    {
        int mapSize = mapSize(zoomLevel);
        double x = (clip(tileX * TILE_PIXELS, 0, mapSize) / mapSize) - 0.5;
        double y = 0.5 - (clip(tileY * TILE_PIXELS, 0, mapSize) / mapSize);

        double latitude = 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
        double longitude = 360 * x;
        return new Point(longitude, latitude);
    }

    private static BingTile latitudeLongitudeToTile(double latitude, double longitude, int zoomLevel)
    {
        double x = (longitude + 180) / 360;
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        int mapSize = mapSize(zoomLevel);
        int tileX = (int) clip(x * mapSize, 0, mapSize - 1);
        int tileY = (int) clip(y * mapSize, 0, mapSize - 1);
        return BingTile.fromCoordinates(tileX / TILE_PIXELS, tileY / TILE_PIXELS, zoomLevel);
    }

    private static OGCGeometry tileToPolygon(BingTile tile)
    {
        Point upperLeftCorner = tileXYToLatitudeLongitude(tile.getX(), tile.getY(), tile.getZoomLevel());
        Point lowerRightCorner = tileXYToLatitudeLongitude(tile.getX() + 1, tile.getY() + 1, tile.getZoomLevel());

        Polyline boundary = new Polyline();
        boundary.startPath(upperLeftCorner);
        boundary.lineTo(lowerRightCorner.getX(), upperLeftCorner.getY());
        boundary.lineTo(lowerRightCorner);
        boundary.lineTo(upperLeftCorner.getX(), lowerRightCorner.getY());

        Polygon polygon = new Polygon();
        polygon.add(boundary, false);

        return OGCGeometry.createFromEsriGeometry(polygon, null);
    }

    /**
     * @return true if the geometry is a point or a rectangle
     */
    private static boolean isPointOrRectangle(OGCGeometry geometry, Envelope envelope)
    {
        if (geometry instanceof OGCPoint) {
            return true;
        }

        if (!(geometry instanceof OGCPolygon)) {
            return false;
        }

        OGCPolygon polygon = (OGCPolygon) geometry;
        if (polygon.numInteriorRing() > 0) {
            return false;
        }

        MultiVertexGeometry multiVertexGeometry = (MultiVertexGeometry) polygon.getEsriGeometry();
        if (multiVertexGeometry.getPointCount() != 4) {
            return false;
        }

        Set<Point> corners = new HashSet<>();
        corners.add(new Point(envelope.getXMin(), envelope.getYMin()));
        corners.add(new Point(envelope.getXMin(), envelope.getYMax()));
        corners.add(new Point(envelope.getXMax(), envelope.getYMin()));
        corners.add(new Point(envelope.getXMax(), envelope.getYMax()));

        for (int i = 0; i < 4; i++) {
            Point point = multiVertexGeometry.getPoint(i);
            if (!corners.contains(point)) {
                return false;
            }
        }

        return true;
    }

    private static void checkZoomLevel(long zoomLevel)
    {
        checkCondition(zoomLevel > 0, ZOOM_LEVEL_TOO_SMALL);
        checkCondition(zoomLevel <= MAX_ZOOM_LEVEL, ZOOM_LEVEL_TOO_LARGE);
    }

    private static void checkCoordinate(long coordinate, long zoomLevel)
    {
        checkCondition(coordinate >= 0 && coordinate < (1 << zoomLevel), "XY coordinates for a Bing tile at zoom level %s must be within [0, %s) range", zoomLevel, 1 << zoomLevel);
    }

    private static void checkQuadKey(@SqlType(StandardTypes.VARCHAR) Slice quadkey)
    {
        checkCondition(quadkey.length() > 0, QUAD_KEY_EMPTY);
        checkCondition(quadkey.length() <= MAX_ZOOM_LEVEL, QUAD_KEY_TOO_LONG);
    }

    private static void checkLatitude(double latitude, String errorMessage)
    {
        checkCondition(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE, errorMessage);
    }

    private static void checkLongitude(double longitude, String errorMessage)
    {
        checkCondition(longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE, errorMessage);
    }

    private static void checkCondition(boolean condition, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format(formatString, args));
        }
    }

    private static double clip(double n, double minValue, double maxValue)
    {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    private static int mapSize(int zoomLevel)
    {
        return 256 << zoomLevel;
    }
}
