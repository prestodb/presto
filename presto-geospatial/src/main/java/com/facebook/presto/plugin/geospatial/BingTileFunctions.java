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
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.geospatial.GeometryUtils.contains;
import static com.facebook.presto.geospatial.GeometryUtils.disjoint;
import static com.facebook.presto.geospatial.GeometryUtils.getEnvelope;
import static com.facebook.presto.geospatial.GeometryUtils.getPointCount;
import static com.facebook.presto.geospatial.GeometryUtils.isPointOrRectangle;
import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.geospatial.serde.GeometrySerde.serialize;
import static com.facebook.presto.plugin.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.asin;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toDegrees;
import static java.lang.Math.toIntExact;
import static java.lang.Math.toRadians;
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
    private static final double EARTH_RADIUS_KM = 6371.01;
    private static final int OPTIMIZED_TILING_MIN_ZOOM_LEVEL = 10;
    private static final Block EMPTY_TILE_ARRAY = BIGINT.createFixedSizeBlockBuilder(0).build();

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
    public static final class BingTileCoordinatesFunction
    {
        private static final RowType BING_TILE_COORDINATES_ROW_TYPE = RowType.anonymous(ImmutableList.of(INTEGER, INTEGER));

        private final PageBuilder pageBuilder;

        public BingTileCoordinatesFunction()
        {
            pageBuilder = new PageBuilder(ImmutableList.of(BING_TILE_COORDINATES_ROW_TYPE));
        }

        @SqlType("row(x integer,y integer)")
        public Block bingTileCoordinates(@SqlType(BingTileType.NAME) long input)
        {
            if (pageBuilder.isFull()) {
                pageBuilder.reset();
            }
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            BingTile tile = BingTile.decode(input);
            BlockBuilder tileBlockBuilder = blockBuilder.beginBlockEntry();
            INTEGER.writeLong(tileBlockBuilder, tile.getX());
            INTEGER.writeLong(tileBlockBuilder, tile.getY());
            blockBuilder.closeEntry();
            pageBuilder.declarePosition();

            return BING_TILE_COORDINATES_ROW_TYPE.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
        }
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

    @Description("Given a (longitude, latitude) point, returns the surrounding Bing tiles at the specified zoom level")
    @ScalarFunction("bing_tiles_around")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block bingTilesAround(
            @SqlType(StandardTypes.DOUBLE) double latitude,
            @SqlType(StandardTypes.DOUBLE) double longitude,
            @SqlType(StandardTypes.INTEGER) long zoomLevel)
    {
        checkLatitude(latitude, LATITUDE_OUT_OF_RANGE);
        checkLongitude(longitude, LONGITUDE_OUT_OF_RANGE);
        checkZoomLevel(zoomLevel);

        long mapSize = mapSize(toIntExact(zoomLevel));
        long maxTileIndex = (mapSize / TILE_PIXELS) - 1;

        int tileX = longitudeToTileX(longitude, mapSize);
        int tileY = longitudeToTileY(latitude, mapSize);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 9);
        for (int i = -1; i <= 1; i++) {
            for (int j = -1; j <= 1; j++) {
                int x = tileX + i;
                int y = tileY + j;
                if (x >= 0 && x <= maxTileIndex && y >= 0 && y <= maxTileIndex) {
                    BIGINT.writeLong(blockBuilder, BingTile.fromCoordinates(x, y, toIntExact(zoomLevel)).encode());
                }
            }
        }
        return blockBuilder.build();
    }

    @Description("Given a (latitude, longitude) point, a radius in kilometers and a zoom level, " +
            "returns a minimum set of Bing tiles at specified zoom level that cover a circle of " +
            "specified radius around the specified point.")
    @ScalarFunction("bing_tiles_around")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block bingTilesAround(
            @SqlType(StandardTypes.DOUBLE) double latitude,
            @SqlType(StandardTypes.DOUBLE) double longitude,
            @SqlType(StandardTypes.INTEGER) long zoomLevelAsLong,
            @SqlType(StandardTypes.DOUBLE) double radiusInKm)
    {
        checkLatitude(latitude, LATITUDE_OUT_OF_RANGE);
        checkLongitude(longitude, LONGITUDE_OUT_OF_RANGE);
        checkZoomLevel(zoomLevelAsLong);
        checkCondition(radiusInKm >= 0, "Radius must be >= 0");
        checkCondition(radiusInKm <= 1_000, "Radius must be <= 1,000 km");

        int zoomLevel = toIntExact(zoomLevelAsLong);
        long mapSize = mapSize(zoomLevel);
        int maxTileIndex = (int) (mapSize / TILE_PIXELS) - 1;

        int tileY = longitudeToTileY(latitude, mapSize);
        int tileX = longitudeToTileX(longitude, mapSize);

        // Find top, bottom, left and right tiles from center of circle
        double topLatitude = addDistanceToLatitude(latitude, radiusInKm, 0);
        BingTile topTile = latitudeLongitudeToTile(topLatitude, longitude, zoomLevel);

        double bottomLatitude = addDistanceToLatitude(latitude, radiusInKm, 180);
        BingTile bottomTile = latitudeLongitudeToTile(bottomLatitude, longitude, zoomLevel);

        double leftLongitude = addDistanceToLongitude(latitude, longitude, radiusInKm, 270);
        BingTile leftTile = latitudeLongitudeToTile(latitude, leftLongitude, zoomLevel);

        double rightLongitude = addDistanceToLongitude(latitude, longitude, radiusInKm, 90);
        BingTile rightTile = latitudeLongitudeToTile(latitude, rightLongitude, zoomLevel);

        boolean wrapAroundX = rightTile.getX() < leftTile.getX();

        int tileCountX = wrapAroundX ?
                (rightTile.getX() + maxTileIndex - leftTile.getX() + 2) :
                (rightTile.getX() - leftTile.getX() + 1);

        int tileCountY = bottomTile.getY() - topTile.getY() + 1;

        int totalTileCount = tileCountX * tileCountY;
        checkCondition(totalTileCount <= 1_000_000,
                "The number of input tiles is too large (more than 1M) to compute a set of covering Bing tiles.");

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, totalTileCount);

        for (int i = 0; i < tileCountX; i++) {
            int x = (leftTile.getX() + i) % (maxTileIndex + 1);
            BIGINT.writeLong(blockBuilder, BingTile.fromCoordinates(x, tileY, zoomLevel).encode());
        }

        for (int y = topTile.getY(); y <= bottomTile.getY(); y++) {
            if (y != tileY) {
                BIGINT.writeLong(blockBuilder, BingTile.fromCoordinates(tileX, y, zoomLevel).encode());
            }
        }

        GreatCircleDistanceToPoint distanceToCenter = new GreatCircleDistanceToPoint(latitude, longitude);

        // Remove tiles from each corner if they are outside the radius
        for (int x = rightTile.getX(); x != tileX; x = (x == 0) ? maxTileIndex : x - 1) {
            // Top Right Corner
            boolean include = false;
            for (int y = topTile.getY(); y < tileY; y++) {
                BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                if (include) {
                    BIGINT.writeLong(blockBuilder, tile.encode());
                }
                else {
                    Point bottomLeftCorner = tileXYToLatitudeLongitude(tile.getX(), tile.getY() + 1, tile.getZoomLevel());
                    if (withinDistance(distanceToCenter, radiusInKm, bottomLeftCorner)) {
                        include = true;
                        BIGINT.writeLong(blockBuilder, tile.encode());
                    }
                }
            }

            // Bottom Right Corner
            include = false;
            for (int y = bottomTile.getY(); y > tileY; y--) {
                BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                if (include) {
                    BIGINT.writeLong(blockBuilder, tile.encode());
                }
                else {
                    Point topLeftCorner = tileXYToLatitudeLongitude(tile.getX(), tile.getY(), tile.getZoomLevel());
                    if (withinDistance(distanceToCenter, radiusInKm, topLeftCorner)) {
                        include = true;
                        BIGINT.writeLong(blockBuilder, tile.encode());
                    }
                }
            }
        }

        for (int x = leftTile.getX(); x != tileX; x = (x + 1) % (maxTileIndex + 1)) {
            // Top Left Corner
            boolean include = false;
            for (int y = topTile.getY(); y < tileY; y++) {
                BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                if (include) {
                    BIGINT.writeLong(blockBuilder, tile.encode());
                }
                else {
                    Point bottomRightCorner = tileXYToLatitudeLongitude(tile.getX() + 1, tile.getY() + 1, tile.getZoomLevel());
                    if (withinDistance(distanceToCenter, radiusInKm, bottomRightCorner)) {
                        include = true;
                        BIGINT.writeLong(blockBuilder, tile.encode());
                    }
                }
            }

            // Bottom Left Corner
            include = false;
            for (int y = bottomTile.getY(); y > tileY; y--) {
                BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                if (include) {
                    BIGINT.writeLong(blockBuilder, tile.encode());
                }
                else {
                    Point topRightCorner = tileXYToLatitudeLongitude(tile.getX() + 1, tile.getY(), tile.getZoomLevel());
                    if (withinDistance(distanceToCenter, radiusInKm, topRightCorner)) {
                        include = true;
                        BIGINT.writeLong(blockBuilder, tile.encode());
                    }
                }
            }
        }

        return blockBuilder.build();
    }

    @Description("Given a Bing tile, returns the polygon representation of the tile")
    @ScalarFunction("bing_tile_polygon")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice bingTilePolygon(@SqlType(BingTileType.NAME) long input)
    {
        BingTile tile = BingTile.decode(input);

        return serialize(tileToEnvelope(tile));
    }

    @Description("Given a geometry and a zoom level, returns the minimum set of Bing tiles that fully covers that geometry")
    @ScalarFunction("geometry_to_bing_tiles")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block geometryToBingTiles(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(StandardTypes.INTEGER) long zoomLevelInput)
    {
        checkZoomLevel(zoomLevelInput);

        int zoomLevel = toIntExact(zoomLevelInput);

        OGCGeometry ogcGeometry = deserialize(input);
        if (ogcGeometry.isEmpty()) {
            return EMPTY_TILE_ARRAY;
        }

        Envelope envelope = getEnvelope(ogcGeometry);

        checkLatitude(envelope.getYMin(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLatitude(envelope.getYMax(), LATITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMin(), LONGITUDE_SPAN_OUT_OF_RANGE);
        checkLongitude(envelope.getXMax(), LONGITUDE_SPAN_OUT_OF_RANGE);

        boolean pointOrRectangle = isPointOrRectangle(ogcGeometry, envelope);

        BingTile leftUpperTile = latitudeLongitudeToTile(envelope.getYMax(), envelope.getXMin(), zoomLevel);
        BingTile rightLowerTile = getTileCoveringLowerRightCorner(envelope, zoomLevel);

        // XY coordinates start at (0,0) in the left upper corner and increase left to right and top to bottom
        long tileCount = (long) (rightLowerTile.getX() - leftUpperTile.getX() + 1) * (rightLowerTile.getY() - leftUpperTile.getY() + 1);

        checkGeometryToBingTilesLimits(ogcGeometry, pointOrRectangle, tileCount);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, toIntExact(tileCount));
        if (pointOrRectangle || zoomLevel <= OPTIMIZED_TILING_MIN_ZOOM_LEVEL) {
            // Collect tiles covering the bounding box and check each tile for intersection with the geometry.
            // Skip intersection check if geometry is a point or rectangle. In these cases, by definition,
            // all tiles covering the bounding box intersect the geometry.
            for (int x = leftUpperTile.getX(); x <= rightLowerTile.getX(); x++) {
                for (int y = leftUpperTile.getY(); y <= rightLowerTile.getY(); y++) {
                    BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
                    if (pointOrRectangle || !disjoint(tileToEnvelope(tile), ogcGeometry)) {
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
                appendIntersectingSubtiles(ogcGeometry, zoomLevel, tile, blockBuilder);
            }
        }

        return blockBuilder.build();
    }

    private static BingTile getTileCoveringLowerRightCorner(Envelope envelope, int zoomLevel)
    {
        BingTile tile = latitudeLongitudeToTile(envelope.getYMin(), envelope.getXMax(), zoomLevel);

        // If the tile covering the lower right corner of the envelope overlaps the envelope only
        // at the border then return a tile shifted to the left and/or top
        int deltaX = 0;
        int deltaY = 0;
        Point upperLeftCorner = tileXYToLatitudeLongitude(tile.getX(), tile.getY(), tile.getZoomLevel());
        if (upperLeftCorner.getX() == envelope.getXMax()) {
            deltaX = -1;
        }
        if (upperLeftCorner.getY() == envelope.getYMin()) {
            deltaY = -1;
        }

        if (deltaX != 0 || deltaY != 0) {
            return BingTile.fromCoordinates(tile.getX() + deltaX, tile.getY() + deltaY, tile.getZoomLevel());
        }

        return tile;
    }

    private static void checkGeometryToBingTilesLimits(OGCGeometry ogcGeometry, boolean pointOrRectangle, long tileCount)
    {
        if (pointOrRectangle) {
            checkCondition(tileCount <= 1_000_000, "The number of input tiles is too large (more than 1M) to compute a set of covering Bing tiles.");
        }
        else {
            checkCondition((int) tileCount == tileCount, "The zoom level is too high to compute a set of covering Bing tiles.");
            long complexity = 0;
            try {
                complexity = multiplyExact(tileCount, getPointCount(ogcGeometry));
            }
            catch (ArithmeticException e) {
                checkCondition(false, "The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. " +
                        "Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
            }
            checkCondition(complexity <= 25_000_000, "The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. " +
                    "Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
        }
    }

    private static double addDistanceToLongitude(
            @SqlType(StandardTypes.DOUBLE) double latitude,
            @SqlType(StandardTypes.DOUBLE) double longitude,
            @SqlType(StandardTypes.DOUBLE) double radiusInKm,
            @SqlType(StandardTypes.DOUBLE) double bearing)
    {
        double latitudeInRadians = toRadians(latitude);
        double longitudeInRadians = toRadians(longitude);
        double bearingInRadians = toRadians(bearing);
        double radiusRatio = radiusInKm / EARTH_RADIUS_KM;

        // Haversine formula
        double newLongitude = toDegrees(longitudeInRadians +
                atan2(sin(bearingInRadians) * sin(radiusRatio) * cos(latitudeInRadians),
                        cos(radiusRatio) - sin(latitudeInRadians) * sin(latitudeInRadians)));

        if (newLongitude > MAX_LONGITUDE) {
            return MIN_LONGITUDE + (newLongitude - MAX_LONGITUDE);
        }

        if (newLongitude < MIN_LONGITUDE) {
            return MAX_LONGITUDE + (newLongitude - MIN_LONGITUDE);
        }

        return newLongitude;
    }

    private static double addDistanceToLatitude(
            @SqlType(StandardTypes.DOUBLE) double latitude,
            @SqlType(StandardTypes.DOUBLE) double radiusInKm,
            @SqlType(StandardTypes.DOUBLE) double bearing)
    {
        double latitudeInRadians = toRadians(latitude);
        double bearingInRadians = toRadians(bearing);
        double radiusRatio = radiusInKm / EARTH_RADIUS_KM;

        // Haversine formula
        double newLatitude = toDegrees(asin(sin(latitudeInRadians) * cos(radiusRatio) +
                cos(latitudeInRadians) * sin(radiusRatio) * cos(bearingInRadians)));

        if (newLatitude > MAX_LATITUDE) {
            return MAX_LATITUDE;
        }

        if (newLatitude < MIN_LATITUDE) {
            return MIN_LATITUDE;
        }

        return newLatitude;
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
    private static void appendIntersectingSubtiles(
            OGCGeometry ogcGeometry,
            int zoomLevel,
            BingTile tile,
            BlockBuilder blockBuilder)
    {
        int tileZoomLevel = tile.getZoomLevel();
        checkArgument(tileZoomLevel <= zoomLevel);

        Envelope tileEnvelope = tileToEnvelope(tile);
        if (tileZoomLevel == zoomLevel) {
            if (!disjoint(tileEnvelope, ogcGeometry)) {
                BIGINT.writeLong(blockBuilder, tile.encode());
            }
            return;
        }

        if (contains(ogcGeometry, tileEnvelope)) {
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

        if (disjoint(tileEnvelope, ogcGeometry)) {
            return;
        }

        int minX = 2 * tile.getX();
        int minY = 2 * tile.getY();
        int nextZoomLevel = tileZoomLevel + 1;
        verify(nextZoomLevel <= MAX_ZOOM_LEVEL);
        for (int x = minX; x < minX + 2; x++) {
            for (int y = minY; y < minY + 2; y++) {
                appendIntersectingSubtiles(
                        ogcGeometry,
                        zoomLevel,
                        BingTile.fromCoordinates(x, y, nextZoomLevel),
                        blockBuilder);
            }
        }
    }

    private static Point tileXYToLatitudeLongitude(int tileX, int tileY, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        double x = (clip(tileX * TILE_PIXELS, 0, mapSize) / mapSize) - 0.5;
        double y = 0.5 - (clip(tileY * TILE_PIXELS, 0, mapSize) / mapSize);

        double latitude = 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
        double longitude = 360 * x;
        return new Point(longitude, latitude);
    }

    /**
     * Returns a Bing tile at a given zoom level containing a point at a given latitude and longitude.
     * Latitude must be within [-85.05112878, 85.05112878] range. Longitude must be within [-180, 180] range.
     * Zoom levels from 1 to 23 are supported.
     */
    private static BingTile latitudeLongitudeToTile(double latitude, double longitude, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        int tileX = longitudeToTileX(longitude, mapSize);
        int tileY = longitudeToTileY(latitude, mapSize);
        return BingTile.fromCoordinates(tileX, tileY, zoomLevel);
    }

    /**
     * Given latitude and longitude in degrees, and the level of detail, the pixel XY coordinates can be calculated as follows:
     * sinLatitude = sin(latitude * pi/180)
     * pixelX = ((longitude + 180) / 360) * 256 * 2level
     * pixelY = (0.5 – log((1 + sinLatitude) / (1 – sinLatitude)) / (4 * pi)) * 256 * 2level
     * The latitude and longitude are assumed to be on the WGS 84 datum. Even though Bing Maps uses a spherical projection,
     * it’s important to convert all geographic coordinates into a common datum, and WGS 84 was chosen to be that datum.
     * The longitude is assumed to range from -180 to +180 degrees, and the latitude must be clipped to range from -85.05112878 to 85.05112878.
     * This avoids a singularity at the poles, and it causes the projected map to be square.
     * <p>
     * reference: https://msdn.microsoft.com/en-us/library/bb259689.aspx
     */
    private static int longitudeToTileX(double longitude, long mapSize)
    {
        double x = (longitude + 180) / 360;
        return axisToCoordinates(x, mapSize);
    }

    private static int longitudeToTileY(double latitude, long mapSize)
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

    private static Envelope tileToEnvelope(BingTile tile)
    {
        Point upperLeftCorner = tileXYToLatitudeLongitude(tile.getX(), tile.getY(), tile.getZoomLevel());
        Point lowerRightCorner = tileXYToLatitudeLongitude(tile.getX() + 1, tile.getY() + 1, tile.getZoomLevel());
        return new Envelope(upperLeftCorner.getX(), lowerRightCorner.getY(), lowerRightCorner.getX(), upperLeftCorner.getY());
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

    private static boolean withinDistance(GreatCircleDistanceToPoint distanceFunction, double maxDistance, Point point)
    {
        return distanceFunction.distance(point.getY(), point.getX()) <= maxDistance;
    }

    private static final class GreatCircleDistanceToPoint
    {
        private double sinLatitude;
        private double cosLatitude;
        private double radianLongitude;

        private GreatCircleDistanceToPoint(double latitude, double longitude)
        {
            double radianLatitude = toRadians(latitude);

            this.sinLatitude = sin(radianLatitude);
            this.cosLatitude = cos(radianLatitude);

            this.radianLongitude = toRadians(longitude);
        }

        public double distance(double latitude2, double longitude2)
        {
            double radianLatitude2 = toRadians(latitude2);
            double sin2 = sin(radianLatitude2);
            double cos2 = cos(radianLatitude2);

            double deltaLongitude = radianLongitude - toRadians(longitude2);
            double cosDeltaLongitude = cos(deltaLongitude);

            double t1 = cos2 * sin(deltaLongitude);
            double t2 = cosLatitude * sin2 - sinLatitude * cos2 * cosDeltaLongitude;
            double t3 = sinLatitude * sin2 + cosLatitude * cos2 * cosDeltaLongitude;
            return atan2(sqrt(t1 * t1 + t2 * t2), t3) * EARTH_RADIUS_KM;
        }
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

    private static long mapSize(int zoomLevel)
    {
        return 256L << zoomLevel;
    }
}
