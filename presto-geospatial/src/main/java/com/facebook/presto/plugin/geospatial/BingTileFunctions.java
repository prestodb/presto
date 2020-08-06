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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.geospatial.serde.GeometrySerializationType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.geospatial.GeometryUtils.contains;
import static com.facebook.presto.geospatial.GeometryUtils.disjoint;
import static com.facebook.presto.geospatial.GeometryUtils.isPointOrRectangle;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserialize;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeType;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.serialize;
import static com.facebook.presto.plugin.geospatial.BingTile.MAX_ZOOM_LEVEL;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.EARTH_RADIUS_KM;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.LATITUDE_OUT_OF_RANGE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.LONGITUDE_OUT_OF_RANGE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MAX_LONGITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LATITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.MIN_LONGITUDE;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.TILE_PIXELS;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkCondition;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkCoordinate;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkLatitude;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkLongitude;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkQuadKey;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.checkZoomLevel;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.findDissolvedTileCovering;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.findMinimalTileCovering;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.latitudeLongitudeToTile;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.latitudeToTileY;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.longitudeToTileX;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.mapSize;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.tileToEnvelope;
import static com.facebook.presto.plugin.geospatial.BingTileUtils.tileXYToLatitudeLongitude;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.asin;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
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
    private static final int OPTIMIZED_TILING_MIN_ZOOM_LEVEL = 10;
    private static final Block EMPTY_TILE_ARRAY = BIGINT.createFixedSizeBlockBuilder(0).build();

    private BingTileFunctions() {}

    @Description("Encodes a Bing tile into a bigint")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(BingTileType.NAME) long tile)
    {
        return tile;
    }

    @Description("Decodes a Bing tile from a bigint")
    @ScalarOperator(CAST)
    @SqlType(BingTileType.NAME)
    public static long castFromBigint(@SqlType(StandardTypes.BIGINT) long tile)
    {
        try {
            BingTile.decode(tile);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    format("Invalid bigint tile encoding: %s", tile));
        }
        return tile;
    }

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

    @Description("Given a (latitude, longitude) point, returns the containing Bing tile at the specified zoom level")
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
        int tileY = latitudeToTileY(latitude, mapSize);

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

        int tileY = latitudeToTileY(latitude, mapSize);
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
                "The number of tiles covering input rectangle exceeds the limit of 1M. Number of tiles: %d. Radius: %.1f km. Zoom level: %d.",
                totalTileCount, radiusInKm, zoomLevel);

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

    @Description("Return the parent for a Bing tile")
    @ScalarFunction("bing_tile_parent")
    @SqlType(BingTileType.NAME)
    public static long bingTileParent(@SqlType(BingTileType.NAME) long input)
    {
        BingTile tile = BingTile.decode(input);
        try {
            return tile.findParent().encode();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Return the parent for the given zoom level for a Bing tile")
    @ScalarFunction("bing_tile_parent")
    @SqlType(BingTileType.NAME)
    public static long bingTileParent(@SqlType(BingTileType.NAME) long input, @SqlType(StandardTypes.INTEGER) long newZoom)
    {
        BingTile tile = BingTile.decode(input);
        try {
            return tile.findParent(toIntExact(newZoom)).encode();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Return the children for a Bing tile")
    @ScalarFunction("bing_tile_children")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block bingTileChildren(@SqlType(BingTileType.NAME) long input)
    {
        BingTile tile = BingTile.decode(input);
        try {
            List<BingTile> children = tile.findChildren();
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, children.size());
            children.stream().forEach(child -> BIGINT.writeLong(blockBuilder, child.encode()));
            return blockBuilder.build();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Return the children for the given zoom level for a Bing tile")
    @ScalarFunction("bing_tile_children")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block bingTileChildren(@SqlType(BingTileType.NAME) long input, @SqlType(StandardTypes.INTEGER) long newZoom)
    {
        BingTile tile = BingTile.decode(input);
        try {
            List<BingTile> children = tile.findChildren(toIntExact(newZoom));
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, children.size());
            children.stream().forEach(child -> BIGINT.writeLong(blockBuilder, child.encode()));
            return blockBuilder.build();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @Description("Given a geometry and a maximum zoom level, returns the minimum set of dissolved Bing tiles that fully covers that geometry")
    @ScalarFunction("geometry_to_dissolved_bing_tiles")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block geometryToDissolvedBingTiles(
            @SqlType(GEOMETRY_TYPE_NAME) Slice input,
            @SqlType(StandardTypes.INTEGER) long maxZoomLevel)
    {
        OGCGeometry ogcGeometry = deserialize(input);
        if (ogcGeometry.isEmpty()) {
            return EMPTY_TILE_ARRAY;
        }

        List<BingTile> covering = findDissolvedTileCovering(ogcGeometry, toIntExact(maxZoomLevel));
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, covering.size());
        for (BingTile tile : covering) {
            BIGINT.writeLong(blockBuilder, tile.encode());
        }

        return blockBuilder.build();
    }

    @Description("Given a geometry and a zoom level, returns the minimum set of Bing tiles of that zoom level that fully covers that geometry")
    @ScalarFunction("geometry_to_bing_tiles")
    @SqlType("array(" + BingTileType.NAME + ")")
    public static Block geometryToBingTiles(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(StandardTypes.INTEGER) long zoomLevelInput)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isEmpty()) {
            return EMPTY_TILE_ARRAY;
        }

        int zoomLevel = toIntExact(zoomLevelInput);
        GeometrySerializationType type = deserializeType(input);
        List<BingTile> covering;
        if (type == GeometrySerializationType.POINT || type == GeometrySerializationType.ENVELOPE) {
            covering = findMinimalTileCovering(envelope, zoomLevel);
        }
        else {
            OGCGeometry ogcGeometry = deserialize(input);
            if (isPointOrRectangle(ogcGeometry, envelope)) {
                covering = findMinimalTileCovering(envelope, zoomLevel);
            }
            else {
                covering = findMinimalTileCovering(ogcGeometry, zoomLevel);
            }
        }

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, covering.size());
        for (BingTile tile : covering) {
            BIGINT.writeLong(blockBuilder, tile.encode());
        }

        return blockBuilder.build();
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
}
