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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

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
    static int longitudeToTileX(double longitude, long mapSize)
    {
        double x = (longitude + 180) / 360;
        return axisToCoordinates(x, mapSize);
    }

    static int longitudeToTileY(double latitude, long mapSize)
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
}
