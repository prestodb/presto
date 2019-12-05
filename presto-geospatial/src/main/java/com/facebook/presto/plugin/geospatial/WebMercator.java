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

/**
 * A class to convert {@link BingTile} objects to latitude/longitudes via the
 * Web Mercator projection.
 */
public class WebMercator
{
    static final int TILE_PIXELS = 256;
    static final double MIN_LATITUDE = -85.05112878;
    static final double MAX_LATITUDE = 85.05112878;
    static final double MIN_LONGITUDE = -180;
    static final double MAX_LONGITUDE = 180;
    static final double EARTH_RADIUS_KM = 6371.01;

    private WebMercator() {}

    /**
     * Given a zoomLevel, find the size of the map in pixels.
     *
     * @param zoomLevel
     * @return size of the map in pixels
     */
    public static long mapSize(int zoomLevel)
    {
        return 256L << zoomLevel;
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
    public static int longitudeToTileX(double longitude, long mapSize)
    {
        double x = (longitude + 180) / 360;
        return axisToCoordinates(x, mapSize);
    }

    public static int latitudeToTileY(double latitude, long mapSize)
    {
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);
        return axisToCoordinates(y, mapSize);
    }

    /**
     * Returns a Bing tile at a given zoom level containing a point at a given latitude and longitude.
     * Latitude must be within [-85.05112878, 85.05112878] range. Longitude must be within [-180, 180] range.
     * Zoom levels from 0 to 23 are supported.
     */
    public static BingTile bingTileAtLatitudeLongitude(double latitude, double longitude, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        int tileX = longitudeToTileX(longitude, mapSize);
        int tileY = latitudeToTileY(latitude, mapSize);
        return BingTile.fromCoordinates(tileX, tileY, zoomLevel);
    }

    /**
     * Returns the minimum longitude of a tile.
     * <p>
     * This is the same as the longitude of the North West corner.
     */
    public static double tileXToLongitude(int tileX, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        double x = (clip(tileX * TILE_PIXELS, 0, mapSize) / mapSize) - 0.5;
        return 360 * x;
    }

    /**
     * Returns the maximum latitude of a tile.
     * <p>
     * This is the same as the latitude of the North West corner.
     */
    public static double tileYToLatitude(int tileY, int zoomLevel)
    {
        long mapSize = mapSize(zoomLevel);
        double y = 0.5 - (clip(tileY * TILE_PIXELS, 0, mapSize) / mapSize);
        return 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
    }

    private static double clip(double n, double minValue, double maxValue)
    {
        return Math.min(Math.max(n, minValue), maxValue);
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
