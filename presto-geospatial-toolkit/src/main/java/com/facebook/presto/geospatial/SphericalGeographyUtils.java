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

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;

import java.util.EnumSet;
import java.util.Set;

import static com.facebook.presto.geospatial.GeometryType.POINT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toDegrees;
import static java.lang.Math.toRadians;
import static java.lang.String.format;

public class SphericalGeographyUtils
{
    public static final double EARTH_RADIUS_KM = 6371.01;
    public static final double EARTH_RADIUS_M = EARTH_RADIUS_KM * 1000.0;
    private static final float MIN_LATITUDE = -90;
    private static final float MAX_LATITUDE = 90;
    private static final float MIN_LONGITUDE = -180;
    private static final float MAX_LONGITUDE = 180;
    private static final Joiner OR_JOINER = Joiner.on(" or ");
    private static final Set<GeometryType> ALLOWED_SPHERICAL_DISTANCE_TYPES = EnumSet.of(POINT);

    private SphericalGeographyUtils() {}

    public static void checkLatitude(double latitude)
    {
        if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Latitude must be between -90 and 90");
        }
    }

    public static void checkLongitude(double longitude)
    {
        if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Longitude must be between -180 and 180");
        }
    }

    public static Double sphericalDistance(OGCGeometry leftGeometry, OGCGeometry rightGeometry)
    {
        if (leftGeometry.isEmpty() || rightGeometry.isEmpty()) {
            return null;
        }

        validateSphericalType("ST_Distance", leftGeometry, ALLOWED_SPHERICAL_DISTANCE_TYPES);
        validateSphericalType("ST_Distance", rightGeometry, ALLOWED_SPHERICAL_DISTANCE_TYPES);
        Point leftPoint = (Point) leftGeometry.getEsriGeometry();
        Point rightPoint = (Point) rightGeometry.getEsriGeometry();

        // greatCircleDistance returns distance in KM.
        return greatCircleDistance(leftPoint.getY(), leftPoint.getX(), rightPoint.getY(), rightPoint.getX()) * 1000;
    }

    /**
     * Calculate the distance between two points on Earth.
     *
     * This assumes a spherical Earth, and uses the Vincenty formula.
     * (https://en.wikipedia.org/wiki/Great-circle_distance)
     */
    public static double greatCircleDistance(
            double latitude1,
            double longitude1,
            double latitude2,
            double longitude2)
    {
        checkLatitude(latitude1);
        checkLongitude(longitude1);
        checkLatitude(latitude2);
        checkLongitude(longitude2);

        double radianLatitude1 = toRadians(latitude1);
        double radianLatitude2 = toRadians(latitude2);

        double sin1 = sin(radianLatitude1);
        double cos1 = cos(radianLatitude1);
        double sin2 = sin(radianLatitude2);
        double cos2 = cos(radianLatitude2);

        double deltaLongitude = toRadians(longitude1) - toRadians(longitude2);
        double cosDeltaLongitude = cos(deltaLongitude);

        double t1 = cos2 * sin(deltaLongitude);
        double t2 = cos1 * sin2 - sin1 * cos2 * cosDeltaLongitude;
        double t3 = sin1 * sin2 + cos1 * cos2 * cosDeltaLongitude;
        return atan2(sqrt(t1 * t1 + t2 * t2), t3) * EARTH_RADIUS_KM;
    }

    public static void validateSphericalType(String function, OGCGeometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("When applied to SphericalGeography inputs, %s only supports %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }

    public static final class CartesianPoint
    {
        private final double x;
        private final double y;
        private final double z;

        /**
         * @param p: Point expected to be in earth spherical coordinates (Long, Lat)
         */
        public CartesianPoint(Point p)
        {
            // Angle from North Pole down to Latitude, in Radians
            double phi = toRadians(90 - p.getY());
            double sinPhi = Math.sin(phi);
            // Angle from Greenwich to Longitude, in Radians
            double theta = toRadians(p.getX());

            x = EARTH_RADIUS_KM * sinPhi * Math.cos(theta);
            y = EARTH_RADIUS_KM * sinPhi * Math.sin(theta);
            z = EARTH_RADIUS_KM * Math.cos(phi);
        }

        /**
         *
         * @param x in cartesian coordinate
         * @param y in cartesian coordinate
         * @param z in cartesian coordinate
         */
        public CartesianPoint(double x, double y, double z)
        {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        public double getX()
        {
            return x;
        }

        public double getY()
        {
            return y;
        }

        public double getZ()
        {
            return z;
        }

        public Point asSphericalPoint()
        {
            // Angle from North Pole down to Latitude, in Radians
            double phi = Math.atan2(Math.sqrt(x * x + y * y), z);
            // Angle from Greenwich to Longitude, in Radians
            double theta = Math.atan2(y, x);
            double latitude = 90 - toDegrees(phi);
            double longitude = toDegrees(theta);
            return new Point(longitude, latitude);
        }
    }
}
