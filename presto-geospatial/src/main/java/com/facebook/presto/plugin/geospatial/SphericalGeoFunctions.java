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
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCPoint;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.KdbTreeType;
import com.facebook.presto.geospatial.KdbTree;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.SphericalGeographyUtils;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.util.EnumSet;

import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.geospatial.GeometryType.LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POINT;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POLYGON;
import static com.facebook.presto.geospatial.GeometryType.POINT;
import static com.facebook.presto.geospatial.GeometryType.POLYGON;
import static com.facebook.presto.geospatial.GeometryUtils.wktFromJtsGeometry;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.CartesianPoint;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.EARTH_RADIUS_M;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.checkLatitude;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.checkLongitude;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.sphericalDistance;
import static com.facebook.presto.geospatial.SphericalGeographyUtils.validateSphericalType;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.deserialize;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.PI;
import static java.lang.Math.toRadians;
import static java.lang.String.format;

public final class SphericalGeoFunctions
{
    private static final EnumSet<Geometry.Type> GEOMETRY_TYPES_FOR_SPHERICAL_GEOGRAPHY = EnumSet.of(
            Type.Point, Type.Polyline, Type.Polygon, Type.MultiPoint);

    private SphericalGeoFunctions() {}

    @Description("Converts a Geometry object to a SphericalGeography object")
    @ScalarFunction("to_spherical_geography")
    @SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME)
    public static Slice toSphericalGeography(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        // "every point in input is in range" <=> "the envelope of input is in range"
        Envelope envelope = deserializeEnvelope(input);
        if (!envelope.isEmpty()) {
            checkLatitude(envelope.getYMin());
            checkLatitude(envelope.getYMax());
            checkLongitude(envelope.getXMin());
            checkLongitude(envelope.getXMax());
        }
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (geometry.is3D()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot convert 3D geometry to a spherical geography");
        }

        GeometryCursor cursor = geometry.getEsriGeometryCursor();
        while (true) {
            com.esri.core.geometry.Geometry subGeometry = cursor.next();
            if (subGeometry == null) {
                break;
            }

            if (!GEOMETRY_TYPES_FOR_SPHERICAL_GEOGRAPHY.contains(subGeometry.getType())) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot convert geometry of this type to spherical geography: " + subGeometry.getType());
            }
        }

        return input;
    }

    @Description("Converts a SphericalGeography object to a Geometry object.")
    @ScalarFunction("to_geometry")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice toGeometry(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice input)
    {
        // Every SphericalGeography object is a valid geometry object
        return input;
    }

    @Description("Returns the Well-Known Text (WKT) representation of the geometry")
    @ScalarFunction("ST_AsText")
    @SqlType(VARCHAR)
    public static Slice stSphericalAsText(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice input)
    {
        return utf8Slice(wktFromJtsGeometry(deserialize(input)));
    }

    @SqlNullable
    @Description("Returns the great-circle distance in meters between two SphericalGeography points.")
    @ScalarFunction("ST_Distance")
    @SqlType(DOUBLE)
    public static Double stSphericalDistance(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice left, @SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice right)
    {
        return sphericalDistance(EsriGeometrySerde.deserialize(left), EsriGeometrySerde.deserialize(right));
    }

    @SqlNullable
    @Description("Returns the area of a geometry on the Earth's surface using spherical model")
    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static Double stSphericalArea(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        validateSphericalType("ST_Area", geometry, EnumSet.of(POLYGON, MULTI_POLYGON));

        Polygon polygon = (Polygon) geometry.getEsriGeometry();

        // See https://www.movable-type.co.uk/scripts/latlong.html
        // and http://osgeo-org.1560.x6.nabble.com/Area-of-a-spherical-polygon-td3841625.html
        // and https://www.element84.com/blog/determining-if-a-spherical-polygon-contains-a-pole
        // for the underlying Maths

        double sphericalExcess = 0.0;

        int numPaths = polygon.getPathCount();
        for (int i = 0; i < numPaths; i++) {
            double sign = polygon.isExteriorRing(i) ? 1.0 : -1.0;
            sphericalExcess += sign * Math.abs(computeSphericalExcess(polygon, polygon.getPathStart(i), polygon.getPathEnd(i)));
        }

        // Math.abs is required here because for Polygons with a 2D area of 0
        // isExteriorRing returns false for the exterior ring
        return Math.abs(sphericalExcess * EARTH_RADIUS_M * EARTH_RADIUS_M);
    }

    @ScalarFunction
    @Description("Calculates the great-circle distance between two points on the Earth's surface in kilometers")
    @SqlType(DOUBLE)
    public static double greatCircleDistance(
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude2,
            @SqlType(DOUBLE) double longitude2)
    {
        return SphericalGeographyUtils.greatCircleDistance(latitude1, longitude1, latitude2, longitude2);
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a given geometry")
    @SqlType("array(int)")
    public static Block spatialPartitions(@SqlType(KdbTreeType.NAME) Object kdbTree, @SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice geometry)
    {
        Envelope envelope = deserializeEnvelope(geometry);
        if (envelope.isEmpty()) {
            // Empty geometry
            return null;
        }

        return GeoFunctions.spatialPartitions((KdbTree) kdbTree, new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a geometry representing a set of points within specified distance from the input geometry")
    @SqlType("array(int)")
    public static Block spatialPartitions(@SqlType(KdbTreeType.NAME) Object kdbTree, @SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice geometry, @SqlType(DOUBLE) double distance)
    {
        if (isNaN(distance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is NaN");
        }

        if (isInfinite(distance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is infinite");
        }

        if (distance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is negative");
        }

        Envelope envelope = deserializeEnvelope(geometry);
        if (envelope.isEmpty()) {
            return null;
        }

        Rectangle expandedEnvelope2D = new Rectangle(envelope.getXMin() - distance, envelope.getYMin() - distance, envelope.getXMax() + distance, envelope.getYMax() + distance);
        return GeoFunctions.spatialPartitions((KdbTree) kdbTree, expandedEnvelope2D);
    }

    @SqlNullable
    @Description("Returns the great-circle length in meters of a linestring or multi-linestring on Earth's surface")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static Double stSphericalLength(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        validateSphericalType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        MultiPath lineString = (MultiPath) geometry.getEsriGeometry();

        double sum = 0;

        // sum up paths on (multi)linestring
        for (int path = 0; path < lineString.getPathCount(); path++) {
            if (lineString.getPathSize(path) < 2) {
                continue;
            }

            // sum up distances between adjacent points on this path
            int pathStart = lineString.getPathStart(path);
            Point prev = lineString.getPoint(pathStart);
            for (int i = pathStart + 1; i < lineString.getPathEnd(path); i++) {
                Point next = lineString.getPoint(i);
                sum += greatCircleDistance(prev.getY(), prev.getX(), next.getY(), next.getX());
                prev = next;
            }
        }

        return sum * 1000;
    }

    @SqlNullable
    @Description("Returns the Point value that is the mathematical centroid of a Spherical Geography")
    @ScalarFunction("ST_Centroid")
    @SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME)
    public static Slice stSphericalCentroid(@SqlType(SPHERICAL_GEOGRAPHY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        // TODO: add support for other types e.g. POLYGON
        validateSphericalType("ST_Centroid", geometry, EnumSet.of(POINT, MULTI_POINT));
        if (geometry instanceof OGCPoint) {
            return input;
        }

        OGCGeometryCollection geometryCollection = (OGCGeometryCollection) geometry;
        for (int i = 0; i < geometryCollection.numGeometries(); i++) {
            OGCGeometry g = geometryCollection.geometryN(i);
            validateSphericalType("ST_Centroid", g, EnumSet.of(POINT));
            Point p = (Point) g.getEsriGeometry();
            checkLongitude(p.getX());
            checkLatitude(p.getY());
        }

        Point centroid;
        if (geometryCollection.numGeometries() == 1) {
            centroid = (Point) geometryCollection.geometryN(0).getEsriGeometry();
        }
        else {
            double x3DTotal = 0;
            double y3DTotal = 0;
            double z3DTotal = 0;

            for (int i = 0; i < geometryCollection.numGeometries(); i++) {
                CartesianPoint cp = new CartesianPoint((Point) geometryCollection.geometryN(i).getEsriGeometry());
                x3DTotal += cp.getX();
                y3DTotal += cp.getY();
                z3DTotal += cp.getZ();
            }

            double centroidVectorLength = Math.sqrt(x3DTotal * x3DTotal + y3DTotal * y3DTotal + z3DTotal * z3DTotal);
            if (centroidVectorLength == 0.0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected error. Average vector length adds to zero (%f, %f, %f)", x3DTotal, y3DTotal, z3DTotal));
            }
            centroid = new CartesianPoint(
                    x3DTotal / centroidVectorLength,
                    y3DTotal / centroidVectorLength,
                    z3DTotal / centroidVectorLength).asSphericalPoint();
        }
        return EsriGeometrySerde.serialize(new OGCPoint(centroid, geometryCollection.getEsriSpatialReference()));
    }

    private static double computeSphericalExcess(Polygon polygon, int start, int end)
    {
        // Our calculations rely on not processing the same point twice
        if (polygon.getPoint(end - 1).equals(polygon.getPoint(start))) {
            end = end - 1;
        }

        if (end - start < 3) {
            // A path with less than 3 distinct points is not valid for calculating an area
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Polygon is not valid: a loop contains less then 3 vertices.");
        }

        Point point = new Point();

        // Initialize the calculator with the last point
        polygon.getPoint(end - 1, point);
        SphericalExcessCalculator calculator = new SphericalExcessCalculator(point);

        for (int i = start; i < end; i++) {
            polygon.getPoint(i, point);
            calculator.add(point);
        }

        return calculator.computeSphericalExcess();
    }

    private static class SphericalExcessCalculator
    {
        private static final double TWO_PI = 2 * Math.PI;
        private static final double THREE_PI = 3 * Math.PI;

        private double sphericalExcess;
        private double courseDelta;

        private boolean firstPoint;
        private double firstInitialBearing;
        private double previousFinalBearing;

        private double previousPhi;
        private double previousCos;
        private double previousSin;
        private double previousTan;
        private double previousLongitude;

        private boolean done;

        public SphericalExcessCalculator(Point endPoint)
        {
            previousPhi = toRadians(endPoint.getY());
            previousSin = Math.sin(previousPhi);
            previousCos = Math.cos(previousPhi);
            previousTan = Math.tan(previousPhi / 2);
            previousLongitude = toRadians(endPoint.getX());
            firstPoint = true;
        }

        private void add(Point point)
                throws IllegalStateException
        {
            checkState(!done, "Computation of spherical excess is complete");

            double phi = toRadians(point.getY());
            double tan = Math.tan(phi / 2);
            double longitude = toRadians(point.getX());

            // We need to check for that specifically
            // Otherwise calculating the bearing is not deterministic
            if (longitude == previousLongitude && phi == previousPhi) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Polygon is not valid: it has two identical consecutive vertices");
            }

            double deltaLongitude = longitude - previousLongitude;
            sphericalExcess += 2 * Math.atan2(Math.tan(deltaLongitude / 2) * (previousTan + tan), 1 + previousTan * tan);

            double cos = Math.cos(phi);
            double sin = Math.sin(phi);
            double sinOfDeltaLongitude = Math.sin(deltaLongitude);
            double cosOfDeltaLongitude = Math.cos(deltaLongitude);

            // Initial bearing from previous to current
            double y = sinOfDeltaLongitude * cos;
            double x = previousCos * sin - previousSin * cos * cosOfDeltaLongitude;
            double initialBearing = (Math.atan2(y, x) + TWO_PI) % TWO_PI;

            // Final bearing from previous to current = opposite of bearing from current to previous
            double finalY = -sinOfDeltaLongitude * previousCos;
            double finalX = previousSin * cos - previousCos * sin * cosOfDeltaLongitude;
            double finalBearing = (Math.atan2(finalY, finalX) + PI) % TWO_PI;

            // When processing our first point we don't yet have a previousFinalBearing
            if (firstPoint) {
                // So keep our initial bearing around, and we'll use it at the end
                // with the last final bearing
                firstInitialBearing = initialBearing;
                firstPoint = false;
            }
            else {
                courseDelta += (initialBearing - previousFinalBearing + THREE_PI) % TWO_PI - PI;
            }

            courseDelta += (finalBearing - initialBearing + THREE_PI) % TWO_PI - PI;

            previousFinalBearing = finalBearing;
            previousCos = cos;
            previousSin = sin;
            previousPhi = phi;
            previousTan = tan;
            previousLongitude = longitude;
        }

        public double computeSphericalExcess()
        {
            if (!done) {
                // Now that we have our last final bearing, we can calculate the remaining course delta
                courseDelta += (firstInitialBearing - previousFinalBearing + THREE_PI) % TWO_PI - PI;

                // The courseDelta should be 2Pi or - 2Pi, unless a pole is enclosed (and then it should be ~ 0)
                // In which case we need to correct the spherical excess by 2Pi
                if (Math.abs(courseDelta) < PI / 4) {
                    sphericalExcess = Math.abs(sphericalExcess) - TWO_PI;
                }
                done = true;
            }

            return sphericalExcess;
        }
    }
}
