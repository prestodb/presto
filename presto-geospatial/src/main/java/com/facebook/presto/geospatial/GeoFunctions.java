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
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static com.facebook.presto.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.LINE_STRING;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.MULTI_LINE_STRING;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.MULTI_POINT;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.MULTI_POLYGON;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.POINT;
import static com.facebook.presto.geospatial.GeometryUtils.GeometryTypeName.POLYGON;
import static com.facebook.presto.geospatial.GeometryUtils.deserialize;
import static com.facebook.presto.geospatial.GeometryUtils.serialize;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;

public final class GeoFunctions
{
    private GeoFunctions() {}

    @Description("Returns a Geometry type LineString object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_LineFromText")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice parseLine(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry geometry = geometryFromText(input);
        validateType("ST_LineFromText", geometry, EnumSet.of(LINE_STRING));
        return serialize(geometry);
    }

    @Description("Returns a Geometry type Point object with the given coordinate values")
    @ScalarFunction("ST_Point")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPoint(@SqlType(StandardTypes.DOUBLE) double x, @SqlType(StandardTypes.DOUBLE) double y)
    {
        OGCGeometry geometry = createFromEsriGeometry(new Point(x, y), null);
        return serialize(geometry);
    }

    @Description("Returns a Geometry type Polygon object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_Polygon")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPolygon(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry geometry = geometryFromText(input);
        validateType("ST_Polygon", geometry, EnumSet.of(POLYGON));
        return serialize(geometry);
    }

    @Description("Returns the area of a polygon using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Area")
    @SqlType(StandardTypes.DOUBLE)
    public static double stArea(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Area", geometry, EnumSet.of(POLYGON, MULTI_POLYGON));
        return geometry.getEsriGeometry().calculateArea2D();
    }

    @Description("Returns a Geometry type object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_GeometryFromText")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryFromText(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        return serialize(geometryFromText(input));
    }

    @SqlNullable
    @Description("Returns the Well-Known Text (WKT) representation of the geometry")
    @ScalarFunction("ST_AsText")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stAsText(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return Slices.utf8Slice(deserialize(input).asText());
    }

    @Description("Returns the Point value that is the mathematical centroid of a Geometry")
    @ScalarFunction("ST_Centroid")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stCentroid(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Centroid", geometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        GeometryTypeName typeName = GeometryUtils.valueOf(geometry.geometryType());
        if (typeName == POINT) {
            return input;
        }

        int pointCount = ((MultiVertexGeometry) geometry.getEsriGeometry()).getPointCount();
        if (pointCount == 0) {
            return serialize(createFromEsriGeometry(new Point(), geometry.getEsriSpatialReference()));
        }

        Point centroid = null;
        switch (typeName) {
            case MULTI_POINT:
                centroid = computePointsCentroid((MultiVertexGeometry) geometry.getEsriGeometry());
                break;
            case LINE_STRING:
            case MULTI_LINE_STRING:
                centroid = computeLineCentroid((Polyline) geometry.getEsriGeometry());
                break;
            case POLYGON:
                centroid = computePolygonCentroid((Polygon) geometry.getEsriGeometry());
                break;
            case MULTI_POLYGON:
                centroid = computeMultiPolygonCentroid((OGCMultiPolygon) geometry);
                break;
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid typeName: " + typeName);
        }
        return serialize(createFromEsriGeometry(centroid, geometry.getEsriSpatialReference()));
    }

    @Description("Return the coordinate dimension of the Geometry")
    @ScalarFunction("ST_CoordDim")
    @SqlType(StandardTypes.TINYINT)
    public static long stCoordinateDimension(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserialize(input).coordinateDimension();
    }

    @Description("Returns the inherent dimension of this Geometry object, which must be less than or equal to the coordinate dimension")
    @ScalarFunction("ST_Dimension")
    @SqlType(StandardTypes.TINYINT)
    public static long stDimension(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserialize(input).dimension();
    }

    @SqlNullable
    @Description("Returns TRUE if the LineString or Multi-LineString's start and end points are coincident")
    @ScalarFunction("ST_IsClosed")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsClosed(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_IsClosed", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        MultiPath lines = (MultiPath) geometry.getEsriGeometry();
        int pathCount = lines.getPathCount();
        for (int i = 0; i < pathCount; i++) {
            Point start = lines.getPoint(lines.getPathStart(i));
            Point end = lines.getPoint(lines.getPathEnd(i) - 1);
            if (!end.equals(start)) {
                return false;
            }
        }
        return true;
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is an empty geometrycollection, polygon, point etc")
    @ScalarFunction("ST_IsEmpty")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserialize(input).isEmpty();
    }

    @Description("Returns the length of a LineString or Multi-LineString using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Length")
    @SqlType(StandardTypes.DOUBLE)
    public static double stLength(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        return geometry.getEsriGeometry().calculateLength2D();
    }

    @Description("Returns X maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMax")
    @SqlType(StandardTypes.DOUBLE)
    public static double stXMax(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = getEnvelope(geometry);
        return envelope.getXMax();
    }

    @Description("Returns Y maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMax")
    @SqlType(StandardTypes.DOUBLE)
    public static double stYMax(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = getEnvelope(geometry);
        return envelope.getYMax();
    }

    @Description("Returns X minima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMin")
    @SqlType(StandardTypes.DOUBLE)
    public static double stXMin(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = getEnvelope(geometry);
        return envelope.getXMin();
    }

    @Description("Returns Y minima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMin")
    @SqlType(StandardTypes.DOUBLE)
    public static double stYMin(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = getEnvelope(geometry);
        return envelope.getYMin();
    }

    @SqlNullable
    @Description("Returns the cardinality of the collection of interior rings of a polygon")
    @ScalarFunction("ST_NumInteriorRing")
    @SqlType(StandardTypes.BIGINT)
    public static Long stNumInteriorRings(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_NumInteriorRing", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return Long.valueOf(((OGCPolygon) geometry).numInteriorRing());
    }

    @Description("Returns the number of points in a Geometry")
    @ScalarFunction("ST_NumPoints")
    @SqlType(StandardTypes.BIGINT)
    public static long stNumPoints(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.getEsriGeometry().isEmpty()) {
            return 0;
        }
        else if (GeometryUtils.valueOf(geometry.geometryType()) == POINT) {
            return 1;
        }
        return ((MultiVertexGeometry) geometry.getEsriGeometry()).getPointCount();
    }

    @SqlNullable
    @Description("Returns TRUE if and only if the line is closed and simple")
    @ScalarFunction("ST_IsRing")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsRing(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_IsRing", geometry, EnumSet.of(LINE_STRING));
        OGCLineString line = (OGCLineString) geometry;
        return line.isClosed() && line.isSimple();
    }

    @SqlNullable
    @Description("Returns the first point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_StartPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stStartPoint(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_StartPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        MultiPath lines = (MultiPath) geometry.getEsriGeometry();
        SpatialReference reference = geometry.getEsriSpatialReference();
        return serialize(createFromEsriGeometry(lines.getPoint(0), reference));
    }

    @SqlNullable
    @Description("Returns the last point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_EndPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEndPoint(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_EndPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        MultiPath lines = (MultiPath) geometry.getEsriGeometry();
        SpatialReference reference = geometry.getEsriSpatialReference();
        return serialize(createFromEsriGeometry(lines.getPoint(lines.getPointCount() - 1), reference));
    }

    @SqlNullable
    @Description("Return the X coordinate of the point")
    @ScalarFunction("ST_X")
    @SqlType(StandardTypes.DOUBLE)
    public static Double stX(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_X", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return Double.valueOf(((OGCPoint) geometry).X());
    }

    @SqlNullable
    @Description("Return the Y coordinate of the point")
    @ScalarFunction("ST_Y")
    @SqlType(StandardTypes.DOUBLE)
    public static Double stY(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Y", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return Double.valueOf(((OGCPoint) geometry).Y());
    }

    @Description("Returns the closure of the combinatorial boundary of this Geometry")
    @ScalarFunction("ST_Boundary")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBoundary(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        return serialize(geometry.boundary());
    }

    @Description("Returns the bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_Envelope")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEnvelope(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        SpatialReference reference = geometry.getEsriSpatialReference();
        Envelope envelope = getEnvelope(geometry);
        return serialize(createFromEsriGeometry(envelope, reference));
    }

    @Description("Returns the Geometry value that represents the point set difference of two geometries")
    @ScalarFunction("ST_Difference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stDifference(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return serialize(leftGeometry.difference(rightGeometry));
    }

    @Description("Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units")
    @ScalarFunction("ST_Distance")
    @SqlType(StandardTypes.DOUBLE)
    public static double stDistance(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.distance(rightGeometry);
    }

    @SqlNullable
    @Description("Returns a line string representing the exterior ring of the POLYGON")
    @ScalarFunction("ST_ExteriorRing")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stExteriorRing(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_ExteriorRing", geometry, EnumSet.of(POLYGON, MULTI_POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return serialize(((OGCPolygon) geometry).exteriorRing());
    }

    @Description("Returns the Geometry value that represents the point set intersection of two Geometries")
    @ScalarFunction("ST_Intersection")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stIntersection(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return serialize(leftGeometry.intersection(rightGeometry));
    }

    @Description("Returns the Geometry value that represents the point set symmetric difference of two Geometries")
    @ScalarFunction("ST_SymDifference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stSymmetricDifference(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return serialize(leftGeometry.symDifference(rightGeometry));
    }

    @SqlNullable
    @Description("Returns TRUE if and only if no points of right lie in the exterior of left, and at least one point of the interior of left lies in the interior of right")
    @ScalarFunction("ST_Contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContains(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.contains(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the supplied geometries have some, but not all, interior points in common")
    @ScalarFunction("ST_Crosses")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stCrosses(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.crosses(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries do not spatially intersect - if they do not share any space together")
    @ScalarFunction("ST_Disjoint")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stDisjoint(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.disjoint(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the given geometries represent the same geometry")
    @ScalarFunction("ST_Equals")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEquals(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.equals(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries spatially intersect in 2D - (share any portion of space) and FALSE if they don't (they are Disjoint)")
    @ScalarFunction("ST_Intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersects(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.intersects(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries share space, are of the same dimension, but are not completely contained by each other")
    @ScalarFunction("ST_Overlaps")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stOverlaps(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.overlaps(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is spatially related to another Geometry")
    @ScalarFunction("ST_Relate")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stRelate(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right, @SqlType(StandardTypes.VARCHAR) Slice relation)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.relate(rightGeometry, relation.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect")
    @ScalarFunction("ST_Touches")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stTouches(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.touches(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the geometry A is completely inside geometry B")
    @ScalarFunction("ST_Within")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stWithin(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.within(rightGeometry);
    }

    private static OGCGeometry geometryFromText(Slice input)
    {
        OGCGeometry geometry = OGCGeometry.fromText(input.toStringUtf8());
        geometry.setSpatialReference(null);
        return geometry;
    }

    private static void validateType(String function, OGCGeometry geometry, Set<GeometryTypeName> validTypes)
    {
        GeometryTypeName type = GeometryUtils.valueOf(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, function + " only applies to " + StringUtils.join(validTypes, " or ") + ". Input type is: " + type);
        }
    }

    private static Envelope getEnvelope(OGCGeometry geometry)
    {
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope;
    }

    private static void verifySameSpatialReference(OGCGeometry leftGeometry, OGCGeometry rightGeometry)
    {
        checkArgument(Objects.equals(leftGeometry.getEsriSpatialReference(), rightGeometry.getEsriSpatialReference()), "Input geometries must have the same spatial reference");
    }

    // Points centroid is arithmetic mean of the input points
    private static Point computePointsCentroid(MultiVertexGeometry multiVertex)
    {
        double xSum = 0;
        double ySum = 0;
        for (int i = 0; i < multiVertex.getPointCount(); i++) {
            Point point = multiVertex.getPoint(i);
            xSum += point.getX();
            ySum += point.getY();
        }
        return new Point(xSum / multiVertex.getPointCount(), ySum / multiVertex.getPointCount());
    }

    // Lines centroid is weighted mean of each line segment, weight in terms of line length
    private static Point computeLineCentroid(Polyline polyline)
    {
        double xSum = 0;
        double ySum = 0;
        double weightSum = 0;
        for (int i = 0; i < polyline.getPathCount(); i++) {
            Point startPoint = polyline.getPoint(polyline.getPathStart(i));
            Point endPoint = polyline.getPoint(polyline.getPathEnd(i) - 1);
            double dx = endPoint.getX() - startPoint.getX();
            double dy = endPoint.getY() - startPoint.getY();
            double length = Math.sqrt(dx * dx + dy * dy);
            weightSum += length;
            xSum += (startPoint.getX() + endPoint.getX()) * length / 2;
            ySum += (startPoint.getY() + endPoint.getY()) * length / 2;
        }
        return new Point(xSum / weightSum, ySum / weightSum);
    }

    // Polygon centroid: area weighted average of centroids in case of holes
    private static Point computePolygonCentroid(Polygon polygon)
    {
        int pathCount = polygon.getPathCount();

        if (pathCount == 1) {
            return getPolygonSansHolesCentroid(polygon);
        }

        double xSum = 0;
        double ySum = 0;
        double areaSum = 0;

        for (int i = 0; i < pathCount; i++) {
            int startIndex = polygon.getPathStart(i);
            int endIndex = polygon.getPathEnd(i);

            Polygon sansHoles = getSubPolygon(polygon, startIndex, endIndex);

            Point centroid = getPolygonSansHolesCentroid(sansHoles);
            double area = sansHoles.calculateArea2D();

            xSum += centroid.getX() * area;
            ySum += centroid.getY() * area;
            areaSum += area;
        }

        return new Point(xSum / areaSum, ySum / areaSum);
    }

    private static Polygon getSubPolygon(Polygon polygon, int startIndex, int endIndex)
    {
        Polyline boundary = new Polyline();
        boundary.startPath(polygon.getPoint(startIndex));
        for (int i = startIndex + 1; i < endIndex; i++) {
            Point current = polygon.getPoint(i);
            boundary.lineTo(current);
        }

        final Polygon newPolygon = new Polygon();
        newPolygon.add(boundary, false);
        return newPolygon;
    }

    // Polygon sans holes centroid:
    // c[x] = (Sigma(x[i] + x[i + 1]) * (x[i] * y[i + 1] - x[i + 1] * y[i]), for i = 0 to N - 1) / (6 * signedArea)
    // c[y] = (Sigma(y[i] + y[i + 1]) * (x[i] * y[i + 1] - x[i + 1] * y[i]), for i = 0 to N - 1) / (6 * signedArea)
    private static Point getPolygonSansHolesCentroid(Polygon polygon)
    {
        int pointCount = polygon.getPointCount();
        double xSum = 0;
        double ySum = 0;
        double signedArea = 0;
        for (int i = 0; i < pointCount; i++) {
            Point current = polygon.getPoint(i);
            Point next = polygon.getPoint((i + 1) % polygon.getPointCount());
            double ladder = current.getX() * next.getY() - next.getX() * current.getY();
            xSum += (current.getX() + next.getX()) * ladder;
            ySum += (current.getY() + next.getY()) * ladder;
            signedArea += ladder / 2;
        }
        return new Point(xSum / (signedArea * 6), ySum / (signedArea * 6));
    }

    // MultiPolygon centroid is weighted mean of each polygon, weight in terms of polygon area
    private static Point computeMultiPolygonCentroid(OGCMultiPolygon multiPolygon)
    {
        double xSum = 0;
        double ySum = 0;
        double weightSum = 0;
        for (int i = 0; i < multiPolygon.numGeometries(); i++) {
            Point centroid = computePolygonCentroid((Polygon) multiPolygon.geometryN(i).getEsriGeometry());
            Polygon polygon = (Polygon) multiPolygon.geometryN(i).getEsriGeometry();
            double weight = polygon.calculateArea2D();
            weightSum += weight;
            xSum += centroid.getX() * weight;
            ySum += centroid.getY() * weight;
        }
        return new Point(xSum / weightSum, ySum / weightSum);
    }
}
