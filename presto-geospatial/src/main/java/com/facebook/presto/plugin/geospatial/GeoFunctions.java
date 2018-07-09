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
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.NonSimpleResult;
import com.esri.core.geometry.NonSimpleResult.Reason;
import com.esri.core.geometry.OperatorSimplifyOGC;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.geospatial.serde.GeometrySerializationType;
import com.facebook.presto.geospatial.serde.JtsGeometrySerde;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.linearref.LengthIndexedLine;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.esri.core.geometry.NonSimpleResult.Reason.Clustering;
import static com.esri.core.geometry.NonSimpleResult.Reason.Cracking;
import static com.esri.core.geometry.NonSimpleResult.Reason.CrossOver;
import static com.esri.core.geometry.NonSimpleResult.Reason.DegenerateSegments;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCDisconnectedInterior;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCPolygonSelfTangency;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCPolylineSelfTangency;
import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static com.facebook.presto.geospatial.GeometryType.LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POINT;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POLYGON;
import static com.facebook.presto.geospatial.GeometryType.POINT;
import static com.facebook.presto.geospatial.GeometryType.POLYGON;
import static com.facebook.presto.geospatial.GeometryUtils.getPointCount;
import static com.facebook.presto.geospatial.serde.GeometrySerde.deserialize;
import static com.facebook.presto.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.serde.GeometrySerde.deserializeType;
import static com.facebook.presto.geospatial.serde.GeometrySerde.serialize;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toIntExact;
import static java.lang.Math.toRadians;
import static java.lang.String.format;
import static org.locationtech.jts.simplify.TopologyPreservingSimplifier.simplify;

public final class GeoFunctions
{
    private static final Joiner OR_JOINER = Joiner.on(" or ");
    private static final Slice EMPTY_POLYGON = serialize(new OGCPolygon(new Polygon(), null));
    private static final Slice EMPTY_MULTIPOINT = serialize(createFromEsriGeometry(new MultiPoint(), null, true));
    private static final double EARTH_RADIUS_KM = 6371.01;
    private static final Map<Reason, String> NON_SIMPLE_REASONS = ImmutableMap.<Reason, String>builder()
            .put(DegenerateSegments, "Degenerate segments")
            .put(Clustering, "Repeated points")
            .put(Cracking, "Intersecting or overlapping segments")
            .put(CrossOver, "Self-intersection")
            .put(OGCPolylineSelfTangency, "Self-tangency")
            .put(OGCPolygonSelfTangency, "Self-tangency")
            .put(OGCDisconnectedInterior, "Disconnected interior")
            .build();

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
    public static Slice stPoint(@SqlType(DOUBLE) double x, @SqlType(DOUBLE) double y)
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

    @Description("Returns the 2D Euclidean area of a geometry")
    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static double stArea(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);

        // The Esri geometry library does not support area for geometry collections. We compute the area
        // of collections by summing the area of the individual components.
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (type == GeometryType.GEOMETRY_COLLECTION) {
            double area = 0.0;
            GeometryCursor cursor = geometry.getEsriGeometryCursor();
            while (true) {
                com.esri.core.geometry.Geometry esriGeometry = cursor.next();
                if (esriGeometry == null) {
                    return area;
                }

                area += esriGeometry.calculateArea2D();
            }
        }
        return geometry.getEsriGeometry().calculateArea2D();
    }

    @Description("Returns a Geometry type object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_GeometryFromText")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryFromText(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        return serialize(geometryFromText(input));
    }

    @Description("Returns the Well-Known Text (WKT) representation of the geometry")
    @ScalarFunction("ST_AsText")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stAsText(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return utf8Slice(deserialize(input).asText());
    }

    @SqlNullable
    @Description("Returns the geometry that represents all points whose distance from the specified geometry is less than or equal to the specified distance")
    @ScalarFunction("ST_Buffer")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBuffer(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(DOUBLE) double distance)
    {
        if (Double.isNaN(distance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is NaN");
        }

        if (distance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is negative");
        }

        if (distance == 0) {
            return input;
        }

        OGCGeometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        return serialize(geometry.buffer(distance));
    }

    @Description("Returns the Point value that is the mathematical centroid of a Geometry")
    @ScalarFunction("ST_Centroid")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stCentroid(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Centroid", geometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        GeometryType geometryType = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (geometryType == GeometryType.POINT) {
            return input;
        }

        int pointCount = ((MultiVertexGeometry) geometry.getEsriGeometry()).getPointCount();
        if (pointCount == 0) {
            return serialize(createFromEsriGeometry(new Point(), geometry.getEsriSpatialReference()));
        }

        Point centroid;
        switch (geometryType) {
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
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Unexpected geometry type: " + geometryType);
        }
        return serialize(createFromEsriGeometry(centroid, geometry.getEsriSpatialReference()));
    }

    @Description("Returns the minimum convex geometry that encloses all input geometries")
    @ScalarFunction("ST_ConvexHull")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stConvexHull(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_ConvexHull", geometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        if (geometry.isEmpty()) {
            return input;
        }
        if (GeometryType.getForEsriGeometryType(geometry.geometryType()) == POINT) {
            return input;
        }
        OGCGeometry convexHull = geometry.convexHull();
        if (convexHull.isEmpty()) {
            // This happens for a single-point multi-point because of a bug in ESRI library - https://github.com/Esri/geometry-api-java/issues/172
            return serialize(createFromEsriGeometry(((MultiVertexGeometry) geometry.getEsriGeometry()).getPoint(0), null));
        }
        if (GeometryType.getForEsriGeometryType(convexHull.geometryType()) == MULTI_POLYGON) {
            MultiVertexGeometry multiVertex = (MultiVertexGeometry) convexHull.getEsriGeometry();
            if (multiVertex.getPointCount() == 2) {
                // This happens when all points of the input geometry are on the same line because of a bug in ESRI library - https://github.com/Esri/geometry-api-java/issues/172
                OGCGeometry linestring = createFromEsriGeometry(new Polyline(multiVertex.getPoint(0), multiVertex.getPoint(1)), null);
                return serialize(linestring);
            }
        }
        return serialize(convexHull);
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

    @Description("Returns TRUE if this Geometry has no anomalous geometric points, such as self intersection or self tangency")
    @ScalarFunction("ST_IsSimple")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean stIsSimple(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        return geometry.isEmpty() || geometry.isSimple();
    }

    @Description("Returns true if the input geometry is well formed")
    @ScalarFunction("ST_IsValid")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean stIsValid(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        GeometryCursor cursor = deserialize(input).getEsriGeometryCursor();
        while (true) {
            com.esri.core.geometry.Geometry geometry = cursor.next();
            if (geometry == null) {
                return true;
            }

            if (!OperatorSimplifyOGC.local().isSimpleOGC(geometry, null, true, null, null)) {
                return false;
            }
        }
    }

    @Description("Returns the reason for why the input geometry is not valid. Returns null if the input is valid.")
    @ScalarFunction("geometry_invalid_reason")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice invalidReason(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        GeometryCursor cursor = deserialize(input).getEsriGeometryCursor();
        NonSimpleResult result = new NonSimpleResult();
        while (true) {
            com.esri.core.geometry.Geometry geometry = cursor.next();
            if (geometry == null) {
                return null;
            }

            if (!OperatorSimplifyOGC.local().isSimpleOGC(geometry, null, true, result, null)) {
                String reasonText = NON_SIMPLE_REASONS.getOrDefault(result.m_reason, result.m_reason.name());

                if (!(geometry instanceof MultiVertexGeometry)) {
                    return utf8Slice(reasonText);
                }

                MultiVertexGeometry multiVertexGeometry = (MultiVertexGeometry) geometry;
                if (result.m_vertexIndex1 >= 0 && result.m_vertexIndex2 >= 0) {
                    Point point1 = multiVertexGeometry.getPoint(result.m_vertexIndex1);
                    Point point2 = multiVertexGeometry.getPoint(result.m_vertexIndex2);
                    return utf8Slice(format("%s at or near (%s %s) and (%s %s)", reasonText, point1.getX(), point1.getY(), point2.getX(), point2.getY()));
                }

                if (result.m_vertexIndex1 >= 0) {
                    Point point = multiVertexGeometry.getPoint(result.m_vertexIndex1);
                    return utf8Slice(format("%s at or near (%s %s)", reasonText, point.getX(), point.getY()));
                }

                return utf8Slice(reasonText);
            }
        }
    }

    @Description("Returns the length of a LineString or Multi-LineString using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static double stLength(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        return geometry.getEsriGeometry().calculateLength2D();
    }

    @SqlNullable
    @Description("Returns a float between 0 and 1 representing the location of the closest point on the LineString to the given Point, as a fraction of total 2d line length.")
    @ScalarFunction("line_locate_point")
    @SqlType(DOUBLE)
    public static Double lineLocatePoint(@SqlType(GEOMETRY_TYPE_NAME) Slice lineSlice, @SqlType(GEOMETRY_TYPE_NAME) Slice pointSlice)
    {
        Geometry line = JtsGeometrySerde.deserialize(lineSlice);
        Geometry point = JtsGeometrySerde.deserialize(pointSlice);

        if (line.isEmpty() || point.isEmpty()) {
            return null;
        }

        GeometryType lineType = GeometryType.getForJtsGeometryType(line.getGeometryType());
        if (lineType != GeometryType.LINE_STRING && lineType != GeometryType.MULTI_LINE_STRING) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("First argument to line_locate_point must be a LineString or a MultiLineString. Got: %s", line.getGeometryType()));
        }

        GeometryType pointType = GeometryType.getForJtsGeometryType(point.getGeometryType());
        if (pointType != GeometryType.POINT) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Second argument to line_locate_point must be a Point. Got: %s", point.getGeometryType()));
        }

        return new LengthIndexedLine(line).indexOf(point.getCoordinate()) / line.getLength();
    }

    @SqlNullable
    @Description("Returns X maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMax")
    @SqlType(DOUBLE)
    public static Double stXMax(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return null;
        }
        return envelope.getXMax();
    }

    @SqlNullable
    @Description("Returns Y maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMax")
    @SqlType(DOUBLE)
    public static Double stYMax(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return null;
        }
        return envelope.getYMax();
    }

    @SqlNullable
    @Description("Returns X minima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMin")
    @SqlType(DOUBLE)
    public static Double stXMin(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return null;
        }
        return envelope.getXMin();
    }

    @SqlNullable
    @Description("Returns Y minima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMin")
    @SqlType(DOUBLE)
    public static Double stYMin(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return null;
        }
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

    @SqlNullable
    @Description("Returns an array of interior rings of a polygon")
    @ScalarFunction("ST_InteriorRings")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stInteriorRings(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_InteriorRings", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }

        OGCPolygon polygon = (OGCPolygon) geometry;
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, polygon.numInteriorRing());
        for (int i = 0; i < polygon.numInteriorRing(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize(polygon.interiorRingN(i)));
        }
        return blockBuilder.build();
    }

    @Description("Returns the cardinality of the geometry collection")
    @ScalarFunction("ST_NumGeometries")
    @SqlType(StandardTypes.INTEGER)
    public static long stNumGeometries(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return 0;
        }
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!type.isMultitype()) {
            return 1;
        }
        return ((OGCGeometryCollection) geometry).numGeometries();
    }

    @Description("Returns a geometry that represents the point set union of the input geometries. This function doesn't support geometry collections.")
    @ScalarFunction("ST_Union")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stUnion(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        // Only supports Geometry but not GeometryCollection due to ESRI library limitation
        // https://github.com/Esri/geometry-api-java/issues/176
        // https://github.com/Esri/geometry-api-java/issues/177
        OGCGeometry leftGeometry = deserialize(left);
        validateType("ST_Union", leftGeometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        if (leftGeometry.isEmpty()) {
            return right;
        }

        OGCGeometry rightGeometry = deserialize(right);
        validateType("ST_Union", rightGeometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        if (rightGeometry.isEmpty()) {
            return left;
        }

        return serialize(leftGeometry.union(rightGeometry));
    }

    @SqlNullable
    @Description("Returns the geometry element at the specified index (indices started with 1)")
    @ScalarFunction("ST_GeometryN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!type.isMultitype()) {
            if (index == 1) {
                return input;
            }
            return null;
        }
        OGCGeometryCollection geometryCollection = ((OGCGeometryCollection) geometry);
        if (index < 1 || index > geometryCollection.numGeometries()) {
            return null;
        }
        OGCGeometry ogcGeometry = geometryCollection.geometryN((int) index - 1);
        return serialize(ogcGeometry);
    }

    @SqlNullable
    @Description("Returns the vertex of a linestring at the specified index (indices started with 1) ")
    @ScalarFunction("ST_PointN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPointN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_PointN", geometry, EnumSet.of(LINE_STRING));

        OGCLineString linestring = (OGCLineString) geometry;
        if (index < 1 || index > linestring.numPoints()) {
            return null;
        }
        return serialize(linestring.pointN(toIntExact(index) - 1));
    }

    @SqlNullable
    @Description("Returns an array of geometries in the specified collection")
    @ScalarFunction("ST_Geometries")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stGeometries(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!type.isMultitype()) {
            BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 1);
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry));
            return blockBuilder.build();
        }

        OGCGeometryCollection collection = (OGCGeometryCollection) geometry;
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, collection.numGeometries());
        for (int i = 0; i < collection.numGeometries(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize(collection.geometryN(i)));
        }
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("Returns the interior ring element at the specified index (indices start at 1)")
    @ScalarFunction("ST_InteriorRingN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stInteriorRingN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_InteriorRingN", geometry, EnumSet.of(POLYGON));
        OGCPolygon polygon = (OGCPolygon) geometry;
        if (index < 1 || index > polygon.numInteriorRing()) {
            return null;
        }
        OGCGeometry interiorRing = polygon.interiorRingN(toIntExact(index) - 1);
        return serialize(interiorRing);
    }

    @Description("Returns the number of points in a Geometry")
    @ScalarFunction("ST_NumPoints")
    @SqlType(StandardTypes.BIGINT)
    public static long stNumPoints(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return getPointCount(deserialize(input));
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

    @Description("Returns a \"simplified\" version of the given geometry")
    @ScalarFunction("simplify_geometry")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice simplifyGeometry(@SqlType(GEOMETRY_TYPE_NAME) Slice input,
                                         @SqlType(DOUBLE) double distanceTolerance)
    {
        if (Double.isNaN(distanceTolerance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is NaN");
        }

        if (distanceTolerance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is negative");
        }

        if (distanceTolerance == 0) {
            return input;
        }

        return JtsGeometrySerde.serialize(simplify(JtsGeometrySerde.deserialize(input), distanceTolerance));
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
    @SqlType(DOUBLE)
    public static Double stX(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_X", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((OGCPoint) geometry).X();
    }

    @SqlNullable
    @Description("Return the Y coordinate of the point")
    @ScalarFunction("ST_Y")
    @SqlType(DOUBLE)
    public static Double stY(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        validateType("ST_Y", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((OGCPoint) geometry).Y();
    }

    @Description("Returns the closure of the combinatorial boundary of this Geometry")
    @ScalarFunction("ST_Boundary")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBoundary(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.isEmpty() && GeometryType.getForEsriGeometryType(geometry.geometryType()) == LINE_STRING) {
            // OCGGeometry#boundary crashes with NPE for LINESTRING EMPTY
            return EMPTY_MULTIPOINT;
        }
        return serialize(geometry.boundary());
    }

    @Description("Returns the bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_Envelope")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEnvelope(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return EMPTY_POLYGON;
        }
        return serialize(envelope);
    }

    @SqlNullable
    @Description("Returns the lower left and upper right corners of bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_EnvelopeAsPts")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stEnvelopeAsPts(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope == null) {
            return null;
        }
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 2);
        Point lowerLeftCorner = new Point(envelope.getXMin(), envelope.getYMin());
        Point upperRightCorner = new Point(envelope.getXMax(), envelope.getYMax());
        GEOMETRY.writeSlice(blockBuilder, serialize(createFromEsriGeometry(lowerLeftCorner, null, false)));
        GEOMETRY.writeSlice(blockBuilder, serialize(createFromEsriGeometry(upperRightCorner, null, false)));
        return blockBuilder.build();
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
    @SqlType(DOUBLE)
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
        validateType("ST_ExteriorRing", geometry, EnumSet.of(POLYGON));
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
        if (deserializeType(left) == GeometrySerializationType.ENVELOPE && deserializeType(right) == GeometrySerializationType.ENVELOPE) {
            Envelope leftEnvelope = deserializeEnvelope(left);
            Envelope rightEnvelope = deserializeEnvelope(right);

            // Envelope#intersect updates leftEnvelope to the intersection of the two envelopes
            if (!leftEnvelope.intersect(rightEnvelope)) {
                return EMPTY_POLYGON;
            }

            Envelope intersection = leftEnvelope;
            if (intersection.getXMin() == intersection.getXMax()) {
                if (intersection.getYMin() == intersection.getYMax()) {
                    return serialize(createFromEsriGeometry(new Point(intersection.getXMin(), intersection.getXMax()), null));
                }
                return serialize(createFromEsriGeometry(new Polyline(new Point(intersection.getXMin(), intersection.getYMin()), new Point(intersection.getXMin(), intersection.getYMax())), null));
            }

            if (intersection.getYMin() == intersection.getYMax()) {
                return serialize(createFromEsriGeometry(new Polyline(new Point(intersection.getXMin(), intersection.getYMin()), new Point(intersection.getXMax(), intersection.getYMin())), null));
            }

            return serialize(intersection);
        }

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
        if (!envelopes(left, right, Envelope::contains)) {
            return false;
        }
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
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
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
        if (!envelopes(left, right, Envelope::intersect)) {
            return true;
        }
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
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
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
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
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
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
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
        if (!envelopes(right, left, Envelope::contains)) {
            return false;
        }
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.within(rightGeometry);
    }

    @Description("Returns the type of the geometry")
    @ScalarFunction("ST_GeometryType")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stGeometryType(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return GeometrySerde.getGeometryType(input).standardName();
    }

    @ScalarFunction
    @Description("Calculates the great-circle distance between two points on the Earth's surface in kilometers")
    @SqlType(StandardTypes.DOUBLE)
    public static double greatCircleDistance(
            @SqlType(StandardTypes.DOUBLE) double latitude1,
            @SqlType(StandardTypes.DOUBLE) double longitude1,
            @SqlType(StandardTypes.DOUBLE) double latitude2,
            @SqlType(StandardTypes.DOUBLE) double longitude2)
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

    private static void checkLatitude(double latitude)
    {
        if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < -90 || latitude > 90) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Latitude must be between -90 and 90");
        }
    }

    private static void checkLongitude(double longitude)
    {
        if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < -180 || longitude > 180) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Longitude must be between -180 and 180");
        }
    }

    private static OGCGeometry geometryFromText(Slice input)
    {
        OGCGeometry geometry;
        try {
            geometry = OGCGeometry.fromText(input.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKT: " + input.toStringUtf8(), e);
        }
        geometry.setSpatialReference(null);
        return geometry;
    }

    private static void validateType(String function, OGCGeometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
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
            double length = sqrt(dx * dx + dy * dy);
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

    private static boolean envelopes(Slice left, Slice right, EnvelopesPredicate predicate)
    {
        Envelope leftEnvelope = deserializeEnvelope(left);
        Envelope rightEnvelope = deserializeEnvelope(right);
        if (leftEnvelope == null || rightEnvelope == null) {
            return false;
        }
        return predicate.apply(leftEnvelope, rightEnvelope);
    }

    private interface EnvelopesPredicate
    {
        boolean apply(Envelope left, Envelope right);
    }
}
