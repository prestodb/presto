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
import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.ListeningGeometryCursor;
import com.esri.core.geometry.NonSimpleResult.Reason;
import com.esri.core.geometry.OperatorUnion;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.KdbTreeType;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.geospatial.KdbTree;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.geospatial.serde.EsriGeometrySerde;
import com.facebook.presto.geospatial.serde.GeometrySerializationType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.operation.distance.DistanceOp;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.esri.core.geometry.NonSimpleResult.Reason.Clustering;
import static com.esri.core.geometry.NonSimpleResult.Reason.Cracking;
import static com.esri.core.geometry.NonSimpleResult.Reason.CrossOver;
import static com.esri.core.geometry.NonSimpleResult.Reason.DegenerateSegments;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCDisconnectedInterior;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCPolygonSelfTangency;
import static com.esri.core.geometry.NonSimpleResult.Reason.OGCPolylineSelfTangency;
import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.geospatial.GeometryType.LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_LINE_STRING;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POINT;
import static com.facebook.presto.geospatial.GeometryType.MULTI_POLYGON;
import static com.facebook.presto.geospatial.GeometryType.POINT;
import static com.facebook.presto.geospatial.GeometryType.POLYGON;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsEmptyLineString;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsEmptyPoint;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsEmptyPolygon;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsLineString;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsMultiPoint;
import static com.facebook.presto.geospatial.GeometryUtils.createJtsPoint;
import static com.facebook.presto.geospatial.GeometryUtils.flattenCollection;
import static com.facebook.presto.geospatial.GeometryUtils.getGeometryInvalidReason;
import static com.facebook.presto.geospatial.GeometryUtils.getPointCount;
import static com.facebook.presto.geospatial.GeometryUtils.jsonFromJtsGeometry;
import static com.facebook.presto.geospatial.GeometryUtils.jtsGeometryFromJson;
import static com.facebook.presto.geospatial.GeometryUtils.jtsGeometryFromWkt;
import static com.facebook.presto.geospatial.GeometryUtils.wktFromJtsGeometry;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeEnvelope;
import static com.facebook.presto.geospatial.serde.EsriGeometrySerde.deserializeType;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.deserialize;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.serialize;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.setAll;
import static java.util.Objects.requireNonNull;
import static org.locationtech.jts.simplify.TopologyPreservingSimplifier.simplify;

public final class GeoFunctions
{
    private static final Joiner OR_JOINER = Joiner.on(" or ");
    private static final Slice EMPTY_POLYGON = serialize(createJtsEmptyPolygon());
    private static final Map<Reason, String> NON_SIMPLE_REASONS = ImmutableMap.<Reason, String>builder()
            .put(DegenerateSegments, "Degenerate segments")
            .put(Clustering, "Repeated points")
            .put(Cracking, "Intersecting or overlapping segments")
            .put(CrossOver, "Self-intersection")
            .put(OGCPolylineSelfTangency, "Self-tangency")
            .put(OGCPolygonSelfTangency, "Self-tangency")
            .put(OGCDisconnectedInterior, "Disconnected interior")
            .build();
    private static final int NUMBER_OF_DIMENSIONS = 3;
    private static final Block EMPTY_ARRAY_OF_INTS = IntegerType.INTEGER.createFixedSizeBlockBuilder(0).build();

    private GeoFunctions() {}

    @Description("Returns a Geometry type LineString object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_LineFromText")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice parseLine(@SqlType(VARCHAR) Slice input)
    {
        Geometry geometry = jtsGeometryFromWkt(input.toStringUtf8());
        validateType("ST_LineFromText", geometry, EnumSet.of(LINE_STRING));
        return serialize(geometry);
    }

    @Description("Returns a LineString from an array of points")
    @ScalarFunction("ST_LineString")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stLineString(@SqlType("array(" + GEOMETRY_TYPE_NAME + ")") Block input)
    {
        CoordinateSequence coordinates = readPointCoordinates(input, "ST_LineString", true);
        if (coordinates.size() < 2) {
            return serialize(createJtsEmptyLineString());
        }

        return serialize(createJtsLineString(coordinates));
    }

    @Description("Returns a Geometry type Point object with the given coordinate values")
    @ScalarFunction("ST_Point")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPoint(@SqlType(DOUBLE) double x, @SqlType(DOUBLE) double y)
    {
        return serialize(createJtsPoint(x, y));
    }

    @SqlNullable
    @Description("Returns a multi-point geometry formed from input points")
    @ScalarFunction("ST_MultiPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stMultiPoint(@SqlType("array(" + GEOMETRY_TYPE_NAME + ")") Block input)
    {
        CoordinateSequence coordinates = readPointCoordinates(input, "ST_MultiPoint", false);
        if (coordinates.size() == 0) {
            return null;
        }

        return serialize(createJtsMultiPoint(coordinates));
    }

    private static CoordinateSequence readPointCoordinates(Block input, String functionName, boolean forbidDuplicates)
    {
        PackedCoordinateSequenceFactory coordinateSequenceFactory = new PackedCoordinateSequenceFactory();
        double[] coordinates = new double[2 * input.getPositionCount()];
        double lastX = Double.NaN;
        double lastY = Double.NaN;
        for (int i = 0; i < input.getPositionCount(); i++) {
            if (input.isNull(i)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to %s: null at index %s", functionName, i + 1));
            }

            BasicSliceInput slice = new BasicSliceInput(GEOMETRY.getSlice(input, i));
            GeometrySerializationType type = GeometrySerializationType.getForCode(slice.readByte());
            if (type != GeometrySerializationType.POINT) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to %s: geometry is not a point: %s at index %s", functionName, type.toString(), i + 1));
            }

            double x = slice.readDouble();
            double y = slice.readDouble();

            if (Double.isNaN(x) || Double.isNaN(x)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid input to %s: empty point at index %s", functionName, i + 1));
            }
            if (forbidDuplicates && x == lastX && y == lastY) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                        format("Invalid input to %s: consecutive duplicate points at index %s", functionName, i + 1));
            }

            lastX = x;
            lastY = y;
            coordinates[2 * i] = x;
            coordinates[2 * i + 1] = y;
        }

        return coordinateSequenceFactory.create(coordinates, 2);
    }

    @Description("Returns a Geometry type Polygon object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_Polygon")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPolygon(@SqlType(VARCHAR) Slice input)
    {
        Geometry geometry = jtsGeometryFromWkt(input.toStringUtf8());
        validateType("ST_Polygon", geometry, EnumSet.of(POLYGON));
        return serialize(geometry);
    }

    @Description("Returns the 2D Euclidean area of a geometry")
    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static double stArea(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserialize(input).getArea();
    }

    @Description("Returns a Geometry type object from Well-Known Text representation (WKT)")
    @ScalarFunction("ST_GeometryFromText")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryFromText(@SqlType(VARCHAR) Slice input)
    {
        return serialize(jtsGeometryFromWkt(input.toStringUtf8()));
    }

    @Description("Returns a Geometry type object from Well-Known Binary representation (WKB)")
    @ScalarFunction("ST_GeomFromBinary")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeomFromBinary(@SqlType(VARBINARY) Slice input)
    {
        return EsriGeometrySerde.serialize(geomFromBinary(input));
    }

    @Description("Returns the Well-Known Text (WKT) representation of the geometry")
    @ScalarFunction("ST_AsText")
    @SqlType(VARCHAR)
    public static Slice stAsText(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return utf8Slice(wktFromJtsGeometry(deserialize(input)));
    }

    @Description("Returns the Well-Known Binary (WKB) representation of the geometry")
    @ScalarFunction("ST_AsBinary")
    @SqlType(VARBINARY)
    public static Slice stAsBinary(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        try {
            return wrappedBuffer(EsriGeometrySerde.deserialize(input).asBinary());
        }
        catch (GeometryException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid geometry: " + e.getMessage(), e);
        }
    }

    @SqlNullable
    @Description("Returns the geometry that represents all points whose distance from the specified geometry is less than or equal to the specified distance")
    @ScalarFunction("ST_Buffer")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBuffer(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(DOUBLE) double distance)
    {
        if (isNaN(distance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is NaN");
        }

        if (distance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distance is negative");
        }

        if (distance == 0) {
            return input;
        }

        Geometry geometry = deserialize(input);
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
        Geometry geometry = deserialize(input);
        validateType("ST_Centroid", geometry, EnumSet.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON));
        GeometryType geometryType = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (geometryType == GeometryType.POINT) {
            return input;
        }

        if (geometry.getNumPoints() == 0) {
            return serialize(createJtsEmptyPoint());
        }
        return serialize(geometry.getCentroid());
    }

    @Description("Returns the minimum convex geometry that encloses all input geometries")
    @ScalarFunction("ST_ConvexHull")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stConvexHull(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        if (geometry.isEmpty()) {
            return input;
        }
        if (GeometryType.getForEsriGeometryType(geometry.geometryType()) == POINT) {
            return input;
        }
        return EsriGeometrySerde.serialize(geometry.convexHull());
    }

    @Description("Return the coordinate dimension of the Geometry")
    @ScalarFunction("ST_CoordDim")
    @SqlType(TINYINT)
    public static long stCoordinateDimension(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return EsriGeometrySerde.deserialize(input).coordinateDimension();
    }

    @Description("Returns the inherent dimension of this Geometry object, which must be less than or equal to the coordinate dimension")
    @ScalarFunction("ST_Dimension")
    @SqlType(TINYINT)
    public static long stDimension(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserialize(input).getDimension();
    }

    @SqlNullable
    @Description("Returns TRUE if the LineString or Multi-LineString's start and end points are coincident")
    @ScalarFunction("ST_IsClosed")
    @SqlType(BOOLEAN)
    public static Boolean stIsClosed(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_IsClosed", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        if (geometry instanceof LineString) {
            return ((LineString) geometry).isClosed();
        }
        else if (geometry instanceof MultiLineString) {
            return ((MultiLineString) geometry).isClosed();
        }

        // This would be handled in validateType, but for completeness.
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid type for isClosed: %s", geometry.getGeometryType()));
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is an empty geometrycollection, polygon, point etc")
    @ScalarFunction("ST_IsEmpty")
    @SqlType(BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return deserializeEnvelope(input).isEmpty();
    }

    @Description("Returns TRUE if this Geometry has no anomalous geometric points, such as self intersection or self tangency")
    @ScalarFunction("ST_IsSimple")
    @SqlType(BOOLEAN)
    public static boolean stIsSimple(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        try {
            return deserialize(input).isSimple();
        }
        catch (PrestoException e) {
            if (e.getCause() instanceof TopologyException) {
                return false;
            }
            throw e;
        }
    }

    @Description("Returns true if the input geometry is well formed")
    @ScalarFunction("ST_IsValid")
    @SqlType(BOOLEAN)
    public static boolean stIsValid(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        try {
            return deserialize(input).isValid();
        }
        catch (PrestoException e) {
            if (e.getCause() instanceof TopologyException) {
                return false;
            }
            throw e;
        }
    }

    @Description("Returns the reason for why the input geometry is not valid. Returns null if the input is valid.")
    @ScalarFunction("geometry_invalid_reason")
    @SqlType(VARCHAR)
    @SqlNullable
    public static Slice invalidReason(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        try {
            Geometry geometry = deserialize(input);
            return getGeometryInvalidReason(geometry).map(Slices::utf8Slice).orElse(null);
        }
        catch (PrestoException e) {
            if (e.getCause() instanceof TopologyException) {
                return utf8Slice(e.getMessage());
            }
            throw e;
        }
    }

    @Description("Returns the length of a LineString or Multi-LineString using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static double stLength(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Length", geometry, EnumSet.of(LINE_STRING, MULTI_LINE_STRING));
        return geometry.getLength();
    }

    @SqlNullable
    @Description("Returns a float between 0 and 1 representing the location of the closest point on the LineString to the given Point, as a fraction of total 2d line length.")
    @ScalarFunction("line_locate_point")
    @SqlType(DOUBLE)
    public static Double lineLocatePoint(@SqlType(GEOMETRY_TYPE_NAME) Slice lineSlice, @SqlType(GEOMETRY_TYPE_NAME) Slice pointSlice)
    {
        Geometry line = deserialize(lineSlice);
        Geometry point = deserialize(pointSlice);

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

    @Description("Returns the point in the line at the fractional length.")
    @ScalarFunction("line_interpolate_point")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice lineInterpolatePoint(@SqlType(GEOMETRY_TYPE_NAME) Slice lineSlice, @SqlType(DOUBLE) double fraction)
    {
        if (!(0.0 <= fraction && fraction <= 1.0)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("line_interpolate_point: Fraction must be between 0 and 1, but is %s", fraction));
        }

        Geometry geometry = deserialize(lineSlice);
        validateType("line_interpolate_point", geometry, ImmutableSet.of(LINE_STRING));
        LineString line = (LineString) geometry;

        if (line.isEmpty()) {
            return serialize(createJtsEmptyPoint());
        }

        org.locationtech.jts.geom.Coordinate coordinate = new LengthIndexedLine(line).extractPoint(fraction * line.getLength());
        return serialize(createJtsPoint(coordinate));
    }

    @SqlNullable
    @Description("Returns X maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMax")
    @SqlType(DOUBLE)
    public static Double stXMax(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isEmpty()) {
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
        if (envelope.isEmpty()) {
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
        if (envelope.isEmpty()) {
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
        if (envelope.isEmpty()) {
            return null;
        }
        return envelope.getYMin();
    }

    @SqlNullable
    @Description("Returns the cardinality of the collection of interior rings of a polygon")
    @ScalarFunction("ST_NumInteriorRing")
    @SqlType(BIGINT)
    public static Long stNumInteriorRings(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_NumInteriorRing", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return Long.valueOf(((org.locationtech.jts.geom.Polygon) geometry).getNumInteriorRing());
    }

    @SqlNullable
    @Description("Returns an array of interior rings of a polygon")
    @ScalarFunction("ST_InteriorRings")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stInteriorRings(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_InteriorRings", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }

        org.locationtech.jts.geom.Polygon polygon = (org.locationtech.jts.geom.Polygon) geometry;
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, polygon.getNumInteriorRing());
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize((LineString) polygon.getInteriorRingN(i)));
        }
        return blockBuilder.build();
    }

    @Description("Returns the cardinality of the geometry collection")
    @ScalarFunction("ST_NumGeometries")
    @SqlType(INTEGER)
    public static long stNumGeometries(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return 0;
        }
        return geometry.getNumGeometries();
    }

    @Description("Returns a geometry that represents the point set union of the input geometries.")
    @ScalarFunction("ST_Union")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stUnion(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        return stUnion(ImmutableList.of(left, right));
    }

    @Description("Returns a geometry that represents the point set union of the input geometries.")
    @ScalarFunction("geometry_union")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice geometryUnion(@SqlType("array(" + GEOMETRY_TYPE_NAME + ")") Block input)
    {
        return stUnion(getGeometrySlicesFromBlock(input));
    }

    private static Slice stUnion(Iterable<Slice> slices)
    {
        // The current state of Esri/geometry-api-java does not allow support for multiple dimensions being
        // fed to the union operator without dropping the lower dimensions:
        // https://github.com/Esri/geometry-api-java/issues/199
        // When operating over a collection of geometries, it is more efficient to reuse the same operator
        // for the entire operation.  Therefore, split the inputs and operators by dimension, and then union
        // each dimension's result at the end.
        ListeningGeometryCursor[] cursorsByDimension = new ListeningGeometryCursor[NUMBER_OF_DIMENSIONS];
        GeometryCursor[] operatorsByDimension = new GeometryCursor[NUMBER_OF_DIMENSIONS];

        setAll(cursorsByDimension, i -> new ListeningGeometryCursor());
        setAll(operatorsByDimension, i -> OperatorUnion.local().execute(cursorsByDimension[i], null, null));

        Iterator<Slice> slicesIterator = slices.iterator();
        if (!slicesIterator.hasNext()) {
            return null;
        }
        while (slicesIterator.hasNext()) {
            Slice slice = slicesIterator.next();
            // Ignore null inputs
            if (slice.getInput().available() == 0) {
                continue;
            }

            for (OGCGeometry geometry : flattenCollection(EsriGeometrySerde.deserialize(slice))) {
                int dimension = geometry.dimension();
                cursorsByDimension[dimension].tick(geometry.getEsriGeometry());
                operatorsByDimension[dimension].tock();
            }
        }

        List<OGCGeometry> outputs = new ArrayList<>();
        for (GeometryCursor operator : operatorsByDimension) {
            OGCGeometry unionedGeometry = createFromEsriGeometry(operator.next(), null);
            if (unionedGeometry != null) {
                outputs.add(unionedGeometry);
            }
        }

        if (outputs.size() == 1) {
            return EsriGeometrySerde.serialize(outputs.get(0));
        }
        return EsriGeometrySerde.serialize(new OGCConcreteGeometryCollection(outputs, null).flattenAndRemoveOverlaps().reduceFromMulti());
    }

    @SqlNullable
    @Description("Returns the geometry element at the specified index (indices started with 1)")
    @ScalarFunction("ST_GeometryN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!type.isMultitype()) {
            if (index == 1) {
                return input;
            }
            return null;
        }
        GeometryCollection geometryCollection = ((GeometryCollection) geometry);
        if (index < 1 || index > geometryCollection.getNumGeometries()) {
            return null;
        }
        return serialize(geometryCollection.getGeometryN((int) index - 1));
    }

    @SqlNullable
    @Description("Returns the vertex of a linestring at the specified index (indices started with 1) ")
    @ScalarFunction("ST_PointN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stPointN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_PointN", geometry, EnumSet.of(LINE_STRING));

        LineString linestring = (LineString) geometry;
        if (index < 1 || index > linestring.getNumPoints()) {
            return null;
        }
        return serialize(linestring.getPointN(toIntExact(index) - 1));
    }

    @SqlNullable
    @Description("Returns an array of geometries in the specified collection")
    @ScalarFunction("ST_Geometries")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stGeometries(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!type.isMultitype()) {
            BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 1);
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry));
            return blockBuilder.build();
        }

        GeometryCollection collection = (GeometryCollection) geometry;
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, collection.getNumGeometries());
        for (int i = 0; i < collection.getNumGeometries(); i++) {
            GEOMETRY.writeSlice(blockBuilder, serialize(collection.getGeometryN(i)));
        }
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("Returns the interior ring element at the specified index (indices start at 1)")
    @ScalarFunction("ST_InteriorRingN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stInteriorRingN(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(INTEGER) long index)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_InteriorRingN", geometry, EnumSet.of(POLYGON));
        org.locationtech.jts.geom.Polygon polygon = (org.locationtech.jts.geom.Polygon) geometry;
        if (index < 1 || index > polygon.getNumInteriorRing()) {
            return null;
        }
        return serialize(polygon.getInteriorRingN(toIntExact(index) - 1));
    }

    @Description("Returns the number of points in a Geometry")
    @ScalarFunction("ST_NumPoints")
    @SqlType(BIGINT)
    public static long stNumPoints(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return getPointCount(EsriGeometrySerde.deserialize(input));
    }

    @SqlNullable
    @Description("Returns TRUE if and only if the line is closed and simple")
    @ScalarFunction("ST_IsRing")
    @SqlType(BOOLEAN)
    public static Boolean stIsRing(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
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
        Geometry geometry = deserialize(input);
        validateType("ST_StartPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        return serialize(((LineString) geometry).getStartPoint());
    }

    @Description("Returns a \"simplified\" version of the given geometry")
    @ScalarFunction("simplify_geometry")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice simplifyGeometry(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(DOUBLE) double distanceTolerance)
    {
        if (isNaN(distanceTolerance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is NaN");
        }

        if (distanceTolerance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "distanceTolerance is negative");
        }

        if (distanceTolerance == 0) {
            return input;
        }

        return serialize(simplify(deserialize(input), distanceTolerance));
    }

    @SqlNullable
    @Description("Returns the last point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_EndPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEndPoint(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_EndPoint", geometry, EnumSet.of(LINE_STRING));
        if (geometry.isEmpty()) {
            return null;
        }
        return serialize(((LineString) geometry).getEndPoint());
    }

    @SqlNullable
    @Description("Returns an array of points in a geometry")
    @ScalarFunction("ST_Points")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stPoints(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        if (geometry.isEmpty()) {
            return null;
        }

        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, geometry.getNumPoints());
        buildPointsBlock(geometry, blockBuilder);

        return blockBuilder.build();
    }

    private static void buildPointsBlock(Geometry geometry, BlockBuilder blockBuilder)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (type == GeometryType.POINT) {
            GEOMETRY.writeSlice(blockBuilder, serialize(geometry));
        }
        else if (type == GeometryType.GEOMETRY_COLLECTION) {
            GeometryCollection collection = (GeometryCollection) geometry;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                Geometry entry = collection.getGeometryN(i);
                buildPointsBlock(entry, blockBuilder);
            }
        }
        else {
            GeometryFactory geometryFactory = geometry.getFactory();
            Coordinate[] vertices = geometry.getCoordinates();
            for (Coordinate coordinate : vertices) {
                GEOMETRY.writeSlice(blockBuilder, serialize(geometryFactory.createPoint(coordinate)));
            }
        }
    }

    @SqlNullable
    @Description("Return the X coordinate of the point")
    @ScalarFunction("ST_X")
    @SqlType(DOUBLE)
    public static Double stX(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_X", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((org.locationtech.jts.geom.Point) geometry).getX();
    }

    @SqlNullable
    @Description("Return the Y coordinate of the point")
    @ScalarFunction("ST_Y")
    @SqlType(DOUBLE)
    public static Double stY(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_Y", geometry, EnumSet.of(POINT));
        if (geometry.isEmpty()) {
            return null;
        }
        return ((org.locationtech.jts.geom.Point) geometry).getY();
    }

    @Description("Returns the closure of the combinatorial boundary of this Geometry")
    @ScalarFunction("ST_Boundary")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBoundary(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return serialize(deserialize(input).getBoundary());
    }

    @Description("Returns the bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_Envelope")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEnvelope(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isEmpty()) {
            return EMPTY_POLYGON;
        }
        return EsriGeometrySerde.serialize(envelope);
    }

    @SqlNullable
    @Description("Returns the lower left and upper right corners of bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_EnvelopeAsPts")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block stEnvelopeAsPts(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isEmpty()) {
            return null;
        }
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 2);
        org.locationtech.jts.geom.Point lowerLeftCorner = createJtsPoint(envelope.getXMin(), envelope.getYMin());
        org.locationtech.jts.geom.Point upperRightCorner = createJtsPoint(envelope.getXMax(), envelope.getYMax());
        GEOMETRY.writeSlice(blockBuilder, serialize(lowerLeftCorner));
        GEOMETRY.writeSlice(blockBuilder, serialize(upperRightCorner));
        return blockBuilder.build();
    }

    @Description("Returns the bounding rectangle of a Geometry expanded by distance.")
    @ScalarFunction("expand_envelope")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice expandEnvelope(@SqlType(GEOMETRY_TYPE_NAME) Slice input, @SqlType(DOUBLE) double distance)
    {
        if (isNaN(distance)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "expand_envelope: distance is NaN");
        }

        if (distance < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("expand_envelope: distance %s is negative", distance));
        }

        Envelope envelope = deserializeEnvelope(input);
        if (envelope.isEmpty()) {
            return EMPTY_POLYGON;
        }
        return EsriGeometrySerde.serialize(new Envelope(
                envelope.getXMin() - distance,
                envelope.getYMin() - distance,
                envelope.getXMax() + distance,
                envelope.getYMax() + distance));
    }

    @Description("Returns the Geometry value that represents the point set difference of two geometries")
    @ScalarFunction("ST_Difference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stDifference(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return EsriGeometrySerde.serialize(leftGeometry.difference(rightGeometry));
    }

    @SqlNullable
    @Description("Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units")
    @ScalarFunction("ST_Distance")
    @SqlType(DOUBLE)
    public static Double stDistance(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.isEmpty() || rightGeometry.isEmpty() ? null : leftGeometry.distance(rightGeometry);
    }

    @SqlNullable
    @Description("Return the closest points on the two geometries")
    @ScalarFunction("geometry_nearest_points")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block geometryNearestPoints(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        Geometry leftGeometry = deserialize(left);
        Geometry rightGeometry = deserialize(right);
        if (leftGeometry.isEmpty() || rightGeometry.isEmpty()) {
            return null;
        }
        try {
            Coordinate[] nearestCoordinates = DistanceOp.nearestPoints(leftGeometry, rightGeometry);
            BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, 2);
            GEOMETRY.writeSlice(blockBuilder, serialize(createJtsPoint(nearestCoordinates[0])));
            GEOMETRY.writeSlice(blockBuilder, serialize(createJtsPoint(nearestCoordinates[1])));
            return blockBuilder.build();
        }
        catch (TopologyException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
    }

    @SqlNullable
    @Description("Returns a line string representing the exterior ring of the POLYGON")
    @ScalarFunction("ST_ExteriorRing")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stExteriorRing(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Geometry geometry = deserialize(input);
        validateType("ST_ExteriorRing", geometry, EnumSet.of(POLYGON));
        if (geometry.isEmpty()) {
            return null;
        }
        return serialize(((org.locationtech.jts.geom.Polygon) geometry).getExteriorRing());
    }

    @Description("Returns the Geometry value that represents the point set intersection of two Geometries")
    @ScalarFunction("ST_Intersection")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stIntersection(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        GeometrySerializationType leftType = deserializeType(left);
        GeometrySerializationType rightType = deserializeType(right);

        if (leftType == GeometrySerializationType.ENVELOPE && rightType == GeometrySerializationType.ENVELOPE) {
            Envelope leftEnvelope = deserializeEnvelope(left);
            Envelope rightEnvelope = deserializeEnvelope(right);

            // Envelope#intersect updates leftEnvelope to the intersection of the two envelopes
            if (!leftEnvelope.intersect(rightEnvelope)) {
                return EMPTY_POLYGON;
            }

            Envelope intersection = leftEnvelope;
            if (intersection.getXMin() == intersection.getXMax() || intersection.getYMin() == intersection.getYMax()) {
                if (intersection.getXMin() == intersection.getXMax() && intersection.getYMin() == intersection.getYMax()) {
                    return EsriGeometrySerde.serialize(createFromEsriGeometry(new Point(intersection.getXMin(), intersection.getYMin()), null));
                }
                return EsriGeometrySerde.serialize(createFromEsriGeometry(new Polyline(new Point(intersection.getXMin(), intersection.getYMin()), new Point(intersection.getXMax(), intersection.getYMax())), null));
            }

            return EsriGeometrySerde.serialize(intersection);
        }

        // If one side is an envelope, then if it contains the other's envelope we can just return the other geometry.
        if (leftType == GeometrySerializationType.ENVELOPE
                && deserializeEnvelope(left).contains(deserializeEnvelope(right))) {
            return right;
        }

        if (rightType == GeometrySerializationType.ENVELOPE
                && deserializeEnvelope(right).contains(deserializeEnvelope(left))) {
            return left;
        }

        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return EsriGeometrySerde.serialize(leftGeometry.intersection(rightGeometry));
    }

    @Description("Returns the Geometry value that represents the point set symmetric difference of two Geometries")
    @ScalarFunction("ST_SymDifference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stSymmetricDifference(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return EsriGeometrySerde.serialize(leftGeometry.symDifference(rightGeometry));
    }

    @SqlNullable
    @Description("Returns TRUE if and only if no points of right lie in the exterior of left, and at least one point of the interior of left lies in the interior of right")
    @ScalarFunction("ST_Contains")
    @SqlType(BOOLEAN)
    public static Boolean stContains(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::contains)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.contains(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the supplied geometries have some, but not all, interior points in common")
    @ScalarFunction("ST_Crosses")
    @SqlType(BOOLEAN)
    public static Boolean stCrosses(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.crosses(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries do not spatially intersect - if they do not share any space together")
    @ScalarFunction("ST_Disjoint")
    @SqlType(BOOLEAN)
    public static Boolean stDisjoint(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::intersect)) {
            return true;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.disjoint(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the given geometries represent the same geometry")
    @ScalarFunction("ST_Equals")
    @SqlType(BOOLEAN)
    public static Boolean stEquals(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.Equals(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries spatially intersect in 2D - (share any portion of space) and FALSE if they don't (they are Disjoint)")
    @ScalarFunction("ST_Intersects")
    @SqlType(BOOLEAN)
    public static Boolean stIntersects(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.intersects(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries share space, are of the same dimension, but are not completely contained by each other")
    @ScalarFunction("ST_Overlaps")
    @SqlType(BOOLEAN)
    public static Boolean stOverlaps(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.overlaps(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is spatially related to another Geometry")
    @ScalarFunction("ST_Relate")
    @SqlType(BOOLEAN)
    public static Boolean stRelate(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right, @SqlType(VARCHAR) Slice relation)
    {
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.relate(rightGeometry, relation.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect")
    @ScalarFunction("ST_Touches")
    @SqlType(BOOLEAN)
    public static Boolean stTouches(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(left, right, Envelope::intersect)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.touches(rightGeometry);
    }

    @SqlNullable
    @Description("Returns TRUE if the geometry A is completely inside geometry B")
    @ScalarFunction("ST_Within")
    @SqlType(BOOLEAN)
    public static Boolean stWithin(@SqlType(GEOMETRY_TYPE_NAME) Slice left, @SqlType(GEOMETRY_TYPE_NAME) Slice right)
    {
        if (!envelopes(right, left, Envelope::contains)) {
            return false;
        }
        OGCGeometry leftGeometry = EsriGeometrySerde.deserialize(left);
        OGCGeometry rightGeometry = EsriGeometrySerde.deserialize(right);
        verifySameSpatialReference(leftGeometry, rightGeometry);
        return leftGeometry.within(rightGeometry);
    }

    @Description("Returns the type of the geometry")
    @ScalarFunction("ST_GeometryType")
    @SqlType(VARCHAR)
    public static Slice stGeometryType(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        return EsriGeometrySerde.getGeometryType(input).standardName();
    }

    @Description("Recursively flattens GeometryCollections")
    @ScalarFunction("flatten_geometry_collections")
    @SqlType("array(" + GEOMETRY_TYPE_NAME + ")")
    public static Block flattenGeometryCollections(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = EsriGeometrySerde.deserialize(input);
        List<OGCGeometry> components = Streams.stream(
                flattenCollection(geometry)).collect(toImmutableList());
        BlockBuilder blockBuilder = GEOMETRY.createBlockBuilder(null, components.size());
        for (OGCGeometry component : components) {
            GEOMETRY.writeSlice(blockBuilder, EsriGeometrySerde.serialize(component));
        }
        return blockBuilder.build();
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a given geometry")
    @SqlType("array(int)")
    public static Block spatialPartitions(@SqlType(KdbTreeType.NAME) Object kdbTree, @SqlType(GEOMETRY_TYPE_NAME) Slice geometry)
    {
        Envelope envelope = deserializeEnvelope(geometry);
        if (envelope.isEmpty()) {
            // Empty geometry
            return null;
        }

        return spatialPartitions((KdbTree) kdbTree, new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax()));
    }

    @ScalarFunction
    @SqlNullable
    @Description("Returns an array of spatial partition IDs for a geometry representing a set of points within specified distance from the input geometry")
    @SqlType("array(int)")
    public static Block spatialPartitions(@SqlType(KdbTreeType.NAME) Object kdbTree, @SqlType(GEOMETRY_TYPE_NAME) Slice geometry, @SqlType(DOUBLE) double distance)
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
        return spatialPartitions((KdbTree) kdbTree, expandedEnvelope2D);
    }

    @ScalarFunction("geometry_from_geojson")
    @Description("Returns a geometry from a geo JSON string")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice geometryFromGeoJson(@SqlType(VARCHAR) Slice input)
    {
        return serialize(jtsGeometryFromJson(input.toStringUtf8()));
    }

    @SqlNullable
    @ScalarFunction("geometry_as_geojson")
    @Description("Returns geo JSON string based on the input geometry")
    @SqlType(VARCHAR)
    public static Slice geometryAsGeoJson(@SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        Optional<String> geoJson = jsonFromJtsGeometry(deserialize(input));
        if (geoJson.isPresent()) {
            return utf8Slice(geoJson.get());
        }
        else {
            return null;
        }
    }

    // Package visible for SphericalGeoFunctions
    /*package*/ static Block spatialPartitions(KdbTree kdbTree, Rectangle envelope)
    {
        Map<Integer, Rectangle> partitions = kdbTree.findIntersectingLeaves(envelope);
        if (partitions.isEmpty()) {
            return EMPTY_ARRAY_OF_INTS;
        }

        BlockBuilder blockBuilder = IntegerType.INTEGER.createFixedSizeBlockBuilder(partitions.size());
        for (int id : partitions.keySet()) {
            blockBuilder.writeInt(id);
        }

        return blockBuilder.build();
    }

    private static OGCGeometry geomFromBinary(Slice input)
    {
        requireNonNull(input, "input is null");
        OGCGeometry geometry;
        try {
            geometry = OGCGeometry.fromBinary(input.toByteBuffer().slice());
        }
        catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKB", e);
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

    private static void validateType(String function, Geometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForJtsGeometryType(geometry.getGeometryType());
        if (!validTypes.contains(type)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }

    private static void verifySameSpatialReference(OGCGeometry leftGeometry, OGCGeometry rightGeometry)
    {
        checkArgument(Objects.equals(leftGeometry.getEsriSpatialReference(), rightGeometry.getEsriSpatialReference()), "Input geometries must have the same spatial reference");
    }

    private static boolean envelopes(Slice left, Slice right, EnvelopesPredicate predicate)
    {
        Envelope leftEnvelope = deserializeEnvelope(left);
        Envelope rightEnvelope = deserializeEnvelope(right);
        if (leftEnvelope.isEmpty() || rightEnvelope.isEmpty()) {
            return false;
        }
        return predicate.apply(leftEnvelope, rightEnvelope);
    }

    private interface EnvelopesPredicate
    {
        boolean apply(Envelope left, Envelope right);
    }
    private static Iterable<Slice> getGeometrySlicesFromBlock(Block block)
    {
        requireNonNull(block, "block is null");
        return () -> new Iterator<Slice>()
        {
            private int iteratorPosition;

            @Override
            public boolean hasNext()
            {
                return iteratorPosition != block.getPositionCount();
            }

            @Override
            public Slice next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("Slices have been consumed");
                }
                return GEOMETRY.getSlice(block, iteratorPosition++);
            }
        };
    }
}
