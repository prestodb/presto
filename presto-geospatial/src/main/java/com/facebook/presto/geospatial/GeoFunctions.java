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
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorCrosses;
import com.esri.core.geometry.OperatorDisjoint;
import com.esri.core.geometry.OperatorEquals;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.OperatorTouches;
import com.esri.core.geometry.OperatorWithin;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.reflect.Method;

import static com.facebook.presto.geospatial.GeometryUtils.deserialize;
import static com.facebook.presto.geospatial.GeometryUtils.serialize;

public final class GeoFunctions
{
    private static final String LINE = "LineString";
    private static final String MULTILINE = "MultiLineString";
    private static final String MULTIPOINT = "MultiPoint";
    private static final String POLYGON = "Polygon";
    private static final String POINT = "Point";
    private static final String GEOMETRY = "Geometry";

    private GeoFunctions() {}

    @Description("Construct Line Geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stLine(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry line = OGCGeometry.fromText(input.toStringUtf8());
        line.setSpatialReference(null);
        return serialize(line);
    }

    @Description("Construct Point Geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stPoint(@SqlType(StandardTypes.DOUBLE) double longitude, @SqlType(StandardTypes.DOUBLE) double latitude)
    {
        OGCGeometry geometry = OGCGeometry.createFromEsriGeometry(new Point(longitude, latitude), null);
        return serialize(geometry);
    }

    @Description("Construct Polygon Geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stPolygon(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry polygon = OGCGeometry.fromText(input.toStringUtf8());
        polygon.setSpatialReference(null);
        return serialize(polygon);
    }

    @Description("Returns area of input polygon")
    @ScalarFunction("st_area")
    @SqlType(StandardTypes.DOUBLE)
    public static double stArea(@SqlType(GEOMETRY) Slice input)
    {
        return deserialize(input).getEsriGeometry().calculateArea2D();
    }

    @Description("Returns Geometry of input wkt text")
    @ScalarFunction("st_geometry_from_wkt")
    @SqlType(GEOMETRY)
    public static Slice stGeometryFromWkt(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        return serialize(OGCGeometry.fromText(input.toStringUtf8()));
    }

    @SqlNullable
    @Description("Returns wtk text of input Geometry")
    @ScalarFunction("st_geometry_to_wkt")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stGeometryToWkt(@SqlType(GEOMETRY) Slice input)
    {
        return Slices.utf8Slice(deserialize(input).asText());
    }

    @Description("Returns point that is the center of the polygon's envelope")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stCentroid(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        String type = geometry.geometryType();
        if (type.equals(POLYGON)) {
            SpatialReference reference = geometry.getEsriSpatialReference();
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);
            Point centroid = new Point((envelope.getXMin() + envelope.getXMax()) / 2, (envelope.getYMin() + envelope.getYMax()) / 2);
            return serialize(OGCGeometry.createFromEsriGeometry(centroid, reference));
        }
        throw new IllegalArgumentException("st_centroid only applies to polygon. Input type is: " + type);
    }

    @Description("Returns count of coordinate components")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stCoordinateDimension(@SqlType(GEOMETRY) Slice input)
    {
        return deserialize(input).coordinateDimension();
    }

    @Description("Returns spatial dimension of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stDimension(@SqlType(GEOMETRY) Slice input)
    {
        return deserialize(input).dimension();
    }

    @SqlNullable
    @Description("Returns true if and only if the line is closed")
    @ScalarFunction("st_is_closed")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsClosed(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(LINE) || geometry.geometryType().equals(MULTILINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            int pathCount = lines.getPathCount();
            boolean result = true;
            for (int i = 0; result && i < pathCount; i++) {
                Point start = lines.getPoint(lines.getPathStart(i));
                Point end = lines.getPoint(lines.getPathEnd(i) - 1);
                result = result && end.equals(start);
            }
            return result;
        }
        throw new IllegalArgumentException("st_is_closed only support line and multiline. Input type is: " + geometry.geometryType());
    }

    @SqlNullable
    @Description("Returns true if and only if the geometry is empty")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(GEOMETRY) Slice input)
    {
        return deserialize(input).isEmpty();
    }

    @Description("Returns the length of line")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stLength(@SqlType(GEOMETRY) Slice input)
    {
        return deserialize(input).getEsriGeometry().calculateLength2D();
    }

    @Description("Returns the maximum X coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMaxX(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getXMax();
    }

    @Description("Returns the maximum Y coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMaxY(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getYMax();
    }

    @Description("Returns the minimum X coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMinX(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getXMin();
    }

    @Description("Returns the minimum Y coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMinY(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getYMin();
    }

    @Description("Returns the number of interior rings in the polygon")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stInteriorRingNumber(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(POLYGON)) {
            return ((OGCPolygon) geometry).numInteriorRing();
        }
        throw new IllegalArgumentException("st_interior_ring_number only applies to polygon. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the number of points in the geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stPointNumber(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(POLYGON)) {
            Polygon polygon = (Polygon) geometry.getEsriGeometry();
            return polygon.getPointCount() + polygon.getPathCount();
        }
        else if (geometry.geometryType().equals(POINT)) {
            return geometry.getEsriGeometry().isEmpty() ? 0 : 1;
        }
        else if (geometry.geometryType().equals(MULTIPOINT)) {
            return ((MultiPoint) geometry.getEsriGeometry()).getPointCount();
        }
        return ((MultiPath) geometry.getEsriGeometry()).getPointCount();
    }

    @SqlNullable
    @Description("Returns true if and only if the line is closed and simple")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsRing(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(LINE)) {
            OGCLineString line = (OGCLineString) geometry;
            return line.isClosed() && line.isSimple();
        }
        throw new IllegalArgumentException("st_is_ring only applies to linestring. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the start point of an line")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stStartPoint(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(LINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            SpatialReference reference = geometry.getEsriSpatialReference();
            return serialize(OGCGeometry.createFromEsriGeometry(lines.getPoint(0), reference));
        }
        throw new IllegalArgumentException("st_start_point only applies to linestring. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the end point of an line")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stEndPoint(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(LINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            SpatialReference reference = geometry.getEsriSpatialReference();
            return serialize(
              OGCGeometry.createFromEsriGeometry(lines.getPoint(lines.getPointCount() - 1), reference));
        }
        throw new IllegalArgumentException("st_end_point only applies to linestring. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the X coordinate of point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stX(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(POINT)) {
            return ((OGCPoint) geometry).X();
        }
        throw new IllegalArgumentException("st_x only applies to point. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the Y coordinate of point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stY(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(POINT)) {
            return ((OGCPoint) geometry).Y();
        }
        throw new IllegalArgumentException("st_y only applies to point. Input type is: " + geometry.geometryType());
    }

    @Description("Returns the boundary geometry of input geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stBoundary(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        OGCGeometry bound = geometry.boundary();
        if (bound.geometryType().equals(MULTILINE) && ((OGCMultiLineString) bound).numGeometries() == 1) {
            bound = ((OGCMultiLineString) bound).geometryN(0);
        }
        return serialize(bound);
    }

    @Description("Returns envelope of the input geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stEnvelope(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        SpatialReference reference = geometry.getEsriSpatialReference();
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return serialize(OGCGeometry.createFromEsriGeometry(envelope, reference));
    }

    @Description("Returns geometry difference of left geometry and right geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stDifference(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        return serialize(leftGeometry.difference(rightGeometry));
    }

    @Description("Returns distance between left geometry and right geometry")
    @ScalarFunction("st_distance")
    @SqlType(StandardTypes.DOUBLE)
    public static double stDistance(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        return leftGeometry.distance(rightGeometry);
    }

    @Description("Returns the exterior ring of the polygon")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stExteriorRing(@SqlType(GEOMETRY) Slice input)
    {
        OGCGeometry geometry = deserialize(input);
        if (geometry.geometryType().equals(POLYGON)) {
            try {
                Method method = OGCPolygon.class.getMethod("exteriorRing");
                OGCLineString exteriorRing = (OGCLineString) method.invoke(geometry);
                return serialize(exteriorRing);
            }
            catch (Exception e) {
                throw new IllegalArgumentException("st_exterior_ring only applies to polygon. Input type is: " + geometry.geometryType());
            }
        }
        throw new IllegalArgumentException("st_exterior_ring only applies to polygon. Input type is: " + geometry.geometryType());
    }

    @Description("Returns geometry intersection of left geometry and right geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stIntersection(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        return serialize(leftGeometry.intersection(rightGeometry));
    }

    @Description("Returns geometry symmetric difference of left geometry and right geometry")
    @ScalarFunction
    @SqlType(GEOMETRY)
    public static Slice stSymmetricDifference(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        return serialize(leftGeometry.symDifference(rightGeometry));
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry contains right geometry")
    @ScalarFunction("st_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContains(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation containsOperator = OperatorContains.local();
        return containsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry crosses right geometry")
    @ScalarFunction("st_crosses")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stCrosses(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation crossesOperator = OperatorCrosses.local();
        return crossesOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if the intersection of left geometry and right geometry is empty")
    @ScalarFunction("st_disjoint")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stDisjoint(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation disjointOperator = OperatorDisjoint.local();
        return disjointOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if the envelopes of left geometry and right geometry intersect")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEnvelopeIntersect(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        Envelope leftEnvelope = new Envelope();
        Envelope rightEnvelope = new Envelope();
        leftGeometry.getEsriGeometry().queryEnvelope(leftEnvelope);
        rightGeometry.getEsriGeometry().queryEnvelope(rightEnvelope);
        return leftEnvelope.isIntersecting(rightEnvelope);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry equals right geometry")
    @ScalarFunction("st_equals")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEquals(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation equalsOperator = OperatorEquals.local();
        return equalsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry intersects right geometry")
    @ScalarFunction("st_intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersects(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation intersectsOperator = OperatorIntersects.local();
        return intersectsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry overlaps right geometry")
    @ScalarFunction("st_overlaps")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stOverlaps(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation overlapsOperator = OperatorOverlaps.local();
        return overlapsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry has the specified DE-9IM relationship with right geometry")
    @ScalarFunction("st_relate")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stRelate(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right, @SqlType(StandardTypes.VARCHAR) Slice relation)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        return leftGeometry.relate(rightGeometry, relation.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry touches right geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stTouches(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation touchesOperator = OperatorTouches.local();
        return touchesOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry is within right geometry")
    @ScalarFunction("st_within")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stWithin(@SqlType(GEOMETRY) Slice left, @SqlType(GEOMETRY) Slice right)
    {
        OGCGeometry leftGeometry = deserialize(left);
        OGCGeometry rightGeometry = deserialize(right);
        OperatorSimpleRelation withinOperator = OperatorWithin.local();
        return withinOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }
}
