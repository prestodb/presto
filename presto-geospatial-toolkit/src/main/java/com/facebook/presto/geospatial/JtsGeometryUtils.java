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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.util.Objects.requireNonNull;

public class JtsGeometryUtils
{
    /**
     * Shape type codes from ERSI's specification
     * https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
     */
    private enum EsriShapeType
    {
        POINT(1),
        POLYLINE(3),
        POLYGON(5),
        MULTI_POINT(8);

        final int code;

        EsriShapeType(int code)
        {
            this.code = code;
        }

        static EsriShapeType valueOf(int code)
        {
            for (EsriShapeType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }

            throw new IllegalArgumentException("Unsupported ESRI shape code: " + code);
        }
    }

    /**
     * Shape types defined by JTS.
     */
    private static final String JTS_POINT = "Point";
    private static final String JTS_POLYGON = "Polygon";
    private static final String JTS_LINESTRING = "LineString";
    private static final String JTS_MULTI_POINT = "MultiPoint";
    private static final String JTS_MULTI_POLYGON = "MultiPolygon";
    private static final String JTS_MULTI_LINESTRING = "MultiLineString";
    private static final String JTS_GEOMETRY_COLLECTION = "GeometryCollection";

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private JtsGeometryUtils() {}

    /**
     * Deserializes ESRI shape as described in
     * https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
     * into JTS Geometry object.
     */
    public static Geometry deserialize(Slice shape)
    {
        if (shape == null) {
            return null;
        }

        BasicSliceInput input = shape.getInput();
        int geometryType = input.readByte();
        if (geometryType != GeometryUtils.GeometryTypeName.GEOMETRY_COLLECTION.code()) {
            return readGeometry(input);
        }

        if (input.available() == 0) {
            return GEOMETRY_FACTORY.createGeometryCollection();
        }

        // GeometryCollection: geometry-type|len-of-shape1|bytes-of-shape1|len-of-shape2|bytes-of-shape2...
        List<Geometry> geometries = new ArrayList<>();
        while (input.available() > 0) {
            int length = input.readInt();
            geometries.add(readGeometry(input.readSlice(length).getInput()));
        }

        return GEOMETRY_FACTORY.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry)
    {
        SliceOutput output = new DynamicSliceOutput(100);

        boolean inGeometryCollection = geometry.getGeometryType().equals(JTS_GEOMETRY_COLLECTION);
        if (inGeometryCollection) {
            output.writeByte(GeometryUtils.GeometryTypeName.GEOMETRY_COLLECTION.code());
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                writeGeometry(geometry.getGeometryN(i), output, true);
            }
        }
        else {
            writeGeometry(geometry, output, false);
        }
        return output.slice();
    }

    private static void writeGeometry(Geometry geometry, SliceOutput output, boolean inGeometryCollection)
    {
        switch (geometry.getGeometryType()) {
            case JTS_POINT:
                writePoint((Point) geometry, output, inGeometryCollection);
                break;
            case JTS_MULTI_POINT:
                writeMultiPoint((MultiPoint) geometry, output, inGeometryCollection);
                break;
            case JTS_LINESTRING:
            case JTS_MULTI_LINESTRING:
                writePolyline(geometry, output, inGeometryCollection);
                break;
            case JTS_POLYGON:
            case JTS_MULTI_POLYGON:
                writePolygon(geometry, output, inGeometryCollection);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
        }
    }

    private static Geometry readGeometry(SliceInput input)
    {
        requireNonNull(input, "input is null");
        int geometryType = input.readInt();
        switch (EsriShapeType.valueOf(geometryType)) {
            case POINT:
                return readPoint(input);
            case POLYLINE:
                return readPolyline(input);
            case POLYGON:
                return readPolygon(input);
            case MULTI_POINT:
                return readMultiPoint(input);
            default:
                throw new UnsupportedOperationException("Invalid geometry type: " + geometryType);
        }
    }

    private static Point readPoint(SliceInput input)
    {
        requireNonNull(input, "input is null");
        Coordinate coordinates = readCoordinate(input);
        if (isEsriNaN(coordinates.x) || isEsriNaN(coordinates.y)) {
            return GEOMETRY_FACTORY.createPoint();
        }
        return GEOMETRY_FACTORY.createPoint(coordinates);
    }

    private static void writePoint(Point point, SliceOutput output, boolean inGeometryCollection)
    {
        requireNonNull(output, "output is null");
        if (inGeometryCollection) {
            output.writeInt(SIZE_OF_DOUBLE * 2 + SIZE_OF_INT);
        }
        else {
            output.writeByte(GeometryUtils.GeometryTypeName.POINT.code());
        }

        output.writeInt(EsriShapeType.POINT.code);
        if (!point.isEmpty()) {
            writeCoordinate(point.getCoordinate(), output);
        }
        else {
            writeCoordinate(new Coordinate(Double.NaN, Double.NaN), output);
        }
    }

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
    }

    private static void writeCoordinate(Coordinate coordinate, SliceOutput output)
    {
        requireNonNull(output, "output is null");
        output.writeDouble(coordinate.x);
        output.writeDouble(coordinate.y);
    }

    private static void writeCoordinates(Coordinate[] coordinates, SliceOutput output)
    {
        requireNonNull(coordinates, "coordinates is null");
        requireNonNull(output, "output is null");
        for (Coordinate coordinate : coordinates) {
            writeCoordinate(coordinate, output);
        }
    }

    private static Geometry readMultiPoint(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int pointCount = input.readInt();
        Point[] points = new Point[pointCount];
        for (int i = 0; i < pointCount; i++) {
            points[i] = readPoint(input);
        }
        return GEOMETRY_FACTORY.createMultiPoint(points);
    }

    private static void writeMultiPoint(MultiPoint geometry, SliceOutput output, boolean inGeometryCollection)
    {
        requireNonNull(geometry, "geometry is null");
        requireNonNull(output, "output is null");

        int numPoints = geometry.getNumPoints();
        if (inGeometryCollection) {
            output.writeInt(SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT + SIZE_OF_DOUBLE * 2 * numPoints);
        }
        else {
            output.writeByte(GeometryUtils.GeometryTypeName.MULTI_POINT.code());
        }

        output.writeInt(EsriShapeType.MULTI_POINT.code);

        writeEnvelope(geometry.getEnvelopeInternal(), output);

        output.writeInt(numPoints);
        for (Coordinate coordinate : geometry.getCoordinates()) {
            writeCoordinate(coordinate, output);
        }
    }

    private static Geometry readPolyline(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            return GEOMETRY_FACTORY.createLineString();
        }

        int pointCount = input.readInt();
        LineString[] lineStrings = new LineString[partCount];

        int[] partLengths = new int[partCount];
        input.readInt();    // skip first index; it is always 0
        if (partCount > 1) {
            partLengths[0] = input.readInt();
            for (int i = 1; i < partCount - 1; i++) {
                partLengths[i] = input.readInt() - partLengths[i - 1];
            }
            partLengths[partCount - 1] = pointCount - partLengths[partCount - 2];
        }
        else {
            partLengths[0] = pointCount;
        }

        for (int i = 0; i < partCount; i++) {
            lineStrings[i] = GEOMETRY_FACTORY.createLineString(readCoordinates(input, partLengths[i]));
        }

        if (lineStrings.length == 1) {
            return lineStrings[0];
        }
        return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
    }

    private static void writePolyline(Geometry geometry, SliceOutput output, boolean inGeometryCollection)
    {
        requireNonNull(geometry, "polyline is null");
        requireNonNull(output, "output is null");

        int numParts;
        int numPoints = geometry.getNumPoints();
        switch (geometry.getGeometryType()) {
            case JTS_MULTI_LINESTRING:
                numParts = geometry.getNumGeometries();
                break;
            case JTS_LINESTRING:
                numParts = numPoints > 0 ? 1 : 0;
                break;
            default:
                throw new IllegalArgumentException("Expected LineString or MultiLineString, but got " + geometry.getGeometryType());
        }

        if (inGeometryCollection) {
            output.writeInt(SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT * 2 + SIZE_OF_INT * numParts +
                    SIZE_OF_DOUBLE * 2 * numPoints);
        }
        else {
            switch (geometry.getGeometryType()) {
                case JTS_LINESTRING:
                    output.writeByte(GeometryUtils.GeometryTypeName.LINE_STRING.code());
                    break;
                case JTS_MULTI_LINESTRING:
                    output.writeByte(GeometryUtils.GeometryTypeName.MULTI_LINE_STRING.code());
                    break;
            }
        }

        output.writeInt(EsriShapeType.POLYLINE.code);

        writeEnvelope(geometry.getEnvelopeInternal(), output);

        output.writeInt(numParts);

        output.writeInt(numPoints);

        if (numParts > 0) {
            output.writeInt(0);
            for (int i = 0; i < numParts - 1; i++) {
                output.writeInt(geometry.getGeometryN(i).getNumPoints());
            }
        }

        writeCoordinates(geometry.getCoordinates(), output);
    }

    private static Coordinate[] readCoordinates(SliceInput input, int count)
    {
        requireNonNull(input, "input is null");
        verify(count > 0);
        Coordinate[] coordinates = new Coordinate[count];
        for (int i = 0; i < count; i++) {
            coordinates[i] = readCoordinate(input);
        }
        return coordinates;
    }

    private static Geometry readPolygon(SliceInput input)
    {
        requireNonNull(input, "input is null");
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            return GEOMETRY_FACTORY.createPolygon();
        }

        int pointCount = input.readInt();
        int[] startIndexes = new int[partCount];
        for (int i = 0; i < partCount; i++) {
            startIndexes[i] = input.readInt();
        }

        int[] partLengths = new int[partCount];
        if (partCount > 1) {
            partLengths[0] = startIndexes[1];
            for (int i = 1; i < partCount - 1; i++) {
                partLengths[i] = startIndexes[i + 1] - startIndexes[i];
            }
        }
        partLengths[partCount - 1] = pointCount - startIndexes[partCount - 1];

        LinearRing shell = null;
        List<LinearRing> holes = new ArrayList<>();
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < partCount; i++) {
            Coordinate[] coordinates = readCoordinates(input, partLengths[i]);
            if (!isClockwise(coordinates)) {
                holes.add(GEOMETRY_FACTORY.createLinearRing(coordinates));
                continue;
            }
            if (shell != null) {
                polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));
            }
            shell = GEOMETRY_FACTORY.createLinearRing(coordinates);
            holes.clear();
        }
        polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));

        if (polygons.size() == 1) {
            return polygons.get(0);
        }
        return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static void writePolygon(Geometry geometry, SliceOutput output, boolean inGeometryCollection)
    {
        requireNonNull(geometry, "geometry is null");
        requireNonNull(output, "output is null");
        if (!geometry.getGeometryType().equals(JTS_POLYGON) && !geometry.getGeometryType().equals(JTS_MULTI_POLYGON)) {
            throw new IllegalArgumentException("Expected LineString or MultiLineString, but got " + geometry.getGeometryType());
        }

        int numGeometries = geometry.getNumGeometries();
        int numParts = 0;
        int numPoints = geometry.getNumPoints();
        for (int i = 0; i < numGeometries; i++) {
            Polygon polygon = (Polygon) geometry.getGeometryN(i);
            if (polygon.getNumPoints() > 0) {
                numParts += polygon.getNumInteriorRing() + 1;
            }
        }

        if (inGeometryCollection) {
            output.writeInt(SIZE_OF_INT + SIZE_OF_DOUBLE * 4 + SIZE_OF_INT * (2 + numParts)
                    + SIZE_OF_DOUBLE * 2 * numPoints);
        }
        else {
            switch (geometry.getGeometryType()) {
                case JTS_MULTI_POLYGON:
                    output.writeByte(GeometryUtils.GeometryTypeName.MULTI_POLYGON.code());
                    break;
                case JTS_POLYGON:
                    output.writeByte(GeometryUtils.GeometryTypeName.POLYGON.code());
                    break;
            }
        }

        output.writeInt(EsriShapeType.POLYGON.code);

        writeEnvelope(geometry.getEnvelopeInternal(), output);

        output.writeInt(numParts);

        output.writeInt(numPoints);

        // Index of first point in part
        if (numParts > 0) {
            int partIndex = 0;
            output.writeInt(partIndex);
            for (int i = 0; i < numGeometries; i++) {
                Polygon polygon = (Polygon) geometry.getGeometryN(i);
                if (polygon.getNumPoints() <= 0) {
                    continue;
                }

                int numInteriorRing = polygon.getNumInteriorRing();

                if (i == numGeometries - 1 && numInteriorRing == 0) {
                    // this is the last part
                    break;
                }
                partIndex += polygon.getExteriorRing().getNumPoints();
                output.writeInt(partIndex);

                for (int j = 0; j < numInteriorRing; j++) {
                    if (i == numGeometries - 1 && j == numInteriorRing - 1) {
                        // this is the last part
                        break;
                    }
                    partIndex += polygon.getInteriorRingN(j).getNumPoints();
                    output.writeInt(partIndex);
                }
            }
        }

        writeCoordinates(geometry.getCoordinates(), output);
    }

    private static void skipEnvelope(SliceInput input)
    {
        requireNonNull(input, "input is null");
        input.skip(4 * SIZE_OF_DOUBLE);
    }

    private static void writeEnvelope(Envelope envelope, SliceOutput output)
    {
        requireNonNull(envelope, "envelope is null");
        requireNonNull(output, "output is null");

        output.writeDouble(envelope.getMinX());
        output.writeDouble(envelope.getMinY());
        output.writeDouble(envelope.getMaxX());
        output.writeDouble(envelope.getMaxY());
    }

    private static boolean isClockwise(Coordinate[] coordinates)
    {
        // Sum over the edges: (x2 − x1) * (y2 + y1).
        // If the result is positive the curve is clockwise,
        // if it's negative the curve is counter-clockwise.
        double area = 0;
        for (int i = 1; i < coordinates.length; i++) {
            area += (coordinates[i].x - coordinates[i - 1].x) * (coordinates[i].y + coordinates[i - 1].y);
        }
        area += (coordinates[0].x - coordinates[coordinates.length - 1].x) * (coordinates[0].y + coordinates[coordinates.length - 1].y);
        return area > 0;
    }
}
