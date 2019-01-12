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
package io.prestosql.geospatial.serde;

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

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.prestosql.geospatial.GeometryUtils.translateToAVNaN;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class JtsGeometrySerde
{
    // TODO: Are we sure this is thread safe?
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private JtsGeometrySerde() {}

    public static Geometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
        return readGeometry(input, type);
    }

    private static Geometry readGeometry(BasicSliceInput input, GeometrySerializationType type)
    {
        switch (type) {
            case POINT:
                return readPoint(input);
            case MULTI_POINT:
                return readMultiPoint(input);
            case LINE_STRING:
                return readPolyline(input, false);
            case MULTI_LINE_STRING:
                return readPolyline(input, true);
            case POLYGON:
                return readPolygon(input, false);
            case MULTI_POLYGON:
                return readPolygon(input, true);
            case GEOMETRY_COLLECTION:
                return readGeometryCollection(input);
            case ENVELOPE:
                return readEnvelope(input);
            default:
                throw new UnsupportedOperationException("Unexpected type: " + type);
        }
    }

    private static Point readPoint(SliceInput input)
    {
        Coordinate coordinates = readCoordinate(input);
        if (isNaN(coordinates.x) || isNaN(coordinates.y)) {
            return GEOMETRY_FACTORY.createPoint();
        }
        return GEOMETRY_FACTORY.createPoint(coordinates);
    }

    private static Geometry readMultiPoint(SliceInput input)
    {
        skipEsriType(input);
        skipEnvelope(input);
        int pointCount = input.readInt();
        Point[] points = new Point[pointCount];
        for (int i = 0; i < pointCount; i++) {
            points[i] = readPoint(input);
        }
        return GEOMETRY_FACTORY.createMultiPoint(points);
    }

    private static Geometry readPolyline(SliceInput input, boolean multitype)
    {
        skipEsriType(input);
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            if (multitype) {
                return GEOMETRY_FACTORY.createMultiLineString();
            }
            return GEOMETRY_FACTORY.createLineString();
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

        LineString[] lineStrings = new LineString[partCount];

        for (int i = 0; i < partCount; i++) {
            lineStrings[i] = GEOMETRY_FACTORY.createLineString(readCoordinates(input, partLengths[i]));
        }

        if (multitype) {
            return GEOMETRY_FACTORY.createMultiLineString(lineStrings);
        }
        verify(lineStrings.length == 1);
        return lineStrings[0];
    }

    private static Geometry readPolygon(SliceInput input, boolean multitype)
    {
        skipEsriType(input);
        skipEnvelope(input);
        int partCount = input.readInt();
        if (partCount == 0) {
            if (multitype) {
                return GEOMETRY_FACTORY.createMultiPolygon();
            }
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
            if (isClockwise(coordinates)) {
                // next polygon has started
                if (shell != null) {
                    polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));
                    holes.clear();
                }
                else {
                    verify(holes.isEmpty(), "shell is null but holes found");
                }
                shell = GEOMETRY_FACTORY.createLinearRing(coordinates);
            }
            else {
                verify(shell != null, "shell is null but hole found");
                holes.add(GEOMETRY_FACTORY.createLinearRing(coordinates));
            }
        }
        polygons.add(GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0])));

        if (multitype) {
            return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
        }
        return getOnlyElement(polygons);
    }

    private static Geometry readGeometryCollection(BasicSliceInput input)
    {
        List<Geometry> geometries = new ArrayList<>();
        while (input.available() > 0) {
            // skip length
            input.readInt();
            GeometrySerializationType type = GeometrySerializationType.getForCode(input.readByte());
            geometries.add(readGeometry(input, type));
        }
        return GEOMETRY_FACTORY.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }

    private static Geometry readEnvelope(SliceInput input)
    {
        verify(input.available() > 0);
        double xMin = input.readDouble();
        double yMin = input.readDouble();
        double xMax = input.readDouble();
        double yMax = input.readDouble();

        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(xMin, yMin);
        coordinates[1] = new Coordinate(xMin, yMax);
        coordinates[2] = new Coordinate(xMax, yMax);
        coordinates[3] = new Coordinate(xMax, yMin);
        coordinates[4] = coordinates[0];
        return GEOMETRY_FACTORY.createPolygon(coordinates);
    }

    private static void skipEsriType(SliceInput input)
    {
        input.readInt();
    }

    private static void skipEnvelope(SliceInput input)
    {
        requireNonNull(input, "input is null");
        int skipLength = 4 * SIZE_OF_DOUBLE;
        verify(input.skip(skipLength) == skipLength);
    }

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
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

    /**
     * Serialize JTS {@link Geometry} shape into an ESRI shape
     */
    public static Slice serialize(Geometry geometry)
    {
        requireNonNull(geometry, "input is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        writeGeometry(geometry, output);
        return output.slice();
    }

    private static void writeGeometry(Geometry geometry, DynamicSliceOutput output)
    {
        switch (geometry.getGeometryType()) {
            case "Point":
                writePoint((Point) geometry, output);
                break;
            case "MultiPoint":
                writeMultiPoint((MultiPoint) geometry, output);
                break;
            case "LineString":
                writePolyline(geometry, output, false);
                break;
            case "MultiLineString":
                writePolyline(geometry, output, true);
                break;
            case "Polygon":
                writePolygon(geometry, output, false);
                break;
            case "MultiPolygon":
                writePolygon(geometry, output, true);
                break;
            case "GeometryCollection":
                writeGeometryCollection(geometry, output);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type : " + geometry.getGeometryType());
        }
    }

    private static void writePoint(Point point, SliceOutput output)
    {
        output.writeByte(GeometrySerializationType.POINT.code());
        if (!point.isEmpty()) {
            writeCoordinate(point.getCoordinate(), output);
        }
        else {
            output.writeDouble(NaN);
            output.writeDouble(NaN);
        }
    }

    private static void writeMultiPoint(MultiPoint geometry, SliceOutput output)
    {
        output.writeByte(GeometrySerializationType.MULTI_POINT.code());
        output.writeInt(EsriShapeType.MULTI_POINT.code);
        writeEnvelope(geometry, output);
        output.writeInt(geometry.getNumPoints());
        for (Coordinate coordinate : geometry.getCoordinates()) {
            writeCoordinate(coordinate, output);
        }
    }

    private static void writePolyline(Geometry geometry, SliceOutput output, boolean multitype)
    {
        int numParts;
        int numPoints = geometry.getNumPoints();
        if (multitype) {
            numParts = geometry.getNumGeometries();
            output.writeByte(GeometrySerializationType.MULTI_LINE_STRING.code());
        }
        else {
            numParts = numPoints > 0 ? 1 : 0;
            output.writeByte(GeometrySerializationType.LINE_STRING.code());
        }

        output.writeInt(EsriShapeType.POLYLINE.code);

        writeEnvelope(geometry, output);

        output.writeInt(numParts);
        output.writeInt(numPoints);

        int partIndex = 0;
        for (int i = 0; i < numParts; i++) {
            output.writeInt(partIndex);
            partIndex += geometry.getGeometryN(i).getNumPoints();
        }

        writeCoordinates(geometry.getCoordinates(), output);
    }

    private static void writePolygon(Geometry geometry, SliceOutput output, boolean multitype)
    {
        int numGeometries = geometry.getNumGeometries();
        int numParts = 0;
        int numPoints = geometry.getNumPoints();
        for (int i = 0; i < numGeometries; i++) {
            Polygon polygon = (Polygon) geometry.getGeometryN(i);
            if (polygon.getNumPoints() > 0) {
                numParts += polygon.getNumInteriorRing() + 1;
            }
        }

        if (multitype) {
            output.writeByte(GeometrySerializationType.MULTI_POLYGON.code());
        }
        else {
            output.writeByte(GeometrySerializationType.POLYGON.code());
        }

        output.writeInt(EsriShapeType.POLYGON.code);

        writeEnvelope(geometry, output);

        output.writeInt(numParts);
        output.writeInt(numPoints);

        if (numParts == 0) {
            return;
        }

        int[] partIndexes = new int[numParts];
        boolean[] shellPart = new boolean[numParts];

        int currentPart = 0;
        int currentPoint = 0;
        for (int i = 0; i < numGeometries; i++) {
            Polygon polygon = (Polygon) geometry.getGeometryN(i);

            partIndexes[currentPart] = currentPoint;
            shellPart[currentPart] = true;
            currentPart++;
            currentPoint += polygon.getExteriorRing().getNumPoints();

            int holesCount = polygon.getNumInteriorRing();
            for (int holeIndex = 0; holeIndex < holesCount; holeIndex++) {
                partIndexes[currentPart] = currentPoint;
                shellPart[currentPart] = false;
                currentPart++;
                currentPoint += polygon.getInteriorRingN(holeIndex).getNumPoints();
            }
        }

        for (int partIndex : partIndexes) {
            output.writeInt(partIndex);
        }

        Coordinate[] coordinates = geometry.getCoordinates();
        canonicalizePolygonCoordinates(coordinates, partIndexes, shellPart);
        writeCoordinates(coordinates, output);
    }

    private static void writeGeometryCollection(Geometry collection, DynamicSliceOutput output)
    {
        output.appendByte(GeometrySerializationType.GEOMETRY_COLLECTION.code());
        for (int geometryIndex = 0; geometryIndex < collection.getNumGeometries(); geometryIndex++) {
            Geometry geometry = collection.getGeometryN(geometryIndex);
            int startPosition = output.size();

            // leave 4 bytes for the shape length
            output.appendInt(0);
            writeGeometry(geometry, output);

            int endPosition = output.size();
            int length = endPosition - startPosition - Integer.BYTES;

            output.getUnderlyingSlice().setInt(startPosition, length);
        }
    }

    private static void writeCoordinate(Coordinate coordinate, SliceOutput output)
    {
        output.writeDouble(translateToAVNaN(coordinate.x));
        output.writeDouble(translateToAVNaN(coordinate.y));
    }

    private static void writeCoordinates(Coordinate[] coordinates, SliceOutput output)
    {
        for (Coordinate coordinate : coordinates) {
            writeCoordinate(coordinate, output);
        }
    }

    private static void writeEnvelope(Geometry geometry, SliceOutput output)
    {
        if (geometry.isEmpty()) {
            for (int i = 0; i < 4; i++) {
                output.writeDouble(NaN);
            }
            return;
        }

        Envelope envelope = geometry.getEnvelopeInternal();
        output.writeDouble(envelope.getMinX());
        output.writeDouble(envelope.getMinY());
        output.writeDouble(envelope.getMaxX());
        output.writeDouble(envelope.getMaxY());
    }

    private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int[] partIndexes, boolean[] shellPart)
    {
        for (int part = 0; part < partIndexes.length - 1; part++) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[part], partIndexes[part + 1], shellPart[part]);
        }
        if (partIndexes.length > 0) {
            canonicalizePolygonCoordinates(coordinates, partIndexes[partIndexes.length - 1], coordinates.length, shellPart[partIndexes.length - 1]);
        }
    }

    private static void canonicalizePolygonCoordinates(Coordinate[] coordinates, int start, int end, boolean isShell)
    {
        boolean isClockwise = isClockwise(coordinates, start, end);

        if ((isShell && !isClockwise) || (!isShell && isClockwise)) {
            // shell has to be counter clockwise
            reverse(coordinates, start, end);
        }
    }

    private static boolean isClockwise(Coordinate[] coordinates)
    {
        return isClockwise(coordinates, 0, coordinates.length);
    }

    private static boolean isClockwise(Coordinate[] coordinates, int start, int end)
    {
        // Sum over the edges: (x2 − x1) * (y2 + y1).
        // If the result is positive the curve is clockwise,
        // if it's negative the curve is counter-clockwise.
        double area = 0;
        for (int i = start + 1; i < end; i++) {
            area += (coordinates[i].x - coordinates[i - 1].x) * (coordinates[i].y + coordinates[i - 1].y);
        }
        area += (coordinates[start].x - coordinates[end - 1].x) * (coordinates[start].y + coordinates[end - 1].y);
        return area > 0;
    }

    private static void reverse(Coordinate[] coordinates, int start, int end)
    {
        verify(start <= end, "start must be less or equal than end");
        for (int i = start; i < start + ((end - start) / 2); i++) {
            Coordinate buffer = coordinates[i];
            coordinates[i] = coordinates[start + end - i - 1];
            coordinates[start + end - i - 1] = buffer;
        }
    }

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
    }
}
