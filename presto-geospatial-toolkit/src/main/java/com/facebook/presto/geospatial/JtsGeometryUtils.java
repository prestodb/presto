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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
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

    private static Coordinate readCoordinate(SliceInput input)
    {
        requireNonNull(input, "input is null");
        return new Coordinate(input.readDouble(), input.readDouble());
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

    private static void skipEnvelope(SliceInput input)
    {
        requireNonNull(input, "input is null");
        input.skip(4 * SIZE_OF_DOUBLE);
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
