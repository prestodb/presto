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
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.OperatorImportFromESRIShape;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.esri.core.geometry.Geometry.Type.Unknown;
import static com.esri.core.geometry.GeometryEngine.geometryToEsriShape;
import static com.facebook.presto.geospatial.GeometryUtils.isEsriNaN;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class GeometrySerde
{
    private GeometrySerde() {}

    public static Slice serialize(OGCGeometry input)
    {
        requireNonNull(input, "input is null");
        DynamicSliceOutput output = new DynamicSliceOutput(100);
        writeGeometry(output, input);
        return output.slice();
    }

    private static void writeGeometry(DynamicSliceOutput output, OGCGeometry geometry)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        switch (type) {
            case POINT:
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                writeSimpleGeometry(output, type, geometry);
                break;
            case GEOMETRY_COLLECTION: {
                verify(geometry instanceof OGCConcreteGeometryCollection);
                writeGeometryCollection(output, (OGCConcreteGeometryCollection) geometry);
                break;
            }
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static void writeGeometryCollection(DynamicSliceOutput output, OGCGeometryCollection collection)
    {
        output.appendByte(GeometryType.GEOMETRY_COLLECTION.code());
        for (int geometryIndex = 0; geometryIndex < collection.numGeometries(); geometryIndex++) {
            OGCGeometry geometry = collection.geometryN(geometryIndex);
            int startPosition = output.size();

            // leave 4 bytes for the shape length
            output.appendInt(0);
            writeGeometry(output, geometry);

            int endPosition = output.size();
            int length = endPosition - startPosition - Integer.BYTES;

            output.getUnderlyingSlice().setInt(startPosition, length);
        }
    }

    private static void writeSimpleGeometry(DynamicSliceOutput output, GeometryType type, OGCGeometry geometry)
    {
        output.appendByte(type.code());
        Geometry esriGeometry = requireNonNull(geometry.getEsriGeometry(), "esriGeometry is null");
        byte[] shape = geometryToEsriShape(esriGeometry);
        output.appendBytes(shape);
    }

    public static OGCGeometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();
        verify(input.available() > 0);
        int length = input.available() - 1;
        GeometryType type = GeometryType.getForCode(input.readByte());
        return readGeometry(input, shape, type, length);
    }

    private static OGCGeometry readGeometry(BasicSliceInput input, Slice inputSlice, GeometryType type, int length)
    {
        switch (type) {
            case POINT:
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                return readSimpleGeometry(input, inputSlice, type, length);
            case GEOMETRY_COLLECTION:
                return readGeometryCollection(input, inputSlice);
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static OGCConcreteGeometryCollection readGeometryCollection(BasicSliceInput input, Slice inputSlice)
    {
        // GeometryCollection: geometryType|len-of-shape1|bytes-of-shape1|len-of-shape2|bytes-of-shape2...
        List<OGCGeometry> geometries = new ArrayList<>();
        while (input.available() > 0) {
            int length = input.readInt() - 1;
            GeometryType type = GeometryType.getForCode(input.readByte());
            geometries.add(readGeometry(input, inputSlice, type, length));
        }
        return new OGCConcreteGeometryCollection(geometries, null);
    }

    private static OGCGeometry readSimpleGeometry(BasicSliceInput input, Slice inputSlice, GeometryType type, int length)
    {
        int currentPosition = toIntExact(input.position());
        ByteBuffer geometryBuffer = inputSlice.toByteBuffer(currentPosition, length).slice();
        input.setPosition(currentPosition + length);
        Geometry esriGeometry = OperatorImportFromESRIShape.local().execute(0, Unknown, geometryBuffer);
        return createFromEsriGeometry(esriGeometry, type.isMultitype());
    }

    private static OGCGeometry createFromEsriGeometry(Geometry geometry, boolean multiType)
    {
        Geometry.Type type = geometry.getType();
        switch (type) {
            case Polygon: {
                if (!multiType && ((Polygon) geometry).getExteriorRingCount() <= 1) {
                    return new OGCPolygon((Polygon) geometry, null);
                }
                return new OGCMultiPolygon((Polygon) geometry, null);
            }
            case Polyline: {
                if (!multiType && ((Polyline) geometry).getPathCount() <= 1) {
                    return new OGCLineString((Polyline) geometry, 0, null);
                }
                return new OGCMultiLineString((Polyline) geometry, null);
            }
            case MultiPoint: {
                if (!multiType && ((MultiPoint) geometry).getPointCount() <= 1) {
                    if (geometry.isEmpty()) {
                        return new OGCPoint(new Point(), null);
                    }
                    return new OGCPoint(((MultiPoint) geometry).getPoint(0), null);
                }
                return new OGCMultiPoint((MultiPoint) geometry, null);
            }
            case Point: {
                if (!multiType) {
                    return new OGCPoint((Point) geometry, null);
                }
                return new OGCMultiPoint((Point) geometry, null);
            }
            default:
                throw new IllegalArgumentException("Unexpected geometry type: " + type);
        }
    }

    @Nullable
    public static Envelope deserializeEnvelope(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        BasicSliceInput input = shape.getInput();

        if (input.available() == 0) {
            return null;
        }

        int length = input.available() - 1;
        GeometryType type = GeometryType.getForCode(input.readByte());
        return getEnvelope(input, type, length);
    }

    private static Envelope getEnvelope(BasicSliceInput input, GeometryType type, int length)
    {
        switch (type) {
            case POINT:
                return getPointEnvelope(input);
            case MULTI_POINT:
            case LINE_STRING:
            case MULTI_LINE_STRING:
            case POLYGON:
            case MULTI_POLYGON:
                return getSimpleGeometryEnvelope(input, length);
            case GEOMETRY_COLLECTION:
                return getGeometryCollectionOverallEnvelope(input);
            default:
                throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static Envelope getGeometryCollectionOverallEnvelope(BasicSliceInput input)
    {
        Envelope overallEnvelope = null;
        while (input.available() > 0) {
            int length = input.readInt() - 1;
            GeometryType type = GeometryType.getForCode(input.readByte());
            Envelope envelope = getEnvelope(input, type, length);
            overallEnvelope = merge(overallEnvelope, envelope);
        }
        return overallEnvelope;
    }

    private static Envelope getSimpleGeometryEnvelope(BasicSliceInput input, int length)
    {
        // skip type injected by esri
        input.readInt();

        double xMin = input.readDouble();
        double yMin = input.readDouble();
        double xMax = input.readDouble();
        double yMax = input.readDouble();

        int skipLength = length - (4 * Double.BYTES) - Integer.BYTES;
        verify(input.skip(skipLength) == skipLength);

        if (isEsriNaN(xMin) || isEsriNaN(yMin) || isEsriNaN(xMax) || isEsriNaN(yMax)) {
            // TODO: isn't it better to return empty envelope instead?
            return null;
        }
        return new Envelope(xMin, yMin, xMax, yMax);
    }

    private static Envelope getPointEnvelope(BasicSliceInput input)
    {
        // skip type injected by esri
        input.readInt();

        double x = input.readDouble();
        double y = input.readDouble();
        if (isEsriNaN(x) || isEsriNaN(y)) {
            // TODO: isn't it better to return empty envelope instead?
            return null;
        }
        return new Envelope(x, y, x, y);
    }

    @Nullable
    private static Envelope merge(@Nullable Envelope left, @Nullable Envelope right)
    {
        if (left == null) {
            return right;
        }
        else if (right == null) {
            return left;
        }
        else {
            right.merge(left);
        }
        return right;
    }
}
