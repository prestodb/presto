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
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

public class GeometrySerde
{
    private static final int POINT_TYPE = 1;

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
        if (shape == null) {
            return null;
        }
        BasicSliceInput input = shape.getInput();

        Envelope overallEnvelope = null;
        if (input.available() > 0) {
            byte code = input.readByte();
            boolean isGeometryCollection = (code == GeometryType.GEOMETRY_COLLECTION.code());
            while (input.available() > 0) {
                int length = isGeometryCollection ? input.readInt() : input.available();
                ByteBuffer buffer = input.readSlice(length).toByteBuffer().order(LITTLE_ENDIAN);
                int type = buffer.getInt();
                Envelope envelope = null;
                if (type == POINT_TYPE) {    // point
                    double x = buffer.getDouble();
                    double y = buffer.getDouble();
                    if (!GeometryUtils.isEsriNaN(x)) {
                        verify(!GeometryUtils.isEsriNaN(y));
                        envelope = new Envelope(x, y, x, y);
                    }
                }
                else {
                    double xMin = buffer.getDouble();
                    double yMin = buffer.getDouble();
                    double xMax = buffer.getDouble();
                    double yMax = buffer.getDouble();
                    if (!GeometryUtils.isEsriNaN(xMin)) {
                        verify(!GeometryUtils.isEsriNaN(xMax));
                        verify(!GeometryUtils.isEsriNaN(yMin));
                        verify(!GeometryUtils.isEsriNaN(yMax));
                        envelope = new Envelope(xMin, yMin, xMax, yMax);
                    }
                }
                if (envelope != null) {
                    if (overallEnvelope == null) {
                        overallEnvelope = envelope;
                    }
                    else {
                        overallEnvelope.merge(envelope);
                    }
                }
            }
        }

        return overallEnvelope;
    }
}
