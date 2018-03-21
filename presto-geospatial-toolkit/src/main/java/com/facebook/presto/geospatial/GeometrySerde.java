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
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.base.Verify;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.esri.core.geometry.Geometry.Type.Unknown;
import static com.esri.core.geometry.GeometryEngine.geometryToEsriShape;
import static com.esri.core.geometry.OperatorImportFromESRIShape.local;
import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Collections.emptyList;

public class GeometrySerde
{
    private static final int POINT_TYPE = 1;

    private GeometrySerde() {}

    public static Slice serialize(OGCGeometry input)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(100);

        GeometryType type = GeometryType.getForEsriGeometryType(input.geometryType());
        sliceOutput.appendByte(type.code());
        GeometryCursor cursor = input.getEsriGeometryCursor();
        while (true) {
            Geometry geometry = cursor.next();
            if (geometry == null) {
                break;
            }
            byte[] shape = geometryToEsriShape(geometry);
            if (type == GeometryType.GEOMETRY_COLLECTION) {
                sliceOutput.appendInt(shape.length);
            }
            sliceOutput.appendBytes(shape);
        }
        return sliceOutput.slice();
    }

    public static OGCGeometry deserialize(Slice shape)
    {
        if (shape == null) {
            return null;
        }
        BasicSliceInput input = shape.getInput();

        // GeometryCollection: geometryType|len-of-shape1|bytes-of-shape1|len-of-shape2|bytes-of-shape2...
        List<OGCGeometry> geometries = new ArrayList<>();

        if (input.available() > 0) {
            byte code = input.readByte();
            boolean isGeometryCollection = (code == GeometryType.GEOMETRY_COLLECTION.code());
            while (input.available() > 0) {
                geometries.add(readGeometry(isGeometryCollection, input));
            }
        }

        if (geometries.isEmpty()) {
            return new OGCConcreteGeometryCollection(emptyList(), null);
        }
        else if (geometries.size() == 1) {
            return geometries.get(0);
        }
        return new OGCConcreteGeometryCollection(geometries, null);
    }

    private static OGCGeometry readGeometry(boolean isGeometryCollection, BasicSliceInput input)
    {
        int length = isGeometryCollection ? input.readInt() : input.available();
        ByteBuffer buffer = input.readSlice(length).toByteBuffer().slice().order(LITTLE_ENDIAN);
        Geometry esriGeometry = local().execute(0, Unknown, buffer);
        return createFromEsriGeometry(esriGeometry, null);
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
                        Verify.verify(!GeometryUtils.isEsriNaN(y));
                        envelope = new Envelope(x, y, x, y);
                    }
                }
                else {
                    double xMin = buffer.getDouble();
                    double yMin = buffer.getDouble();
                    double xMax = buffer.getDouble();
                    double yMax = buffer.getDouble();
                    if (!GeometryUtils.isEsriNaN(xMin)) {
                        Verify.verify(!GeometryUtils.isEsriNaN(xMax));
                        Verify.verify(!GeometryUtils.isEsriNaN(yMin));
                        Verify.verify(!GeometryUtils.isEsriNaN(yMax));
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
