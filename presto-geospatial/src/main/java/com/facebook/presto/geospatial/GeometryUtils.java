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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.esri.core.geometry.Geometry.Type.Unknown;
import static com.esri.core.geometry.GeometryEngine.geometryToEsriShape;
import static com.esri.core.geometry.OperatorImportFromESRIShape.local;
import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Collections.emptyList;

public final class GeometryUtils
{
    public enum GeometryTypeName
    {
        POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON, GEOMETRY_COLLECTION
    }

    public static final String POINT = "Point";
    public static final String LINE_STRING = "LineString";
    public static final String POLYGON = "Polygon";
    public static final String MULTI_POINT = "MultiPoint";
    public static final String MULTI_LINE_STRING = "MultiLineString";
    public static final String MULTI_POLYGON = "MultiPolygon";
    public static final String GEOMETRY_COLLECTION = "GeometryCollection";
    public static final int SPATIAL_REFERENCE_UNKNOWN = 0;

    private GeometryUtils() {}

    public static GeometryTypeName valueOf(String type)
    {
        if (type.equals(POINT)) {
            return GeometryTypeName.POINT;
        }
        else if (type.equals(MULTI_POINT)) {
            return GeometryTypeName.MULTI_POINT;
        }
        else if (type.equals(LINE_STRING)) {
            return GeometryTypeName.LINE_STRING;
        }
        else if (type.equals(MULTI_LINE_STRING)) {
            return GeometryTypeName.MULTI_LINE_STRING;
        }
        else if (type.equals(POLYGON)) {
            return GeometryTypeName.POLYGON;
        }
        else if (type.equals(MULTI_POLYGON)) {
            return GeometryTypeName.MULTI_POLYGON;
        }
        else if (type.equals(GEOMETRY_COLLECTION)) {
            return GeometryTypeName.GEOMETRY_COLLECTION;
        }
        throw new IllegalArgumentException("Invalid Geometry Type: " + type);
    }

    public static OGCGeometry deserialize(Slice shape)
    {
        if (shape == null) {
            return null;
        }
        BasicSliceInput input = shape.getInput();

        int spatialReferenceId = input.readInt();
        SpatialReference spatialReference = null;
        if (spatialReferenceId != SPATIAL_REFERENCE_UNKNOWN) {
            spatialReference = SpatialReference.create(spatialReferenceId);
        }

        // GeometryCollection: spatialReferenceId|len-of-shape1|bytes-of-shape1|len-of-shape2|bytes-of-shape2...
        List<OGCGeometry> geometries = new ArrayList<>();
        while (input.available() > 0) {
            int length = input.readInt();
            ByteBuffer buffer = input.readSlice(length).toByteBuffer().slice().order(LITTLE_ENDIAN);
            Geometry esriGeometry = local().execute(0, Unknown, buffer);
            OGCGeometry geometry = createFromEsriGeometry(esriGeometry, spatialReference);
            geometries.add(geometry);
        }

        if (geometries.isEmpty()) {
            return new OGCConcreteGeometryCollection(emptyList(), spatialReference);
        }
        else if (geometries.size() == 1) {
            return geometries.get(0);
        }
        return new OGCConcreteGeometryCollection(geometries, spatialReference);
    }

    public static Slice serialize(OGCGeometry input)
    {
        int spatialReferenceId = input.SRID();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(100);
        sliceOutput.appendInt(spatialReferenceId);

        GeometryCursor cursor = input.getEsriGeometryCursor();
        while (true) {
            Geometry geometry = cursor.next();
            if (geometry == null) {
                break;
            }
            byte[] shape = geometryToEsriShape(geometry);
            sliceOutput.appendInt(shape.length);
            sliceOutput.appendBytes(shape);
        }
        return sliceOutput.slice();
    }
}
