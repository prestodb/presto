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
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.OperatorImportFromESRIShape;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.util.Objects.requireNonNull;

public final class GeometryUtils
{
    public enum OGCType
    {
        UNKNOWN(0),
        ST_POINT(1),
        ST_LINESTRING(2),
        ST_POLYGON(3),
        ST_MULTIPOINT(4),
        ST_MULTILINESTRING(5),
        ST_MULTIPOLYGON(6);

        private final int index;

        private OGCType(int index)
        {
            this.index = index;
        }

        public int getIndex()
        {
            return this.index;
        }
    }

    public static final String POINT = "Point";
    public static final String LINE_STRING = "LineString";
    public static final String POLYGON = "Polygon";
    public static final String MULTI_POINT = "MultiPoint";
    public static final String MULTI_LINE_STRING = "MultiLineString";
    public static final String MULTI_POLYGON = "MultiPolygon";
    public static final int WKID_UNKNOWN = 0;

    private static final int SIZE_WKID = 4;
    private static final int SIZE_TYPE = 1;
    private static final OGCType[] values = OGCType.values();

    private GeometryUtils() {}

    public static OGCGeometry deserialize(Slice shape)
    {
        requireNonNull(shape, "shape is null");
        int wkid = shape.getInt(0);
        ByteBuffer shapeBuffer = getShapeByteBuffer(shape);
        if (shapeBuffer.limit() < 4) {
            return null;
        }
        else {
            if (shapeBuffer.getInt(0) == Geometry.Type.Unknown.value()) {
                return null;
            }
            else {
                SpatialReference spatialReference = null;
                if (wkid != WKID_UNKNOWN) {
                    spatialReference = SpatialReference.create(wkid);
                }
                Geometry esriGeom = OperatorImportFromESRIShape.local().execute(0, Geometry.Type.Unknown, shapeBuffer);
                OGCGeometry createdGeom = OGCGeometry.createFromEsriGeometry(esriGeom, spatialReference);
                return createdGeom;
            }
        }
    }

    public static final OGCType getOGCType(int value)
    {
        if (value < 0 || value >= values.length) {
            return OGCType.UNKNOWN;
        }
        else {
            return values[value];
        }
    }

    public static Slice serialize(OGCGeometry ogcGeometry)
    {
        int wkid;
        try {
            wkid = ogcGeometry.SRID();
        }
        catch (NullPointerException npe) {
            wkid = 0;
        }

        OGCType ogcType;
        String typeName;
        try {
            typeName = ogcGeometry.geometryType();

            if (typeName.equals(POINT)) {
                ogcType = OGCType.ST_POINT;
            }
            else if (typeName.equals(LINE_STRING)) {
                ogcType = OGCType.ST_LINESTRING;
            }
            else if (typeName.equals(POLYGON)) {
                ogcType = OGCType.ST_POLYGON;
            }
            else if (typeName.equals(MULTI_POINT)) {
                ogcType = OGCType.ST_MULTIPOINT;
            }
            else if (typeName.equals(MULTI_LINE_STRING)) {
                ogcType = OGCType.ST_MULTILINESTRING;
            }
            else if (typeName.equals(MULTI_POLYGON)) {
                ogcType = OGCType.ST_MULTIPOLYGON;
            }
            else {
                ogcType = OGCType.UNKNOWN;
            }
        }
        catch (NullPointerException npe) {
            ogcType = OGCType.UNKNOWN;
        }

        return serialize(ogcGeometry.getEsriGeometry(), wkid, ogcType);
    }

    private static Slice serialize(Geometry geometry, int wkid, OGCType type)
    {
        if (geometry == null) {
            return null;
        }

        byte[] shape = GeometryEngine.geometryToEsriShape(geometry);

        if (shape == null) {
            return null;
        }

        Slice result = Slices.allocate(shape.length + SIZE_WKID + SIZE_TYPE);
        result.setInt(0, wkid);
        result.setByte(SIZE_WKID, (byte) type.getIndex());
        result.setBytes(SIZE_WKID + SIZE_TYPE, shape);
        return result;
    }

    public static OGCType getGeomType(Slice slice)
    {
        int s = (int) slice.getByte(SIZE_WKID);
        return getOGCType(s);
    }

    public static int getWkid(Slice slice)
    {
        return slice.getInt(0);
    }

    private static ByteBuffer getShapeByteBuffer(Slice geomref)
    {
        byte [] geomBytes = geomref.getBytes();
        int offset = SIZE_WKID + SIZE_TYPE;
        return ByteBuffer.wrap(geomBytes, offset, geomBytes.length - offset).slice().order(ByteOrder.LITTLE_ENDIAN);
    }
}
