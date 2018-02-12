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
package com.esri.core.geometry;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfDoubleArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.lang.Math.toIntExact;

/**
 * This class uses package private classes MultiPathImpl and MultiPointImpl from
 * com.esri.core.geometry package and therefore is located in com.esri.core.geometry
 * package.
 */
public class GeometryMemorySizeUtilsPackageWorkaround
{
    private static final int VERTEX_DESCRIPTOR_INSTANCE_SIZE = ClassLayout.parseClass(VertexDescription.class).instanceSize() +
            toIntExact(sizeOfDoubleArray(2) + sizeOfIntArray(21));
    private static final int MULTI_POINT_IMPL_INSTANCE_SIZE = ClassLayout.parseClass(MultiPointImpl.class).instanceSize();
    private static final int MULTI_PATH_IMPL_INSTANCE_SIZE = ClassLayout.parseClass(MultiPathImpl.class).instanceSize();
    private static final int ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE = ClassLayout.parseClass(AttributeStreamOfDbl.class).instanceSize();
    private static final int ATTRIBUTE_STREAM_OF_INT32_INSTANCE_SIZE = ClassLayout.parseClass(AttributeStreamOfInt32.class).instanceSize();
    private static final int ATTRIBUTE_STREAM_OF_INT8_INSTANCE_SIZE = ClassLayout.parseClass(AttributeStreamOfInt8.class).instanceSize();

    private static final int ENVELOPE_INSTANCE_SIZE = ClassLayout.parseClass(Envelope.class).instanceSize() +
            VERTEX_DESCRIPTOR_INSTANCE_SIZE + ClassLayout.parseClass(Envelope2D.class).instanceSize();

    private static final int POINT_INSTANCE_SIZE = ClassLayout.parseClass(OGCPoint.class).instanceSize() +
            ClassLayout.parseClass(Point.class).instanceSize() + toIntExact(sizeOfDoubleArray(2)) + VERTEX_DESCRIPTOR_INSTANCE_SIZE;

    private static final int MULTI_POINT_INSTANCE_SIZE = ClassLayout.parseClass(OGCMultiPoint.class).instanceSize() +
            ClassLayout.parseClass(MultiPoint.class).instanceSize() +
            MULTI_POINT_IMPL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE;

    private static final int LINE_STRING_INSTANCE_SIZE = ClassLayout.parseClass(OGCLineString.class).instanceSize() +
            ClassLayout.parseClass(Polyline.class).instanceSize() +
            MULTI_PATH_IMPL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT32_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT8_INSTANCE_SIZE;

    private static final int MULTI_LINE_STRING_INSTANCE_SIZE = ClassLayout.parseClass(OGCMultiLineString.class).instanceSize() +
            ClassLayout.parseClass(Polyline.class).instanceSize() +
            MULTI_PATH_IMPL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT32_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT8_INSTANCE_SIZE;

    private static final int POLYGON_INSTANCE_SIZE = ClassLayout.parseClass(OGCPolygon.class).instanceSize() +
            ClassLayout.parseClass(Polygon.class).instanceSize() +
            MULTI_PATH_IMPL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT32_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT8_INSTANCE_SIZE;

    private static final int MULTI_POLYGON_INSTANCE_SIZE = ClassLayout.parseClass(OGCMultiPolygon.class).instanceSize() +
            ClassLayout.parseClass(Polygon.class).instanceSize() +
            MULTI_PATH_IMPL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_DBL_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT32_INSTANCE_SIZE + ATTRIBUTE_STREAM_OF_INT8_INSTANCE_SIZE;

    private static final int GEOMETRY_COLLECTION_INSTANCE_SIZE = ClassLayout.parseClass(OGCConcreteGeometryCollection.class).instanceSize();

    private GeometryMemorySizeUtilsPackageWorkaround() {}

    public static long getEstimatedMemorySizeInBytes(OGCGeometry geometry)
    {
        switch (geometry.geometryType()) {
            case "Point":
                return POINT_INSTANCE_SIZE;
            case "MultiPoint":
                int pointCount = ((MultiVertexGeometry) geometry.getEsriGeometry()).getPointCount();
                return MULTI_POINT_INSTANCE_SIZE + ENVELOPE_INSTANCE_SIZE + sizeOfDoubleArray(2 * pointCount);
            case "LineString":
                return LINE_STRING_INSTANCE_SIZE + getEstimatedMemorySizeInBytes((MultiPath) geometry.getEsriGeometry());
            case "MultiLineString":
                return MULTI_LINE_STRING_INSTANCE_SIZE + getEstimatedMemorySizeInBytes((MultiPath) geometry.getEsriGeometry());
            case "Polygon":
                return POLYGON_INSTANCE_SIZE + getEstimatedMemorySizeInBytes((MultiPath) geometry.getEsriGeometry());
            case "MultiPolygon":
                return MULTI_POLYGON_INSTANCE_SIZE + getEstimatedMemorySizeInBytes((MultiPath) geometry.getEsriGeometry());
            case "GeometryCollection":
                return getEstimatedMemorySizeInBytes((OGCGeometryCollection) geometry);
            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + geometry.geometryType());
        }
    }

    private static long getEstimatedMemorySizeInBytes(MultiPath multiPath)
    {
        int pointCount = multiPath.getPointCount();
        int pathCount = multiPath.getPathCount();
        return ENVELOPE_INSTANCE_SIZE + sizeOfDoubleArray(2 * pointCount) + sizeOfIntArray(pathCount) + sizeOfByteArray(pathCount);
    }

    private static long getEstimatedMemorySizeInBytes(OGCGeometryCollection collection)
    {
        long size = GEOMETRY_COLLECTION_INSTANCE_SIZE;
        for (int i = 0; i < collection.numGeometries(); i++) {
            size += getEstimatedMemorySizeInBytes(collection.geometryN(i));
        }
        return size;
    }
}
