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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class GeoShape
{
    public Slice id;
    public Slice geoShape;

    public GeoShape(Slice id, Slice geoShape)
    {
        this.id = id;
        this.geoShape = geoShape;
    }

    public static List<GeoShape> deserialize(Slice slice)
    {
        List<GeoShape> list = new ArrayList<>();
        int geoShapeSize = slice.getInt(0);
        int offset = SIZE_OF_INT + SIZE_OF_INT * 2 * geoShapeSize;
        for (int i = 0; i < geoShapeSize; ++i) {
            int shapeIdIndex = SIZE_OF_INT + SIZE_OF_INT * 2 * i;
            int shapeIdSize = slice.getInt(shapeIdIndex);
            Slice idSlice = Slices.wrappedBuffer(slice.getBytes(offset, shapeIdSize));
            int shapeSize = slice.getInt(shapeIdIndex + SIZE_OF_INT);
            Slice shapeSlice = Slices.wrappedBuffer(slice.getBytes(offset + shapeIdSize, shapeSize));
            list.add(new GeoShape(idSlice, shapeSlice));
            offset += (shapeIdSize + shapeSize);
        }
        return list;
    }

    public static byte[] serialize(Collection<GeoShape> geoShapes)
    {
        // GeoShape memory arrangement
        // number_of_elements + (length_of_id + length_of_data) * size + (id_data + shape_data) * size
        List<byte[]> dataBytes = new ArrayList<>();
        int totalBytes = SIZE_OF_INT + (SIZE_OF_INT * 2) * geoShapes.size();
        for (GeoShape combo : geoShapes) {
            byte[] idBytes = combo.id.getBytes();
            byte[] shapeBytes = combo.geoShape.getBytes();
            totalBytes += (idBytes.length + shapeBytes.length);
            dataBytes.add(idBytes);
            dataBytes.add(shapeBytes);
        }
        Slice slice = Slices.allocate(totalBytes);
        slice.setInt(0, geoShapes.size());
        for (int i = 0; i < geoShapes.size(); ++i) {
            slice.setInt(SIZE_OF_INT * (2 * i + 1), dataBytes.get(2 * i).length);
            slice.setInt(SIZE_OF_INT * (2 * i + 2), dataBytes.get(2 * i + 1).length);
        }
        int offset = SIZE_OF_INT + SIZE_OF_INT * 2 * geoShapes.size();
        for (byte[] bytes : dataBytes) {
            slice.setBytes(offset, bytes);
            offset += bytes.length;
        }
        return slice.getBytes();
    }
}
