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
package com.facebook.presto.hive.util;

import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SerDeUtils
{
    private SerDeUtils() {}

    public static Slice getBlockSlice(Object object, ObjectInspector objectInspector, Type type)
    {
        return checkNotNull(serializeObject(null, object, objectInspector, type), "serialized result is null");
    }

    @VisibleForTesting
    static Slice serializeObject(BlockBuilder builder, Object object, ObjectInspector inspector, Type type)
    {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                serializePrimitive(builder, object, (PrimitiveObjectInspector) inspector, type);
                return null;
            case LIST:
                return serializeList(builder, object, (ListObjectInspector) inspector, type);
            case MAP:
                return serializeMap(builder, object, (MapObjectInspector) inspector, type);
            case STRUCT:
                return serializeStruct(builder, object, (StructObjectInspector) inspector, type);
        }
        throw new RuntimeException("Unknown object inspector category: " + inspector.getCategory());
    }

    private static void serializePrimitive(BlockBuilder builder, Object object, PrimitiveObjectInspector inspector, Type type)
    {
        checkNotNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                type.writeBoolean(builder, ((BooleanObjectInspector) inspector).get(object));
                return;
            case BYTE:
                type.writeLong(builder, ((ByteObjectInspector) inspector).get(object));
                return;
            case SHORT:
                type.writeLong(builder, ((ShortObjectInspector) inspector).get(object));
                return;
            case INT:
                type.writeLong(builder, ((IntObjectInspector) inspector).get(object));
                return;
            case LONG:
                type.writeLong(builder, ((LongObjectInspector) inspector).get(object));
                return;
            case FLOAT:
                type.writeDouble(builder, ((FloatObjectInspector) inspector).get(object));
                return;
            case DOUBLE:
                type.writeDouble(builder, ((DoubleObjectInspector) inspector).get(object));
                return;
            case STRING:
                type.writeSlice(builder, Slices.utf8Slice(((StringObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
            case DATE:
                type.writeLong(builder, formatDateAsLong(object, (DateObjectInspector) inspector));
                return;
            case TIMESTAMP:
                type.writeLong(builder, formatTimestampAsLong(object, (TimestampObjectInspector) inspector));
                return;
            case BINARY:
                type.writeSlice(builder, Slices.wrappedBuffer(((BinaryObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
        }
        throw new RuntimeException("Unknown primitive type: " + inspector.getPrimitiveCategory());
    }

    private static Slice serializeList(BlockBuilder builder, Object object, ListObjectInspector inspector, Type type)
    {
        List<?> list = inspector.getList(object);
        if (list == null) {
            checkNotNull(builder, "parent builder is null").appendNull();
            return null;
        }

        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        BlockBuilder currentBuilder = createBlockBuilder(type.getTypeParameters().get(0), list.size());

        for (Object element : list) {
            serializeObject(currentBuilder, element, elementInspector, type.getTypeParameters().get(0));
        }

        SliceOutput out = new DynamicSliceOutput(1024);
        currentBuilder.getEncoding().writeBlock(out, currentBuilder.build());

        if (builder != null) {
            type.writeSlice(builder, out.slice());
        }

        return out.slice();
    }

    private static Slice serializeMap(BlockBuilder builder, Object object, MapObjectInspector inspector, Type type)
    {
        Map<?, ?> map = inspector.getMap(object);
        if (map == null) {
            checkNotNull(builder, "parent builder is null").appendNull();
            return null;
        }

        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        BlockBuilder keyBuilder = createBlockBuilder(keyType, map.size());
        BlockBuilder valueBuilder = createBlockBuilder(valueType, map.size());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyBuilder, entry.getKey(), keyInspector, keyType);
                serializeObject(valueBuilder, entry.getValue(), valueInspector, valueType);
            }
        }

        SliceOutput out = new DynamicSliceOutput(1024);
        keyType.getEncoding().writeBlock(out, keyBuilder.build());
        valueType.getEncoding().writeBlock(out, valueBuilder.build());

        if (builder != null) {
            type.writeSlice(builder, out.slice());
        }

        return out.slice();
    }

    private static Slice serializeStruct(BlockBuilder builder, Object object, StructObjectInspector inspector, Type type)
    {
        if (object == null) {
            checkNotNull(builder, "parent builder is null").appendNull();
            return null;
        }

        BlockBuilder currentBuilder = createBlockBuilder(type, inspector.getAllStructFieldRefs().size());
        int index = 0;
        for (StructField field : inspector.getAllStructFieldRefs()) {
            serializeObject(currentBuilder, inspector.getStructFieldData(object, field), field.getFieldObjectInspector(), type.getTypeParameters().get(index++));
        }

        SliceOutput out = new DynamicSliceOutput(1024);
        currentBuilder.getEncoding().writeBlock(out, currentBuilder.build());

        if (builder != null) {
            type.writeSlice(builder, out.slice());
        }

        return out.slice();
    }

    private static BlockBuilder createBlockBuilder(Type type, int size)
    {
        return type.createBlockBuilder(new BlockBuilderStatus(), size);
    }

    private static long formatDateAsLong(Object object, DateObjectInspector inspector)
    {
        if (object instanceof LazyDate) {
            return ((LazyDate) object).getWritableObject().getDays();
        }
        if (object instanceof DateWritable) {
            return ((DateWritable) object).getDays();
        }

        // Hive will return java.sql.Date at midnight in JVM time zone
        long millisLocal = inspector.getPrimitiveJavaObject(object).getTime();
        // Convert it to midnight in UTC
        long millisUtc = DateTimeZone.getDefault().getMillisKeepLocal(DateTimeZone.UTC, millisLocal);
        // Convert midnight UTC to days
        return TimeUnit.MILLISECONDS.toDays(millisUtc);
    }

    private static long formatTimestampAsLong(Object object, TimestampObjectInspector inspector)
    {
        Timestamp timestamp = getTimestamp(object, inspector);
        return timestamp.getTime();
    }

    private static Timestamp getTimestamp(Object object, TimestampObjectInspector inspector)
    {
        // handle broken ObjectInspectors
        if (object instanceof TimestampWritable) {
            return ((TimestampWritable) object).getTimestamp();
        }
        return inspector.getPrimitiveJavaObject(object);
    }
}
