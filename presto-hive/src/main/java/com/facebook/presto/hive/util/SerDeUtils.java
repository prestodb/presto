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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
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

import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SerDeUtils
{
    private SerDeUtils() {}

    public static Block getBlockObject(Type type, Object object, ObjectInspector objectInspector)
    {
        return requireNonNull(serializeObject(type, null, object, objectInspector), "serialized result is null");
    }

    public static Block serializeObject(Type type, BlockBuilder builder, Object object, ObjectInspector inspector)
    {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                serializePrimitive(builder, object, (PrimitiveObjectInspector) inspector);
                return null;
            case LIST:
                return serializeList(type, builder, object, (ListObjectInspector) inspector);
            case MAP:
                return serializeMap(type, builder, object, (MapObjectInspector) inspector);
            case STRUCT:
                return serializeStruct(type, builder, object, (StructObjectInspector) inspector);
        }
        throw new RuntimeException("Unknown object inspector category: " + inspector.getCategory());
    }

    private static void serializePrimitive(BlockBuilder builder, Object object, PrimitiveObjectInspector inspector)
    {
        requireNonNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        switch (inspector.getPrimitiveCategory()) {
            case BOOLEAN:
                BooleanType.BOOLEAN.writeBoolean(builder, ((BooleanObjectInspector) inspector).get(object));
                return;
            case BYTE:
                IntegerType.INTEGER.writeLong(builder, ((ByteObjectInspector) inspector).get(object));
                return;
            case SHORT:
                IntegerType.INTEGER.writeLong(builder, ((ShortObjectInspector) inspector).get(object));
                return;
            case INT:
                IntegerType.INTEGER.writeLong(builder, ((IntObjectInspector) inspector).get(object));
                return;
            case LONG:
                BigintType.BIGINT.writeLong(builder, ((LongObjectInspector) inspector).get(object));
                return;
            case FLOAT:
                DoubleType.DOUBLE.writeDouble(builder, ((FloatObjectInspector) inspector).get(object));
                return;
            case DOUBLE:
                DoubleType.DOUBLE.writeDouble(builder, ((DoubleObjectInspector) inspector).get(object));
                return;
            case STRING:
                VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(((StringObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
            case DATE:
                DateType.DATE.writeLong(builder, formatDateAsLong(object, (DateObjectInspector) inspector));
                return;
            case TIMESTAMP:
                TimestampType.TIMESTAMP.writeLong(builder, formatTimestampAsLong(object, (TimestampObjectInspector) inspector));
                return;
            case BINARY:
                VARBINARY.writeSlice(builder, Slices.wrappedBuffer(((BinaryObjectInspector) inspector).getPrimitiveJavaObject(object)));
                return;
            case DECIMAL:
                // TODO: optimize so Type doesn't have to be instantiated
                DecimalType decimalType = createDecimalType(inspector.precision(), inspector.scale());
                HiveDecimalWritable hiveDecimal = ((HiveDecimalObjectInspector) inspector).getPrimitiveWritableObject(object);
                if (decimalType.isShort()) {
                    decimalType.writeLong(builder, DecimalUtils.getShortDecimalValue(hiveDecimal, decimalType.getScale()));
                }
                else {
                    decimalType.writeSlice(builder, DecimalUtils.getLongDecimalValue(hiveDecimal, decimalType.getScale()));
                }
                return;
        }
        throw new RuntimeException("Unknown primitive type: " + inspector.getPrimitiveCategory());
    }

    private static Block serializeList(Type type, BlockBuilder builder, Object object, ListObjectInspector inspector)
    {
        List<?> list = inspector.getList(object);
        if (list == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), list.size());
        }

        for (Object element : list) {
            serializeObject(elementType, currentBuilder, element, elementInspector);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeMap(Type type, BlockBuilder builder, Object object, MapObjectInspector inspector)
    {
        Map<?, ?> map = inspector.getMap(object);
        if (map == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);
        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), map.size());
        }

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, entry.getKey(), keyInspector);
                serializeObject(valueType, currentBuilder, entry.getValue(), valueInspector);
            }
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object, StructObjectInspector inspector)
    {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        List<? extends StructField> allStructFieldRefs = inspector.getAllStructFieldRefs();
        checkArgument(typeParameters.size() == allStructFieldRefs.size());
        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), typeParameters.size());
        }

        for (int i = 0; i < typeParameters.size(); i++) {
            StructField field = allStructFieldRefs.get(i);
            serializeObject(typeParameters.get(i), currentBuilder, inspector.getStructFieldData(object, field), field.getFieldObjectInspector());
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
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
