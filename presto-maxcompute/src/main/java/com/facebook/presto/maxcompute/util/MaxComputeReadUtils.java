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
package com.facebook.presto.maxcompute.util;

import com.aliyun.odps.data.SimpleStruct;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import org.joda.time.chrono.ISOChronology;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public final class MaxComputeReadUtils
{
    private MaxComputeReadUtils() {}

    public static Block serializeObject(Type type, BlockBuilder builder, Object object)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case StandardTypes.ARRAY:
                return serializeArray(type, builder, object);

            case StandardTypes.MAP:
                return serializeMap(type, builder, object);

            case StandardTypes.ROW:
                return serializeStruct(type, builder, object);

            case StandardTypes.BIGINT:
            case StandardTypes.INTEGER:
            case StandardTypes.SMALLINT:
            case StandardTypes.TINYINT:
            case StandardTypes.BOOLEAN:
            case StandardTypes.DATE:
                // case StandardTypes.DECIMAL:
            case StandardTypes.REAL:
            case StandardTypes.DOUBLE:
            case StandardTypes.TIMESTAMP:
            case StandardTypes.TIME:
            case StandardTypes.VARBINARY:
            case StandardTypes.VARCHAR:
                serializePrimitive(type, builder, object);
                return null;
        }

        throw new RuntimeException("Unknown data type: " + type);
    }

    private static Block serializeMap(Type type, BlockBuilder builder, Object object)
    {
        Map<?, ?> map = (Map) object;
        if (map == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder currentBuilder;

        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }
        currentBuilder = builder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, entry.getKey());
                serializeObject(valueType, currentBuilder, entry.getValue());
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        else {
            return null;
        }
    }

    private static Block serializeArray(Type type, BlockBuilder builder, Object object)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null");
            builder.appendNull();
            return null;
        }

        List<?> list = (ArrayList) object;

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "Array must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        }
        else {
            currentBuilder = elementType.createBlockBuilder(null, list.size());
        }

        for (Object element : list) {
            serializeObject(elementType, currentBuilder, element);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        }
        else {
            return currentBuilder.build();
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null");
            builder.appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();

        BlockBuilder currentBuilder;

        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }

        currentBuilder = builder.beginBlockEntry();

        for (int i = 0; i < typeParameters.size(); i++) {
            Optional<String> fieldName = type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName();
            String name = "";
            if (fieldName.isPresent()) {
                name = fieldName.get().toLowerCase(ENGLISH);
            }

            SimpleStruct simpleStruct = (SimpleStruct) object;
            Object value = simpleStruct.getFieldValue(name);
            serializeObject(typeParameters.get(i), currentBuilder, value);
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        else {
            return null;
        }
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object)
    {
        requireNonNull(builder, "builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (type.equals(BIGINT)) {
            type.writeLong(builder, (Long) object);
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(builder, (Integer) object);
        }
        else if (type.equals(SMALLINT)) {
            type.writeLong(builder, (Short) object);
        }
        else if (type.equals(TINYINT)) {
            type.writeLong(builder, (Byte) object);
        }
        else if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, (Boolean) object);
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, (Double) object);
        }
        else if (type.equals(DATE)) {
            type.writeLong(builder, formatDateAsLong(object));
        }
        else if (type.equals(REAL)) {
            type.writeLong(builder, floatToRawIntBits((Float) object));
        }
        else if (type.equals(TIMESTAMP)) {
            type.writeLong(builder, formatTimestampAsLong(object));
        }
        else if (type.equals(VARCHAR) || type.equals(VARBINARY) || type instanceof VarcharType) {
            // VARCHAR(XXX)、VARCHAR、BINARY
            type.writeSlice(builder, utf8Slice(object.toString()));
        }
        else {
            throw new IllegalArgumentException("Unknown primitive type: " + type.getDisplayName());
        }
    }

    private static long formatDateAsLong(Object object)
    {
        long localMillis = ((Date) object).getTime();
        // Convert it to a midnight in UTC
        long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
        // convert to days
        return TimeUnit.MILLISECONDS.toDays(utcMillis);
    }

    private static long formatTimestampAsLong(Object object)
    {
        if (object instanceof Date) {
            Date date = (Date) object;
            return date.getTime();
        }

        Timestamp timestamp = (Timestamp) object;
        return timestamp.getTime();
    }
}
