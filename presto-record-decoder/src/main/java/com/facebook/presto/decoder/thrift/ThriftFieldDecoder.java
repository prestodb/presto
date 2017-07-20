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
package com.facebook.presto.decoder.thrift;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftFieldDecoder
        implements FieldDecoder<Object>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.of(boolean.class, long.class, double.class, Slice.class, Block.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return ThriftRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public FieldValueProvider decode(Object value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        return new ObjectValueProvider(value, columnHandle);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class ObjectValueProvider
            extends FieldValueProvider
    {
        protected final Object value;
        protected final DecoderColumnHandle columnHandle;

        public ObjectValueProvider(Object value, DecoderColumnHandle columnHandle)
        {
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.value = value;
        }

        @Override
        public final boolean accept(DecoderColumnHandle columnHandle)
        {
            return this.columnHandle.equals(columnHandle);
        }

        @Override
        public final boolean isNull()
        {
            return value == null;
        }

        @Override
        public boolean getBoolean()
        {
            return isNull() ? false : (Boolean) value;
        }

        @Override
        public long getLong()
        {
            return isNull() ? 0L : getLongExpressedValue(value);
        }

        private static long getLongExpressedValue(Object value)
        {
            if (value instanceof Date) {
                long storageTime = ((Date) value).getTime();
                // convert date from VM current time zone to UTC
                long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
                return TimeUnit.MILLISECONDS.toDays(utcMillis);
            }
            if (value instanceof Timestamp) {
                long parsedJvmMillis = ((Timestamp) value).getTime();
                DateTimeZone jvmTimeZone = DateTimeZone.getDefault();
                long convertedMillis = jvmTimeZone.convertUTCToLocal(parsedJvmMillis);

                return convertedMillis;
            }
            if (value instanceof Float) {
                return floatToRawIntBits(((Float) value));
            }
            return ((Number) value).longValue();
        }

        @Override
        public double getDouble()
        {
            return isNull() ? 0.0d : (Double) value;
        }

        @Override
        public Slice getSlice()
        {
            return isNull() ? EMPTY_SLICE : getSliceExpressedValue(value, columnHandle.getType());
        }

        private static Slice getSliceExpressedValue(Object value, Type type)
        {
            Slice sliceValue;
            if (value instanceof String) {
                sliceValue = Slices.utf8Slice((String) value);
            }
            else if (value instanceof byte[]) {
                sliceValue = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof Integer) {
                sliceValue = Slices.utf8Slice(value.toString());
            }
            else {
                throw new IllegalStateException("unsupported string field type: " + value.getClass().getName());
            }
            if (isVarcharType(type)) {
                sliceValue = truncateToLength(sliceValue, type);
            }
            if (isCharType(type)) {
                sliceValue = trimSpacesAndTruncateToLength(sliceValue, type);
            }

            return sliceValue;
        }

        @Override
        public Block getBlock()
        {
            if (isNull()) {
                return null;
            }

            Type type = columnHandle.getType();
            return serializeObject(type, null, value);
        }

        private static Block serializeObject(Type type, BlockBuilder builder, Object object)
        {
            if (!isStructuralType(type)) {
                serializePrimitive(type, builder, object);
                return null;
            }
            else if (isArrayType(type)) {
                return serializeList(type, builder, object);
            }
            else if (isMapType(type)) {
                return serializeMap(type, builder, object);
            }
            else if (isRowType(type)) {
                return serializeStruct(type, builder, object);
            }
            throw new RuntimeException("Unknown object type: " + type);
        }

        private static Block serializeList(Type type, BlockBuilder builder, Object object)
        {
            List<?> list = (List) object;
            if (list == null) {
                requireNonNull(builder, "parent builder is null").appendNull();
                return null;
            }

            List<Type> typeParameters = type.getTypeParameters();
            checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
            Type elementType = typeParameters.get(0);

            BlockBuilder currentBuilder;
            if (builder != null) {
                currentBuilder = builder.beginBlockEntry();
            }
            else {
                currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), list.size());
            }

            for (Object element : list) {
                serializeObject(elementType, currentBuilder, element);
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
            if (builder != null) {
                currentBuilder = builder.beginBlockEntry();
            }
            else {
                currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), map.size());
            }

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                // Hive skips map entries with null keys
                if (entry.getKey() != null) {
                    serializeObject(keyType, currentBuilder, entry.getKey());
                    serializeObject(valueType, currentBuilder, entry.getValue());
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

        private static Block serializeStruct(Type type, BlockBuilder builder, Object object)
        {
            if (object == null) {
                requireNonNull(builder, "parent builder is null").appendNull();
                return null;
            }

            List<Type> typeParameters = type.getTypeParameters();
            ThriftGenericRow structData = (ThriftGenericRow) object;
            BlockBuilder currentBuilder;
            if (builder != null) {
                currentBuilder = builder.beginBlockEntry();
            }
            else {
                currentBuilder = new InterleavedBlockBuilder(typeParameters, new BlockBuilderStatus(), typeParameters.size());
            }

            for (int i = 0; i < typeParameters.size(); i++) {
                // TODO: Handle cases where ids are not consecutive
                Object fieldValue = structData.getFieldValueForThriftId((short) (i + 1));
                serializeObject(typeParameters.get(i), currentBuilder, fieldValue);
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

        private static void serializePrimitive(Type type, BlockBuilder builder, Object object)
        {
            requireNonNull(builder, "parent builder is null");

            if (object == null) {
                builder.appendNull();
                return;
            }

            if (BOOLEAN.equals(type)) {
                BOOLEAN.writeBoolean(builder, (Boolean) object);
            }
            else if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)
                    || REAL.equals(type) || DATE.equals(type) || TIMESTAMP.equals(type)) {
                type.writeLong(builder, getLongExpressedValue(object));
            }
            else if (DOUBLE.equals(type)) {
                DOUBLE.writeDouble(builder, ((Number) object).doubleValue());
            }
            else if (isVarcharType(type) || VARBINARY.equals(type) || isCharType(type)) {
                type.writeSlice(builder, getSliceExpressedValue(object, type));
            }
            else {
                throw new UnsupportedOperationException("Unsupported primitive type: " + type);
            }
        }

        public static boolean isArrayType(Type type)
        {
            return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
        }

        public static boolean isMapType(Type type)
        {
            return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
        }

        public static boolean isRowType(Type type)
        {
            return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
        }

        public static boolean isStructuralType(Type type)
        {
            String baseName = type.getTypeSignature().getBase();
            return baseName.equals(StandardTypes.MAP) || baseName.equals(StandardTypes.ARRAY) || baseName.equals(StandardTypes.ROW);
        }
    }
}
