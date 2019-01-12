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
package io.prestosql.decoder.avro;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroColumnDecoder
{
    private final Type columnType;
    private final String columnMapping;
    private final String columnName;

    public AvroColumnDecoder(DecoderColumnHandle columnHandle)
    {
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            this.columnType = columnHandle.getType();
            this.columnMapping = columnHandle.getMapping();

            this.columnName = columnHandle.getName();
            checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnName);
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
            checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
            checkArgument(columnHandle.getMapping() != null, "mapping not defined for column '%s'", columnName);

            checkArgument(isSupportedType(columnType), "Unsupported column type '%s' for column '%s'", columnType, columnName);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(GENERIC_USER_ERROR, e);
        }
    }

    private boolean isSupportedType(Type type)
    {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type.getTypeSignature().getBase().equalsIgnoreCase(ARRAY)) {
            checkArgument(type.getTypeParameters().size() == 1, "expecting exactly one type parameter for array");
            return isSupportedPrimitive(type.getTypeParameters().get(0));
        }

        if (type.getTypeSignature().getBase().equalsIgnoreCase(MAP)) {
            List<Type> typeParameters = type.getTypeParameters();
            checkArgument(typeParameters.size() == 2, "expecting exactly two type parameters for map");
            checkArgument(typeParameters.get(0) instanceof VarcharType, "Unsupported column type '%s' for map key", typeParameters.get(0));
            return isSupportedPrimitive(type.getTypeParameters().get(1));
        }
        return false;
    }

    private boolean isSupportedPrimitive(Type type)
    {
        return isVarcharType(type) ||
                ImmutableList.of(
                        BigintType.BIGINT,
                        BooleanType.BOOLEAN,
                        DoubleType.DOUBLE,
                        VarbinaryType.VARBINARY).contains(type);
    }

    public FieldValueProvider decodeField(GenericRecord avroRecord)
    {
        Object avroColumnValue = locateNode(avroRecord, columnMapping);
        return new ObjectValueProvider(avroColumnValue, columnType, columnName);
    }

    private static Object locateNode(GenericRecord element, String columnMapping)
    {
        Object value = element;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(columnMapping)) {
            if (value == null) {
                return null;
            }
            value = ((GenericRecord) value).get(pathElement);
        }
        return value;
    }

    private static class ObjectValueProvider
            extends FieldValueProvider
    {
        private final Object value;
        private final Type columnType;
        private final String columnName;

        public ObjectValueProvider(Object value, Type columnType, String columnName)
        {
            this.value = value;
            this.columnType = columnType;
            this.columnName = columnName;
        }

        @Override
        public boolean isNull()
        {
            return value == null;
        }

        @Override
        public double getDouble()
        {
            if (value instanceof Double || value instanceof Float) {
                return ((Number) value).doubleValue();
            }
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public boolean getBoolean()
        {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public long getLong()
        {
            if (value instanceof Long || value instanceof Integer) {
                return ((Number) value).longValue();
            }
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public Slice getSlice()
        {
            return AvroColumnDecoder.getSlice(value, columnType, columnName);
        }

        @Override
        public Block getBlock()
        {
            return serializeObject(null, value, columnType, columnName);
        }
    }

    private static Slice getSlice(Object value, Type type, String columnName)
    {
        switch (type.getTypeSignature().getBase()) {
            case VARCHAR:
                if (value instanceof Utf8) {
                    return truncateToLength(utf8Slice(value.toString()), type);
                }
            case VARBINARY:
                if (value instanceof ByteBuffer) {
                    return Slices.wrappedBuffer((ByteBuffer) value);
                }
            default:
                throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
        }
    }

    private static Block serializeObject(BlockBuilder builder, Object value, Type type, String columnName)
    {
        switch (type.getTypeSignature().getBase()) {
            case ARRAY:
                return serializeList(builder, value, type, columnName);
            case MAP:
                return serializeMap(builder, value, type, columnName);
            default:
                serializeGeneric(builder, value, type, columnName);
        }
        return null;
    }

    private static Block serializeList(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            requireNonNull(blockBuilder, "parent blockBuilder is null").appendNull();
            return blockBuilder.build();
        }

        List<?> list = (List) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type elementType = typeParameters.get(0);

        BlockBuilder currentBlockBuilder;
        if (blockBuilder != null) {
            currentBlockBuilder = blockBuilder.beginBlockEntry();
        }
        else {
            currentBlockBuilder = elementType.createBlockBuilder(null, list.size());
        }

        for (Object element : list) {
            serializeObject(currentBlockBuilder, element, elementType, columnName);
        }

        if (blockBuilder != null) {
            blockBuilder.closeEntry();
            return null;
        }
        return currentBlockBuilder.build();
    }

    private static void serializeGeneric(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        requireNonNull(blockBuilder, "parent blockBuilder is null");

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        switch (type.getTypeSignature().getBase()) {
            case BOOLEAN:
                type.writeBoolean(blockBuilder, (Boolean) value);
                break;
            case BIGINT:
                type.writeLong(blockBuilder, (Long) value);
                break;
            case DOUBLE:
                type.writeDouble(blockBuilder, (Double) value);
                break;
            case VARCHAR:
            case VARBINARY:
                type.writeSlice(blockBuilder, getSlice(value, type, columnName));
                break;
            default:
                throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
        }
    }

    private static Block serializeMap(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            requireNonNull(blockBuilder, "parent blockBuilder is null").appendNull();
            return blockBuilder.build();
        }

        Map<?, ?> map = (Map) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder entryBuilder;
        boolean builderSynthesized = false;

        if (blockBuilder == null) {
            builderSynthesized = true;
            blockBuilder = type.createBlockBuilder(null, 1);
        }
        entryBuilder = blockBuilder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() != null) {
                keyType.writeSlice(entryBuilder, truncateToLength(utf8Slice(entry.getKey().toString()), keyType));
                serializeObject(entryBuilder, entry.getValue(), valueType, columnName);
            }
        }

        blockBuilder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(blockBuilder, 0);
        }
        return null;
    }
}
