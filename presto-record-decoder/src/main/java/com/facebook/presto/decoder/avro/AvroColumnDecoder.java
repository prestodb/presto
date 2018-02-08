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
package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;

import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
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
}
