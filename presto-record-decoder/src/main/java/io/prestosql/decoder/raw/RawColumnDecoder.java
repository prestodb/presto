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
package io.prestosql.decoder.raw;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.Varchars;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RawColumnDecoder
{
    private enum FieldType
    {
        BYTE(Byte.SIZE),
        SHORT(Short.SIZE),
        INT(Integer.SIZE),
        LONG(Long.SIZE),
        FLOAT(Float.SIZE),
        DOUBLE(Double.SIZE);

        private final int size;

        FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }
    }

    private static final Pattern MAPPING_PATTERN = Pattern.compile("(\\d+)(?::(\\d+))?");

    private final String columnName;
    private final Type columnType;
    private final FieldType fieldType;
    private final int start;
    private final OptionalInt end;

    public RawColumnDecoder(DecoderColumnHandle columnHandle)
    {
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnHandle.getName());
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());

            columnName = columnHandle.getName();
            columnType = columnHandle.getType();

            try {
                fieldType = columnHandle.getDataFormat() == null ?
                        FieldType.BYTE :
                        FieldType.valueOf(columnHandle.getDataFormat().toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(format("invalid dataFormat '%s' for column '%s'", columnHandle.getDataFormat(), columnName));
            }

            String mapping = Optional.ofNullable(columnHandle.getMapping()).orElse("0");
            Matcher mappingMatcher = MAPPING_PATTERN.matcher(mapping);
            if (!mappingMatcher.matches()) {
                throw new IllegalArgumentException(format("invalid mapping format '%s' for column '%s'", mapping, columnName));
            }
            start = parseInt(mappingMatcher.group(1));
            if (mappingMatcher.group(2) != null) {
                end = OptionalInt.of(parseInt(mappingMatcher.group(2)));
            }
            else {
                if (!isVarcharType(columnType)) {
                    end = OptionalInt.of(start + fieldType.getSize());
                }
                else {
                    end = OptionalInt.empty();
                }
            }

            checkArgument(start >= 0, "start offset %s for column '%s' must be greater or equal 0", start, columnName);
            end.ifPresent(endValue -> {
                checkArgument(endValue >= 0, "end offset %s for column '%s' must be greater or equal 0", endValue, columnName);
                checkArgument(endValue >= start, "end offset %s for column '%s' must greater or equal start offset", endValue, columnName);
            });

            checkArgument(isSupportedType(columnType), "Unsupported column type '%s' for column '%s'", columnType.getDisplayName(), columnName);

            if (columnType == BIGINT) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
            }
            if (columnType == INTEGER) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE, FieldType.SHORT, FieldType.INT);
            }
            if (columnType == SMALLINT) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE, FieldType.SHORT);
            }
            if (columnType == TINYINT) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE);
            }
            if (columnType == BOOLEAN) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
            }
            if (columnType == DOUBLE) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.DOUBLE, FieldType.FLOAT);
            }
            if (isVarcharType(columnType)) {
                checkFieldTypeOneOf(fieldType, columnName, FieldType.BYTE);
            }

            if (!isVarcharType(columnType)) {
                checkArgument(!end.isPresent() || end.getAsInt() - start == fieldType.getSize(),
                        "Bytes mapping for column '%s' does not match dataFormat '%s'; expected %s bytes but got %s",
                        columnName,
                        fieldType.getSize(),
                        end.getAsInt() - start);
            }
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_USER_ERROR, e);
        }
    }

    private static boolean isSupportedType(Type type)
    {
        if (isVarcharType(type)) {
            return true;
        }
        if (ImmutableList.of(BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN, DOUBLE).contains(type)) {
            return true;
        }
        return false;
    }

    private void checkFieldTypeOneOf(FieldType declaredFieldType, String columnName, FieldType... allowedFieldTypes)
    {
        if (!Arrays.asList(allowedFieldTypes).contains(declaredFieldType)) {
            throw new IllegalArgumentException(format(
                    "Wrong dataFormat '%s' specified for column '%s'; %s type implies use of %s",
                    declaredFieldType.name(),
                    columnName,
                    columnType.getDisplayName(),
                    Joiner.on("/").join(allowedFieldTypes)));
        }
    }

    public FieldValueProvider decodeField(byte[] value)
    {
        requireNonNull(value, "value is null");

        int actualEnd = end.orElse(value.length);

        if (start > value.length) {
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format(
                    "start offset %s for column '%s' must be less that or equal to value length %s",
                    start,
                    columnName,
                    value.length));
        }

        if (actualEnd > value.length) {
            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format(
                    "end offset %s for column '%s' must be less that or equal to value length %s",
                    actualEnd,
                    columnName,
                    value.length));
        }
        return new RawValueProvider(ByteBuffer.wrap(value, start, actualEnd - start), fieldType, columnName, columnType);
    }

    private static class RawValueProvider
            extends FieldValueProvider
    {
        private final ByteBuffer value;
        private final FieldType fieldType;
        private final String columnName;
        private final Type columnType;
        private final int size;

        public RawValueProvider(ByteBuffer value, FieldType fieldType, String columnName, Type columnType)
        {
            this.value = value;
            this.fieldType = fieldType;
            this.columnName = columnName;
            this.columnType = columnType;
            this.size = value.limit() - value.position();
        }

        @Override
        public final boolean isNull()
        {
            return size == 0;
        }

        @Override
        public boolean getBoolean()
        {
            checkEnoughBytes();
            switch (fieldType) {
                case BYTE:
                    return value.get() != 0;
                case SHORT:
                    return value.getShort() != 0;
                case INT:
                    return value.getInt() != 0;
                case LONG:
                    return value.getLong() != 0;
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion '%s' to boolean not supported", fieldType));
            }
        }

        @Override
        public long getLong()
        {
            checkEnoughBytes();
            switch (fieldType) {
                case BYTE:
                    return value.get();
                case SHORT:
                    return value.getShort();
                case INT:
                    return value.getInt();
                case LONG:
                    return value.getLong();
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion '%s' to long not supported", fieldType));
            }
        }

        @Override
        public double getDouble()
        {
            checkEnoughBytes();
            switch (fieldType) {
                case FLOAT:
                    return value.getFloat();
                case DOUBLE:
                    return value.getDouble();
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion '%s' to double not supported", fieldType));
            }
        }

        private void checkEnoughBytes()
        {
            checkState(size >= fieldType.getSize(), "minimum byte size for column '%s' is %s, found %s,", columnName, fieldType.getSize(), size);
        }

        @Override
        public Slice getSlice()
        {
            Slice slice = Slices.wrappedBuffer(value.slice());
            return Varchars.truncateToLength(slice, columnType);
        }
    }
}
