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
package com.facebook.presto.kafka.encoder.raw;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.kafka.encoder.AbstractRowEncoder;
import com.facebook.presto.kafka.encoder.EncoderColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class RawRowEncoder
        extends AbstractRowEncoder
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
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, BOOLEAN);

    public static final String NAME = "raw";

    private final List<ColumnMapping> columnMappings;
    private final ByteBuffer buffer;

    public RawRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        super(session, columnHandles);

        for (EncoderColumnHandle handle : this.columnHandles) {
            checkArgument(isSupportedType(handle.getType()), "Unsupported column type '%s' for column '%s'", handle.getType().getDisplayName(), handle.getName());
            checkArgument(handle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", handle.getFormatHint(), handle.getName());
        }

        // parse column mappings from column handles
        this.columnMappings = this.columnHandles.stream().map(ColumnMapping::new).collect(toImmutableList());

        for (ColumnMapping mapping : this.columnMappings) {
            if (mapping.getLength() != mapping.getFieldType().getSize() && !isVarcharType(mapping.getType())) {
                throw new IndexOutOfBoundsException(format(
                        "Mapping length '%s' is not equal to expected length '%s' for column '%s'",
                        mapping.getLength(),
                        mapping.getFieldType().getSize(),
                        mapping.getName()));
            }
        }

        // check that column mappings don't overlap and that there are no gaps
        int position = 0;
        for (ColumnMapping mapping : this.columnMappings) {
            checkArgument(mapping.getStart() == position, format(
                    "Start mapping '%s' for column '%s' does not equal expected mapping '%s'",
                    mapping.getStart(),
                    mapping.getName(),
                    position));
            checkArgument(mapping.getEnd() > mapping.getStart(), format(
                    "End mapping '%s' for column '%s' is less than or equal to start '%s'",
                    mapping.getEnd(),
                    mapping.getName(),
                    mapping.getStart()));
            position += mapping.getLength();
        }

        this.buffer = ByteBuffer.allocate(position);
    }

    private static class ColumnMapping
    {
        private final String name;
        private final Type type;
        private final FieldType fieldType;
        private final int start;
        private final int end;

        public ColumnMapping(EncoderColumnHandle columnHandle)
        {
            this.name = columnHandle.getName();
            this.type = columnHandle.getType();

            this.fieldType = parseFieldType(columnHandle.getDataFormat(), this.name);
            checkFieldType(this.name, this.type, this.fieldType);

            Optional<String> mapping = Optional.ofNullable(columnHandle.getMapping());
            if (mapping.isPresent()) {
                Matcher mappingMatcher = MAPPING_PATTERN.matcher(mapping.get());
                if (!mappingMatcher.matches()) {
                    throw new IllegalArgumentException(format("Invalid mapping for column '%s'", this.name));
                }

                if (mappingMatcher.group(2) != null) {
                    this.start = parseOffset(mappingMatcher.group(1), "start", this.name);
                    this.end = parseOffset(mappingMatcher.group(2), "end", this.name);
                }
                else {
                    this.start = parseOffset(mappingMatcher.group(1), "start", this.name);
                    this.end = this.start + this.fieldType.getSize();
                }
            }
            else {
                throw new IllegalArgumentException(format("No mapping defined for column '%s'", this.name));
            }
        }

        private static int parseOffset(String group, String offsetName, String columnName)
        {
            try {
                return parseInt(group);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("Unable to parse '%s' offset for column '%s'", offsetName, columnName), e);
            }
        }

        private static FieldType parseFieldType(String dataFormat, String columnName)
        {
            try {
                if (!dataFormat.equals("")) {
                    return FieldType.valueOf(dataFormat.toUpperCase(Locale.ENGLISH));
                }
                return FieldType.BYTE;
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(format("Invalid dataFormat '%s' for column '%s'", dataFormat, columnName));
            }
        }

        private static void checkFieldType(String columnName, Type columnType, FieldType fieldType)
        {
            if (columnType == BIGINT) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
            }
            else if (columnType == INTEGER) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT);
            }
            else if (columnType == SMALLINT) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT);
            }
            else if (columnType == TINYINT) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
            }
            else if (columnType == BOOLEAN) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
            }
            else if (columnType == DOUBLE) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.DOUBLE, FieldType.FLOAT);
            }
            else if (isVarcharType(columnType)) {
                checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
            }
        }

        private static void checkFieldTypeOneOf(FieldType declaredFieldType, String columnName, Type columnType, FieldType... allowedFieldTypes)
        {
            checkArgument(Arrays.asList(allowedFieldTypes).contains(declaredFieldType),
                    format("Wrong dataformat '%s' specified for column '%s'; %s type implies use of %s",
                            declaredFieldType.name(),
                            columnName,
                            columnType,
                            Joiner.on("/").join(allowedFieldTypes)));
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }

        public int getStart()
        {
            return start;
        }

        public int getEnd()
        {
            return end;
        }

        public FieldType getFieldType()
        {
            return fieldType;
        }

        public int getLength()
        {
            return end - start;
        }
    }

    private static boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendLong(long value)
    {
        buffer.putLong(value);
    }

    @Override
    protected void appendInt(int value)
    {
        buffer.putInt(value);
    }

    @Override
    protected void appendShort(short value)
    {
        buffer.putShort(value);
    }

    @Override
    protected void appendByte(byte value)
    {
        buffer.put(value);
    }

    @Override
    protected void appendDouble(double value)
    {
        buffer.putDouble(value);
    }

    @Override
    protected void appendFloat(float value)
    {
        buffer.putFloat(value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        buffer.put((byte) (value ? 1 : 0));
    }

    @Override
    protected void appendString(String value)
    {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        checkArgument(valueBytes.length == columnMappings.get(currentColumnIndex).getLength(), format(
                "length '%s' of message '%s' for column '%s' does not equal expected length '%s'",
                valueBytes.length,
                value,
                columnHandles.get(currentColumnIndex).getName(),
                columnMappings.get(currentColumnIndex).getLength()));
        buffer.put(valueBytes, 0, valueBytes.length);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        byte[] valueBytes = value.array();
        checkArgument(valueBytes.length == columnMappings.get(currentColumnIndex).getLength(), format(
                "length '%s' of message for column '%s' does not equal expected length '%s'",
                valueBytes.length,
                columnHandles.get(currentColumnIndex).getName(),
                columnMappings.get(currentColumnIndex).getLength()));
        buffer.put(valueBytes, 0, valueBytes.length);
    }

    @Override
    public byte[] toByteArray()
    {
        // make sure entire row has been updated with new values
        checkArgument(currentColumnIndex == columnHandles.size(), format("Missing %d columns", columnHandles.size() - currentColumnIndex + 1));

        resetColumnIndex(); // reset currentColumnIndex to prepare for next row
        buffer.clear(); // set buffer position back to 0 to prepare for next row, this method does not affect the backing byte array
        return buffer.array();
    }
}
