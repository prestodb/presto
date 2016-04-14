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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Row
{
    private static final DateTimeFormatter DATE_PARSER = ISODateTimeFormat.date();
    private static final DateTimeFormatter TIME_PARSER = DateTimeFormat.forPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_PARSER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final List<Field> fields = new ArrayList<>();

    public Row() {}

    public Row(Row row)
    {
        requireNonNull(row, "row is null");
        fields.addAll(row.fields.stream().map(Field::new).collect(Collectors.toList()));
    }

    public Row addField(Field field)
    {
        requireNonNull(field, "field is null");
        fields.add(field);
        return this;
    }

    public Row addField(Object value, Type type)
    {
        requireNonNull(type, "type is null");
        fields.add(new Field(value, type));
        return this;
    }

    public Field getField(int i)
    {
        return fields.get(i);
    }

    /**
     * Gets a list of all internal fields. Any changes to this list will affect this row.
     *
     * @return List of fields
     */
    public List<Field> getFields()
    {
        return fields;
    }

    public int length()
    {
        return fields.size();
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(fields.toArray());
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof Row && Objects.equals(this.fields, ((Row) obj).getFields());
    }

    @Override
    public String toString()
    {
        if (fields.isEmpty()) {
            return "()";
        }
        else {
            StringBuilder builder = new StringBuilder("(");
            for (Field f : fields) {
                builder.append(f).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            return builder.append(')').toString();
        }
    }

    /**
     * Creates a new {@link Row} from the given delimited string based on the given {@link RowSchema}
     *
     * @param schema Row's schema
     * @param str String to parse
     * @param delimiter Delimiter of the string
     * @return A new Row
     * @throws PrestoException If the length of the split string is not equal to the length of the schema
     * @throws PrestoException If the schema contains an unsupported type
     */
    public static Row fromString(RowSchema schema, String str, char delimiter)
    {
        Row row = new Row();

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<String> fields = builder.addAll(Splitter.on(delimiter).split(str)).build();

        if (fields.size() != schema.getLength()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Number of split tokens is not equal to schema length. Expected %s received %s. Schema: %s, fields {%s}, delimiter %s", schema.getLength(), fields.size(), schema, StringUtils.join(fields, ","), delimiter));
        }

        for (int i = 0; i < fields.size(); ++i) {
            Type type = schema.getColumn(i).getType();
            row.addField(valueFromString(fields.get(i), type), type);
        }

        return row;
    }

    /**
     * Converts the given String into a Java object based on the given Presto type
     *
     * @param str String to convert
     * @param type Presto Type
     * @return Java object
     * @throws PrestoException If the type is not supported by this function
     */
    public static Object valueFromString(String str, Type type)
    {
        if (str == null || str.isEmpty()) {
            return null;
        }
        else if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            ImmutableList.Builder<Object> listBuilder = ImmutableList.builder();
            for (String element : Splitter.on(',').split(str)) {
                listBuilder.add(valueFromString(element, elementType));
            }
            return AccumuloRowSerializer.getBlockFromArray(elementType, listBuilder.build());
        }
        else if (Types.isMapType(type)) {
            Type keyType = Types.getKeyType(type);
            Type valueType = Types.getValueType(type);
            ImmutableMap.Builder<Object, Object> mapBuilder = ImmutableMap.builder();
            for (String element : Splitter.on(',').split(str)) {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                List<String> keyValue = builder.addAll(Splitter.on("->").split(element)).build();
                checkArgument(keyValue.size() == 2, format("Map element %s has %d entries, not 2", element, keyValue.size()));

                mapBuilder.put(valueFromString(keyValue.get(0), keyType), valueFromString(keyValue.get(1), valueType));
            }
            return AccumuloRowSerializer.getBlockFromMap(type, mapBuilder.build());
        }
        else if (type.equals(BIGINT)) {
            return Long.parseLong(str);
        }
        else if (type.equals(BOOLEAN)) {
            return Boolean.parseBoolean(str);
        }
        else if (type.equals(DATE)) {
            return new Date(TimeUnit.MILLISECONDS.toDays(DATE_PARSER.parseDateTime(str).getMillis()));
        }
        else if (type.equals(DOUBLE)) {
            return Double.parseDouble(str);
        }
        else if (type.equals(INTEGER)) {
            return Integer.parseInt(str);
        }
        else if (type.equals(REAL)) {
            return Float.parseFloat(str);
        }
        else if (type.equals(SMALLINT)) {
            return Short.parseShort(str);
        }
        else if (type.equals(TIME)) {
            return new Time(TIME_PARSER.parseDateTime(str).getMillis());
        }
        else if (type.equals(TIMESTAMP)) {
            return new Timestamp(TIMESTAMP_PARSER.parseDateTime(str).getMillis());
        }
        else if (type.equals(TINYINT)) {
            return Byte.valueOf(str);
        }
        else if (type.equals(VARBINARY)) {
            return str.getBytes(UTF_8);
        }
        else if (type instanceof VarcharType) {
            return str;
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + type);
        }
    }
}
