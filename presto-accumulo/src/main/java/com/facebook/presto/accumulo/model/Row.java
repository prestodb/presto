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

import com.facebook.presto.accumulo.io.AccumuloPageSink;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.commons.lang.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.accumulo.AccumuloErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Class to contain an entire Presto row, made up of {@link Field} objects.
 * <p>
 * Used by {@link AccumuloPageSink} for writing data as well as the
 * test cases.
 */
public class Row
{
    private List<Field> fields = new ArrayList<>();

    /**
     * Creates a new instance of {@link Row}.
     */
    public Row()
    {}

    /**
     * Copy constructor from one Row to another
     *
     * @param row Row, copied
     */
    public Row(Row row)
    {
        requireNonNull(row, "row is null");
        for (Field field : row.fields) {
            fields.add(new Field(field));
        }
    }

    /**
     * Appends the given field to the end of the row
     *
     * @param field Field to append
     * @return this, for fluent programming
     */
    public Row addField(Field field)
    {
        requireNonNull(field, "field is null");
        fields.add(field);
        return this;
    }

    /**
     * Appends the a new {@link Field} of the given object and type to the end of the row
     *
     * @param value Value of the field
     * @param type Type of the field
     * @return this, for fluent programming
     */
    public Row addField(Object value, Type type)
    {
        requireNonNull(type, "type is null");
        fields.add(new Field(value, type));
        return this;
    }

    /**
     * Gets the field at the given index
     *
     * @param i Index in the row to retrieve
     * @return Field
     * @throws IndexOutOfBoundsException If the index is out of bounds
     */
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

    /**
     * Gets the length of the row, i.e. number of fields
     *
     * @return Length
     */
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
        return !(obj == null || !(obj instanceof Row))
                && Objects.equals(this.fields, ((Row) obj).getFields());
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
     * Creates a new {@link Row} from the given delimited string based on the given
     * {@link RowSchema}. Only supports plain Presto types
     *
     * @param schema Row's schema
     * @param str String to parse
     * @param delimiter Delimiter of the string
     * @return A new Row
     * @throws PrestoException If the length of the split string is not equal to the length of the
     * @throws PrestoException If the schema contains an unsupported type
     */
    public static Row fromString(RowSchema schema, String str, char delimiter)
    {
        Row row = new Row();

        String[] fields = StringUtils.split(str, delimiter);

        if (fields.length != schema.getLength()) {
            throw new PrestoException(VALIDATION,
                    String.format(
                            "Number of split tokens is not equal to schema length.  "
                                    + "Expected %s received %s. Schema: %s, fields {%s}, delimiter %s",
                            schema.getLength(), fields.length, schema,
                            StringUtils.join(fields, ","), delimiter));
        }

        for (int i = 0; i < fields.length; ++i) {
            Type type = schema.getColumn(i).getType();

            if (type instanceof BigintType) {
                row.addField(Long.parseLong(fields[i]), BIGINT);
            }
            else if (type instanceof BooleanType) {
                row.addField(Boolean.parseBoolean(fields[i]), BOOLEAN);
            }
            else if (type instanceof DateType) {
                row.addField(
                        new Date(TimeUnit.MILLISECONDS.toDays(Date.valueOf(fields[i]).getTime())),
                        DATE);
            }
            else if (type instanceof DoubleType) {
                row.addField(Double.parseDouble(fields[i]), DOUBLE);
            }
            else if (type instanceof TimeType) {
                row.addField(Time.valueOf(fields[i]), TIME);
            }
            else if (type instanceof TimestampType) {
                row.addField(Timestamp.valueOf(fields[i]), TIMESTAMP);
            }
            else if (type instanceof VarbinaryType) {
                row.addField(fields[i].getBytes(), VARBINARY);
            }
            else if (type instanceof VarcharType) {
                row.addField(fields[i], VARCHAR);
            }
            else {
                throw new PrestoException(NOT_SUPPORTED,
                        "Unsupported type " + type);
            }
        }

        return row;
    }
}
