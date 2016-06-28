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
import com.facebook.presto.accumulo.io.AccumuloPageSink;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Class to contain a single field within a Presto {@link Row}.
 * <p>
 * Used by {@link AccumuloPageSink} for writing data as well as the
 * test cases.
 */
public class Field
{
    private Object value;
    private Type type;
    private boolean indexed;

    /**
     * Constructs a new instance of {@link Field} with the given value and Presto type. Default is
     * not indexed.
     *
     * @param value Java object that matches the
     * @param type Presto type
     * @throws PrestoException If the given value is not or cannot be converted to the given Presto type
     */
    public Field(Object value, Type type)
    {
        this(value, type, false);
    }

    /**
     * Constructs a new instance of {@link Field} with the given value and Presto type.
     *
     * @param value Java object that matches the
     * @param type Presto type
     * @param indexed True if this column is indexed, false otherwise
     * @throws PrestoException If the given value is not or cannot be converted to the given Presto type
     */
    public Field(Object value, Type type, boolean indexed)
    {
        this.value = cleanObject(value, type);
        this.type = requireNonNull(type, "type is null");
        this.indexed = indexed;
    }

    /**
     * Constructs a new {@link Field} from the given Field via shallow copy
     *
     * @param field Field to copy
     */
    public Field(Field field)
    {
        this.type = field.type;

        if (Types.isArrayType(this.type) || Types.isMapType(this.type)) {
            this.value = field.value;
            return;
        }

        if (type.equals(BIGINT)) {
            this.value = field.getLong();
        }
        else if (type.equals(BOOLEAN)) {
            this.value = field.getBoolean();
        }
        else if (type.equals(DATE)) {
            this.value = new Date(field.getDate().getTime());
        }
        else if (type.equals(DOUBLE)) {
            this.value = field.getDouble();
        }
        else if (type.equals(FLOAT)) {
            this.value = field.getFloat();
        }
        else if (type.equals(INTEGER)) {
            this.value = field.getInt();
        }
        else if (type.equals(SMALLINT)) {
            this.value = field.getShort();
        }
        else if (type.equals(TIME)) {
            this.value = new Time(field.getTime().getTime());
        }
        else if (type.equals(TIMESTAMP)) {
            this.value = new Timestamp(field.getTimestamp().getTime());
        }
        else if (type.equals(TINYINT)) {
            this.value = field.getByte();
        }
        else if (type.equals(VARBINARY)) {
            this.value = Arrays.copyOf(field.getVarbinary(), field.getVarbinary().length);
        }
        else if (type.equals(VARCHAR)) {
            this.value = field.getVarchar();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED,
                    "Unsupported type " + type);
        }
    }

    /**
     * Gets the Presto type
     *
     * @return Type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * Gets the value of the field as a Block. For array types.
     *
     * @return Value as Block
     * @see AccumuloRowSerializer#getArrayFromBlock
     */
    public Block getArray()
    {
        return (Block) value;
    }

    /**
     * Gets the value of the field as a Long. For BIGINT types
     *
     * @return Value as Long
     */
    public Long getLong()
    {
        return (Long) value;
    }

    /**
     * Gets the value of the field as a Boolean. For BOOLEAN types
     *
     * @return Value as Boolean
     */
    public Boolean getBoolean()
    {
        return (Boolean) value;
    }

    /**
     * Gets the value of the field as a Byte. For TINYINT types
     *
     * @return Value as Byte
     */
    public Byte getByte()
    {
        return (Byte) value;
    }

    /**
     * Gets the value of the field as a Date. For DATE types
     *
     * @return Value as Date
     */
    public Date getDate()
    {
        return (Date) value;
    }

    /**
     * Gets the value of the field as a Double. For DOUBLE types
     *
     * @return Value as Double
     */
    public Double getDouble()
    {
        return (Double) value;
    }

    /**
     * Gets the value of the field as a Float. For FLOAT types
     *
     * @return Value as Float
     */
    public Float getFloat()
    {
        return (Float) value;
    }

    /**
     * Gets the value of the field as a Double. For INTEGER types
     *
     * @return Value as Integer
     */
    public Integer getInt()
    {
        return (Integer) value;
    }

    /**
     * Gets the value of the field as a Block. For map types.
     *
     * @return Value as Block
     * @see AccumuloRowSerializer#getMapFromBlock
     */
    public Block getMap()
    {
        return (Block) value;
    }

    /**
     * Gets the value of the field
     *
     * @return Value as Object
     */
    public Object getObject()
    {
        return value;
    }

    /**
     * Gets the value of the field as a Short. For SMALLINT types
     *
     * @return Value as Short
     */
    public Short getShort()
    {
        return (Short) value;
    }

    /**
     * Gets the value of the field as a Timestamp. For TIMESTAMP types
     *
     * @return Value as Timestamp
     */
    public Timestamp getTimestamp()
    {
        return (Timestamp) value;
    }

    /**
     * Gets the value of the field as a Time. For TIME types
     *
     * @return Value as Time
     */
    public Time getTime()
    {
        return (Time) value;
    }

    /**
     * Gets the value of the field as a byte array. For VARBINARY types
     *
     * @return Value as byte[]
     */
    public byte[] getVarbinary()
    {
        return (byte[]) value;
    }

    /**
     * Gets the value of the field as a String. For VARCHAR types
     *
     * @return Value as String
     */
    public String getVarchar()
    {
        return (String) value;
    }

    /**
     * Gets a Boolean value indicating whether or not this field is indexed
     *
     * @return True if indexed, false otherwise
     */
    public boolean isIndexed()
    {
        return indexed;
    }

    /**
     * Gets a Boolean value indicating whether or not this field is null
     *
     * @return True if null, false otherwise
     */
    public boolean isNull()
    {
        return value == null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type, indexed);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean retval = true;
        if (obj instanceof Field) {
            Field field = (Field) obj;
            if (type.equals(field.getType())) {
                if (this.isNull() && field.isNull()) {
                    retval = true;
                }
                else if (this.isNull() ^ field.isNull()) {
                    retval = false;
                }
                else if (type.equals(VARBINARY)) {
                    // special case for byte arrays
                    // aren't they so fancy
                    retval = Arrays.equals((byte[]) value, (byte[]) field.getObject());
                }
                else if (type.equals(DATE) || type.equals(TIME)
                        || type.equals(TIMESTAMP)) {
                    retval = value.toString().equals(field.getObject().toString());
                }
                else {
                    if (value instanceof Block) {
                        retval = equals((Block) value, (Block) field.getObject());
                    }
                    else {
                        retval = value.equals(field.getObject());
                    }
                }
            }
        }
        return retval;
    }

    private boolean equals(Block block1, Block block2)
    {
        boolean retval = block1.getPositionCount() == block2.getPositionCount();
        for (int i = 0; i < block1.getPositionCount() && retval; ++i) {
            if (block1 instanceof ArrayBlock && block2 instanceof ArrayBlock) {
                retval = equals(block1.getObject(i, Block.class), block2.getObject(i, Block.class));
            }
            else {
                retval = block1.compareTo(i, 0, block1.getLength(i), block2, i, 0, block2.getLength(i)) == 0;
            }
        }
        return retval;
    }

    @Override
    public String toString()
    {
        if (value == null) {
            return "null";
        }

        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            StringBuilder builder = new StringBuilder("ARRAY [");
            for (Object element : AccumuloRowSerializer.getArrayFromBlock(elementType, this.getArray())) {
                if (Types.isArrayType(elementType)) {
                    Type eet = Types.getElementType(elementType);
                    builder.append(new Field(AccumuloRowSerializer.getBlockFromArray(eet, (List<?>) element),
                            elementType)).append(',');
                }
                else if (Types.isMapType(elementType)) {
                    builder.append(
                            new Field(AccumuloRowSerializer.getBlockFromMap(elementType, (Map<?, ?>) element), elementType))
                            .append(',');
                }
                else {
                    builder.append(new Field(element, elementType)).append(',');
                }
            }

            return builder.deleteCharAt(builder.length() - 1).append("]").toString();
        }

        if (Types.isMapType(type)) {
            StringBuilder builder = new StringBuilder("MAP(");
            StringBuilder keys = new StringBuilder("ARRAY [");
            StringBuilder values = new StringBuilder("ARRAY [");
            for (Entry<Object, Object> entry : AccumuloRowSerializer
                    .getMapFromBlock(type, this.getMap()).entrySet()) {
                Type keyType = Types.getKeyType(type);
                if (Types.isArrayType(keyType)) {
                    keys.append(new Field(AccumuloRowSerializer
                            .getBlockFromArray(Types.getElementType(keyType), (List<?>) entry.getKey()), keyType))
                            .append(',');
                }
                else if (Types.isMapType(keyType)) {
                    keys.append(new Field(
                            AccumuloRowSerializer.getBlockFromMap(keyType, (Map<?, ?>) entry.getKey()), keyType))
                            .append(',');
                }
                else {
                    keys.append(new Field(entry.getKey(), keyType)).append(',');
                }

                Type valueType = Types.getValueType(type);
                if (Types.isArrayType(valueType)) {
                    values.append(new Field(AccumuloRowSerializer.getBlockFromArray(
                            Types.getElementType(valueType), (List<?>) entry.getValue()), valueType)).append(',');
                }
                else if (Types.isMapType(valueType)) {
                    values.append(new Field(
                            AccumuloRowSerializer.getBlockFromMap(valueType, (Map<?, ?>) entry.getValue()),
                            valueType)).append(',');
                }
                else {
                    values.append(new Field(entry.getValue(), valueType)).append(',');
                }
            }

            keys.deleteCharAt(keys.length() - 1).append(']');
            values.deleteCharAt(values.length() - 1).append(']');
            return builder.append(keys).append(", ").append(values).append(")").toString();
        }

        // Validate the object is the given type
        if (type.equals(BIGINT) || type.equals(BOOLEAN) || type.equals(DOUBLE) || type.equals(FLOAT) || type.equals(INTEGER) || type.equals(TINYINT) || type.equals(SMALLINT)) {
            return value.toString();
        }
        else if (type.equals(DATE)) {
            return "DATE '" + value.toString() + "'";
        }
        else if (type.equals(TIME)) {
            return "TIME '" + value.toString() + "'";
        }
        else if (type.equals(TIMESTAMP)) {
            return "TIMESTAMP '" + value.toString() + "'";
        }
        else if (type.equals(VARBINARY)) {
            return "CAST('" + new String((byte[]) value, UTF_8).replaceAll("'", "''") + "' AS VARBINARY)";
        }
        else if (type instanceof VarcharType) {
            return "'" + value.toString().replaceAll("'", "''") + "'";
        }
        else {
            throw new PrestoException(NOT_SUPPORTED,
                    "Unsupported PrestoType " + type);
        }
    }

    /**
     * Does it's damnedest job to convert the given object to the given type.
     *
     * @param value Object to convert
     * @param type Destination Presto type
     * @return Null if null, the converted type of it could convert it, or the same value if it is
     * fine just the way it is :D
     * @throws PrestoException If the given object is not any flavor of the given type
     */
    private Object cleanObject(Object value, Type type)
    {
        if (value == null) {
            return null;
        }

        // Array? Better be a block!
        if (Types.isArrayType(type)) {
            if (!(value instanceof Block)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Block, but " + value.getClass());
            }
            return value;
        }

        // Map? Better be a block!
        if (Types.isMapType(type)) {
            if (!(value instanceof Block)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Block, but " + value.getClass());
            }
            return value;
        }

        // And now for the plain types
        if (type.equals(BIGINT)) {
            if (!(value instanceof Long)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long, but " + value.getClass());
            }
        }
        else if (type.equals(INTEGER)) {
            if (value instanceof Long) {
                return ((Long) value).intValue();
            }

            if (!(value instanceof Integer)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Integer, but " + value.getClass());
            }
        }
        else if (type.equals(BOOLEAN)) {
            if (!(value instanceof Boolean)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Boolean, but " + value.getClass());
            }
            return value;
        }
        else if (type.equals(DATE)) {
            if (value instanceof Long) {
                return new Date((Long) value);
            }

            if (value instanceof Calendar) {
                return new Date(((Calendar) value).getTime().getTime());
            }

            if (!(value instanceof Date)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Calendar, Date, or Long, but " + value.getClass());
            }
        }
        else if (type.equals(DOUBLE)) {
            if (!(value instanceof Double)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Double, but " + value.getClass());
            }
        }
        else if (type.equals(FLOAT)) {
            if (value instanceof Long) {
                return Float.intBitsToFloat(((Long) value).intValue());
            }

            if (value instanceof Integer) {
                return Float.intBitsToFloat((Integer) value);
            }

            if (!(value instanceof Float)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Float, but " + value.getClass());
            }
        }
        else if (type.equals(SMALLINT)) {
            if (value instanceof Long) {
                return ((Long) value).shortValue();
            }

            if (value instanceof Integer) {
                return ((Integer) value).shortValue();
            }

            if (!(value instanceof Short)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Short, but " + value.getClass());
            }
        }
        else if (type.equals(TIME)) {
            if (value instanceof Long) {
                return new Time((Long) value);
            }

            if (!(value instanceof Time)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Time, but " + value.getClass());
            }
        }
        else if (type.equals(TIMESTAMP)) {
            if (value instanceof Long) {
                return new Timestamp((Long) value);
            }

            if (!(value instanceof Timestamp)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Timestamp, but " + value.getClass());
            }
        }
        else if (type.equals(TINYINT)) {
            if (value instanceof Long) {
                return ((Long) value).byteValue();
            }

            if (value instanceof Integer) {
                return ((Integer) value).byteValue();
            }

            if (value instanceof Short) {
                return ((Short) value).byteValue();
            }

            if (!(value instanceof Byte)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Byte, but " + value.getClass());
            }
        }
        else if (type.equals(VARBINARY)) {
            if (value instanceof Slice) {
                return ((Slice) value).getBytes();
            }

            if (!(value instanceof byte[])) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Slice byte[], but " + value.getClass());
            }
        }
        else if (type instanceof VarcharType) {
            if (value instanceof Slice) {
                return new String(((Slice) value).getBytes(), UTF_8);
            }

            if (!(value instanceof String)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Slice or String, but " + value.getClass());
            }
        }
        else {
            throw new PrestoException(INTERNAL_ERROR,
                    "Unsupported PrestoType " + type);
        }

        return value;
    }
}
