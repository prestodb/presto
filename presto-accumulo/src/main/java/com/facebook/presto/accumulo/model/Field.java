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
import static java.lang.Enum.valueOf;
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
     * @param v Java object that matches the
     * @param t Presto type
     * @throws PrestoException If the given value is not or cannot be converted to the given Presto type
     */
    public Field(Object v, Type t)
    {
        this(v, t, false);
    }

    /**
     * Constructs a new instance of {@link Field} with the given value and Presto type.
     *
     * @param v Java object that matches the
     * @param t Presto type
     * @param indexed True if this column is indexed, false otherwise
     * @throws PrestoException If the given value is not or cannot be converted to the given Presto type
     */
    public Field(Object v, Type t, boolean indexed)
    {
        this.value = cleanObject(v, t);
        this.type = requireNonNull(t, "type is null");
        this.indexed = indexed;
    }

    /**
     * Constructs a new {@link Field} from the given Field via shallow copy
     *
     * @param f Field to copy
     */
    public Field(Field f)
    {
        this.type = f.type;

        if (Types.isArrayType(this.type) || Types.isMapType(this.type)) {
            this.value = f.value;
            return;
        }

        if (type.equals(BIGINT)) {
            this.value = f.getLong();
        }
        else if (type.equals(BOOLEAN)) {
            this.value = f.getBoolean();
        }
        else if (type.equals(DATE)) {
            this.value = new Date(f.getDate().getTime());
        }
        else if (type.equals(DOUBLE)) {
            this.value = f.getDouble();
        }
        else if (type.equals(FLOAT)) {
            this.value = f.getFloat();
        }
        else if (type.equals(INTEGER)) {
            this.value = f.getInt();
        }
        else if (type.equals(SMALLINT)) {
            this.value = f.getShort();
        }
        else if (type.equals(TIME)) {
            this.value = new Time(f.getTime().getTime());
        }
        else if (type.equals(TIMESTAMP)) {
            this.value = new Timestamp(f.getTimestamp().getTime());
        }
        else if (type.equals(TINYINT)) {
            this.value = f.getByte();
        }
        else if (type.equals(VARBINARY)) {
            this.value = Arrays.copyOf(f.getVarbinary(), f.getVarbinary().length);
        }
        else if (type.equals(VARCHAR)) {
            this.value = f.getVarchar();
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
            Field f = (Field) obj;
            if (type.equals(f.getType())) {
                if (this.isNull() && f.isNull()) {
                    retval = true;
                }
                else if (this.isNull() ^ f.isNull()) {
                    retval = false;
                }
                else if (type.equals(VARBINARY)) {
                    // special case for byte arrays
                    // aren't they so fancy
                    retval = Arrays.equals((byte[]) value, (byte[]) f.getObject());
                }
                else if (type.equals(DATE) || type.equals(TIME)
                        || type.equals(TIMESTAMP)) {
                    retval = value.toString().equals(f.getObject().toString());
                }
                else {
                    if (value instanceof Block) {
                        retval = equals((Block) value, (Block) f.getObject());
                    }
                    else {
                        retval = value.equals(f.getObject());
                    }
                }
            }
        }
        return retval;
    }

    private boolean equals(Block b1, Block b2)
    {
        boolean retval = b1.getPositionCount() == b2.getPositionCount();
        for (int i = 0; i < b1.getPositionCount() && retval; ++i) {
            if (b1 instanceof ArrayBlock && b2 instanceof ArrayBlock) {
                retval = equals(b1.getObject(i, Block.class), b2.getObject(i, Block.class));
            }
            else {
                retval = b1.compareTo(i, 0, b1.getLength(i), b2, i, 0, b2.getLength(i)) == 0;
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
            Type et = Types.getElementType(type);
            StringBuilder bldr = new StringBuilder("ARRAY [");
            for (Object f : AccumuloRowSerializer.getArrayFromBlock(et, this.getArray())) {
                if (Types.isArrayType(et)) {
                    Type eet = Types.getElementType(et);
                    bldr.append(new Field(AccumuloRowSerializer.getBlockFromArray(eet, (List<?>) f),
                            et)).append(',');
                }
                else if (Types.isMapType(et)) {
                    bldr.append(
                            new Field(AccumuloRowSerializer.getBlockFromMap(et, (Map<?, ?>) f), et))
                            .append(',');
                }
                else {
                    bldr.append(new Field(f, et)).append(',');
                }
            }

            return bldr.deleteCharAt(bldr.length() - 1).append("]").toString();
        }

        if (Types.isMapType(type)) {
            StringBuilder bldr = new StringBuilder("MAP(");
            StringBuilder keys = new StringBuilder("ARRAY [");
            StringBuilder values = new StringBuilder("ARRAY [");
            for (Entry<Object, Object> e : AccumuloRowSerializer
                    .getMapFromBlock(type, this.getMap()).entrySet()) {
                Type kt = Types.getKeyType(type);
                if (Types.isArrayType(kt)) {
                    keys.append(new Field(AccumuloRowSerializer
                            .getBlockFromArray(Types.getElementType(kt), (List<?>) e.getKey()), kt))
                            .append(',');
                }
                else if (Types.isMapType(kt)) {
                    keys.append(new Field(
                            AccumuloRowSerializer.getBlockFromMap(kt, (Map<?, ?>) e.getKey()), kt))
                            .append(',');
                }
                else {
                    keys.append(new Field(e.getKey(), kt)).append(',');
                }

                Type vt = Types.getValueType(type);
                if (Types.isArrayType(vt)) {
                    values.append(new Field(AccumuloRowSerializer.getBlockFromArray(
                            Types.getElementType(vt), (List<?>) e.getValue()), vt)).append(',');
                }
                else if (Types.isMapType(vt)) {
                    values.append(new Field(
                            AccumuloRowSerializer.getBlockFromMap(vt, (Map<?, ?>) e.getValue()),
                            vt)).append(',');
                }
                else {
                    values.append(new Field(e.getValue(), vt)).append(',');
                }
            }

            keys.deleteCharAt(keys.length() - 1).append(']');
            values.deleteCharAt(values.length() - 1).append(']');
            return bldr.append(keys).append(", ").append(values).append(")").toString();
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
     * @param v Object to convert
     * @param t Destination Presto type
     * @return Null if null, the converted type of it could convert it, or the same value if it is
     * fine just the way it is :D
     * @throws PrestoException If the given object is not any flavor of the given type
     */
    private Object cleanObject(Object v, Type t)
    {
        if (v == null) {
            return null;
        }

        // Array? Better be a block!
        if (Types.isArrayType(t)) {
            if (!(v instanceof Block)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Block, but " + v.getClass());
            }
            return v;
        }

        // Map? Better be a block!
        if (Types.isMapType(t)) {
            if (!(v instanceof Block)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Block, but " + v.getClass());
            }
            return v;
        }

        // And now for the plain types
        if (t.equals(BIGINT)) {
            if (!(v instanceof Long)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long, but " + v.getClass());
            }
        }
        else if (t.equals(INTEGER)) {
            if (v instanceof Long) {
                return ((Long) v).intValue();
            }

            if (!(v instanceof Integer)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Integer, but " + v.getClass());
            }
        }
        else if (t.equals(BOOLEAN)) {
            if (!(v instanceof Boolean)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Boolean, but " + v.getClass());
            }
            return v;
        }
        else if (t.equals(DATE)) {
            if (v instanceof Long) {
                return new Date((Long) v);
            }

            if (v instanceof Calendar) {
                return new Date(((Calendar) v).getTime().getTime());
            }

            if (!(v instanceof Date)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Calendar, Date, or Long, but " + v.getClass());
            }
        }
        else if (t.equals(DOUBLE)) {
            if (!(v instanceof Double)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Double, but " + v.getClass());
            }
        }
        else if (t.equals(FLOAT)) {
            if (v instanceof Long) {
                return Float.intBitsToFloat(((Long) v).intValue());
            }

            if (v instanceof Integer) {
                return Float.intBitsToFloat((Integer) v);
            }

            if (!(v instanceof Float)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Float, but " + v.getClass());
            }
        }
        else if (t.equals(SMALLINT)) {
            if (v instanceof Long) {
                return ((Long) v).shortValue();
            }

            if (v instanceof Integer) {
                return ((Integer) v).shortValue();
            }

            if (!(v instanceof Short)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Short, but " + v.getClass());
            }
        }
        else if (t.equals(TIME)) {
            if (v instanceof Long) {
                return new Time((Long) v);
            }

            if (!(v instanceof Time)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Time, but " + v.getClass());
            }
        }
        else if (t.equals(TIMESTAMP)) {
            if (v instanceof Long) {
                return new Timestamp((Long) v);
            }

            if (!(v instanceof Timestamp)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Timestamp, but " + v.getClass());
            }
        }
        else if (t.equals(TINYINT)) {
            if (v instanceof Long) {
                return ((Long) v).byteValue();
            }

            if (v instanceof Integer) {
                return ((Integer) v).byteValue();
            }

            if (v instanceof Short) {
                return ((Short) v).byteValue();
            }

            if (!(v instanceof Byte)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Byte, but " + v.getClass());
            }
        }
        else if (t.equals(VARBINARY)) {
            if (v instanceof Slice) {
                return ((Slice) v).getBytes();
            }

            if (!(v instanceof byte[])) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Slice byte[], but " + v.getClass());
            }
        }
        else if (t instanceof VarcharType) {
            if (v instanceof Slice) {
                return new String(((Slice) v).getBytes());
            }

            if (!(v instanceof String)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Slice or String, but " + v.getClass());
            }
        }
        else {
            throw new PrestoException(INTERNAL_ERROR,
                    "Unsupported PrestoType " + t);
        }

        return v;
    }
}
