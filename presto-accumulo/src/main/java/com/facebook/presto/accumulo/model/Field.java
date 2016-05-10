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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
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
        this.type = t;
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

        switch (type.getDisplayName()) {
            case StandardTypes.BIGINT:
                this.value = Long.valueOf(f.getBigInt());
                break;
            case StandardTypes.BOOLEAN:
                this.value = new Boolean(f.getBoolean());
                break;
            case StandardTypes.DATE:
                this.value = new Date(f.getDate().getTime());
                break;
            case StandardTypes.DOUBLE:
                this.value = Double.valueOf(f.getDouble());
                break;
            case StandardTypes.TIME:
                this.value = new Time(f.getTime().getTime());
                break;
            case StandardTypes.TIMESTAMP:
                this.value = new Timestamp(f.getTimestamp().getTime());
                break;
            case StandardTypes.VARBINARY:
                this.value = Arrays.copyOf(f.getVarbinary(), f.getVarbinary().length);
                break;
            case StandardTypes.VARCHAR:
                this.value = new String(f.getVarchar());
                break;
            default:
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
    public Long getBigInt()
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
     * Gets the value of the field as a Double. For INTEGER types
     *
     * @return Value as Integer
     */
    public Integer getInt()
    {
        return (Integer) value;
    }

    /**
     * Throws an exciting exception
     *
     * @return Maybe the interval date to second, someday
     * @throws UnsupportedOperationException Just because
     */
    public Object getIntervalDateToSecond()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws an exciting exception
     *
     * @return Maybe the interval year to month, someday
     * @throws UnsupportedOperationException Just because
     */
    public Object getIntervalYearToMonth()
    {
        throw new UnsupportedOperationException();
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
     * Gets the value of the field as a Timestamp. For TIMESTAMP types
     *
     * @return Value as Timestamp
     */
    public Timestamp getTimestamp()
    {
        return (Timestamp) value;
    }

    /**
     * Throws an exciting exception
     *
     * @return Maybe the time stamp with time zone, someday
     * @throws UnsupportedOperationException Just because
     */
    public Object getTimestampWithTimeZone()
    {
        throw new UnsupportedOperationException();
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
     * Throws an exciting exception
     *
     * @return Maybe the time with time zone, someday
     * @throws UnsupportedOperationException Just because
     */
    public Object getTimeWithTimeZone()
    {
        throw new UnsupportedOperationException();
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
                else if (type.equals(VarbinaryType.VARBINARY)) {
                    // special case for byte arrays
                    // aren't they so fancy
                    retval = Arrays.equals((byte[]) value, (byte[]) f.getObject());
                }
                else if (type.equals(DateType.DATE) || type.equals(TimeType.TIME)
                        || type.equals(TimestampType.TIMESTAMP)) {
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
        if (type instanceof BigintType || type instanceof BooleanType || type instanceof DoubleType) {
            return value.toString();
        }
        else if (type instanceof DateType) {
            return "DATE '" + ((Date) value).toString() + "'";
        }
        else if (type instanceof TimeType) {
            return "TIME '" + ((Time) value).toString() + "'";
        }
        else if (type instanceof TimestampType) {
            return "TIMESTAMP '" + ((Timestamp) value).toString() + "'";
        }
        else if (type instanceof VarbinaryType) {
            return "CAST('" + new String((byte[]) value) + "' AS VARBINARY)";
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
            return v;
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
        if (t instanceof BigintType) {
            if (!(v instanceof Long)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long, but " + v.getClass());
            }
        }
        else if (t instanceof IntegerType) {
            if (v instanceof Long) {
                return ((Long) v).intValue();
            }

            if (!(v instanceof Integer)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Integer, but " + v.getClass());
            }
        }
        else if (t instanceof BooleanType) {
            if (!(v instanceof Boolean)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Boolean, but " + v.getClass());
            }
            return new Boolean((boolean) v);
        }
        else if (t instanceof DateType) {
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
        else if (t instanceof DoubleType) {
            if (!(v instanceof Double)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Double, but " + v.getClass());
            }
        }
        else if (t instanceof TimeType) {
            if (v instanceof Long) {
                return new Time((Long) v);
            }

            if (!(v instanceof Time)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Time, but " + v.getClass());
            }
        }
        else if (t instanceof TimestampType) {
            if (v instanceof Long) {
                return new Timestamp((Long) v);
            }

            if (!(v instanceof Timestamp)) {
                throw new PrestoException(INTERNAL_ERROR,
                        "Object is not a Long or Timestamp, but " + v.getClass());
            }
        }
        else if (t instanceof VarbinaryType) {
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
