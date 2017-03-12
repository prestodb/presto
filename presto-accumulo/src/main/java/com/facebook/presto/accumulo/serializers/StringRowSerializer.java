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
package com.facebook.presto.accumulo.serializers;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
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
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link StringRowSerializer} that encodes and decodes Presto column values as human-readable String objects.
 */
public class StringRowSerializer
        implements AccumuloRowSerializer
{
    private final Map<String, Map<String, String>> familyQualifierColumnMap = new HashMap<>();
    private final Map<String, Object> columnValues = new HashMap<>();
    private final Text rowId = new Text();
    private final Text family = new Text();
    private final Text qualifier = new Text();
    private final Text value = new Text();

    private boolean rowOnly = false;
    private String rowIdName;

    @Override
    public void setRowIdName(String name)
    {
        this.rowIdName = name;
    }

    @Override
    public void setRowOnly(boolean rowOnly)
    {
        this.rowOnly = rowOnly;
    }

    @Override
    public void setMapping(String name, String family, String qualifier)
    {
        columnValues.put(name, null);
        Map<String, String> qualifierColumnMap = familyQualifierColumnMap.get(family);
        if (qualifierColumnMap == null) {
            qualifierColumnMap = new HashMap<>();
            familyQualifierColumnMap.put(family, qualifierColumnMap);
        }

        qualifierColumnMap.put(qualifier, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    @Override
    public void deserialize(Entry<Key, Value> entry)
            throws IOException
    {
        if (!columnValues.containsKey(rowIdName)) {
            entry.getKey().getRow(rowId);
            columnValues.put(rowIdName, rowId.toString());
        }

        if (rowOnly) {
            return;
        }

        entry.getKey().getColumnFamily(family);
        entry.getKey().getColumnQualifier(qualifier);

        if (family.equals(ROW_ID_COLUMN) && qualifier.equals(ROW_ID_COLUMN)) {
            return;
        }

        value.set(entry.getValue().get());
        columnValues.put(familyQualifierColumnMap.get(family.toString()).get(qualifier.toString()), value.toString());
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null;
    }

    @Override
    public Block getArray(String name, Type type)
    {
        throw new PrestoException(NOT_SUPPORTED, "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setArray(Text text, Type type, Block block)
    {
        throw new PrestoException(NOT_SUPPORTED, "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public boolean getBoolean(String name)
    {
        return Boolean.parseBoolean(getFieldValue(name));
    }

    @Override
    public void setBoolean(Text text, Boolean value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public byte getByte(String name)
    {
        return Byte.parseByte(getFieldValue(name));
    }

    @Override
    public void setByte(Text text, Byte value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public Date getDate(String name)
    {
        return new Date(DAYS.toMillis(Long.parseLong(getFieldValue(name))));
    }

    @Override
    public void setDate(Text text, Date value)
    {
        text.set(Long.toString(MILLISECONDS.toDays(value.getTime())).getBytes(UTF_8));
    }

    @Override
    public double getDouble(String name)
    {
        return Double.parseDouble(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public float getFloat(String name)
    {
        return Float.parseFloat(getFieldValue(name));
    }

    @Override
    public void setFloat(Text text, Float value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public int getInt(String name)
    {
        return Integer.parseInt(getFieldValue(name));
    }

    @Override
    public void setInt(Text text, Integer value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public long getLong(String name)
    {
        return Long.parseLong(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public Block getMap(String name, Type type)
    {
        throw new PrestoException(NOT_SUPPORTED, "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setMap(Text text, Type type, Block block)
    {
        throw new PrestoException(NOT_SUPPORTED, "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public short getShort(String name)
    {
        return Short.parseShort(getFieldValue(name));
    }

    @Override
    public void setShort(Text text, Short value)
    {
        text.set(value.toString().getBytes(UTF_8));
    }

    @Override
    public Time getTime(String name)
    {
        return new Time(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value)
    {
        text.set(Long.toString(value.getTime()).getBytes(UTF_8));
    }

    @Override
    public Timestamp getTimestamp(String name)
    {
        return new Timestamp(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value)
    {
        text.set(Long.toString(value.getTime()).getBytes(UTF_8));
    }

    @Override
    public byte[] getVarbinary(String name)
    {
        return getFieldValue(name).getBytes(UTF_8);
    }

    @Override
    public void setVarbinary(Text text, byte[] value)
    {
        text.set(value);
    }

    @Override
    public String getVarchar(String name)
    {
        return getFieldValue(name);
    }

    @Override
    public void setVarchar(Text text, String value)
    {
        text.set(value.getBytes(UTF_8));
    }

    private String getFieldValue(String name)
    {
        return columnValues.get(name).toString();
    }

    @Override
    public byte[] encode(Type type, Object value)
    {
        Text text = new Text();
        if (Types.isArrayType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "arrays are not (yet?) supported for StringRowSerializer");
        }
        else if (Types.isMapType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "maps are not (yet?) supported for StringRowSerializer");
        }
        else if (type.equals(BIGINT) && value instanceof Integer) {
            setLong(text, ((Integer) value).longValue());
        }
        else if (type.equals(BIGINT) && value instanceof Long) {
            setLong(text, (Long) value);
        }
        else if (type.equals(BOOLEAN)) {
            setBoolean(text, value.equals(Boolean.TRUE));
        }
        else if (type.equals(DATE)) {
            setDate(text, (Date) value);
        }
        else if (type.equals(DOUBLE)) {
            setDouble(text, (Double) value);
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            setInt(text, (Integer) value);
        }
        else if (type.equals(INTEGER) && value instanceof Long) {
            setInt(text, ((Long) value).intValue());
        }
        else if (type.equals(REAL)) {
            setFloat(text, (Float) value);
        }
        else if (type.equals(SMALLINT)) {
            setShort(text, (Short) value);
        }
        else if (type.equals(TIME)) {
            setTime(text, (Time) value);
        }
        else if (type.equals(TIMESTAMP)) {
            setTimestamp(text, (Timestamp) value);
        }
        else if (type.equals(TINYINT)) {
            setByte(text, (Byte) value);
        }
        else if (type.equals(VARBINARY) && value instanceof byte[]) {
            setVarbinary(text, (byte[]) value);
        }
        else if (type.equals(VARBINARY) && value instanceof Slice) {
            setVarbinary(text, ((Slice) value).getBytes());
        }
        else if (type.equals(VARCHAR) && value instanceof String) {
            setVarchar(text, ((String) value));
        }
        else if (type.equals(VARCHAR) && value instanceof Slice) {
            setVarchar(text, ((Slice) value).toStringUtf8());
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("StringLexicoder does not support encoding type %s, object class is %s", type, value.getClass()));
        }

        return text.copyBytes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(Type type, byte[] value)
    {
        String strValue = new String(value);
        if (Types.isArrayType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "arrays are not (yet?) supported for StringRowSerializer");
        }
        else if (Types.isMapType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "maps are not (yet?) supported for StringRowSerializer");
        }
        else if (type.equals(BIGINT)) {
            return (T) (Long) Long.parseLong(strValue);
        }
        else if (type.equals(BOOLEAN)) {
            return (T) (Boolean) Boolean.parseBoolean(strValue);
        }
        else if (type.equals(DATE)) {
            return (T) (Long) Long.parseLong(strValue);
        }
        else if (type.equals(DOUBLE)) {
            return (T) (Double) Double.parseDouble(strValue);
        }
        else if (type.equals(INTEGER)) {
            return (T) (Long) ((Integer) Integer.parseInt(strValue)).longValue();
        }
        else if (type.equals(REAL)) {
            return (T) (Double) ((Float) Float.parseFloat(strValue)).doubleValue();
        }
        else if (type.equals(SMALLINT)) {
            return (T) (Long) ((Short) Short.parseShort(strValue)).longValue();
        }
        else if (type.equals(TIME)) {
            return (T) (Long) Long.parseLong(strValue);
        }
        else if (type.equals(TIMESTAMP)) {
            return (T) (Long) Long.parseLong(strValue);
        }
        else if (type.equals(TINYINT)) {
            return (T) (Long) ((Byte) Byte.parseByte(strValue)).longValue();
        }
        else if (type.equals(VARBINARY)) {
            return (T) value;
        }
        else if (type.equals(VARCHAR)) {
            return (T) new String(value);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "StringLexicoder does not support decoding type " + type);
        }
    }
}
