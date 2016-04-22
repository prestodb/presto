/*
 * Copyright 2016 Bloomberg L.P.
 *
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

import static com.facebook.presto.accumulo.AccumuloErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

/**
 * Implementation of {@link StringRowSerializer} that encodes and decodes Presto column values as
 * human-readable String objects.
 */
public class StringRowSerializer
        implements AccumuloRowSerializer
{
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, Object> columnValues = new HashMap<>();
    private Text rowId = new Text();
    private Text cf = new Text();
    private Text cq = new Text();
    private Text value = new Text();
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
    public void setMapping(String name, String fam, String qual)
    {
        columnValues.put(name, null);
        Map<String, String> q2pc = f2q2pc.get(fam);
        if (q2pc == null) {
            q2pc = new HashMap<>();
            f2q2pc.put(fam, q2pc);
        }

        q2pc.put(qual, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    @Override
    public void deserialize(Entry<Key, Value> kvp)
            throws IOException
    {
        if (!columnValues.containsKey(rowIdName)) {
            kvp.getKey().getRow(rowId);
            columnValues.put(rowIdName, rowId.toString());
        }

        if (rowOnly) {
            return;
        }

        kvp.getKey().getColumnFamily(cf);
        kvp.getKey().getColumnQualifier(cq);
        value.set(kvp.getValue().get());
        columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()), value.toString());
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null;
    }

    @Override
    public Block getArray(String name, Type type)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setArray(Text text, Type type, Block block)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public boolean getBoolean(String name)
    {
        return Boolean.parseBoolean(getFieldValue(name));
    }

    @Override
    public void setBoolean(Text text, Boolean value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public Date getDate(String name)
    {
        return new Date(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public double getDouble(String name)
    {
        return Double.parseDouble(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public int getInt(String name)
    {
        return Integer.parseInt(getFieldValue(name));
    }

    @Override
    public void setInt(Text text, Integer value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public long getLong(String name)
    {
        return Long.parseLong(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public Block getMap(String name, Type type)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setMap(Text text, Type type, Block block)
    {
        throw new PrestoException(NOT_SUPPORTED,
                "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public Time getTime(String name)
    {
        return new Time(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public Timestamp getTimestamp(String name)
    {
        return new Timestamp(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public byte[] getVarbinary(String name)
    {
        return getFieldValue(name).getBytes();
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
        text.set(value.getBytes());
    }

    private String getFieldValue(String name)
    {
        return columnValues.get(name).toString();
    }

    @Override
    public byte[] encode(Type type, Object v)
    {
        Text t = new Text();
        if (Types.isArrayType(type)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "arrays are not (yet?) supported for StringRowSerializer");
        }
        else if (Types.isMapType(type)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "maps are not (yet?) supported for StringRowSerializer");
        }
        else if (type.equals(BIGINT) && v instanceof Integer) {
            setLong(t, ((Integer) v).longValue());
        }
        else if (type.equals(BIGINT) && v instanceof Long) {
            setLong(t, (Long) v);
        }
        else if (type.equals(BOOLEAN)) {
            setBoolean(t, v.equals(Boolean.TRUE));
        }
        else if (type.equals(DATE)) {
            setDate(t, (Date) v);
        }
        else if (type.equals(DOUBLE)) {
            setDouble(t, (Double) v);
        }
        else if (type.equals(INTEGER) && v instanceof Integer) {
            setInt(t, (Integer) v);
        }
        else if (type.equals(INTEGER) && v instanceof Long) {
            setInt(t, (Integer) v);
        }
        else if (type.equals(TIME)) {
            setTime(t, (Time) v);
        }
        else if (type.equals(TIMESTAMP)) {
            setTimestamp(t, (Timestamp) v);
        }
        else if (type.equals(VARBINARY)) {
            setVarbinary(t, ((Slice) v).getBytes());
        }
        else if (type.equals(VARCHAR)) {
            setVarchar(t, ((Slice) v).toStringUtf8());
        }
        else {
            throw new PrestoException(NOT_SUPPORTED,
                    format("StringLexicoder does not support encoding type %s, object is %s", type, v));
        }

        return t.copyBytes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(Type type, byte[] v)
    {
        String str = new String(v);
        if (Types.isArrayType(type)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "arrays are not (yet?) supported for StringRowSerializer");
        }
        else if (Types.isMapType(type)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "maps are not (yet?) supported for StringRowSerializer");
        }
        else if (type.equals(BIGINT)) {
            return (T) (Long) Long.parseLong(str);
        }
        else if (type.equals(BOOLEAN)) {
            return (T) (Boolean) Boolean.parseBoolean(str);
        }
        else if (type.equals(DATE)) {
            return (T) new Date(Long.parseLong(str));
        }
        else if (type.equals(DOUBLE)) {
            return (T) (Double) Double.parseDouble(str);
        }
        else if (type.equals(TIME)) {
            return (T) new Time(Long.parseLong(str));
        }
        else if (type.equals(TIMESTAMP)) {
            return (T) new Timestamp(Long.parseLong(str));
        }
        else if (type.equals(VARBINARY)) {
            return (T) v;
        }
        else if (type.equals(VARCHAR)) {
            return (T) new String(v);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED,
                    format("StringLexicoder does not support decoding type %s, object is %s", type, v));
        }
    }
}
