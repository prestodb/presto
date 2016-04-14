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
import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ListLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Implementation of {@link AccumuloRowSerializer} that uses Accumulo lexicoders to serialize the
 * values of the Presto columns.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@NotThreadSafe
public class LexicoderRowSerializer
        implements AccumuloRowSerializer
{
    public static final byte[] TRUE = new byte[] {1};
    public static final byte[] FALSE = new byte[] {0};

    private static Map<Type, Lexicoder> lexicoderMap = null;
    private static Map<String, ListLexicoder<?>> listLexicoders = new HashMap<>();
    private static Map<String, MapLexicoder<?, ?>> mapLexicoders = new HashMap<>();
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, byte[]> columnValues = new HashMap<>();
    private Text rowId = new Text();
    private Text cf = new Text();
    private Text cq = new Text();
    private Text value = new Text();
    private boolean rowOnly = false;
    private String rowIdName;

    static {
        if (lexicoderMap == null) {
            lexicoderMap = new HashMap<>();
            lexicoderMap.put(BIGINT, new LongLexicoder());
            lexicoderMap.put(BOOLEAN, new BytesLexicoder());
            lexicoderMap.put(DATE, new LongLexicoder());
            lexicoderMap.put(DOUBLE, new DoubleLexicoder());
            lexicoderMap.put(INTEGER, new IntegerLexicoder());
            lexicoderMap.put(TIME, new LongLexicoder());
            lexicoderMap.put(TIMESTAMP, new LongLexicoder());
            lexicoderMap.put(VARBINARY, new BytesLexicoder());
            lexicoderMap.put(VARCHAR, new StringLexicoder());
        }
    }

    @Override
    public void setRowIdName(String name)
    {
        rowIdName = name;
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
            columnValues.put(rowIdName, rowId.copyBytes());
        }

        if (rowOnly) {
            return;
        }

        kvp.getKey().getColumnFamily(cf);
        kvp.getKey().getColumnQualifier(cq);
        value.set(kvp.getValue().get());
        columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()), value.copyBytes());
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null;
    }

    @Override
    public Block getArray(String name, Type type)
    {
        Type elementType = Types.getElementType(type);
        return AccumuloRowSerializer.getBlockFromArray(elementType,
                decode(type, getFieldValue(name)));
    }

    @Override
    public void setArray(Text text, Type type, Block block)
    {
        text.set(encode(type, block));
    }

    @Override
    public boolean getBoolean(String name)
    {
        return getFieldValue(name)[0] == TRUE[0];
    }

    @Override
    public void setBoolean(Text text, Boolean value)
    {
        text.set(encode(BOOLEAN, value));
    }

    @Override
    public Date getDate(String name)
    {
        return new Date(decode(BIGINT, getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value)
    {
        text.set(encode(DATE, value));
    }

    @Override
    public double getDouble(String name)
    {
        return decode(DOUBLE, getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value)
    {
        text.set(encode(DOUBLE, value));
    }

    @Override
    public int getInt(String name)
    {
        return decode(INTEGER, getFieldValue(name));
    }

    @Override
    public void setInt(Text text, Integer value)
    {
        text.set(encode(INTEGER, value));
    }

    @Override
    public long getLong(String name)
    {
        return decode(BIGINT, getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value)
    {
        text.set(encode(BIGINT, value));
    }

    @Override
    public Block getMap(String name, Type type)
    {
        return AccumuloRowSerializer.getBlockFromMap(type, decode(type, getFieldValue(name)));
    }

    @Override
    public void setMap(Text text, Type type, Block block)
    {
        text.set(encode(type, block));
    }

    @Override
    public Time getTime(String name)
    {
        return new Time(decode(BIGINT, getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value)
    {
        text.set(encode(TIME, value));
    }

    @Override
    public Timestamp getTimestamp(String name)
    {
        return new Timestamp(decode(TIMESTAMP, getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value)
    {
        text.set(encode(TIMESTAMP, value));
    }

    @Override
    public byte[] getVarbinary(String name)
    {
        return decode(VARBINARY, getFieldValue(name));
    }

    @Override
    public void setVarbinary(Text text, byte[] value)
    {
        text.set(encode(VARBINARY, value));
    }

    @Override
    public String getVarchar(String name)
    {
        return decode(VARCHAR, getFieldValue(name));
    }

    @Override
    public void setVarchar(Text text, String value)
    {
        text.set(encode(VARCHAR, value));
    }

    private byte[] getFieldValue(String name)
    {
        return columnValues.get(name);
    }

    @Override
    public byte[] encode(Type type, Object v)
    {
        Object toEncode;
        if (Types.isArrayType(type)) {
            toEncode =
                    AccumuloRowSerializer.getArrayFromBlock(Types.getElementType(type), (Block) v);
        }
        else if (Types.isMapType(type)) {
            toEncode = AccumuloRowSerializer.getMapFromBlock(type, (Block) v);
        }
        else if (type.equals(BOOLEAN)) {
            toEncode = v.equals(Boolean.TRUE) ? LexicoderRowSerializer.TRUE
                    : LexicoderRowSerializer.FALSE;
        }
        else if (type.equals(DATE) && v instanceof Date) {
            toEncode = ((Date) v).getTime();
        }
        else if (type.equals(TIME) && v instanceof Time) {
            toEncode = ((Time) v).getTime();
        }
        else if (type.equals(TIMESTAMP) && v instanceof Timestamp) {
            toEncode = ((Timestamp) v).getTime();
        }
        else if (type.equals(VARBINARY) && v instanceof Slice) {
            toEncode = ((Slice) v).getBytes();
        }
        else if (type.equals(VARCHAR) && v instanceof Slice) {
            toEncode = ((Slice) v).toStringUtf8();
        }
        else if (type.equals(BIGINT) && v instanceof Integer) {
            toEncode = ((Integer) v).longValue();
        }
        else {
            toEncode = v;
        }

        return getLexicoder(type).encode(toEncode);
    }

    @Override
    public <T> T decode(Type type, byte[] v)
    {
        return (T) getLexicoder(type).decode(v);
    }

    public static Lexicoder getLexicoder(Type type)
    {
        if (Types.isArrayType(type)) {
            return getListLexicoder(type);
        }
        else if (Types.isMapType(type)) {
            return getMapLexicoder(type);
        }
        else {
            Lexicoder l = lexicoderMap.get(type);
            if (l == null) {
                throw new PrestoException(INTERNAL_ERROR,
                        "No lexicoder for type " + type);
            }
            return l;
        }
    }

    /**
     * Gets a ListLexicoder for the given element type.
     *
     * @param eType
     * @return List lexicoder
     */
    private static ListLexicoder getListLexicoder(Type eType)
    {
        ListLexicoder<?> listLexicoder = listLexicoders.get(eType.getDisplayName());
        if (listLexicoder == null) {
            listLexicoder = new ListLexicoder(getLexicoder(Types.getElementType(eType)));
            listLexicoders.put(eType.getDisplayName(), listLexicoder);
        }
        return listLexicoder;
    }

    /**
     * Gets a MapLexicoder for the given Map type.
     *
     * @param type
     * @return Map lexicoder
     */
    private static MapLexicoder getMapLexicoder(Type type)
    {
        MapLexicoder<?, ?> mapLexicoder = mapLexicoders.get(type.getDisplayName());
        if (mapLexicoder == null) {
            mapLexicoder = new MapLexicoder(getLexicoder(Types.getKeyType(type)),
                    getLexicoder(Types.getValueType(type)));
            mapLexicoders.put(type.getDisplayName(), mapLexicoder);
        }
        return mapLexicoder;
    }
}
