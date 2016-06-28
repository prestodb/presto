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
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
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

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.Types.checkType;
import static com.facebook.presto.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
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

/**
 * Implementation of {@link AccumuloRowSerializer} that uses Accumulo lexicoders to serialize the
 * values of the Presto columns.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@NotThreadSafe
public class LexicoderRowSerializer
        implements AccumuloRowSerializer
{
    private static Map<Type, Lexicoder> lexicoderMap = null;
    private static Map<TypeSignature, ListLexicoder<?>> listLexicoders = new HashMap<>();
    private static Map<TypeSignature, MapLexicoder<?, ?>> mapLexicoders = new HashMap<>();
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
            LongLexicoder longLexicoder = new LongLexicoder();
            DoubleLexicoder doubleLexicoder = new DoubleLexicoder();
            lexicoderMap = new HashMap<>();
            lexicoderMap.put(BIGINT, longLexicoder);
            lexicoderMap.put(BOOLEAN, new BooleanLexicoder());
            lexicoderMap.put(DATE, longLexicoder);
            lexicoderMap.put(DOUBLE, doubleLexicoder);
            lexicoderMap.put(FLOAT, doubleLexicoder);
            lexicoderMap.put(INTEGER, longLexicoder);
            lexicoderMap.put(SMALLINT, longLexicoder);
            lexicoderMap.put(TIME, longLexicoder);
            lexicoderMap.put(TIMESTAMP, longLexicoder);
            lexicoderMap.put(TINYINT, longLexicoder);
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
    public void setMapping(String name, String family, String qualifier)
    {
        columnValues.put(name, null);
        Map<String, String> q2pc = f2q2pc.get(family);
        if (q2pc == null) {
            q2pc = new HashMap<>();
            f2q2pc.put(family, q2pc);
        }

        q2pc.put(qualifier, name);
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
            columnValues.put(rowIdName, rowId.copyBytes());
        }

        if (rowOnly) {
            return;
        }

        entry.getKey().getColumnFamily(cf);
        entry.getKey().getColumnQualifier(cq);

        if (cf.equals(ROW_ID_COLUMN) && cq.equals(ROW_ID_COLUMN)) {
            return;
        }

        value.set(entry.getValue().get());
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
        return decode(BOOLEAN, getFieldValue(name));
    }

    @Override
    public void setBoolean(Text text, Boolean value)
    {
        text.set(encode(BOOLEAN, value));
    }

    @Override
    public byte getByte(String name)
    {
        return ((Long) decode(TINYINT, getFieldValue(name))).byteValue();
    }

    @Override
    public void setByte(Text text, Byte value)
    {
        text.set(encode(TINYINT, value));
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
    public float getFloat(String name)
    {
        return ((Double) decode(FLOAT, getFieldValue(name))).floatValue();
    }

    @Override
    public void setFloat(Text text, Float value)
    {
        text.set(encode(FLOAT, value));
    }

    @Override
    public int getInt(String name)
    {
        return ((Long) decode(INTEGER, getFieldValue(name))).intValue();
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
    public short getShort(String name)
    {
        return ((Long) decode(SMALLINT, getFieldValue(name))).shortValue();
    }

    @Override
    public void setShort(Text text, Short value)
    {
        text.set(encode(SMALLINT, value));
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
    public byte[] encode(Type type, Object value)
    {
        Object toEncode;
        if (Types.isArrayType(type)) {
            toEncode =
                    AccumuloRowSerializer.getArrayFromBlock(Types.getElementType(type), (Block) value);
        }
        else if (Types.isMapType(type)) {
            toEncode = AccumuloRowSerializer.getMapFromBlock(type, (Block) value);
        }
        else if (type.equals(BIGINT) && value instanceof Integer) {
            toEncode = ((Integer) value).longValue();
        }
        else if (type.equals(DATE) && value instanceof Date) {
            toEncode = ((Date) value).getTime();
        }
        else if (type.equals(FLOAT) && value instanceof Float) {
            toEncode = ((Float) value).doubleValue();
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            toEncode = ((Integer) value).longValue();
        }
        else if (type.equals(SMALLINT) && value instanceof Short) {
            toEncode = ((Short) value).longValue();
        }
        else if (type.equals(TIME) && value instanceof Time) {
            toEncode = ((Time) value).getTime();
        }
        else if (type.equals(TIMESTAMP) && value instanceof Timestamp) {
            toEncode = ((Timestamp) value).getTime();
        }
        else if (type.equals(TINYINT) && value instanceof Byte) {
            toEncode = ((Byte) value).longValue();
        }
        else if (type.equals(VARBINARY) && value instanceof Slice) {
            toEncode = ((Slice) value).getBytes();
        }
        else if (type instanceof VarcharType && value instanceof Slice) {
            toEncode = ((Slice) value).toStringUtf8();
        }
        else {
            toEncode = value;
        }

        return getLexicoder(type).encode(toEncode);
    }

    @Override
    public <T> T decode(Type type, byte[] value)
    {
        return (T) getLexicoder(type).decode(value);
    }

    public static Lexicoder getLexicoder(Type type)
    {
        if (Types.isArrayType(type)) {
            return getListLexicoder(type);
        }
        else if (Types.isMapType(type)) {
            return getMapLexicoder(type);
        }
        else if (type instanceof VarcharType) {
            return lexicoderMap.get(VARCHAR);
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
     * @param elementType Presto type of the list elements
     * @return List lexicoder
     */
    private static ListLexicoder getListLexicoder(Type elementType)
    {
        ListLexicoder<?> listLexicoder = listLexicoders.get(elementType.getTypeSignature());
        if (listLexicoder == null) {
            listLexicoder = new ListLexicoder(getLexicoder(Types.getElementType(elementType)));
            listLexicoders.put(elementType.getTypeSignature(), listLexicoder);
        }
        return listLexicoder;
    }

    /**
     * Gets a MapLexicoder for the given Map type.
     *
     * @param type Presto MapType
     * @return Map lexicoder
     */
    private static MapLexicoder getMapLexicoder(Type type)
    {
        MapType mapType = checkType(type, MapType.class, "type");
        MapLexicoder<?, ?> mapLexicoder = mapLexicoders.get(mapType.getTypeSignature());
        if (mapLexicoder == null) {
            mapLexicoder = new MapLexicoder(getLexicoder(Types.getKeyType(mapType)),
                    getLexicoder(Types.getValueType(mapType)));
            mapLexicoders.put(mapType.getTypeSignature(), mapLexicoder);
        }
        return mapLexicoder;
    }
}
