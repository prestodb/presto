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
package io.prestosql.plugin.accumulo.serializers;

import io.airlift.slice.Slice;
import io.prestosql.plugin.accumulo.Types;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ListLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.prestosql.plugin.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link AccumuloRowSerializer} that uses Accumulo lexicoders to serialize the values of the Presto columns.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class LexicoderRowSerializer
        implements AccumuloRowSerializer
{
    private static final Map<Type, Lexicoder> LEXICODER_MAP = new HashMap<>();
    private static final Map<TypeSignature, ListLexicoder<?>> LIST_LEXICODERS = new HashMap<>();
    private static final Map<TypeSignature, MapLexicoder<?, ?>> MAP_LEXICODERS = new HashMap<>();

    private final Map<String, Map<String, String>> familyQualifierColumnMap = new HashMap<>();
    private final Map<String, byte[]> columnValues = new HashMap<>();
    private final Text rowId = new Text();
    private final Text family = new Text();
    private final Text qualifier = new Text();
    private final Text value = new Text();

    private boolean rowOnly;
    private String rowIdName;

    static {
        LongLexicoder longLexicoder = new LongLexicoder();
        DoubleLexicoder doubleLexicoder = new DoubleLexicoder();
        LEXICODER_MAP.put(BIGINT, longLexicoder);
        LEXICODER_MAP.put(BOOLEAN, new BooleanLexicoder());
        LEXICODER_MAP.put(DATE, longLexicoder);
        LEXICODER_MAP.put(DOUBLE, doubleLexicoder);
        LEXICODER_MAP.put(INTEGER, longLexicoder);
        LEXICODER_MAP.put(REAL, doubleLexicoder);
        LEXICODER_MAP.put(SMALLINT, longLexicoder);
        LEXICODER_MAP.put(TIME, longLexicoder);
        LEXICODER_MAP.put(TIMESTAMP, longLexicoder);
        LEXICODER_MAP.put(TINYINT, longLexicoder);
        LEXICODER_MAP.put(VARBINARY, new BytesLexicoder());
        LEXICODER_MAP.put(VARCHAR, new StringLexicoder());
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
        Map<String, String> qualifierToNameMap = familyQualifierColumnMap.get(family);
        if (qualifierToNameMap == null) {
            qualifierToNameMap = new HashMap<>();
            familyQualifierColumnMap.put(family, qualifierToNameMap);
        }

        qualifierToNameMap.put(qualifier, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    @Override
    public void deserialize(Entry<Key, Value> entry)
    {
        if (!columnValues.containsKey(rowIdName)) {
            entry.getKey().getRow(rowId);
            columnValues.put(rowIdName, rowId.copyBytes());
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
        columnValues.put(familyQualifierColumnMap.get(family.toString()).get(qualifier.toString()), value.copyBytes());
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
        return AccumuloRowSerializer.getBlockFromArray(elementType, decode(type, getFieldValue(name)));
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
        return new Date(DAYS.toMillis(decode(BIGINT, getFieldValue(name))));
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
        return ((Double) decode(REAL, getFieldValue(name))).floatValue();
    }

    @Override
    public void setFloat(Text text, Float value)
    {
        text.set(encode(REAL, value));
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
            toEncode = AccumuloRowSerializer.getArrayFromBlock(Types.getElementType(type), (Block) value);
        }
        else if (Types.isMapType(type)) {
            toEncode = AccumuloRowSerializer.getMapFromBlock(type, (Block) value);
        }
        else if (type.equals(BIGINT) && value instanceof Integer) {
            toEncode = ((Integer) value).longValue();
        }
        else if (type.equals(DATE) && value instanceof Date) {
            toEncode = MILLISECONDS.toDays(((Date) value).getTime());
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            toEncode = ((Integer) value).longValue();
        }
        else if (type.equals(REAL) && value instanceof Float) {
            toEncode = ((Float) value).doubleValue();
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
            return LEXICODER_MAP.get(VARCHAR);
        }
        else {
            Lexicoder lexicoder = LEXICODER_MAP.get(type);
            if (lexicoder == null) {
                throw new PrestoException(NOT_SUPPORTED, "No lexicoder for type " + type);
            }
            return lexicoder;
        }
    }

    private static ListLexicoder getListLexicoder(Type elementType)
    {
        ListLexicoder<?> listLexicoder = LIST_LEXICODERS.get(elementType.getTypeSignature());
        if (listLexicoder == null) {
            listLexicoder = new ListLexicoder(getLexicoder(Types.getElementType(elementType)));
            LIST_LEXICODERS.put(elementType.getTypeSignature(), listLexicoder);
        }
        return listLexicoder;
    }

    private static MapLexicoder getMapLexicoder(Type type)
    {
        MapLexicoder<?, ?> mapLexicoder = MAP_LEXICODERS.get(type.getTypeSignature());
        if (mapLexicoder == null) {
            mapLexicoder = new MapLexicoder(
                    getLexicoder(Types.getKeyType(type)),
                    getLexicoder(Types.getValueType(type)));
            MAP_LEXICODERS.put(type.getTypeSignature(), mapLexicoder);
        }
        return mapLexicoder;
    }
}
