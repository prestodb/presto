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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Interface for deserializing the data in Accumulo into a Presto row.
 * <p>
 * Provides a means for end-users of the connector to customize how the data in an Accumulo row gets
 * serialized and deserialized from/to a Presto row.
 * <p>
 * The workflow of how this class is called by the Accumulo connector for reading data is as
 * follows:
 * <ol>
 * <li>setRowIdName - Sets the Presto name which is the Accumulo row ID</li>
 * <li>setRowOnly - True if only the row ID is going to be retrieved, false if more data is
 * necessary.</li>
 * <li>setMapping - Multiple calls for each Presto column, setting the mapping of Presto column name
 * to Accumulo column family and qualifier</li>
 * <li>deserialize - Called for each Accumulo entry in the same row. Implements should
 * retrieve the Presto column value from the given key/value pair</li>
 * <li>get* - Called to retrieve the data type for the given Presto column name</li>
 * <li>reset - Begins a new Row, serializer is expected to clear any state</li>
 * <li>If there are more entries left, go back to deserialize, else end!</li>
 * </ol>
 *
 * @see LexicoderRowSerializer
 * @see StringRowSerializer
 */
public interface AccumuloRowSerializer
{
    /**
     * Gets the default AccumuloRowSerializer, {@link LexicoderRowSerializer}.
     *
     * @return Default serializer
     */
    static AccumuloRowSerializer getDefault()
    {
        return new LexicoderRowSerializer();
    }

    /**
     * Sets the Presto name which maps to the Accumulo row ID.
     *
     * @param name Presto column name
     */
    void setRowIdName(String name);

    /**
     * Sets the mapping for the Presto column name to Accumulo family and qualifier.
     *
     * @param name Presto name
     * @param family Accumulo family
     * @param qualifier Accumulo qualifier
     */
    void setMapping(String name, String family, String qualifier);

    /**
     * Sets a Boolean value indicating whether or not only the row ID is going to be retrieved from the serializer.
     *
     * @param rowOnly True if only the row ID is set, false otherwise
     */
    void setRowOnly(boolean rowOnly);

    /**
     * Reset the state of the serializer to prepare for a new set of entries with the same row ID.
     */
    void reset();

    /**
     * Deserialize the given Accumulo entry, retrieving data for the Presto column.
     *
     * @param entry Entry to deserialize
     * @throws IOException If an IO error occurs during deserialization
     */
    void deserialize(Entry<Key, Value> entry)
            throws IOException;

    /**
     * Gets a Boolean value indicating whether or not the Presto column is a null value.
     *
     * @param name Column name
     * @return True if null, false otherwise.
     */
    boolean isNull(String name);

    /**
     * Gets the array Block of the given Presto column.
     *
     * @param name Column name
     * @param type Array type
     * @return True if null, false otherwise.
     */
    Block getArray(String name, Type type);

    /**
     * Encode the given array Block into the given Text object.
     *
     * @param text Text object to set
     * @param type Array type
     * @param block Array block
     */
    void setArray(Text text, Type type, Block block);

    /**
     * Gets the Boolean value of the given Presto column.
     *
     * @param name Column name
     * @return Boolean value
     */
    boolean getBoolean(String name);

    /**
     * Encode the given Boolean value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setBoolean(Text text, Boolean value);

    /**
     * Gets the Byte value of the given Presto column.
     *
     * @param name Column name
     * @return Byte value
     */
    byte getByte(String name);

    /**
     * Encode the given Byte value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setByte(Text text, Byte value);

    /**
     * Gets the Date value of the given Presto column.
     *
     * @param name Column name
     * @return Date value
     */
    Date getDate(String name);

    /**
     * Encode the given Date value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setDate(Text text, Date value);

    /**
     * Gets the Double value of the given Presto column.
     *
     * @param name Column name
     * @return Double value
     */
    double getDouble(String name);

    /**
     * Encode the given Double value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setDouble(Text text, Double value);

    /**
     * Gets the Float value of the given Presto column.
     *
     * @param name Column name
     * @return Float value
     */
    float getFloat(String name);

    /**
     * Encode the given Float value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setFloat(Text text, Float value);

    /**
     * Gets the Integer value of the given Presto column.
     *
     * @param name Column name
     * @return Integer value
     */
    int getInt(String name);

    /**
     * Encode the given Integer value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setInt(Text text, Integer value);

    /**
     * Gets the Long value of the given Presto column.
     *
     * @param name Column name
     * @return Long value
     */
    long getLong(String name);

    /**
     * Encode the given Long value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setLong(Text text, Long value);

    /**
     * Gets the Map value of the given Presto column and Map type.
     *
     * @param name Column name
     * @param type Map type
     * @return Map value
     */
    Block getMap(String name, Type type);

    /**
     * Encode the given map Block into the given Text object.
     *
     * @param text Text object to set
     * @param type Map type
     * @param block Map block
     */
    void setMap(Text text, Type type, Block block);

    /**
     * Gets the Short value of the given Presto column.
     *
     * @param name Column name
     * @return Short value
     */
    short getShort(String name);

    /**
     * Encode the given Short value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setShort(Text text, Short value);

    /**
     * Gets the Time value of the given Presto column.
     *
     * @param name Column name
     * @return Time value
     */
    Time getTime(String name);

    /**
     * Encode the given Time value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setTime(Text text, Time value);

    /**
     * Gets the Timestamp value of the given Presto column.
     *
     * @param name Column name
     * @return Timestamp value
     */
    Timestamp getTimestamp(String name);

    /**
     * Encode the given Timestamp value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setTimestamp(Text text, Timestamp value);

    /**
     * Gets the Varbinary value of the given Presto column.
     *
     * @param name Column name
     * @return Varbinary value
     */
    byte[] getVarbinary(String name);

    /**
     * Encode the given byte[] value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setVarbinary(Text text, byte[] value);

    /**
     * Gets the String value of the given Presto column.
     *
     * @param name Column name
     * @return String value
     */
    String getVarchar(String name);

    /**
     * Encode the given String value into the given Text object.
     *
     * @param text Text object to set
     * @param value Value to encode
     */
    void setVarchar(Text text, String value);

    /**
     * Encodes a Presto Java object to a byte array based on the given type.
     * <p>
     * Java Lists and Maps can be converted to Blocks using
     * {@link AccumuloRowSerializer#getBlockFromArray(Type, java.util.List)} and
     * {@link AccumuloRowSerializer#getBlockFromMap(Type, Map)}
     * <p>
     * <table summary="Expected data types">
     * <tr>
     * <th>Type to Encode</th>
     * <th>Expected Java Object</th>
     * </tr>
     * <tr>
     * <td>ARRAY</td>
     * <td>com.facebook.presto.spi.block.Block</td>
     * </tr>
     * <tr>
     * <td>BIGINT</td>
     * <td>Integer or Long</td>
     * </tr>
     * <tr>
     * <td>BOOLEAN</td>
     * <td>Boolean</td>
     * </tr>
     * <tr>
     * <td>DATE</td>
     * <td>java.sql.Date, Long</td>
     * </tr>
     * <tr>
     * <td>DOUBLE</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>INTEGER</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>Map</td>
     * <td>com.facebook.presto.spi.block.Block</td>
     * </tr>
     * <tr>
     * <td>REAL</td>
     * <td>Float</td>
     * </tr>
     * <tr>
     * <td>SMALLINT</td>
     * <td>Short</td>
     * </tr>
     * <tr>
     * <td>TIME</td>
     * <td>java.sql.Time, Long</td>
     * </tr>
     * <tr>
     * <td>TIMESTAMP</td>
     * <td>java.sql.Timestamp, Long</td>
     * </tr>
     * <tr>
     * <td>TINYINT</td>
     * <td>Byte</td>
     * </tr>
     * <tr>
     * <td>VARBINARY</td>
     * <td>io.airlift.slice.Slice or byte[]</td>
     * </tr>
     * <tr>
     * <td>VARCHAR</td>
     * <td>io.airlift.slice.Slice or String</td>
     * </tr>
     * </table>
     *
     * @param type The presto {@link com.facebook.presto.spi.type.Type}
     * @param value The Java object per the table in the method description
     * @return Encoded bytes
     */
    byte[] encode(Type type, Object value);

    /**
     * Generic function to decode the given byte array to a Java object based on the given type.
     * <p>
     * Blocks from ARRAY and MAP types can be converted
     * to Java Lists and Maps using {@link AccumuloRowSerializer#getArrayFromBlock(Type, Block)}
     * and {@link AccumuloRowSerializer#getMapFromBlock(Type, Block)}
     * <p>
     * <table summary="Expected data types">
     * <tr>
     * <th>Encoded Type</th>
     * <th>Returned Java Object</th>
     * </tr>
     * <tr>
     * <td>ARRAY</td>
     * <td>List&lt;?&gt;</td>
     * </tr>
     * <tr>
     * <td>BIGINT</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>BOOLEAN</td>
     * <td>Boolean</td>
     * </tr>
     * <tr>
     * <td>DATE</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>DOUBLE</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>Map</td>
     * <td>Map&lt;?,?&gt;</td>
     * </tr>
     * <tr>
     * <td>REAL</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>SMALLINT</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>TIME</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>TIMESTAMP</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>TINYINT</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>VARBINARY</td>
     * <td>byte[]</td>
     * </tr>
     * <tr>
     * <td>VARCHAR</td>
     * <td>String</td>
     * </tr>
     * </table>
     *
     * @param type The presto {@link com.facebook.presto.spi.type.Type}
     * @param value Encoded bytes to decode
     * @param <T> The Java type of the object that has been encoded to the given byte array
     * @return The Java object per the table in the method description
     */
    <T> T decode(Type type, byte[] value);

    /**
     * Given the array element type and Presto Block, decodes the Block into a list of values.
     *
     * @param elementType Array element type
     * @param block Array block
     * @return List of values
     */
    static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            arrayBuilder.add(readObject(elementType, block, i));
        }
        return arrayBuilder.build();
    }

    /**
     * Given the map type and Presto Block, decodes the Block into a map of values.
     *
     * @param type Map type
     * @param block Map block
     * @return List of values
     */
    static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        Type keyType = Types.getKeyType(type);
        Type valueType = Types.getValueType(type);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
        }
        return map;
    }

    /**
     * Encodes the given list into a Block.
     *
     * @param elementType Element type of the array
     * @param array Array of elements to encode
     * @return Presto Block
     */
    static Block getBlockFromArray(Type elementType, List<?> array)
    {
        BlockBuilder builder = elementType.createBlockBuilder(new BlockBuilderStatus(), array.size());
        for (Object item : array) {
            writeObject(builder, elementType, item);
        }
        return builder.build();
    }

    /**
     * Encodes the given map into a Block.
     *
     * @param mapType Presto type of the map
     * @param map Map of key/value pairs to encode
     * @return Presto Block
     */
    static Block getBlockFromMap(Type mapType, Map<?, ?> map)
    {
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder builder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), map.size() * 2);

        for (Entry<?, ?> entry : map.entrySet()) {
            writeObject(builder, keyType, entry.getKey());
            writeObject(builder, valueType, entry.getValue());
        }
        return builder.build();
    }

    /**
     * Recursive helper function used by {@link AccumuloRowSerializer#getBlockFromArray} and
     * {@link AccumuloRowSerializer#getBlockFromMap} to add the given object to the given block
     * builder. Supports nested complex types!
     *
     * @param builder Block builder
     * @param type Presto type
     * @param obj Object to write to the block builder
     */
    static void writeObject(BlockBuilder builder, Type type, Object obj)
    {
        if (Types.isArrayType(type)) {
            BlockBuilder arrayBldr = builder.beginBlockEntry();
            Type elementType = Types.getElementType(type);
            for (Object item : (List<?>) obj) {
                writeObject(arrayBldr, elementType, item);
            }
            builder.closeEntry();
        }
        else if (Types.isMapType(type)) {
            BlockBuilder mapBlockBuilder = builder.beginBlockEntry();
            for (Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                writeObject(mapBlockBuilder, Types.getKeyType(type), entry.getKey());
                writeObject(mapBlockBuilder, Types.getValueType(type), entry.getValue());
            }
            builder.closeEntry();
        }
        else {
            TypeUtils.writeNativeValue(type, builder, obj);
        }
    }

    /**
     * Recursive helper function used by {@link AccumuloRowSerializer#getArrayFromBlock} and
     * {@link AccumuloRowSerializer#getMapFromBlock} to decode the Block into a Java type.
     *
     * @param type Presto type
     * @param block Block to decode
     * @param position Position in the block to get
     * @return Java object from the Block
     */
    static Object readObject(Type type, Block block, int position)
    {
        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            return getArrayFromBlock(elementType, block.getObject(position, Block.class));
        }
        else if (Types.isMapType(type)) {
            return getMapFromBlock(type, block.getObject(position, Block.class));
        }
        else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
                return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
            }

            return TypeUtils.readNativeValue(type, block, position);
        }
    }
}
