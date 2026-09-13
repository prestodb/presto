package com.facebook.presto.hbase.util;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hbase.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import io.airlift.slice.Slice;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

/**
 * Interface for deserializing the data in Hbase into a Presto row.
 */
public final class HbaseRowSerializerUtil {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HbaseRowSerializerUtil() {}

  public static byte[] toHbaseBytes(Type type, @Nonnull Object value) {
    Object toEncode;
    if (Types.isArrayType(type)) {
      toEncode = getArrayFromBlock(Types.getElementType(type), (Block) value);
      try {
        return MAPPER.writeValueAsBytes(toEncode);
      } catch (JsonProcessingException e) {
        throw new UnsupportedOperationException("Unsupported type " + type, e);
      }
    } else if (Types.isMapType(type)) {
      toEncode = getMapFromBlock(type, (Block) value);
      try {
        return MAPPER.writeValueAsBytes(toEncode);
      } catch (JsonProcessingException e) {
        throw new UnsupportedOperationException("Unsupported type " + type, e);
      }
    } else if (type.equals(BIGINT)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(DATE)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(INTEGER)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(REAL)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(SMALLINT)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(TIME)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(TIMESTAMP)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(TINYINT)) {
      return Bytes.toBytes((Long) value);
    } else if (type.equals(BOOLEAN)) {
      return Bytes.toBytes((Boolean) value);
    } else if (type.equals(DOUBLE)) {
      return Bytes.toBytes((Double) value);
    } else if (type.equals(VARBINARY)) {
      return ((Slice) value).getBytes();
    } else if (type instanceof VarcharType) {
      return ((Slice) value).getBytes();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported type " + type + " valaueClass " + value.getClass());
    }
  }

  /**
   * Given the array element type and Presto Block, decodes the Block into a list of values.
   *
   * @param elementType Array element type
   * @param block Array block
   * @return List of values
   */
  private static List<Object> getArrayFromBlock(Type elementType, Block block) {
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
  private static Map<Object, Object> getMapFromBlock(Type type, Block block) {
    Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
    Type keyType = Types.getKeyType(type);
    Type valueType = Types.getValueType(type);
    for (int i = 0; i < block.getPositionCount(); i += 2) {
      map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
    }
    return map;
  }

  /**
   * @param type Presto type
   * @param block Block to decode
   * @param position Position in the block to get
   * @return Java object from the Block
   */
  private static Object readObject(Type type, Block block, int position) {
    if (Types.isArrayType(type)) {
      Type elementType = Types.getElementType(type);
      return getArrayFromBlock(elementType, block.getBlock(position));
    } else if (Types.isMapType(type)) {
      return getMapFromBlock(type, block.getBlock(position));
    } else {
      if (type.getJavaType() == Slice.class) {
        Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
        return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
      }

      return TypeUtils.readNativeValue(type, block, position);
    }
  }

  // ------------read--------------

  /**
   * Encodes the given map into a Block.
   *
   * @param mapType Presto type of the map
   * @param bytes hbase byte[]
   * @return Presto Block
   */
  public static Block getBlockFromMap(Type mapType, byte[] bytes) throws IOException {
    Type keyType = mapType.getTypeParameters().get(0);
    Type valueType = mapType.getTypeParameters().get(1);
    Map<?, ?> map = MAPPER.readValue(bytes,
        new MyTypeReference(keyType.getJavaType(), valueType.getJavaType()));

    BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
    BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      writeObject(builder, keyType, entry.getKey());
      writeObject(builder, valueType, entry.getValue());
    }

    mapBlockBuilder.closeEntry();
    return (Block) mapType.getObject(mapBlockBuilder, 0);
  }

  public static class MyTypeReference extends TypeReference<Map<?, ?>> {
    private ParameterizedType type;

    public MyTypeReference(Class<?> keyType, Class<?> valueType) {
      this.type = ParameterizedTypeImpl.make(Map.class,
          new java.lang.reflect.Type[] {keyType, valueType}, null);
    }

    @Override
    public java.lang.reflect.Type getType() {
      return this.type;
    }
  }

  /**
   * Encodes the given list into a Block.
   *
   * @param elementType Element type of the array
   * @param bytes hbase byte[]
   * @return Presto Block
   */
  public static Block getBlockFromArray(Type elementType, byte[] bytes) throws IOException {
    List<?> array = MAPPER.readValue(bytes, List.class);
    BlockBuilder builder = elementType.createBlockBuilder(null, array.size());
    for (Object item : array) {
      writeObject(builder, elementType, item);
    }
    return builder.build();
  }

  /**
   * Recursive helper function used by {@link this#getArrayFromBlock} and
   * {@link this#getMapFromBlock} to add the given object to the given block builder. Supports
   * nested complex types!
   *
   * @param builder Block builder
   * @param type Presto type
   * @param obj Object to write to the block builder
   */
  private static void writeObject(BlockBuilder builder, Type type, Object obj) {
    if (Types.isArrayType(type)) {
      BlockBuilder arrayBldr = builder.beginBlockEntry();
      Type elementType = Types.getElementType(type);
      for (Object item : (List<?>) obj) {
        writeObject(arrayBldr, elementType, item);
      }
      builder.closeEntry();
    } else if (Types.isMapType(type)) {
      BlockBuilder mapBlockBuilder = builder.beginBlockEntry();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
        writeObject(mapBlockBuilder, Types.getKeyType(type), entry.getKey());
        writeObject(mapBlockBuilder, Types.getValueType(type), entry.getValue());
      }
      builder.closeEntry();
    } else {
      TypeUtils.writeNativeValue(type, builder, obj);
    }
  }
}
