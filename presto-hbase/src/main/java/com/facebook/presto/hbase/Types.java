package com.facebook.presto.hbase;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;

/**
 * Utility class for Presto Type-related functionality.
 * 
 * @author spancer.ray
 */
public final class Types {
  private Types() {}

  public static boolean isArrayType(Type type) {
    return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
  }

  public static boolean isMapType(Type type) {
    return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
  }

  /**
   * Gets the element type of the given array type. Does not validate that the given type is an
   * array.
   *
   * @param type An array type
   * @return Element type of the array
   * @throws IndexOutOfBoundsException If type is not an array
   * @see Types#isArrayType
   */
  public static Type getElementType(Type type) {
    return type.getTypeParameters().get(0);
  }

  /**
   * Gets the key type of the given map type. Does not validate that the given type is a map.
   *
   * @param type A map type
   * @return Key type of the map
   * @throws IndexOutOfBoundsException If type is not a map
   * @see Types#isMapType
   */
  public static Type getKeyType(Type type) {
    return type.getTypeParameters().get(0);
  }

  /**
   * Gets the value type of the given map type. Does not validate that the given type is a map.
   *
   * @param type A map type
   * @return Value type of the map
   * @throws IndexOutOfBoundsException If type is not a map
   * @see Types#isMapType
   */
  public static Type getValueType(Type type) {
    return type.getTypeParameters().get(1);
  }
}
