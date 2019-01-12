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
package io.prestosql.plugin.accumulo;

import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

/**
 * Utility class for Presto Type-related functionality.
 */
public final class Types
{
    private Types() {}

    public static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    /**
     * Gets the element type of the given array type. Does not validate that the given type is an array.
     *
     * @param type An array type
     * @return Element type of the array
     * @throws IndexOutOfBoundsException If type is not an array
     * @see Types#isArrayType
     */
    public static Type getElementType(Type type)
    {
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
    public static Type getKeyType(Type type)
    {
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
    public static Type getValueType(Type type)
    {
        return type.getTypeParameters().get(1);
    }
}
