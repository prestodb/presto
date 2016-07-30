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
package com.facebook.presto.accumulo;

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Utility class for Presto Type-related functionality.
 */
public final class Types
{
    private Types() {}

    /**
     * Validates the given value is an instance of the target class.
     *
     * @param <A> Generic type for value
     * @param <B> Generic type for the target, which extends A
     * @param value Instance of an object
     * @param target Expected class type of the value
     * @param name Helpful name for the object which is used for error reporting
     * @return The given value cast to the target
     * @throws NullPointerException If value is null
     * @throws IllegalArgumentException If the value is not an instance of target
     */
    public static <A, B extends A> B checkType(A value, Class<B> target, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value), "%s must be of type %s, not %s", name, target.getName(), value.getClass().getName());
        return target.cast(value);
    }

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
