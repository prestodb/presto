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
package com.facebook.presto.util;

import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public final class TypedSetUtils
{
    private static Map<Class<?>, Class<? extends Set<?>>> TYPE_TO_TYPED_HASH_SET = ImmutableMap.<Class<?>, Class<? extends Set<?>>>builder()
            .put(boolean.class, BooleanOpenHashSet.class)
            .put(byte.class, ByteOpenHashSet.class)
            .put(char.class, CharOpenHashSet.class)
            .put(double.class, DoubleOpenHashSet.class)
            .put(float.class, FloatOpenHashSet.class)
            .put(int.class, IntOpenHashSet.class)
            .put(long.class, LongOpenHashSet.class)
            .put(short.class, ShortOpenHashSet.class)
            .build();

    public static <T> Set<T> toTypedHashSet(Set<T> set, Class<T> javaElementType)
    {
        Class<? extends Set<T>> typedHashSetClass = typedHashSetClassFor(javaElementType);
        try {
            return typedHashSetClass.getConstructor(Collection.class).newInstance(set);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Could not instantiate typed hash set " + typedHashSetClass, e);
        }
    }

    public static boolean in(boolean booleanValue, BooleanOpenHashSet set)
    {
        return set.contains(booleanValue);
    }

    public static boolean in(byte byteValue, ByteOpenHashSet set)
    {
        return set.contains(byteValue);
    }

    public static boolean in(char charValue, CharOpenHashSet set)
    {
        return set.contains(charValue);
    }

    public static boolean in(double doubleValue, DoubleOpenHashSet set)
    {
        return set.contains(doubleValue);
    }

    public static boolean in(float floatValue, FloatOpenHashSet set)
    {
        return set.contains(floatValue);
    }

    public static boolean in(int intValue, IntOpenHashSet set)
    {
        return set.contains(intValue);
    }

    public static boolean in(long longValue, LongOpenHashSet set)
    {
        return set.contains(longValue);
    }

    public static boolean in(short shortValue, ShortOpenHashSet set)
    {
        return set.contains(shortValue);
    }

    public static boolean in(Object objectValue, ObjectOpenHashSet set)
    {
        return set.contains(objectValue);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? extends Set<T>> typedHashSetClassFor(Class<T> javaType)
    {
        if (TYPE_TO_TYPED_HASH_SET.containsKey(javaType)) {
            return (Class) TYPE_TO_TYPED_HASH_SET.get(javaType);
        }
        else {
            checkState(!javaType.isPrimitive());
            return (Class) ObjectOpenHashSet.class;
        }
    }

    private TypedSetUtils()
    {
    }
}
