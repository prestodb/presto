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

import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

import java.util.Collection;
import java.util.Set;

public final class FastutilSetHelper
{
    private FastutilSetHelper() {}

    @SuppressWarnings({"unchecked"})
    public static Set<?> toFastutilHashSet(Set<?> set, Class<?> javaElementType)
    {
        if (javaElementType == boolean.class) {
            return new BooleanOpenHashSet((Collection<Boolean>) set);
        }
        if (javaElementType == byte.class) {
            return new ByteOpenHashSet((Collection<Byte>) set);
        }
        if (javaElementType == char.class) {
            return new CharOpenHashSet((Collection<Character>) set);
        }
        if (javaElementType == double.class) {
            return new DoubleOpenHashSet((Collection<Double>) set);
        }
        if (javaElementType == float.class) {
            return new FloatOpenHashSet((Collection<Float>) set);
        }
        if (javaElementType == int.class) {
            return new IntOpenHashSet((Collection<Integer>) set);
        }
        if (javaElementType == long.class) {
            return new LongOpenHashSet((Collection<Long>) set);
        }
        if (javaElementType == short.class) {
            return new ShortOpenHashSet((Collection<Short>) set);
        }
        return new ObjectOpenHashSet(set);
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

    public static boolean in(Object objectValue, ObjectOpenHashSet<?> set)
    {
        return set.contains(objectValue);
    }
}
