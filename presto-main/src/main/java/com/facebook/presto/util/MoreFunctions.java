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

import com.google.common.base.Function;

import javax.annotation.Nullable;

import java.util.Map;

public class MoreFunctions
{
    public static Function<String, String> toLowerCase()
    {
        return new Function<String, String>()
        {
            @Override
            public String apply(String s)
            {
                return s.toLowerCase();
            }
        };
    }

    public static <K, V> Function<Map.Entry<K, V>, V> valueGetter()
    {
        return new Function<Map.Entry<K, V>, V>()
        {
            @Nullable
            @Override
            public V apply(Map.Entry<K, V> input)
            {
                return input.getValue();
            }
        };
    }

    public static <K, V> Function<Map.Entry<K, V>, K> keyGetter()
    {
        return new Function<Map.Entry<K, V>, K>()
        {
            @Nullable
            @Override
            public K apply(Map.Entry<K, V> input)
            {
                return input.getKey();
            }
        };
    }

    public static <I, T extends I> Function<I, T> cast(final Class<T> clazz)
    {
        return new Function<I, T>()
        {
            @Override
            public T apply(I input)
            {
                return clazz.cast(input);
            }
        };
    }
}
