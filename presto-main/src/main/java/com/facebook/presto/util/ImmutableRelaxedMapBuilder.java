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

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class ImmutableRelaxedMapBuilder<K, V>
{
    private final Map<K, V> map = new HashMap<>();

    public static <K, V> ImmutableRelaxedMapBuilder<K, V> builder()
    {
        return new ImmutableRelaxedMapBuilder<>();
    }

    public ImmutableMap<K, V> build()
    {
        return ImmutableMap.copyOf(map);
    }

    public ImmutableRelaxedMapBuilder<K, V> put(K key, V value)
    {
        requireNonNull(value, "value is null");
        if (map.containsKey(key) && !value.equals(map.get(key))) {
            throw new IllegalArgumentException(
                    "Multiple entries with same " + key + ": " + map.get(key) + " and " + value);
        }
        map.put(key, value);
        return this;
    }

    public ImmutableRelaxedMapBuilder<K, V> put(Map.Entry<? extends K, ? extends V> entry)
    {
        requireNonNull(entry, "entry is null");
        put(entry.getKey(), entry.getValue());
        return this;
    }

    public ImmutableRelaxedMapBuilder<K, V> putAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries)
    {
        requireNonNull(entries, "entries is null");
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
            put(entry);
        }
        return this;
    }

    public ImmutableRelaxedMapBuilder<K, V> putAll(Map<? extends K, ? extends V> map)
    {
        requireNonNull(map, "map is null");
        putAll(map.entrySet());
        return this;
    }
}
