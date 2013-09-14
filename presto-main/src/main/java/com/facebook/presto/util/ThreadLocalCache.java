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

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides a ThreadLocal cache with a maximum cache size per thread.
 * Values must not be null.
 *
 * @param <K> - cache key type
 * @param <V> - cache value type
 */
public abstract class ThreadLocalCache<K, V>
{
    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<LinkedHashMap<K, V>> cache;

    public ThreadLocalCache(final int maxSizePerThread)
    {
        checkArgument(maxSizePerThread > 0, "max size must be greater than zero");
        cache = new ThreadLocal<LinkedHashMap<K, V>>()
        {
            @SuppressWarnings({"CloneableClassWithoutClone", "ClassExtendsConcreteCollection"})
            @Override
            protected LinkedHashMap<K, V> initialValue()
            {
                return new LinkedHashMap<K, V>()
                {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
                    {
                        return size() > maxSizePerThread;
                    }
                };
            }
        };
    }

    @Nonnull
    protected abstract V load(K key);

    public final V get(K key)
    {
        LinkedHashMap<K, V> map = cache.get();

        V value = map.get(key);
        if (value != null) {
            return value;
        }

        value = load(key);
        checkNotNull(value, "value must not be null");
        map.put(key, value);
        return value;
    }
}
