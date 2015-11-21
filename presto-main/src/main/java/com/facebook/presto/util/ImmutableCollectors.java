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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;

import java.util.function.Function;
import java.util.stream.Collector;

public final class ImmutableCollectors
{
    private ImmutableCollectors() {}

    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableList()
    {
        return Collector.<T, ImmutableList.Builder<T>, ImmutableList<T>>of(
                ImmutableList.Builder::new,
                ImmutableList.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableList.Builder::build);
    }

    public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet()
    {
        return Collector.<T, ImmutableSet.Builder<T>, ImmutableSet<T>>of(
                ImmutableSet.Builder::new,
                ImmutableSet.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableSet.Builder::build,
                Collector.Characteristics.UNORDERED);
    }

    public static <T> Collector<T, ?, ImmutableMultiset<T>> toImmutableMultiset()
    {
        return Collector.<T, ImmutableMultiset.Builder<T>, ImmutableMultiset<T>>of(
                ImmutableMultiset.Builder::new,
                ImmutableMultiset.Builder::add,
                (left, right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableMultiset.Builder::build,
                Collector.Characteristics.UNORDERED);
    }

    public static <K, V> Collector<V, ?, ImmutableMap<K, V>> toImmutableMap(Function<V, K> keyMapper)
    {
        return Collector.<V, ImmutableMap.Builder<K, V>, ImmutableMap<K, V>>of(
                ImmutableMap.Builder::new,
                (builder, value) -> builder.put(keyMapper.apply(value), value),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                ImmutableMap.Builder::build);
    }
}
