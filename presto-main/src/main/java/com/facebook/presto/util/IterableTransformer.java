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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class IterableTransformer<E>
{
    private final Iterable<E> iterable;

    IterableTransformer(Iterable<E> iterable)
    {
        this.iterable = iterable;
    }

    public static <T> IterableTransformer<T> on(Iterable<T> iterable)
    {
        return new IterableTransformer<>(iterable);
    }

    public <T> IterableTransformer<T> transform(Function<? super E, T> function)
    {
        return new IterableTransformer<>(Iterables.transform(iterable, function));
    }

    public <T> IterableTransformer<T> cast(final Class<T> clazz)
    {
        return new IterableTransformer<>(Iterables.transform(iterable, new Function<E, T>()
        {
            @Override
            public T apply(E input)
            {
                return clazz.cast(input);
            }
        }));
    }

    public <T> IterableTransformer<T> transformAndFlatten(Function<? super E, ? extends Iterable<T>> function)
    {
        return new IterableTransformer<>(Iterables.concat(Iterables.transform(iterable, function)));
    }

    public <T> NestedIterableTransformer<T> transformNested(Function<? super E, ? extends Iterable<T>> function)
    {
        return new NestedIterableTransformer<>(Iterables.transform(iterable, function));
    }

    public IterableTransformer<E> select(Predicate<? super E> predicate)
    {
        return new IterableTransformer<>(Iterables.filter(iterable, predicate));
    }

    public IterableTransformer<E> orderBy(Comparator<E> ordering)
    {
        return new IterableTransformer<>(Ordering.from(ordering).sortedCopy(iterable));
    }

    public boolean all(Predicate<E> predicate)
    {
        return Iterables.all(iterable, predicate);
    }

    public boolean any(Predicate<E> predicate)
    {
        return Iterables.any(iterable, predicate);
    }

    public <K> MapTransformer<K, E> uniqueIndex(Function<? super E, K> keyFunction)
    {
        return new MapTransformer<>(Maps.uniqueIndex(iterable, keyFunction));
    }

    public <K> Multimap<K, E> index(Function<? super E, K> keyFunction)
    {
        return Multimaps.index(iterable, keyFunction);
    }

    public <V> MapTransformer<E, V> toMap(Function<? super E, V> valueFunction)
    {
        return new MapTransformer<>(Maps.toMap(iterable, valueFunction));
    }

    public List<E> list()
    {
        return ImmutableList.copyOf(iterable);
    }

    public Set<E> set()
    {
        return ImmutableSet.copyOf(iterable);
    }

    public Multiset<E> bag()
    {
        return ImmutableMultiset.copyOf(iterable);
    }

    public Iterable<E> all()
    {
        return iterable;
    }

    public E first()
    {
        return Iterables.getFirst(iterable, null);
    }

    public E only()
    {
        return Iterables.getOnlyElement(iterable);
    }

    public E last()
    {
        return Iterables.getLast(iterable);
    }
}
