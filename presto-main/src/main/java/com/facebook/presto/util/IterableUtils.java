package com.facebook.presto.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IterableUtils
{
    public static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }

    // TODO: replace with Maps.toMap when Guava 14 comes out (http://code.google.com/p/guava-libraries/issues/detail?id=56)
    public static <K, V> Map<K, V> toMap(Iterable<K> keys, Function<K, V> valueFunction)
    {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (K key : ImmutableSet.copyOf(keys)) {
            builder.put(key, valueFunction.apply(key));
        }
        return builder.build();
    }

    public static <T> List<T> limit(Iterable<T> iterable, int limitSize)
    {
        return ImmutableList.copyOf(Iterables.limit(iterable, limitSize));
    }

    public static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
