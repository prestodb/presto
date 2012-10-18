package com.facebook.presto.sql.compiler;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

}
