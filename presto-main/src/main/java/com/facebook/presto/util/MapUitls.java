package com.facebook.presto.util;

import java.util.*;
import java.util.stream.Collectors;

public final class MapUitls {

    private MapUitls() {
    }

    public static <K, V> Collection<K> getTopNKeys(final Map<K, V> map, final long n) {
        return sliceTopN(map.keySet(), n);
    }

    private static <K, V> Collection<V> getTopNValues(Map<K, V> map, long n) {
        return sliceTopN(map.values(), n);
    }

    private static <T> Collection<T> sliceTopN(final Collection<T> collection, final long n) {
        if (collection.isEmpty() || n <= 0) return Collections.emptyList();
        if (n >= collection.size()) return collection;
        return collection.stream().limit(n).collect(Collectors.toList());
    }
}
