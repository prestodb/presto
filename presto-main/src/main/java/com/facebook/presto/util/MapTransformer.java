package com.facebook.presto.util;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Map;

public class MapTransformer<K, V>
{
    private final Map<K, V> map;

    public MapTransformer(Map<K, V> map)
    {
        this.map = map;
    }

    public <V1> MapTransformer<K, V1> transformValues(Function<V, V1> function)
    {
        return new MapTransformer<>(Maps.transformValues(map, function));
    }

    public Map<K, V> map()
    {
        return map;
    }
}
