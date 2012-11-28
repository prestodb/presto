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

}
