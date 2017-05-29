/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public final class CollectionUtils
{

    private CollectionUtils()
    {
    }

    public static <K, V> Map<K, V> failSafeEmpty(Map<K, V> map)
    {
        if (map == null) {
            return ImmutableMap.of();
        }
        return map;
    }

    public static <T> List<T> failSafeEmpty(List<T> list)
    {
        if (list == null) {
            return ImmutableList.of();
        }
        return list;
    }
}
