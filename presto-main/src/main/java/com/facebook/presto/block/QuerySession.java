/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class QuerySession
{
    private final Map<Object, Object> data = new HashMap<>();

    public Object getData(Object key)
    {
        return data.get(key);
    }

    public Object putData(Object key, @Nullable Object value)
    {
        return data.put(key, value);
    }
}
