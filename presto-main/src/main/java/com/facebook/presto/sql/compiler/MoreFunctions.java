package com.facebook.presto.sql.compiler;

import com.google.common.base.Function;

public class MoreFunctions
{
    public static <F, T> Function<F, T> cast(final Class<T> clazz)
    {
        return new Function<F, T>()
        {
            @Override
            public T apply(F input)
            {
                return clazz.cast(input);
            }
        };
    }
}
