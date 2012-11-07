package com.facebook.presto.util;

import com.google.common.base.Function;

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
}
