package com.facebook.presto.sql.analyzer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public class Optionals
{
    public static Predicate<Optional<?>> isPresentPredicate()
    {
        return new Predicate<Optional<?>>()
        {
            @Override
            public boolean apply(Optional<?> input)
            {
                return input.isPresent();
            }
        };
    }

    public static <T> Function<Optional<T>, T> optionalGetter()
    {
        return new Function<Optional<T>, T>()
        {
            @Override
            public T apply(Optional<T> input)
            {
                return input.get();
            }
        };
    }
}
