package com.facebook.presto.util;

import com.google.common.collect.Iterables;

public class NestedIterableTransformer<E>
{
    private final Iterable<Iterable<E>> iterable;

    public NestedIterableTransformer(Iterable<Iterable<E>> iterable)
    {
        this.iterable = iterable;
    }

    public IterableTransformer<E> flatten()
    {
        return new IterableTransformer<>(Iterables.concat(iterable));
    }
}
