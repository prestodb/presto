package com.facebook.presto.sql.compiler;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

public class IterableTransformer<E>
{
    private final Iterable<E> iterable;

    private IterableTransformer(Iterable<E> iterable)
    {
        this.iterable = iterable;
    }

    public static <T> IterableTransformer<T> on(Iterable<T> iterable)
    {
        return new IterableTransformer<>(iterable);
    }

    public <T> IterableTransformer<T> transform(Function<E, T> function)
    {
        return new IterableTransformer<>(Iterables.transform(iterable, function));
    }

    public <T> IterableTransformer<T> cast(Class<T> clazz)
    {
        return new IterableTransformer<T>(Iterables.transform(iterable, MoreFunctions.cast(clazz)));
    }

    public <T> IterableTransformer<T> transformAndFlatten(Function<E, ? extends Iterable<T>> function)
    {
        return new IterableTransformer<>(Iterables.concat(Iterables.transform(iterable, function)));
    }

    public IterableTransformer<E> select(Predicate<E> predicate)
    {
        return new IterableTransformer<E>(Iterables.filter(iterable, predicate));
    }

    public boolean all(Predicate<E> predicate)
    {
        return Iterables.all(iterable, predicate);
    }

    public boolean any(Predicate<E> predicate)
    {
        return Iterables.any(iterable, predicate);
    }

    public List<E> list()
    {
        return ImmutableList.copyOf(iterable);
    }

    public Set<E> set()
    {
        return ImmutableSet.copyOf(iterable);
    }

    public Iterable<E> all()
    {
        return iterable;
    }

    public E first()
    {
        return Iterables.getFirst(iterable, null);
    }

    public E only()
    {
        return Iterables.getOnlyElement(iterable);
    }

    public E last()
    {
        return Iterables.getLast(iterable);
    }

}
