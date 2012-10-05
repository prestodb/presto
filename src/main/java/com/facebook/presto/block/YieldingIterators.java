/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class YieldingIterators
{
    public static <T extends TupleStream> Iterable<T> iterate(final QuerySession session, final YieldingIterable<T> iterable)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(iterable, "iterable is null");
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator()
            {
                return iterable.iterator(session);
            }
        };
    }

    public static <T extends TupleStream> YieldingIterator<T> emptyIterator()
    {
        return new EmptyYieldingIterator<>();
    }

    @SafeVarargs
    public static <T extends TupleStream> YieldingIterator<T> yieldingIterable(T... block)
    {
        return yieldingIterator(ImmutableList.copyOf(block).iterator());
    }

    public static <T extends TupleStream> YieldingIterable<T> yieldingIterable(final Iterable<T> source)
    {
        return new YieldingIterableAdapter<>(source);
    }

    public static <T extends TupleStream> YieldingIterator<T> yieldingIterator(Iterator<T> source)
    {
        return new YieldingIteratorAdapter<>(Iterators.peekingIterator(source));
    }

    private static class EmptyYieldingIterator<T extends TupleStream> implements YieldingIterator<T>
    {
        @Override
        public boolean mustYield()
        {
            return false;
        }

        @Override
        public boolean canAdvance()
        {
            return hasNext();
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public T next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public T peek()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class YieldingIterableAdapter<T extends TupleStream> implements YieldingIterable<T>
    {
        private final Iterable<T> source;

        public YieldingIterableAdapter(Iterable<T> source)
        {
            this.source = source;
        }

        @Override
        public YieldingIterator<T> iterator(QuerySession session)
        {
            Preconditions.checkNotNull(session, "session is null");
            return yieldingIterator(source.iterator());
        }
    }

    private static class YieldingIteratorAdapter<T extends TupleStream> implements YieldingIterator<T>
    {
        private final PeekingIterator<T> source;

        public YieldingIteratorAdapter(PeekingIterator<T> source)
        {
            this.source = source;
        }

        @Override
        public boolean mustYield()
        {
            return false;
        }

        @Override
        public boolean canAdvance()
        {
            return hasNext();
        }

        @Override
        public boolean hasNext()
        {
            return source.hasNext();
        }

        @Override
        public T next()
        {
            return source.next();
        }

        @Override
        public T peek()
        {
            return source.peek();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private YieldingIterators()
    {
    }
}
