/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class BlockIterators
{
    public static <T extends TupleStream> BlockIterator<T> emptyIterator()
    {
        return new EmptyBlockIterator<>();
    }

    @SafeVarargs
    public static <T extends TupleStream> BlockIterator<T> newBlockIterator(T... block)
    {
        return toBlockIterator(ImmutableList.copyOf(block).iterator());
    }

    public static <T extends TupleStream> BlockIterable<T> toBlockIterable(final Iterable<T> source)
    {
        return new StaticBlockIterable<>(source);
    }

    public static <T extends TupleStream> BlockIterator<T> toBlockIterator(Iterator<T> source)
    {
        return new StaticBlockIterator<>(Iterators.peekingIterator(source));
    }

    private static class EmptyBlockIterator<T extends TupleStream> implements BlockIterator<T>
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

    private static class StaticBlockIterable<T extends TupleStream> implements BlockIterable<T>
    {
        private final Iterable<T> source;

        public StaticBlockIterable(Iterable<T> source)
        {
            this.source = source;
        }

        @Override
        public BlockIterator<T> iterator()
        {
            return toBlockIterator(source.iterator());
        }
    }

    private static class StaticBlockIterator<T extends TupleStream> implements BlockIterator<T>
    {
        private final PeekingIterator<T> source;

        public StaticBlockIterator(PeekingIterator<T> source)
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

    private BlockIterators()
    {
    }
}
