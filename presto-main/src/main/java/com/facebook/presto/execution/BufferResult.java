/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class BufferResult<T>
{
    public static <T> BufferResult<T> emptyResults(boolean bufferClosed)
    {
        return new BufferResult<>(bufferClosed, ImmutableList.<T>of());
    }

    @SafeVarargs
    public static <T> BufferResult<T> bufferResult(T firstElement, T... otherElements)
    {
        return new BufferResult<>(false, ImmutableList.<T>builder().add(firstElement).add(otherElements).build());
    }

    private final boolean bufferClosed;
    private final List<T> elements;

    public BufferResult(boolean bufferClosed, List<T> elements)
    {
        this.bufferClosed = bufferClosed;
        this.elements = ImmutableList.copyOf(checkNotNull(elements, "pages is null"));
    }

    public boolean isBufferClosed()
    {
        return bufferClosed;
    }

    public List<T> getElements()
    {
        return elements;
    }

    public int size()
    {
        return elements.size();
    }

    public boolean isEmpty()
    {
        return elements.isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bufferClosed, elements);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BufferResult<?> other = (BufferResult<?>) obj;
        return Objects.equal(this.bufferClosed, other.bufferClosed) && Objects.equal(this.elements, other.elements);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferClosed", bufferClosed)
                .add("elements", elements)
                .toString();
    }
}
