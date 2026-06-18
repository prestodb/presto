/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.jdbc;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This {@code Iterator} is like Guava's {@code Iterators.limit()} but uses a {@code long} limit instead of {@code int}.
 */
final class LengthLimitedIterator<T>
        implements Iterator<T>
{
    private final Iterator<T> iterator;
    private final long limit;
    private long count;

    public LengthLimitedIterator(Iterator<T> iterator, long limit)
    {
        checkArgument(limit >= 0, "limit is negative");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.limit = limit;
    }

    @Override
    public boolean hasNext()
    {
        return count < limit && iterator.hasNext();
    }

    @Override
    public T next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        count++;
        return iterator.next();
    }
}
