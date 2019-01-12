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
package io.prestosql.plugin.raptor.legacy.util;

import org.skife.jdbi.v2.ResultIterator;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SynchronizedResultIterator<T>
        implements ResultIterator<T>
{
    @GuardedBy("this")
    private final ResultIterator<T> iterator;

    @GuardedBy("this")
    private boolean closed;

    public SynchronizedResultIterator(ResultIterator<T> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    @Override
    public synchronized boolean hasNext()
    {
        checkState(!closed, "already closed");
        return iterator.hasNext();
    }

    @Override
    public synchronized T next()
    {
        checkState(!closed, "already closed");
        return iterator.next();
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            iterator.close();
        }
    }
}
