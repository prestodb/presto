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
package com.facebook.presto.memory.context;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;

@ThreadSafe
abstract class AbstractAggregatedMemoryContext
        implements AggregatedMemoryContext
{
    static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private boolean closed;

    @Override
    public AbstractAggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new ChildAggregatedMemoryContext(this);
    }

    @Override
    public LocalMemoryContext newLocalMemoryContext()
    {
        return new SimpleLocalMemoryContext(this);
    }

    @Override
    public synchronized long getBytes()
    {
        return usedBytes;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeContext();
        usedBytes = 0;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("usedBytes", usedBytes)
                .add("closed", closed)
                .toString();
    }

    synchronized void addBytes(long bytes)
    {
        usedBytes = addExact(usedBytes, bytes);
    }

    abstract ListenableFuture<?> updateBytes(long bytes);

    abstract boolean tryUpdateBytes(long delta);

    @Nullable
    abstract AbstractAggregatedMemoryContext getParent();

    abstract void closeContext();

    static long addExact(long usedBytes, long bytes)
    {
        try {
            return Math.addExact(usedBytes, bytes);
        }
        catch (ArithmeticException e) {
            throw new RuntimeException(format("Overflow detected. usedBytes: %d, bytes: %d", usedBytes, bytes), e);
        }
    }
}
