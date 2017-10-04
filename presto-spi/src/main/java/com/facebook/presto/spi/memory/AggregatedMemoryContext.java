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
package com.facebook.presto.spi.memory;

import com.facebook.presto.spi.AggregateTrackingContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class AggregatedMemoryContext
        implements AggregateTrackingContext
{
    @Nullable
    @GuardedBy("this")
    private final AggregatedMemoryContext parentMemoryContext;
    @GuardedBy("this")
    private long usedBytes;
    @GuardedBy("this")
    private boolean closed;

    public AggregatedMemoryContext()
    {
        this.parentMemoryContext = null;
    }

    private AggregatedMemoryContext(AggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
    }

    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new AggregatedMemoryContext(this);
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return new LocalMemoryContext(this);
    }

    public synchronized long getBytes()
    {
        checkState(!closed);
        return usedBytes;
    }

    public synchronized void addBytes(long bytes)
    {
        checkState(!closed);
        if (parentMemoryContext != null) {
            parentMemoryContext.addBytes(bytes);
        }
        usedBytes += bytes;
    }

    @Override
    public void setNotificationListener(TrackingNotificationListener listener) {}

    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (parentMemoryContext != null) {
            parentMemoryContext.addBytes(-usedBytes);
        }
        usedBytes = 0;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("AggregatedMemoryContext{");
        sb.append("usedBytes=").append(usedBytes).append(", ");
        sb.append("closed=").append(closed);
        sb.append('}');
        return sb.toString();
    }

    private static void checkState(boolean expression)
    {
        if (!expression) {
            throw new IllegalStateException();
        }
    }
}
