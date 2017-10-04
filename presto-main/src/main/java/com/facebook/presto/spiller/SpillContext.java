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
package com.facebook.presto.spiller;

import com.facebook.presto.spi.AggregateTrackingContext;
import com.facebook.presto.spi.memory.TrackingNotificationListener;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SpillContext
        implements AggregateTrackingContext
{
    @Nullable
    @GuardedBy("this")
    private final SpillContext parentSpillContext;
    @GuardedBy("this")
    private long spilledBytes;
    @GuardedBy("this")
    private boolean closed;
    @Nullable
    @GuardedBy("this")
    private TrackingNotificationListener notificationListener;

    public SpillContext()
    {
        this.parentSpillContext = null;
    }

    public SpillContext newLocalSpillContext()
    {
        return new SpillContext(this);
    }

    private SpillContext(SpillContext parentSpillContext)
    {
        this.parentSpillContext = requireNonNull(parentSpillContext, "parentSpillContext is null");
    }

    @Override
    public synchronized void addBytes(long bytes)
    {
        checkState(!closed, "Already closed");
        long oldSpilledBytes = this.spilledBytes;
        if (parentSpillContext != null) {
            parentSpillContext.addBytes(bytes);
        }
        spilledBytes += bytes;
        if (notificationListener != null) {
            notificationListener.usageUpdated(oldSpilledBytes, spilledBytes);
        }
    }

    @Override
    public void setNotificationListener(TrackingNotificationListener notificationListener)
    {
        this.notificationListener = requireNonNull(notificationListener, "notificationListener is null");
    }

    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (parentSpillContext != null) {
            parentSpillContext.addBytes(-spilledBytes);
        }
        spilledBytes = 0;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("SpillContext{");
        sb.append("spilledBytes=").append(spilledBytes).append(", ");
        sb.append("closed=").append(closed);
        sb.append('}');
        return sb.toString();
    }
}
