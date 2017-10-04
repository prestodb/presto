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

import com.facebook.presto.spi.LocalTrackingContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class LocalMemoryContext
        implements LocalTrackingContext
{
    @GuardedBy("this")
    private final AggregatedMemoryContext parentMemoryContext;
    @Nullable
    @GuardedBy("this")
    private TrackingNotificationListener notificationListener;
    @GuardedBy("this")
    private long usedBytes;

    public LocalMemoryContext(AggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext);
    }

    public synchronized long getBytes()
    {
        return usedBytes;
    }

    public synchronized void setBytes(long bytes)
    {
        long oldLocalUsedBytes = this.usedBytes;
        this.usedBytes = bytes;
        parentMemoryContext.addBytes(this.usedBytes - oldLocalUsedBytes);
        if (notificationListener != null) {
            notificationListener.usageUpdated(oldLocalUsedBytes, usedBytes);
        }
    }

    public synchronized void addBytes(long bytes)
    {
        setBytes(usedBytes + bytes);
    }

    public synchronized void setNotificationListener(TrackingNotificationListener notificationListener)
    {
        this.notificationListener = notificationListener;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LocalMemoryContext{");
        sb.append("usedBytes=").append(usedBytes);
        sb.append('}');
        return sb.toString();
    }
}
