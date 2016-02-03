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
package com.facebook.presto.execution;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SharedBufferMemoryManager
{
    private final long maxBufferedBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();

    private final SystemMemoryUsageListener systemMemoryUsageListener;

    public SharedBufferMemoryManager(long maxBufferedBytes, SystemMemoryUsageListener systemMemoryUsageListener)
    {
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
        this.systemMemoryUsageListener = requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null");
    }

    public void updateMemoryUsage(long bytesAdded)
    {
        systemMemoryUsageListener.updateSystemMemoryUsage(bytesAdded);
        bufferedBytes.addAndGet(bytesAdded);
    }

    public double getUtilization()
    {
        return bufferedBytes.get() / (double) maxBufferedBytes;
    }

    public boolean isFull()
    {
        return bufferedBytes.get() >= maxBufferedBytes;
    }
}
