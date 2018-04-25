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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class OutputBufferMemoryManager
{
    private final long maxBufferedBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();
    private final AtomicLong peakMemoryUsage = new AtomicLong();
    @GuardedBy("this")
    private SettableFuture<?> notFull;

    private final AtomicBoolean blockOnFull = new AtomicBoolean(true);

    private final Supplier<LocalMemoryContext> systemMemoryContextSupplier;
    private final Executor notificationExecutor;

    public OutputBufferMemoryManager(long maxBufferedBytes, Supplier<LocalMemoryContext> systemMemoryContextSupplier, Executor notificationExecutor)
    {
        requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null");
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
        this.systemMemoryContextSupplier = Suppliers.memoize(systemMemoryContextSupplier::get);
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

        notFull = SettableFuture.create();
        notFull.set(null);
    }

    public synchronized void updateMemoryUsage(long bytesAdded)
    {
        long currentBufferedBytes = bufferedBytes.addAndGet(bytesAdded);
        peakMemoryUsage.accumulateAndGet(currentBufferedBytes, Math::max);
        systemMemoryContextSupplier.get().setBytes(currentBufferedBytes);
        if (!isBufferFull() && !notFull.isDone()) {
            // Complete future in a new thread to avoid making a callback on the caller thread.
            // This make is easier for callers to use this class since they can update the memory
            // usage while holding locks.
            SettableFuture<?> future = this.notFull;
            notificationExecutor.execute(() -> future.set(null));
        }
    }

    public synchronized ListenableFuture<?> getNotFullFuture()
    {
        if (isBufferFull() && notFull.isDone()) {
            notFull = SettableFuture.create();
        }
        return notFull;
    }

    public synchronized void setNoBlockOnFull()
    {
        blockOnFull.set(false);

        // Complete future in a new thread to avoid making a callback on the caller thread.
        SettableFuture<?> future = notFull;
        notificationExecutor.execute(() -> future.set(null));
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }

    public double getUtilization()
    {
        return bufferedBytes.get() / (double) maxBufferedBytes;
    }

    public synchronized boolean isOverutilized()
    {
        return isBufferFull();
    }

    private synchronized boolean isBufferFull()
    {
        return bufferedBytes.get() > maxBufferedBytes && blockOnFull.get();
    }

    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }
}
