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
package io.prestosql.execution.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.memory.context.LocalMemoryContext;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * OutputBufferMemoryManager will block when any condition below holds
 * - the number of buffered bytes exceeds maxBufferedBytes and blockOnFull is true
 * - the memory pool is exhausted
 */
@ThreadSafe
class OutputBufferMemoryManager
{
    private final long maxBufferedBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();
    private final AtomicLong peakMemoryUsage = new AtomicLong();

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private SettableFuture<?> bufferBlockedFuture;
    @GuardedBy("this")
    private ListenableFuture<?> blockedOnMemory = Futures.immediateFuture(null);

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

        bufferBlockedFuture = SettableFuture.create();
        bufferBlockedFuture.set(null);
    }

    public synchronized void updateMemoryUsage(long bytesAdded)
    {
        Optional<LocalMemoryContext> systemMemoryContext = getSystemMemoryContext();

        // If closed is true, that means the task is completed. In that state,
        // the output buffers already ignore the newly added pages, and therefore
        // we can also safely ignore any calls after OutputBufferMemoryManager is closed.
        // If the systemMemoryContext doesn't exist, the task is probably already
        // aborted, so we can just return (see the comment in getSystemMemoryContext()).
        if (closed || !systemMemoryContext.isPresent()) {
            return;
        }

        long currentBufferedBytes = bufferedBytes.addAndGet(bytesAdded);
        peakMemoryUsage.accumulateAndGet(currentBufferedBytes, Math::max);
        this.blockedOnMemory = systemMemoryContext.get().setBytes(currentBufferedBytes);
        if (!isBufferFull() && !isBlockedOnMemory() && !bufferBlockedFuture.isDone()) {
            // Complete future in a new thread to avoid making a callback on the caller thread.
            // This make is easier for callers to use this class since they can update the memory
            // usage while holding locks.
            SettableFuture<?> future = this.bufferBlockedFuture;
            notificationExecutor.execute(() -> future.set(null));
            return;
        }
        this.blockedOnMemory.addListener(this::onMemoryAvailable, notificationExecutor);
    }

    public synchronized ListenableFuture<?> getBufferBlockedFuture()
    {
        if ((isBufferFull() || isBlockedOnMemory()) && bufferBlockedFuture.isDone()) {
            bufferBlockedFuture = SettableFuture.create();
        }
        return bufferBlockedFuture;
    }

    public synchronized void setNoBlockOnFull()
    {
        blockOnFull.set(false);

        // Complete future in a new thread to avoid making a callback on the caller thread.
        SettableFuture<?> future = this.bufferBlockedFuture;
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

    private synchronized boolean isBlockedOnMemory()
    {
        return !blockedOnMemory.isDone();
    }

    @VisibleForTesting
    synchronized void onMemoryAvailable()
    {
        // Do not notify the listeners if the buffer is full
        if (bufferedBytes.get() > maxBufferedBytes) {
            return;
        }

        // notify listeners if the buffer is not full
        SettableFuture<?> future = this.bufferBlockedFuture;
        notificationExecutor.execute(() -> future.set(null));
    }

    public long getPeakMemoryUsage()
    {
        return peakMemoryUsage.get();
    }

    public synchronized void close()
    {
        updateMemoryUsage(-bufferedBytes.get());
        getSystemMemoryContext().ifPresent(LocalMemoryContext::close);
        closed = true;
    }

    private Optional<LocalMemoryContext> getSystemMemoryContext()
    {
        try {
            return Optional.of(systemMemoryContextSupplier.get());
        }
        catch (RuntimeException ignored) {
            // This is possible with races, e.g., a task is created and then immediately aborted,
            // so that the task context hasn't been created yet (as a result there's no memory context available).
        }
        return Optional.empty();
    }
}
