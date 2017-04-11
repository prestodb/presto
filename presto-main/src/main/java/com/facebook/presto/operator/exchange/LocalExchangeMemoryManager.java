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
package com.facebook.presto.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;

@ThreadSafe
public class LocalExchangeMemoryManager
{
    private static final SettableFuture<?> NOT_FULL;

    static {
        NOT_FULL = SettableFuture.create();
        NOT_FULL.set(null);
    }

    private final long maxBufferedBytes;
    private final AtomicLong bufferedBytes = new AtomicLong();

    @GuardedBy("this")
    private SettableFuture<?> notFullFuture = NOT_FULL;

    private final AtomicBoolean blockOnFull = new AtomicBoolean(true);

    public LocalExchangeMemoryManager(long maxBufferedBytes)
    {
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be > 0");
        this.maxBufferedBytes = maxBufferedBytes;
    }

    public void updateMemoryUsage(long bytesAdded)
    {
        SettableFuture<?> future;
        synchronized (this) {
            bufferedBytes.addAndGet(bytesAdded);

            // if we are full, then breakout
            if (bufferedBytes.get() > maxBufferedBytes || notFullFuture.isDone()) {
                return;
            }

            // otherwise, we are not full, so complete the future
            future = notFullFuture;
            notFullFuture = NOT_FULL;
        }

        // complete future outside of lock since this can invoke callbacks
        future.set(null);
    }

    public synchronized ListenableFuture<?> getNotFullFuture()
    {
        // if we are full and still blocking and the current not full future is already complete, create a new one
        if (bufferedBytes.get() > maxBufferedBytes && blockOnFull.get() && notFullFuture.isDone()) {
            notFullFuture = SettableFuture.create();
        }
        return notFullFuture;
    }

    public void setNoBlockOnFull()
    {
        blockOnFull.set(false);

        SettableFuture<?> future;
        synchronized (this) {
            if (notFullFuture.isDone()) {
                return;
            }

            future = notFullFuture;
            notFullFuture = NOT_FULL;
        }

        // complete future outside of lock since this can invoke callbacks
        future.set(null);
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }
}
