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
package com.facebook.presto.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class AsyncQueue<T>
{
    private final int targetQueueSize;

    @GuardedBy("this")
    private final Queue<T> elements;
    // This future is completed when the queue transitions from full to not. But it will be replaced by a new instance of future immediately.
    @GuardedBy("this")
    private SettableFuture<?> notFullSignal = SettableFuture.create();
    // This future is completed when the queue transitions from empty to not. But it will be replaced by a new instance of future immediately.
    @GuardedBy("this")
    private SettableFuture<?> notEmptySignal = SettableFuture.create();
    @GuardedBy("this")
    private boolean finishing = false;

    private Executor executor;

    public AsyncQueue(int targetQueueSize, Executor executor)
    {
        checkArgument(targetQueueSize >= 1, "targetQueueSize must be at least 1");
        this.targetQueueSize = targetQueueSize;
        this.elements = new ArrayDeque<>(targetQueueSize * 2);
        this.executor = requireNonNull(executor);
    }

    public synchronized int size()
    {
        return elements.size();
    }

    public synchronized boolean isFinished()
    {
        return finishing && elements.size() == 0;
    }

    public synchronized void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;

        if (elements.size() == 0) {
            completeAsync(executor, notEmptySignal);
            notEmptySignal = SettableFuture.create();
        }
        else if (elements.size() >= targetQueueSize) {
            completeAsync(executor, notFullSignal);
            notFullSignal = SettableFuture.create();
        }
    }

    public synchronized ListenableFuture<?> offer(T element)
    {
        requireNonNull(element);

        if (finishing) {
            return immediateFuture(null);
        }
        elements.add(element);
        int newSize = elements.size();
        if (newSize == 1) {
            completeAsync(executor, notEmptySignal);
            notEmptySignal = SettableFuture.create();
        }
        if (newSize >= targetQueueSize) {
            return notFullSignal;
        }
        return immediateFuture(null);
    }

    private synchronized List<T> getBatch(int maxSize)
    {
        int oldSize = elements.size();
        int reduceBy = Math.min(maxSize, oldSize);
        if (reduceBy == 0) {
            return ImmutableList.of();
        }
        List<T> result = new ArrayList<>(reduceBy);
        for (int i = 0; i < reduceBy; i++) {
            result.add(elements.remove());
        }
        // This checks that the queue size changed from above threshold to below. Therefore, writers shall be notified.
        if (oldSize >= targetQueueSize && oldSize - reduceBy < targetQueueSize) {
            completeAsync(executor, notFullSignal);
            notFullSignal = SettableFuture.create();
        }
        return result;
    }

    public synchronized ListenableFuture<List<T>> getBatchAsync(int maxSize)
    {
        checkArgument(maxSize >= 0, "maxSize must be at least 0");

        List<T> list = getBatch(maxSize);
        if (!list.isEmpty()) {
            return immediateFuture(list);
        }
        else if (finishing) {
            return immediateFuture(ImmutableList.of());
        }
        else {
            return Futures.transform(notEmptySignal, x -> getBatch(maxSize), executor);
        }
    }

    private static void completeAsync(Executor executor, SettableFuture<?> future)
    {
        executor.execute(() -> future.set(null));
    }
}
