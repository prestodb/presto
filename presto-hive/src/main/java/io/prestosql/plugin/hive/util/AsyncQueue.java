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
package io.prestosql.plugin.hive.util;

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
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
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
    private boolean finishing;
    @GuardedBy("this")
    private int borrowerCount;

    private final Executor executor;

    public AsyncQueue(int targetQueueSize, Executor executor)
    {
        checkArgument(targetQueueSize >= 1, "targetQueueSize must be at least 1");
        this.targetQueueSize = targetQueueSize;
        this.elements = new ArrayDeque<>(targetQueueSize * 2);
        this.executor = requireNonNull(executor);
    }

    /**
     * Returns <tt>true</tt> if all future attempts to retrieve elements from this queue
     * are guaranteed to return empty.
     */
    public synchronized boolean isFinished()
    {
        return finishing && borrowerCount == 0 && elements.size() == 0;
    }

    public synchronized void finish()
    {
        if (finishing) {
            return;
        }
        finishing = true;

        signalIfFinishing();
    }

    private synchronized void signalIfFinishing()
    {
        if (finishing && borrowerCount == 0) {
            if (elements.size() == 0) {
                completeAsync(executor, notEmptySignal);
                notEmptySignal = SettableFuture.create();
            }
            else if (elements.size() >= targetQueueSize) {
                completeAsync(executor, notFullSignal);
                notFullSignal = SettableFuture.create();
            }
        }
    }

    public synchronized ListenableFuture<?> offer(T element)
    {
        requireNonNull(element);

        if (finishing && borrowerCount == 0) {
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
        return borrowBatchAsync(maxSize, elements -> new BorrowResult<>(ImmutableList.of(), elements));
    }

    /**
     * Invoke {@code function} with up to {@code maxSize} elements removed from the head of the queue,
     * and insert elements in the return value to the tail of the queue.
     * <p>
     * If no element is currently available, invocation of {@code function} will be deferred until some
     * element is available, or no more elements will be. Spurious invocation of {@code function} is
     * possible.
     * <p>
     * Insertion through return value of {@code function} will be effective even if {@link #finish()} has been invoked.
     * When borrow (of a non-empty list) is ongoing, {@link #isFinished()} will return false.
     * If an empty list is supplied to {@code function}, it must not return a result indicating intention
     * to insert elements into the queue.
     */
    public <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<T>, BorrowResult<T, O>> function)
    {
        checkArgument(maxSize >= 0, "maxSize must be at least 0");

        ListenableFuture<List<T>> borrowedListFuture;
        synchronized (this) {
            List<T> list = getBatch(maxSize);
            if (!list.isEmpty()) {
                borrowedListFuture = immediateFuture(list);
                borrowerCount++;
            }
            else if (finishing && borrowerCount == 0) {
                borrowedListFuture = immediateFuture(ImmutableList.of());
            }
            else {
                borrowedListFuture = Futures.transform(
                        notEmptySignal,
                        ignored -> {
                            synchronized (this) {
                                List<T> batch = getBatch(maxSize);
                                if (!batch.isEmpty()) {
                                    borrowerCount++;
                                }
                                return batch;
                            }
                        },
                        executor);
            }
        }

        return Futures.transform(
                borrowedListFuture,
                elements -> {
                    // The borrowerCount field was only incremented for non-empty lists.
                    // Decrements should only happen for non-empty lists.
                    // When it should, it must always happen even if the caller-supplied function throws.
                    try {
                        BorrowResult<T, O> borrowResult = function.apply(elements);
                        if (elements.isEmpty()) {
                            checkArgument(borrowResult.getElementsToInsert().isEmpty(), "Function must not insert anything when no element is borrowed");
                            return borrowResult.getResult();
                        }
                        for (T element : borrowResult.getElementsToInsert()) {
                            offer(element);
                        }
                        return borrowResult.getResult();
                    }
                    finally {
                        if (!elements.isEmpty()) {
                            synchronized (this) {
                                borrowerCount--;
                                signalIfFinishing();
                            }
                        }
                    }
                }, directExecutor());
    }

    private static void completeAsync(Executor executor, SettableFuture<?> future)
    {
        executor.execute(() -> future.set(null));
    }

    public static final class BorrowResult<T, R>
    {
        private final List<T> elementsToInsert;
        private final R result;

        public BorrowResult(List<T> elementsToInsert, R result)
        {
            this.elementsToInsert = ImmutableList.copyOf(elementsToInsert);
            this.result = result;
        }

        public List<T> getElementsToInsert()
        {
            return elementsToInsert;
        }

        public R getResult()
        {
            return result;
        }
    }
}
