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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * A simple unbounded queue that can fetch batches of elements asynchronously.
 * <p>
 * This class is designed for multiple writers and multiple readers.
 */
public class AsyncQueue<T>
{
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<?>> notEmptyFuture = new AtomicReference<>(new CompletableFuture<>());

    /**
     * Adds an element to the queue.  Elements added after the queue is finished
     * are normally ignored (without error), but if someone calls finish while
     * {@code add} is running the element may still be added.
     *
     * @throws NullPointerException if the element is null
     */
    public void add(T element)
    {
        requireNonNull(element, "element is null");
        if (finished.get()) {
            return;
        }
        queue.add(element);
        notEmptyFuture.get().complete(null);
    }

    /**
     * Gets a batch of elements from the queue.  If queue is not empty, a
     * completed future containing the queued elements is returned.  If the
     * queue is empty, an incomplete future is returned that completes when
     * an element is added.
     * <p>
     * It is possible that an empty list will be returned as another reader
     * may have consumed all of the data.  A caller should read until, the
     * {@code isFinished} method returns {@code true}.
     *
     * @param maxSize the maximum number of elements to return.
     */
    public CompletableFuture<List<T>> getBatchAsync(int maxSize)
    {
        return notEmptyFuture.get().thenApply(x -> getBatch(maxSize));
    }

    private List<T> getBatch(int maxSize)
    {
        // take up to maxSize elements from the queue
        List<Object> elements = new ArrayList<>(maxSize);
        queue.drainTo(elements, maxSize);

        // if the queue is empty and the current future is finished, create a new one so
        // a new readers can be notified when the queue has elements to read
        if (queue.isEmpty() && !finished.get()) {
            CompletableFuture<?> future = notEmptyFuture.get();
            if (future.isDone()) {
                notEmptyFuture.compareAndSet(future, new CompletableFuture<>());
            }
        }

        // if someone added an element while we were updating the state, complete the current future
        if (!queue.isEmpty() || finished.get()) {
            notEmptyFuture.get().complete(null);
        }

        return ImmutableList.copyOf((Collection<T>) elements);
    }

    /**
     * Finishes the queue. Elements added after the queue is finished
     * are ignored without error.
     */
    public void finish()
    {
        finished.set(true);
        if (queue.isEmpty()) {
            notEmptyFuture.get().complete(null);
        }
    }

    /**
     * Is the queue finished and no more elements can be fetched?
     */
    public boolean isFinished()
    {
        return finished.get() && queue.isEmpty();
    }
}
