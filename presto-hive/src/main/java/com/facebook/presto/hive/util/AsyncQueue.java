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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncQueue<T>
{
    private enum FinishedMarker
    {
        FINISHED
    }

    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final AtomicReference<CompletableFuture<?>> futureReference = new AtomicReference<>(new CompletableFuture<>());

    public void add(T element)
    {
        queue.add(element);
        futureReference.get().complete(null);
    }

    public CompletableFuture<List<T>> getBatchAsync(int maxSize)
    {
        return futureReference.get().thenApply(x -> getBatch(maxSize));
    }

    private List<T> getBatch(int maxSize)
    {
        // wait for at least one element and then take as may extra elements as possible
        // if an error has been registered, the take will succeed immediately because
        // will be at least one finished marker in the queue
        List<Object> elements = new ArrayList<>(maxSize);
        queue.drainTo(elements, maxSize);

        // check if we got the finished marker in our list
        int finishedIndex = elements.indexOf(FinishedMarker.FINISHED);
        if (finishedIndex >= 0) {
            // add the finish marker back to the queue so future callers will not block indefinitely
            queue.add(FinishedMarker.FINISHED);
            // drop all elements after the finish marker (this shouldn't happen in a normal finish, but be safe)
            elements = elements.subList(0, finishedIndex);
        }

        // if the queue is empty and the current future is finished, create a new one so
        // readers can be notified when the queue has elements to read
        if (queue.isEmpty()) {
            CompletableFuture<?> future = futureReference.get();
            if (!future.isDone()) {
                futureReference.compareAndSet(future, new CompletableFuture<>());
            }
        }

        return ImmutableList.copyOf((Collection<T>) elements);
    }

    public void finish()
    {
        queue.add(FinishedMarker.FINISHED);
        futureReference.get().complete(null);
    }

    public boolean isFinished()
    {
        return queue.peek() == FinishedMarker.FINISHED;
    }
}
