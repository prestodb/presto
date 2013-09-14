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
package com.facebook.presto.util;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Bound the number of tasks executed currently for a given key
 */
@ThreadSafe
public class KeyBoundedExecutor<T>
{
    @GuardedBy("this")
    private final Map<T, CountedReference<BoundedExecutor>> executors = new HashMap<>();
    private final ExecutorService executorService;
    private final int maxThreads;

    public KeyBoundedExecutor(ExecutorService executorService)
    {
        this(executorService, 1);
    }

    public KeyBoundedExecutor(ExecutorService executorService, int maxThreads)
    {
        checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.maxThreads = maxThreads;
    }

    public synchronized boolean isActive(T key)
    {
        return executors.containsKey(key);
    }

    public void execute(final T key, final Runnable task)
    {
        final CountedReference<BoundedExecutor> reference = acquireExecutor(key);
        reference.get().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    task.run();
                }
                finally {
                    returnExecutor(key, reference);
                }
            }
        });
    }

    private synchronized CountedReference<BoundedExecutor> acquireExecutor(T key)
    {
        CountedReference<BoundedExecutor> reference = executors.get(key);
        if (reference == null) {
            reference = new CountedReference<>(new BoundedExecutor(executorService, maxThreads));
            executors.put(key, reference);
        }
        else {
            reference.increment();
        }
        return reference;
    }

    private synchronized void returnExecutor(T key, CountedReference<BoundedExecutor> reference)
    {
        reference.decrement();
        if (!reference.isReferenced()) {
            executors.remove(key);
        }
    }

    @NotThreadSafe
    private static class CountedReference<T>
    {
        private final T reference;
        private int count = 1;

        private CountedReference(T reference)
        {
            this.reference = checkNotNull(reference, "reference is null");
        }

        public boolean isReferenced()
        {
            return count > 0;
        }

        public void increment()
        {
            checkState(isReferenced(), "Reference counter misused: %s", count);
            count++;
        }

        public void decrement()
        {
            checkState(isReferenced(), "Reference counter misused: %s", count);
            count--;
        }

        public T get()
        {
            return reference;
        }
    }
}
