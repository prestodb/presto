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

import com.google.common.base.Preconditions;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * THIS CLASS IS COPIED FROM com.facebook.presto.util.BoundedExecutor
 * <p/>
 * Guarantees that no more than maxThreads will be used to execute tasks submitted
 * through execute().
 * <p/>
 * There are a few interesting properties:
 * - Multiple BoundedExecutors over a single coreExecutor will have fair sharing
 * of the coreExecutor threads proportional to their relative maxThread counts, but
 * can use less if not as active.
 * - Tasks submitted to a BoundedExecutor is guaranteed to have those tasks handed to
 * threads in that order.
 * - Will not encounter starvation
 */
@ThreadSafe
public class BoundedExecutor
        implements Executor
{
    private static final Logger log = Logger.get(BoundedExecutor.class);

    private final Queue<Runnable> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final Runnable triggerTask = new Runnable()
    {
        @Override
        public void run()
        {
            executeOrMerge();
        }
    };

    private final Executor coreExecutor;
    private final int maxThreads;

    public BoundedExecutor(Executor coreExecutor, int maxThreads)
    {
        Preconditions.checkNotNull(coreExecutor, "coreExecutor is null");
        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.coreExecutor = coreExecutor;
        this.maxThreads = maxThreads;
    }

    @Override
    public void execute(Runnable task)
    {
        queue.add(task);
        coreExecutor.execute(triggerTask);
        // INVARIANT: every enqueued task is matched with an executeOrMerge() triggerTask
    }

    private void executeOrMerge()
    {
        int size = queueSize.incrementAndGet();
        if (size <= maxThreads) {
            do {
                try {
                    queue.poll().run();
                }
                catch (Throwable e) {
                    log.error(e, "Task failed");
                }
            }
            while (queueSize.getAndDecrement() > maxThreads);
        }
    }
}
