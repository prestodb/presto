package com.facebook.presto.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Guarantees that no more than maxThreads will be used to execute tasks submitted
 * through execute().
 *
 * There are a few interesting properties:
 * - Multiple BoundedExecutors over a single coreExecutor will have fair sharing
 * of the coreExecutor threads proportional to their relative maxThread counts, but
 * can use less if not as active.
 * - Tasks submitted to a BoundedExecutor is guaranteed to have those tasks handed to
 * threads in that order.
 * - Not susceptible to starvation
 */
@ThreadSafe
public class BoundedExecutor
    implements Executor
{
    private final static Logger log = Logger.get(BoundedExecutor.class);

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

    private final Executor executor;
    private final int maxThreads;

    public BoundedExecutor(Executor executor, int maxThreads)
    {
        Preconditions.checkNotNull(executor, "executor is null");
        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.executor = executor;
        this.maxThreads = maxThreads;
    }

    @Override
    public void execute(Runnable task)
    {
        // Queue maintains the order of task submission
        queue.add(task);
        executor.execute(triggerTask);
        // INVARIANT: every enqueued task is matched with an executeOrMerge() triggerTask
    }

    private void executeOrMerge()
    {
        // Incrementing the queue size enables the enqueued task to be retrieved and run.
        int size = queueSize.incrementAndGet();
        if (size <= maxThreads) {
            do {
                // INVARIANT: No more than maxThreads in this loop at all times
                try {
                    queue.poll().run();
                }
                catch (Throwable e) {
                    log.error(e, "Task failed");
                }
            } while (queueSize.getAndDecrement() > maxThreads);
        }
    }
}
