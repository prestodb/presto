package com.facebook.presto.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.Random;
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
 * - Will not encounter starvation
 */
@ThreadSafe
public class BoundedExecutor
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

    private final ExecutorService coreExecutor;
    private final int maxThreads;

    public BoundedExecutor(ExecutorService coreExecutor, int maxThreads)
    {
        Preconditions.checkNotNull(coreExecutor, "coreExecutor is null");
        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.coreExecutor = coreExecutor;
        this.maxThreads = maxThreads;
    }

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
            } while (queueSize.getAndDecrement() > maxThreads);
        }
    }
}
