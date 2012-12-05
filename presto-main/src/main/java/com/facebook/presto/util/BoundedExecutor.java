package com.facebook.presto.util;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final Queue<Runnable> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final ExecutorService coreExecutor;
    private final int maxThreads;

    public BoundedExecutor(ExecutorService coreExecutor, int maxThreads)
    {
        Preconditions.checkNotNull(coreExecutor, "coreExecutor is null");
        Preconditions.checkArgument(maxThreads > 0, "maxThreads must be greater than zero");
        this.coreExecutor = coreExecutor;
        this.maxThreads = maxThreads;
    }

    public BoundedExecutor(ExecutorService coreExecutor)
    {
        this(coreExecutor, 1);
    }

    public void execute(Runnable task)
    {
        queue.add(task);
        coreExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                executeOrMerge();
            }
        });
        // INVARIANT: every enqueued task is matched with an executeOrMerge()
    }

    private void executeOrMerge()
    {
        int size = queueSize.incrementAndGet();
        if (size <= maxThreads) {
            do {
                queue.poll().run();
            } while (queueSize.getAndDecrement() > maxThreads);
        }
    }
}
