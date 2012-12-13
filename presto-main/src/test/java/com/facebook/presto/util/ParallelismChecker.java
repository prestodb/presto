package com.facebook.presto.util;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Wraps a Runnable task and checks that the number of concurrent executions of that task
 * (when executed through this wrapper) does not exceed the specified max.
 */
public class ParallelismChecker
        implements Runnable
{
    private final int maxParallelism;
    private final Runnable task;
    private final AtomicInteger concurrentExecutions = new AtomicInteger(0);

    public ParallelismChecker(int maxParallelism, Runnable task)
    {
        checkArgument(maxParallelism > 0, "maxParallelism must be greater than zero");
        checkNotNull(task, "task is null");
        this.maxParallelism = maxParallelism;
        this.task = task;
    }

    @Override
    public void run()
    {
        try {
            checkState(concurrentExecutions.incrementAndGet() <= maxParallelism, "Max parallelism exceeded");
            task.run();
        }
        finally {
            concurrentExecutions.decrementAndGet();
        }
    }
}
