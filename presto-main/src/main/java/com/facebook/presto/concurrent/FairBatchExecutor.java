package com.facebook.presto.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>Executes batches of tasks such that individual tasks within each batch are interleaved with tasks from other batches.</p>
 *
 * <p>E.g, if two batches containing elements ["a1, "a2", "a3", "a4"] and ["b1", "b2", "b3", "b4", "b5"] are submitted
 * in that order, a possible execution might be:</p>
 *
 * <p>"a1", "b1", "a2", "b2", "a3", "b3", "a4", "b4", "b5"</p>
 *
 * <p>If a third batch ["c1", "c2", "c3"] were submitted when the execution in the example above is at "a3", a possible
 * execution might be:</p>
 *
 * <p>"a1", "b1", "a2", "b2", "a3", "b3", "c1", "a4", "b4", "c2", "b5", "c3"</p>
 *
 * <p>The first task of a batch will not to execute before the first task of a previously submitted task, therefore
 * guaranteeing that no batch will get starved.</p>
 */
public class FairBatchExecutor
{
    private final static Logger log = Logger.get(FairBatchExecutor.class);

    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final int maxConcurrency;
    private final ExecutorService executor;
    private final PriorityBlockingQueue<PrioritizedFutureTask> queue = new PriorityBlockingQueue<>();

    @GuardedBy("this")
    private long basePriority;

    public FairBatchExecutor(int threads, ThreadFactory threadFactory)
    {
        this.maxConcurrency = threads;
        this.executor = new ThreadPoolExecutor(threads, threads,
                1, TimeUnit.MINUTES,
                new SynchronousQueue<Runnable>(),
                threadFactory,
                new ThreadPoolExecutor.DiscardPolicy());
    }

    public void shutdown()
    {
        shutdown.set(true);
        executor.shutdown();
    }

    public <T> List<Future<T>> processBatch(Collection<? extends Callable<T>> tasks)
    {
        Preconditions.checkState(!shutdown.get(), "Executor is already shut down");

        long priority = computeStartingPriority();

        ImmutableList.Builder<Future<T>> result = ImmutableList.builder();
        for (Callable<T> task : tasks) {
            PrioritizedFutureTask<T> future = new PrioritizedFutureTask<>(priority++, task);

            queue.add(future);
            result.add(future);
        }

        // Make sure we have enough processors to achieve the desired concurrency level
        for (int i = 0; i < Math.min(maxConcurrency, tasks.size()); ++i) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    trigger();
                }
            });
        }

        return result.build();
    }

    private long computeStartingPriority()
    {
        synchronized (this) {
            // increment the base priority so that the first pending task
            // of previously submitted batches takes precedence
            basePriority++;
            return basePriority;
        }
    }

    private void updateStartingPriority(long newBase)
    {
        synchronized (this) {
            // update the base priority so that newly submitted batches are
            // interleaved correctly with tasks at the front of the queue
            if (basePriority < newBase) {
                basePriority = newBase;
            }
        }
    }

    private void trigger()
    {
        try {
            while (!Thread.currentThread().isInterrupted() && !shutdown.get()) {
                PrioritizedFutureTask task = queue.take();
                try {
                    task.run();
                }
                catch (Throwable e) {
                    log.error(e, "Task failed");
                }
                finally {
                    updateStartingPriority(task.priority);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class PrioritizedFutureTask<T>
            extends FutureTask<T>
            implements Comparable<PrioritizedFutureTask>
    {
        private final long priority;

        private PrioritizedFutureTask(long priority, Callable<T> callable)
        {
            super(callable);
            this.priority = priority;
        }

        @Override
        public int compareTo(PrioritizedFutureTask o)
        {
            return Longs.compare(priority, o.priority);
        }
    }
}
