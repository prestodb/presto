package com.facebook.presto.util;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Executes tasks on a single-threaded executor for each different shard
 */
@ThreadSafe
public class ShardBoundedExecutor<T>
{
    // Map shards to associated executors (single-threaded)
    @GuardedBy("this")
    private final Map<T, CountedReference<BoundedExecutor>> shardExecutors = new HashMap<>();
    private final ExecutorService executorService;

    public ShardBoundedExecutor(ExecutorService executorService)
    {
        this.executorService = checkNotNull(executorService, "executorService is null");
    }

    public synchronized boolean isActive(T shard)
    {
        return shardExecutors.containsKey(shard);
    }

    public void execute(final T shard, final Runnable task)
    {
        final CountedReference<BoundedExecutor> reference = acquireExecutor(shard);
        reference.get().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    task.run();
                }
                finally {
                    returnExecutor(shard, reference);
                }
            }
        });
    }

    private synchronized CountedReference<BoundedExecutor> acquireExecutor(T shard)
    {
        CountedReference<BoundedExecutor> reference = shardExecutors.get(shard);
        if (reference == null) {
            reference = new CountedReference<>(new BoundedExecutor(executorService, 1));
            shardExecutors.put(shard, reference);
        }
        else {
            reference.increment();
        }
        return reference;
    }

    private synchronized void returnExecutor(T shard, CountedReference<BoundedExecutor> reference)
    {
        reference.decrement();
        if (!reference.isReferenced()) {
            shardExecutors.remove(shard);
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
