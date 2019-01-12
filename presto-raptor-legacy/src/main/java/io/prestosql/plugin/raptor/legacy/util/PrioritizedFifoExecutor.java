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
package io.prestosql.plugin.raptor.legacy.util;

import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class is based on io.airlift.concurrent.BoundedExecutor
 */
@ThreadSafe
public class PrioritizedFifoExecutor<T extends Runnable>
{
    private static final Logger log = Logger.get(PrioritizedFifoExecutor.class);

    private final Queue<FifoRunnableTask<T>> queue;
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final Runnable triggerTask = this::executeOrMerge;

    private final ExecutorService executorService;
    private final int maxThreads;
    private final Comparator<T> taskComparator;

    public PrioritizedFifoExecutor(ExecutorService coreExecutor, int maxThreads, Comparator<T> taskComparator)
    {
        checkArgument(maxThreads > 0, "maxThreads must be greater than zero");

        this.taskComparator = requireNonNull(taskComparator, "taskComparator is null");
        this.executorService = requireNonNull(coreExecutor, "coreExecutor is null");
        this.maxThreads = maxThreads;
        this.queue = new PriorityBlockingQueue<>(maxThreads);
    }

    public ListenableFuture<?> submit(T task)
    {
        FifoRunnableTask<T> fifoTask = new FifoRunnableTask<>(task, sequenceNumber.incrementAndGet(), taskComparator);
        queue.add(fifoTask);
        executorService.submit(triggerTask);
        return fifoTask;
    }

    private void executeOrMerge()
    {
        int size = queueSize.incrementAndGet();
        if (size > maxThreads) {
            return;
        }
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

    private static class FifoRunnableTask<T extends Runnable>
            extends FutureTask<Void>
            implements ListenableFuture<Void>, Comparable<FifoRunnableTask<T>>
    {
        private final ExecutionList executionList = new ExecutionList();
        private final T task;
        private final long sequenceNumber;
        private final Comparator<T> taskComparator;

        public FifoRunnableTask(T task, long sequenceNumber, Comparator<T> taskComparator)
        {
            super(requireNonNull(task, "task is null"), null);
            this.task = task;
            this.sequenceNumber = sequenceNumber;
            this.taskComparator = requireNonNull(taskComparator, "taskComparator is null");
        }

        @Override
        public void addListener(Runnable listener, Executor executor)
        {
            executionList.add(listener, executor);
        }

        @Override
        protected void done()
        {
            executionList.execute();
        }

        @Override
        public int compareTo(FifoRunnableTask<T> other)
        {
            return ComparisonChain.start()
                    .compare(this.task, other.task, taskComparator)
                    .compare(this.sequenceNumber, other.sequenceNumber)
                    .result();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FifoRunnableTask<?> other = (FifoRunnableTask<?>) o;
            return Objects.equals(this.task, other.task) &&
                    Objects.equals(this.sequenceNumber, other.sequenceNumber);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(task, sequenceNumber);
        }
    }
}
