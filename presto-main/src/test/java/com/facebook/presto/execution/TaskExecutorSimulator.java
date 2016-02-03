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
package com.facebook.presto.execution;

import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TaskExecutorSimulator
        implements Closeable
{
    private static final boolean PRINT_TASK_COMPLETION = false;
    private static final boolean PRINT_SPLIT_COMPLETION = false;

    public static void main(String[] args)
            throws Exception
    {
        try (TaskExecutorSimulator simulator = new TaskExecutorSimulator()) {
            simulator.run();
        }
    }

    private final ListeningExecutorService executor;
    private final TaskExecutor taskExecutor;

    public TaskExecutorSimulator()
    {
        executor = listeningDecorator(newCachedThreadPool(threadsNamed(getClass().getSimpleName() + "-%s")));

        taskExecutor = new TaskExecutor(24, 48, new Ticker()
        {
            private final long start = System.nanoTime();

            @Override
            public long read()
            {
                // run 10 times faster than reality
                long now = System.nanoTime();
                return (now - start) * 100;
            }
        });
        taskExecutor.start();
    }

    @Override
    public void close()
    {
        taskExecutor.stop();
        executor.shutdownNow();
    }

    public void run()
            throws Exception
    {
        Multimap<Integer, SimulationTask> tasks = Multimaps.synchronizedListMultimap(ArrayListMultimap.<Integer, SimulationTask>create());
        Set<ListenableFuture<?>> finishFutures = newConcurrentHashSet();
        AtomicBoolean done = new AtomicBoolean();

        long start = System.nanoTime();

        // large tasks
        for (int userId = 0; userId < 2; userId++) {
            ListenableFuture<?> future = createUser("large_" + userId, 100, taskExecutor, done, tasks);
            finishFutures.add(future);
        }

        // small tasks
        for (int userId = 0; userId < 4; userId++) {
            ListenableFuture<?> future = createUser("small_" + userId, 5, taskExecutor, done, tasks);
            finishFutures.add(future);
        }

        // tiny tasks
        for (int userId = 0; userId < 1; userId++) {
            ListenableFuture<?> future = createUser("tiny_" + userId, 1, taskExecutor, done, tasks);
            finishFutures.add(future);
        }

        // warm up
        for (int i = 0; i < 30; i++) {
            MILLISECONDS.sleep(1000);
            System.out.println(taskExecutor);
        }
        tasks.clear();

        // run
        for (int i = 0; i < 60; i++) {
            MILLISECONDS.sleep(1000);
            System.out.println(taskExecutor);
        }

        // capture finished tasks
        Map<Integer, Collection<SimulationTask>> middleTasks;
        synchronized (tasks) {
            middleTasks = new TreeMap<>(tasks.asMap());
        }

        // wait for finish
        done.set(true);
        Futures.allAsList(finishFutures).get(1, TimeUnit.MINUTES);

        Duration runtime = Duration.nanosSince(start).convertToMostSuccinctTimeUnit();
        synchronized (this) {
            System.out.println();
            System.out.println("Simulation finished in  " + runtime);
            System.out.println();

            for (Entry<Integer, Collection<SimulationTask>> entry : middleTasks.entrySet()) {
                Distribution durationDistribution = new Distribution();
                Distribution taskParallelismDistribution = new Distribution();

                for (SimulationTask task : entry.getValue()) {
                    long taskStart = Long.MAX_VALUE;
                    long taskEnd = 0;
                    long totalCpuTime = 0;

                    for (SimulationSplit split : task.getSplits()) {
                        taskStart = Math.min(taskStart, split.getStartNanos());
                        taskEnd = Math.max(taskEnd, split.getDoneNanos());
                        totalCpuTime += MILLISECONDS.toNanos(split.getRequiredProcessMillis());
                    }

                    Duration taskDuration = new Duration(taskEnd - taskStart, NANOSECONDS).convertTo(MILLISECONDS);
                    durationDistribution.add(taskDuration.toMillis());

                    double taskParallelism = 1.0 * totalCpuTime / (taskEnd - taskStart);
                    taskParallelismDistribution.add((long) (taskParallelism * 100));
                }

                System.out.println("Splits " + entry.getKey() + ": Completed " + entry.getValue().size());

                Map<Double, Long> durationPercentiles = durationDistribution.getPercentiles();
                System.out.printf("   wall time ms :: p01 %4s :: p05 %4s :: p10 %4s :: p97 %4s :: p50 %4s :: p75 %4s :: p90 %4s :: p95 %4s :: p99 %4s\n",
                        durationPercentiles.get(0.01),
                        durationPercentiles.get(0.05),
                        durationPercentiles.get(0.10),
                        durationPercentiles.get(0.25),
                        durationPercentiles.get(0.50),
                        durationPercentiles.get(0.75),
                        durationPercentiles.get(0.90),
                        durationPercentiles.get(0.95),
                        durationPercentiles.get(0.99));

                Map<Double, Long> parallelismPercentiles = taskParallelismDistribution.getPercentiles();
                System.out.printf("    parallelism :: p99 %4.2f :: p95 %4.2f :: p90 %4.2f :: p75 %4.2f :: p50 %4.2f :: p25 %4.2f :: p10 %4.2f :: p05 %4.2f :: p01 %4.2f\n",
                        parallelismPercentiles.get(0.99) / 100.0,
                        parallelismPercentiles.get(0.95) / 100.0,
                        parallelismPercentiles.get(0.90) / 100.0,
                        parallelismPercentiles.get(0.75) / 100.0,
                        parallelismPercentiles.get(0.50) / 100.0,
                        parallelismPercentiles.get(0.25) / 100.0,
                        parallelismPercentiles.get(0.10) / 100.0,
                        parallelismPercentiles.get(0.05) / 100.0,
                        parallelismPercentiles.get(0.01) / 100.0);
            }
        }
        Thread.sleep(10);
    }

    private ListenableFuture<?> createUser(String userId,
            int splitsPerTask,
            TaskExecutor taskExecutor,
            AtomicBoolean done,
            Multimap<Integer, SimulationTask> tasks)
    {
        return executor.submit((Callable<Void>) () -> {
            long taskId = 0;
            while (!done.get()) {
                SimulationTask task = new SimulationTask(taskExecutor, new TaskId(userId, "0", String.valueOf(taskId++)));
                task.schedule(splitsPerTask, executor, new Duration(0, MILLISECONDS)).get();
                task.destroy();

                printTaskCompletion(task);

                tasks.put(splitsPerTask, task);
            }
            return null;
        });
    }

    private synchronized void printTaskCompletion(SimulationTask task)
    {
        if (!PRINT_TASK_COMPLETION) {
            return;
        }

        long taskStart = Long.MAX_VALUE;
        long taskEnd = 0;
        long taskQueuedTime = 0;
        long totalCpuTime = 0;

        for (SimulationSplit split : task.getSplits()) {
            taskStart = Math.min(taskStart, split.getStartNanos());
            taskEnd = Math.max(taskEnd, split.getDoneNanos());
            taskQueuedTime += split.getQueuedNanos();
            totalCpuTime += MILLISECONDS.toNanos(split.getRequiredProcessMillis());
        }

        System.out.printf("%-12s %8s %8s %.2f\n",
                task.getTaskId() + ":",
                new Duration(taskQueuedTime, NANOSECONDS).convertTo(MILLISECONDS),
                new Duration(taskEnd - taskStart, NANOSECONDS).convertTo(MILLISECONDS),
                1.0 * totalCpuTime / (taskEnd - taskStart)
        );

        // print split info
        if (PRINT_SPLIT_COMPLETION) {
            for (SimulationSplit split : task.getSplits()) {
                Duration totalQueueTime = new Duration(split.getQueuedNanos(), NANOSECONDS).convertTo(MILLISECONDS);
                Duration executionWallTime = new Duration(split.getDoneNanos() - split.getStartNanos(), NANOSECONDS).convertTo(MILLISECONDS);
                Duration totalWallTime = new Duration(split.getDoneNanos() - split.getCreatedNanos(), NANOSECONDS).convertTo(MILLISECONDS);
                System.out.printf("         %8s %8s %8s\n", totalQueueTime, executionWallTime, totalWallTime);
            }

            System.out.println();
        }
    }

    private static class SimulationTask
    {
        private final long createdNanos = System.nanoTime();

        private final TaskExecutor taskExecutor;
        private final Object taskId;

        private final List<SimulationSplit> splits = new ArrayList<>();
        private final List<ListenableFuture<?>> splitFutures = new ArrayList<>();
        private final TaskHandle taskHandle;

        private SimulationTask(TaskExecutor taskExecutor, TaskId taskId)
        {
            this.taskExecutor = taskExecutor;
            this.taskId = taskId;
            taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS));
        }

        public void destroy()
        {
            taskExecutor.removeTask(taskHandle);
        }

        public ListenableFuture<?> schedule(int splits, ExecutorService executor, Duration entryDelay)
        {
            SettableFuture<Void> future = SettableFuture.create();

            executor.submit((Runnable) () -> {
                try {
                    for (int splitId = 0; splitId < splits; splitId++) {
                        SimulationSplit split = new SimulationSplit(new Duration(80, MILLISECONDS), new Duration(1, MILLISECONDS));
                        SimulationTask.this.splits.add(split);
                        splitFutures.addAll(taskExecutor.enqueueSplits(taskHandle, false, ImmutableList.of(split)));
                        Thread.sleep(entryDelay.toMillis());
                    }

                    Futures.allAsList(splitFutures).get();
                    future.set(null);
                }
                catch (Throwable e) {
                    future.setException(e);
                    throw Throwables.propagate(e);
                }
            });

            return future;
        }

        private Object getTaskId()
        {
            return taskId;
        }

        private long getCreatedNanos()
        {
            return createdNanos;
        }

        private List<SimulationSplit> getSplits()
        {
            return splits;
        }
    }

    private static class SimulationSplit
            implements SplitRunner
    {
        private final long requiredProcessMillis;
        private final long processMillisPerCall;
        private final AtomicLong completedProcessMillis = new AtomicLong();

        private final AtomicInteger calls = new AtomicInteger(0);
        private final long createdNanos = System.nanoTime();
        private final AtomicLong startNanos = new AtomicLong(-1);
        private final AtomicLong doneNanos = new AtomicLong(-1);

        private final AtomicLong queuedNanos = new AtomicLong();

        private long lastCallNanos = createdNanos;

        private SimulationSplit(Duration requiredProcessTime, Duration processTimePerCall)
        {
            this.requiredProcessMillis = requiredProcessTime.toMillis();
            this.processMillisPerCall = processTimePerCall.toMillis();
        }

        private long getRequiredProcessMillis()
        {
            return requiredProcessMillis;
        }

        private long getCreatedNanos()
        {
            return createdNanos;
        }

        private long getStartNanos()
        {
            return startNanos.get();
        }

        private long getDoneNanos()
        {
            return doneNanos.get();
        }

        private long getQueuedNanos()
        {
            return queuedNanos.get();
        }

        @Override
        public boolean isFinished()
        {
            return doneNanos.get() >= 0;
        }

        @Override
        public void close()
        {
        }

        @Override
        public ListenableFuture<?> processFor(Duration duration)
                throws Exception
        {
            long callStart = System.nanoTime();
            startNanos.compareAndSet(-1, callStart);
            calls.incrementAndGet();
            queuedNanos.addAndGet(callStart - lastCallNanos);

            long processMillis = Math.min(requiredProcessMillis - completedProcessMillis.get(), processMillisPerCall);
            MILLISECONDS.sleep(processMillis);
            long completedMillis = completedProcessMillis.addAndGet(processMillis);

            boolean isFinished = completedMillis >= requiredProcessMillis;
            long callEnd = System.nanoTime();
            lastCallNanos = callEnd;
            if (isFinished) {
                doneNanos.compareAndSet(-1, callEnd);
            }

            return Futures.immediateCheckedFuture(null);
        }

        @Override
        public String getInfo()
        {
            return "simulation-split";
        }
    }
}
