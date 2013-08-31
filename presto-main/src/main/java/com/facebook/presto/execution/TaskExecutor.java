package com.facebook.presto.execution;

import com.facebook.presto.util.SetThreadName;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

public class TaskExecutor
{
    private static final Logger log = Logger.get(TaskExecutor.class);

    // each task is guaranteed a minimum number of tasks
    private static final int GUARANTEED_SPLITS_PER_TASK = 5;

    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private final Executor executor;
    private final int runnerThreads;

    private final List<TaskHandle> tasks;

    private final Set<SplitWorker> activeSplits = new HashSet<>();
    private final PriorityBlockingQueue<SplitWorker> preemptedSplits;
    private final Set<SplitWorker> runningSplits = Sets.newSetFromMap(new ConcurrentHashMap<SplitWorker, Boolean>());

    private boolean closed;

    public static TaskExecutor createTaskExecutor(Executor executor, int runnerThreads)
    {
        TaskExecutor taskExecutor = new TaskExecutor(executor, runnerThreads);
        taskExecutor.start();
        return taskExecutor;
    }

    private TaskExecutor(Executor executor, int runnerThreads)
    {
        this.executor = executor;
        this.runnerThreads = runnerThreads;
        this.preemptedSplits = new PriorityBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 10);
        this.tasks = new LinkedList<>();
    }

    private synchronized void start()
    {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < runnerThreads; i++) {
            addRunnerThread();
        }
    }

    public synchronized List<SplitWorker> getPreemptedSplits()
    {
        return ImmutableList.copyOf(preemptedSplits);
    }

    public synchronized Set<SplitWorker> getRunningSplits()
    {
        return ImmutableSet.copyOf(runningSplits);
    }

    @Override
    public synchronized String toString()
    {
        return Objects.toStringHelper(this)
                .add("runnerThreads", runnerThreads)
                .add("activeSplits", activeSplits.size())
                .add("preemptedSplits", preemptedSplits.size())
                .add("runningSplits", runningSplits.size())
                .toString();
    }

    public synchronized void close()
    {
        closed = true;
    }

    private synchronized void addRunnerThread()
    {
        try {
            executor.execute(new Runner());
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    public synchronized TaskHandle addTask(Object taskId)
    {
        TaskHandle taskHandle = new TaskHandle(taskId);
        tasks.add(taskHandle);
        return taskHandle;
    }

    public synchronized void removeTask(TaskHandle taskHandle)
    {
        taskHandle.destroy();
        tasks.remove(taskHandle);
    }

    public synchronized ListenableFuture<?> addSplit(TaskHandle taskHandle, Callable<Boolean> split)
    {
        SplitWorker splitWorker = new SplitWorker(taskHandle, split);
        taskHandle.addSplitWorker(splitWorker);

        if (taskHandle.getRunningSplits() < GUARANTEED_SPLITS_PER_TASK) {
            runSplit(taskHandle.pollNextSplit());
        }

        return splitWorker.getFinishedFuture();
    }

    private synchronized void runSplit(SplitWorker splitWorker)
    {
        if (splitWorker == null) {
            return;
        }
        activeSplits.add(splitWorker);
        preemptedSplits.add(splitWorker);
    }

    private synchronized void splitFinished(SplitWorker splitWorker)
    {
        activeSplits.remove(splitWorker);
        preemptedSplits.remove(splitWorker);

        TaskHandle taskHandle = splitWorker.getTaskHandle();
        taskHandle.splitComplete(splitWorker);

        if (taskHandle.getRunningSplits() < GUARANTEED_SPLITS_PER_TASK) {
            runSplit(taskHandle.pollNextSplit());
        }

        addNewEntrants();
    }

    private synchronized void addNewEntrants()
    {
        int running = activeSplits.size();
        for (int i = 0; i < (2 * runnerThreads) - running; i++) {
            SplitWorker splitWorker = pollNextSplitWorker();
            if (splitWorker == null) {
                break;
            }
            activeSplits.add(splitWorker);
            preemptedSplits.put(splitWorker);
        }
    }

    private synchronized SplitWorker pollNextSplitWorker()
    {
        for (Iterator<TaskHandle> iterator = tasks.iterator(); iterator.hasNext(); ) {
            TaskHandle task = iterator.next();
            SplitWorker splitWorker = task.pollNextSplit();
            if (splitWorker != null) {
                // move task to end of list
                iterator.remove();
                tasks.add(task);

                return splitWorker;
            }
        }
        return null;
    }

    public static class TaskHandle
    {
        private final Object taskId;
        private final Queue<SplitWorker> queuedSplits = new ArrayDeque<>(10);
        private final List<SplitWorker> runningSplits = new ArrayList<>(10);
        private final AtomicLong taskThreadUsageNanos = new AtomicLong();

        private TaskHandle(Object taskId)
        {
            this.taskId = taskId;
        }

        public long addThreadUsageNanos(long durationNanos)
        {
            return taskThreadUsageNanos.addAndGet(durationNanos);
        }

        private Object getTaskId()
        {
            return taskId;
        }

        private void destroy()
        {
            for (SplitWorker runningSplit : runningSplits) {
                runningSplit.destroy();
            }
            runningSplits.clear();

            for (SplitWorker queuedSplit : queuedSplits) {
                queuedSplit.destroy();
            }
            queuedSplits.clear();
        }

        private void addSplitWorker(SplitWorker splitWorker)
        {
            queuedSplits.add(splitWorker);
        }

        private int getRunningSplits()
        {
            return runningSplits.size();
        }

        public long getThreadUsageNanos()
        {
            return taskThreadUsageNanos.get();
        }

        public SplitWorker pollNextSplit()
        {
            SplitWorker split = queuedSplits.poll();
            if (split != null) {
                runningSplits.add(split);
            }
            return split;
        }

        private void splitComplete(SplitWorker split)
        {
            runningSplits.remove(split);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("taskId", taskId)
                    .toString();
        }
    }

    private static class SplitWorker
            implements Callable<Boolean>, Comparable<SplitWorker>
    {
        private final TaskHandle taskHandle;
        private final long workerId;
        private final Callable<Boolean> split;

        private final SettableFuture<?> finishedFuture = SettableFuture.create();

        private final AtomicBoolean closed = new AtomicBoolean();

        private final AtomicInteger priorityLevel = new AtomicInteger();
        private final AtomicLong threadUsageNanos = new AtomicLong();
        private final AtomicLong lastRun = new AtomicLong();

        private SplitWorker(TaskHandle taskHandle, Callable<Boolean> split)
        {
            this.taskHandle = taskHandle;
            this.split = split;
            this.workerId = NEXT_WORKER_ID.getAndIncrement();
        }

        private TaskHandle getTaskHandle()
        {
            return taskHandle;
        }

        private SettableFuture<?> getFinishedFuture()
        {
            return finishedFuture;
        }

        public void destroy()
        {
            closed.set(true);
        }

        @Override
        public Boolean call()
                throws Exception
        {
            try {
                if (priorityLevel.get() > 3) {
                    System.out.print("");
                }
                long start = System.nanoTime();
                boolean finished = split.call();
                long endTime = System.nanoTime();

                // update priority level base on total thread usage of task
                long durationNanos = endTime - start;
                long threadUsageNanos = taskHandle.addThreadUsageNanos(durationNanos);
                this.threadUsageNanos.set(durationNanos);
                priorityLevel.set(calculatePriorityLevel(threadUsageNanos));

                // record last run for prioritization within a level
                lastRun.set(endTime);

                if (finished) {
                    finishedFuture.set(null);
                }
                return finished || closed.get();
            }
            catch (Throwable e) {
                finishedFuture.setException(e);
                throw e;
            }
        }

        public boolean updatePriorityLevel()
        {
            int newPriority = calculatePriorityLevel(taskHandle.getThreadUsageNanos());
            if (newPriority == priorityLevel.getAndSet(newPriority)) {
                return false;
            }

            // update thread usage while if level changed
            threadUsageNanos.set(taskHandle.getThreadUsageNanos());
            return true;
        }

        @Override
        public int compareTo(SplitWorker o)
        {
            int level = priorityLevel.get();

            int result = Ints.compare(level, o.priorityLevel.get());
            if (result != 0) {
                return result;
            }

            if (level < 4) {
                result = Long.compare(threadUsageNanos.get(), threadUsageNanos.get());
            } else {
                result = Long.compare(lastRun.get(), o.lastRun.get());
            }
            if (result != 0) {
                return result;
            }

            return Longs.compare(workerId, o.workerId);
        }

        @Override
        public String toString()
        {
            return String.format("Split %-15s %s %s",
                    taskHandle.getTaskId(),
                    priorityLevel,
                    new Duration(threadUsageNanos.get(), TimeUnit.NANOSECONDS).convertToMostSuccinctTimeUnit());
        }
    }

    private static int calculatePriorityLevel(long threadUsageNanos)
    {
        long millis = TimeUnit.NANOSECONDS.toMillis(threadUsageNanos);

        int priorityLevel;
        if (millis < 50) {
            priorityLevel = 0;
        } else if (millis < 100) {
            priorityLevel = 1;
        } else if (millis < 200) {
            priorityLevel = 2;
        } else if (millis < 400) {
            priorityLevel = 3;
        } else {
            priorityLevel = 4;
        }
        return priorityLevel;
    }

    private class Runner
            implements Runnable
    {
        private final long runnerId = NEXT_RUNNER_ID.getAndIncrement();

        @Override
        public void run()
        {
            try (SetThreadName runnerName = new SetThreadName("SplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    // select next worker
                    SplitWorker split;
                    try {
                        split = preemptedSplits.take();
                        if (split.updatePriorityLevel()) {
                            // priority level changed, return split to queue for re-prioritization
                            preemptedSplits.put(split);
                            continue;
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    // todo do we need a runner id?
                    try (SetThreadName splitName = new SetThreadName(split.toString())) {
                        runningSplits.add(split);

                        boolean finished;
                        try {
                            finished = split.call();
                        }
                        finally {
                            runningSplits.remove(split);
                        }

                        if (!finished) {
                            preemptedSplits.put(split);
                        }
                        else {
                            log.debug("%s is finished", split);
                            splitFinished(split);
                        }
                    }
                    catch (Throwable t) {
                        log.error(t, "Error processing %s", split);
                        splitFinished(split);
                    }
                }
            }
            finally {
                // unless we have been closed, we need to replace this thread
                if (!closed) {
                    addRunnerThread();
                }
            }
        }
    }
}
