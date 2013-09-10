package com.facebook.presto.execution;

import com.facebook.presto.util.SetThreadName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class TaskExecutor
{
    private static final Logger log = Logger.get(TaskExecutor.class);

    // each task is guaranteed a minimum number of tasks
    private static final int GUARANTEED_SPLITS_PER_TASK = 5;

    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private final Executor executor;
    private final int runnerThreads;
    private final int minimumNumberOfTasks;

    private final Ticker ticker;

    private final List<TaskHandle> tasks;

    private final Set<PrioritizedSplitRunner> allSplits = new HashSet<>();
    private final PriorityBlockingQueue<PrioritizedSplitRunner> pendingSplits;
    private final Set<PrioritizedSplitRunner> runningSplits = Sets.newSetFromMap(new ConcurrentHashMap<PrioritizedSplitRunner, Boolean>());
    private final Set<PrioritizedSplitRunner> blockedSplits = Sets.newSetFromMap(new ConcurrentHashMap<PrioritizedSplitRunner, Boolean>());

    private boolean closed;

    public static TaskExecutor createTaskExecutor(Executor executor, int runnerThreads)
    {
        return createTaskExecutor(executor, runnerThreads, Ticker.systemTicker());
    }

    @VisibleForTesting
    public static TaskExecutor createTaskExecutor(Executor executor, int runnerThreads, Ticker ticker)
    {
        TaskExecutor taskExecutor = new TaskExecutor(executor, runnerThreads, ticker);
        taskExecutor.start();
        return taskExecutor;
    }

    private TaskExecutor(Executor executor, int runnerThreads, Ticker ticker)
    {
        this.executor = executor;
        this.runnerThreads = runnerThreads;
        this.ticker = ticker;

        // we assume we need at least two tasks per runner thread to keep the system busy
        this.minimumNumberOfTasks = 2 * runnerThreads;
        this.pendingSplits = new PriorityBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 10);
        this.tasks = new LinkedList<>();
    }

    private synchronized void start()
    {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < runnerThreads; i++) {
            addRunnerThread();
        }
    }

    public List<PrioritizedSplitRunner> getPendingSplits()
    {
        return ImmutableList.copyOf(pendingSplits);
    }

    public Set<PrioritizedSplitRunner> getRunningSplits()
    {
        return ImmutableSet.copyOf(runningSplits);
    }

    public Set<PrioritizedSplitRunner> getBlockedSplits()
    {
        return ImmutableSet.copyOf(blockedSplits);
    }

    @Override
    public synchronized String toString()
    {
        return Objects.toStringHelper(this)
                .add("runnerThreads", runnerThreads)
                .add("allSplits", allSplits.size())
                .add("pendingSplits", pendingSplits.size())
                .add("runningSplits", runningSplits.size())
                .add("blockedSplits", blockedSplits.size())
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

    public synchronized TaskHandle addTask(TaskId taskId)
    {
        TaskHandle taskHandle = new TaskHandle(checkNotNull(taskId, "taskId is null"));
        tasks.add(taskHandle);
        return taskHandle;
    }

    public synchronized void removeTask(TaskHandle taskHandle)
    {
        taskHandle.destroy();
        tasks.remove(taskHandle);
    }

    public synchronized ListenableFuture<?> addSplit(TaskHandle taskHandle, SplitRunner taskSplit)
    {
        PrioritizedSplitRunner prioritizedSplitRunner = new PrioritizedSplitRunner(taskHandle, taskSplit, ticker);
        taskHandle.addSplit(prioritizedSplitRunner);

        scheduleTaskIfNecessary(taskHandle);

        return prioritizedSplitRunner.getFinishedFuture();
    }

    private synchronized void splitFinished(PrioritizedSplitRunner split)
    {
        allSplits.remove(split);
        pendingSplits.remove(split);

        TaskHandle taskHandle = split.getTaskHandle();
        taskHandle.splitComplete(split);

        scheduleTaskIfNecessary(taskHandle);

        addNewEntrants();
    }

    private void scheduleTaskIfNecessary(TaskHandle taskHandle)
    {
        // if task has less than the minimum guaranteed splits running,
        // immediately schedule a new split for this task.  This assures
        // that a task gets its fair amount of consideration (you have to
        // have splits to be considered for running on a thread).
        if (taskHandle.getRunningSplits() < GUARANTEED_SPLITS_PER_TASK) {
            PrioritizedSplitRunner split = taskHandle.pollNextSplit();
            if (split != null) {
                allSplits.add(split);
                pendingSplits.add(split);
            }
        }
    }

    private synchronized void addNewEntrants()
    {
        int running = allSplits.size();
        for (int i = 0; i < minimumNumberOfTasks - running; i++) {
            PrioritizedSplitRunner split = pollNextSplitWorker();
            if (split == null) {
                break;
            }
            allSplits.add(split);
            pendingSplits.put(split);
        }
    }

    private synchronized PrioritizedSplitRunner pollNextSplitWorker()
    {
        // find the first task that produces a split, then move that task to the
        // end of the task list, so we get round robin
        for (Iterator<TaskHandle> iterator = tasks.iterator(); iterator.hasNext(); ) {
            TaskHandle task = iterator.next();
            PrioritizedSplitRunner split = task.pollNextSplit();
            if (split != null) {
                // move task to end of list
                iterator.remove();

                // CAUTION: we are modifying the list in the loop which would normally
                // cause a ConcurrentModificationException but we exit immediately
                tasks.add(task);
                return split;
            }
        }
        return null;
    }

    @NotThreadSafe
    public static class TaskHandle
    {
        private final TaskId taskId;
        private final Queue<PrioritizedSplitRunner> queuedSplits = new ArrayDeque<>(10);
        private final List<PrioritizedSplitRunner> runningSplits = new ArrayList<>(10);
        private final AtomicLong taskThreadUsageNanos = new AtomicLong();

        private TaskHandle(TaskId taskId)
        {
            this.taskId = taskId;
        }

        private long addThreadUsageNanos(long durationNanos)
        {
            return taskThreadUsageNanos.addAndGet(durationNanos);
        }

        private TaskId getTaskId()
        {
            return taskId;
        }

        private void destroy()
        {
            for (PrioritizedSplitRunner runningSplit : runningSplits) {
                runningSplit.destroy();
            }
            runningSplits.clear();

            for (PrioritizedSplitRunner queuedSplit : queuedSplits) {
                queuedSplit.destroy();
            }
            queuedSplits.clear();
        }

        private void addSplit(PrioritizedSplitRunner split)
        {
            queuedSplits.add(split);
        }

        private int getRunningSplits()
        {
            return runningSplits.size();
        }

        private long getThreadUsageNanos()
        {
            return taskThreadUsageNanos.get();
        }

        private PrioritizedSplitRunner pollNextSplit()
        {
            PrioritizedSplitRunner split = queuedSplits.poll();
            if (split != null) {
                runningSplits.add(split);
            }
            return split;
        }

        private void splitComplete(PrioritizedSplitRunner split)
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

    private static class PrioritizedSplitRunner
            implements Comparable<PrioritizedSplitRunner>
    {
        private final TaskHandle taskHandle;
        private final long workerId;
        private final SplitRunner split;

        private final Ticker ticker;

        private final SettableFuture<?> finishedFuture = SettableFuture.create();

        private final AtomicBoolean initialized = new AtomicBoolean();
        private final AtomicBoolean destroyed = new AtomicBoolean();

        private final AtomicInteger priorityLevel = new AtomicInteger();
        private final AtomicLong threadUsageNanos = new AtomicLong();
        private final AtomicLong lastRun = new AtomicLong();

        private PrioritizedSplitRunner(TaskHandle taskHandle, SplitRunner split, Ticker ticker)
        {
            this.taskHandle = taskHandle;
            this.split = split;
            this.ticker = ticker;
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

        public void initializeIfNecessary()
        {
            if (initialized.compareAndSet(false, true)) {
                split.initialize();
            }
        }

        public void destroy()
        {
            destroyed.set(true);
        }

        public boolean isFinished()
        {
            boolean finished = split.isFinished();
            if (finished) {
                finishedFuture.set(null);
            }
            return finished || destroyed.get();
        }

        public ListenableFuture<?> process()
                throws Exception
        {
            try {
                long start = ticker.read();
                ListenableFuture<?> blocked = split.process();
                long endTime = ticker.read();

                // update priority level base on total thread usage of task
                long durationNanos = endTime - start;
                long threadUsageNanos = taskHandle.addThreadUsageNanos(durationNanos);
                this.threadUsageNanos.set(threadUsageNanos);
                priorityLevel.set(calculatePriorityLevel(threadUsageNanos));

                // record last run for prioritization within a level
                lastRun.set(endTime);

                return blocked;
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
        public int compareTo(PrioritizedSplitRunner o)
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
        if (millis < 1000) {
            priorityLevel = 0;
        } else if (millis < 10_000) {
            priorityLevel = 1;
        } else if (millis < 60_000) {
            priorityLevel = 2;
        } else if (millis < 300_000) {
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
                    final PrioritizedSplitRunner split;
                    try {
                        split = pendingSplits.take();
                        if (split.updatePriorityLevel()) {
                            // priority level changed, return split to queue for re-prioritization
                            pendingSplits.put(split);
                            continue;
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    try (SetThreadName splitName = new SetThreadName(split.toString())) {
                        runningSplits.add(split);

                        boolean finished;
                        ListenableFuture<?> blocked;
                        try {
                            split.initializeIfNecessary();
                            blocked = split.process();
                            finished = split.isFinished();
                        }
                        finally {
                            runningSplits.remove(split);
                        }

                        if (finished) {
                            log.debug("%s is finished", split);
                            splitFinished(split);
                        }
                        else {
                            if (blocked.isDone()) {
                                pendingSplits.put(split);
                            }
                            else {
                                blockedSplits.add(split);
                                blocked.addListener(new Runnable() {
                                    @Override
                                    public void run()
                                    {
                                        blockedSplits.remove(split);
                                        split.updatePriorityLevel();
                                        pendingSplits.put(split);
                                    }
                                }, executor);
                            }
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
