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

import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.DoubleSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class TaskExecutor
{
    private static final Logger log = Logger.get(TaskExecutor.class);

    // each task is guaranteed a minimum number of tasks
    private static final int GUARANTEED_SPLITS_PER_TASK = 3;

    // each time we run a split, run it for this length before returning to the pool
    private static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);

    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;

    private final int runnerThreads;
    private final int minimumNumberOfDrivers;

    private final Ticker ticker;

    @GuardedBy("this")
    private final List<TaskHandle> tasks;

    /**
     * All splits registered with the task executor.
     */
    @GuardedBy("this")
    private final Set<PrioritizedSplitRunner> allSplits = new HashSet<>();

    /**
     * Splits waiting for a runner thread.
     */
    private final PriorityBlockingQueue<PrioritizedSplitRunner> pendingSplits;

    /**
     * Splits running on a thread.
     */
    private final Set<PrioritizedSplitRunner> runningSplits = newConcurrentHashSet();

    /**
     * Splits blocked by the driver (typically output buffer is full or input buffer is empty).
     */
    private final Map<PrioritizedSplitRunner, Future<?>> blockedSplits = new ConcurrentHashMap<>();

    private final AtomicLongArray completedTasksPerLevel = new AtomicLongArray(5);

    private final TimeStat queuedTime = new TimeStat(NANOSECONDS);
    private final TimeStat wallTime = new TimeStat(NANOSECONDS);

    private volatile boolean closed;

    @Inject
    public TaskExecutor(TaskManagerConfig config)
    {
        this(requireNonNull(config, "config is null").getMaxWorkerThreads(), config.getMinDrivers());
    }

    public TaskExecutor(int runnerThreads, int minDrivers)
    {
        this(runnerThreads, minDrivers, Ticker.systemTicker());
    }

    @VisibleForTesting
    public TaskExecutor(int runnerThreads, int minDrivers, Ticker ticker)
    {
        checkArgument(runnerThreads > 0, "runnerThreads must be at least 1");

        // we manages thread pool size directly, so create an unlimited pool
        this.executor = newCachedThreadPool(threadsNamed("task-processor-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        this.runnerThreads = runnerThreads;

        this.ticker = requireNonNull(ticker, "ticker is null");

        this.minimumNumberOfDrivers = minDrivers;
        this.pendingSplits = new PriorityBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 10);
        this.tasks = new LinkedList<>();
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < runnerThreads; i++) {
            addRunnerThread();
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        closed = true;
        executor.shutdownNow();
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("runnerThreads", runnerThreads)
                .add("allSplits", allSplits.size())
                .add("pendingSplits", pendingSplits.size())
                .add("runningSplits", runningSplits.size())
                .add("blockedSplits", blockedSplits.size())
                .toString();
    }

    private synchronized void addRunnerThread()
    {
        try {
            executor.execute(new Runner());
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    public synchronized TaskHandle addTask(TaskId taskId, DoubleSupplier utilizationSupplier, int initialSplitConcurrency, Duration splitConcurrencyAdjustFrequency)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(utilizationSupplier, "utilizationSupplier is null");
        TaskHandle taskHandle = new TaskHandle(taskId, utilizationSupplier, initialSplitConcurrency, splitConcurrencyAdjustFrequency);
        tasks.add(taskHandle);
        return taskHandle;
    }

    public void removeTask(TaskHandle taskHandle)
    {
        List<PrioritizedSplitRunner> splits;
        synchronized (this) {
            tasks.remove(taskHandle);
            splits = taskHandle.destroy();

            // stop tracking splits (especially blocked splits which may never unblock)
            allSplits.removeAll(splits);
            blockedSplits.keySet().removeAll(splits);
            pendingSplits.removeAll(splits);
        }

        // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
        for (PrioritizedSplitRunner split : splits) {
            split.destroy();
        }

        // record completed stats
        long threadUsageNanos = taskHandle.getThreadUsageNanos();
        int priorityLevel = calculatePriorityLevel(threadUsageNanos);
        completedTasksPerLevel.incrementAndGet(priorityLevel);

        // replace blocked splits that were terminated
        addNewEntrants();
    }

    public List<ListenableFuture<?>> enqueueSplits(TaskHandle taskHandle, boolean forceStart, List<? extends SplitRunner> taskSplits)
    {
        List<PrioritizedSplitRunner> splitsToDestroy = new ArrayList<>();
        List<ListenableFuture<?>> finishedFutures = new ArrayList<>(taskSplits.size());
        synchronized (this) {
            for (SplitRunner taskSplit : taskSplits) {
                PrioritizedSplitRunner prioritizedSplitRunner = new PrioritizedSplitRunner(taskHandle, taskSplit, ticker);

                if (taskHandle.isDestroyed()) {
                    // If the handle is destroyed, we destroy the task splits to complete the future
                    splitsToDestroy.add(prioritizedSplitRunner);
                }
                else if (forceStart) {
                    // Note: we do not record queued time for forced splits
                    startSplit(prioritizedSplitRunner);
                    // add the runner to the handle so it can be destroyed if the task is canceled
                    taskHandle.recordForcedRunningSplit(prioritizedSplitRunner);
                }
                else {
                    // add this to the work queue for the task
                    taskHandle.enqueueSplit(prioritizedSplitRunner);
                    // if task is under the limit for guaranteed splits, start one
                    scheduleTaskIfNecessary(taskHandle);
                    // if globally we have more resources, start more
                    addNewEntrants();
                }

                finishedFutures.add(prioritizedSplitRunner.getFinishedFuture());
            }
        }
        for (PrioritizedSplitRunner split : splitsToDestroy) {
            split.destroy();
        }
        return finishedFutures;
    }

    private void splitFinished(PrioritizedSplitRunner split)
    {
        synchronized (this) {
            allSplits.remove(split);

            TaskHandle taskHandle = split.getTaskHandle();
            taskHandle.splitComplete(split);

            wallTime.add(Duration.nanosSince(split.createdNanos));

            scheduleTaskIfNecessary(taskHandle);

            addNewEntrants();
        }
        // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
        split.destroy();
    }

    private synchronized void scheduleTaskIfNecessary(TaskHandle taskHandle)
    {
        // if task has less than the minimum guaranteed splits running,
        // immediately schedule a new split for this task.  This assures
        // that a task gets its fair amount of consideration (you have to
        // have splits to be considered for running on a thread).
        if (taskHandle.getRunningSplits() < GUARANTEED_SPLITS_PER_TASK) {
            PrioritizedSplitRunner split = taskHandle.pollNextSplit();
            if (split != null) {
                startSplit(split);
                queuedTime.add(Duration.nanosSince(split.createdNanos));
            }
        }
    }

    private synchronized void addNewEntrants()
    {
        int running = allSplits.size();
        for (int i = 0; i < minimumNumberOfDrivers - running; i++) {
            PrioritizedSplitRunner split = pollNextSplitWorker();
            if (split == null) {
                break;
            }

            queuedTime.add(Duration.nanosSince(split.createdNanos));
            startSplit(split);
        }
    }

    private synchronized void startSplit(PrioritizedSplitRunner split)
    {
        allSplits.add(split);
        pendingSplits.put(split);
    }

    private synchronized PrioritizedSplitRunner pollNextSplitWorker()
    {
        // todo find a better algorithm for this
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

    @ThreadSafe
    public static class TaskHandle
    {
        private final TaskId taskId;
        private final DoubleSupplier utilizationSupplier;
        @GuardedBy("this")
        private final Queue<PrioritizedSplitRunner> queuedSplits = new ArrayDeque<>(10);
        @GuardedBy("this")
        private final List<PrioritizedSplitRunner> runningSplits = new ArrayList<>(10);
        @GuardedBy("this")
        private final List<PrioritizedSplitRunner> forcedRunningSplits = new ArrayList<>(10);
        @GuardedBy("this")
        private long taskThreadUsageNanos;
        @GuardedBy("this")
        private boolean destroyed;
        @GuardedBy("this")
        private final SplitConcurrencyController concurrencyController;

        private final AtomicInteger nextSplitId = new AtomicInteger();

        private TaskHandle(TaskId taskId, DoubleSupplier utilizationSupplier, int initialSplitConcurrency, Duration splitConcurrencyAdjustFrequency)
        {
            this.taskId = taskId;
            this.utilizationSupplier = utilizationSupplier;
            this.concurrencyController = new SplitConcurrencyController(initialSplitConcurrency, splitConcurrencyAdjustFrequency);
        }

        private synchronized long addThreadUsageNanos(long durationNanos)
        {
            concurrencyController.update(durationNanos, utilizationSupplier.getAsDouble(), runningSplits.size());
            taskThreadUsageNanos += durationNanos;
            return taskThreadUsageNanos;
        }

        private TaskId getTaskId()
        {
            return taskId;
        }

        public synchronized boolean isDestroyed()
        {
            return destroyed;
        }

        // Returns any remaining splits. The caller must destroy these.
        private synchronized List<PrioritizedSplitRunner> destroy()
        {
            destroyed = true;

            ImmutableList.Builder<PrioritizedSplitRunner> builder = ImmutableList.builder();
            builder.addAll(forcedRunningSplits);
            builder.addAll(runningSplits);
            builder.addAll(queuedSplits);
            forcedRunningSplits.clear();
            runningSplits.clear();
            queuedSplits.clear();
            return builder.build();
        }

        private synchronized void enqueueSplit(PrioritizedSplitRunner split)
        {
            checkState(!destroyed, "Can not add split to destroyed task handle");
            queuedSplits.add(split);
        }

        private synchronized void recordForcedRunningSplit(PrioritizedSplitRunner split)
        {
            checkState(!destroyed, "Can not add split to destroyed task handle");
            forcedRunningSplits.add(split);
        }

        @VisibleForTesting
        synchronized int getRunningSplits()
        {
            return runningSplits.size();
        }

        private synchronized long getThreadUsageNanos()
        {
            return taskThreadUsageNanos;
        }

        private synchronized PrioritizedSplitRunner pollNextSplit()
        {
            if (destroyed) {
                return null;
            }

            if (runningSplits.size() >= concurrencyController.getTargetConcurrency()) {
                return null;
            }

            PrioritizedSplitRunner split = queuedSplits.poll();
            if (split != null) {
                runningSplits.add(split);
            }
            return split;
        }

        private synchronized void splitComplete(PrioritizedSplitRunner split)
        {
            concurrencyController.splitFinished(split.getSplitThreadUsageNanos(), utilizationSupplier.getAsDouble(), runningSplits.size());
            forcedRunningSplits.remove(split);
            runningSplits.remove(split);
        }

        private int getNextSplitId()
        {
            return nextSplitId.getAndIncrement();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("taskId", taskId)
                    .toString();
        }
    }

    private static class PrioritizedSplitRunner
            implements Comparable<PrioritizedSplitRunner>
    {
        private final long createdNanos = System.nanoTime();

        private final TaskHandle taskHandle;
        private final int splitId;
        private final long workerId;
        private final SplitRunner split;

        private final Ticker ticker;

        private final SettableFuture<?> finishedFuture = SettableFuture.create();

        private final AtomicBoolean destroyed = new AtomicBoolean();

        private final AtomicInteger priorityLevel = new AtomicInteger();
        private final AtomicLong threadUsageNanos = new AtomicLong();
        private final AtomicLong splitThreadUsageNanos = new AtomicLong();
        private final AtomicLong lastRun = new AtomicLong();
        private final AtomicLong start = new AtomicLong();

        private final AtomicLong cpuTime = new AtomicLong();
        private final AtomicLong processCalls = new AtomicLong();

        private PrioritizedSplitRunner(TaskHandle taskHandle, SplitRunner split, Ticker ticker)
        {
            this.taskHandle = taskHandle;
            this.splitId = taskHandle.getNextSplitId();
            this.split = split;
            this.ticker = ticker;
            this.workerId = NEXT_WORKER_ID.getAndIncrement();
        }

        private TaskHandle getTaskHandle()
        {
            return taskHandle;
        }

        private ListenableFuture<?> getFinishedFuture()
        {
            return finishedFuture;
        }

        public void destroy()
        {
            destroyed.set(true);
            try {
                split.close();
            }
            catch (RuntimeException e) {
                log.error(e, "Error closing split for task %s", taskHandle.getTaskId());
            }
        }

        public boolean isFinished()
        {
            boolean finished = split.isFinished();
            if (finished) {
                finishedFuture.set(null);
            }
            return finished || destroyed.get() || taskHandle.isDestroyed();
        }

        public long getSplitThreadUsageNanos()
        {
            return splitThreadUsageNanos.get();
        }

        public ListenableFuture<?> process()
                throws Exception
        {
            try {
                start.compareAndSet(0, System.currentTimeMillis());

                processCalls.incrementAndGet();
                CpuTimer timer = new CpuTimer();
                ListenableFuture<?> blocked = split.processFor(SPLIT_RUN_QUANTA);

                CpuTimer.CpuDuration elapsed = timer.elapsedTime();

                // update priority level base on total thread usage of task
                long durationNanos = elapsed.getWall().roundTo(NANOSECONDS);
                this.splitThreadUsageNanos.addAndGet(durationNanos);
                long threadUsageNanos = taskHandle.addThreadUsageNanos(durationNanos);
                this.threadUsageNanos.set(threadUsageNanos);
                priorityLevel.set(calculatePriorityLevel(threadUsageNanos));

                // record last run for prioritization within a level
                lastRun.set(ticker.read());

                cpuTime.addAndGet(elapsed.getCpu().roundTo(NANOSECONDS));
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

            int result = Integer.compare(level, o.priorityLevel.get());
            if (result != 0) {
                return result;
            }

            if (level < 4) {
                result = Long.compare(threadUsageNanos.get(), o.threadUsageNanos.get());
            }
            else {
                result = Long.compare(lastRun.get(), o.lastRun.get());
            }
            if (result != 0) {
                return result;
            }

            return Long.compare(workerId, o.workerId);
        }

        public int getSplitId()
        {
            return splitId;
        }

        public String getInfo()
        {
            return String.format("Split %-15s-%d %s (start = %s, wall = %s ms, cpu = %s ms, calls = %s)",
                    taskHandle.getTaskId(),
                    splitId,
                    split.getInfo(),
                    start.get(),
                    System.currentTimeMillis() - start.get(),
                    (int) (cpuTime.get() / 1.0e6),
                    processCalls.get());
        }

        @Override
        public String toString()
        {
            return String.format("Split %-15s-%d", taskHandle.getTaskId(), splitId);
        }
    }

    private static int calculatePriorityLevel(long threadUsageNanos)
    {
        long millis = NANOSECONDS.toMillis(threadUsageNanos);

        int priorityLevel;
        if (millis < 1000) {
            priorityLevel = 0;
        }
        else if (millis < 10_000) {
            priorityLevel = 1;
        }
        else if (millis < 60_000) {
            priorityLevel = 2;
        }
        else if (millis < 300_000) {
            priorityLevel = 3;
        }
        else {
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

                    try (SetThreadName splitName = new SetThreadName(split.getTaskHandle().getTaskId() + "-" + split.getSplitId())) {
                        runningSplits.add(split);

                        boolean finished;
                        ListenableFuture<?> blocked;
                        try {
                            blocked = split.process();
                            finished = split.isFinished();
                        }
                        finally {
                            runningSplits.remove(split);
                        }

                        if (finished) {
                            log.debug("%s is finished", split.getInfo());
                            splitFinished(split);
                        }
                        else {
                            if (blocked.isDone()) {
                                pendingSplits.put(split);
                            }
                            else {
                                blockedSplits.put(split, blocked);
                                blocked.addListener(new Runnable()
                                {
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
                        if (t instanceof PrestoException) {
                            PrestoException e = (PrestoException) t;
                            log.error("Error processing %s: %s: %s", split.getInfo(), e.getErrorCode().getName(), e.getMessage());
                        }
                        else {
                            log.error(t, "Error processing %s", split.getInfo());
                        }
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

    //
    // STATS
    //

    @Managed
    public synchronized int getTasks()
    {
        return tasks.size();
    }

    @Managed
    public int getRunnerThreads()
    {
        return runnerThreads;
    }

    @Managed
    public int getMinimumNumberOfDrivers()
    {
        return minimumNumberOfDrivers;
    }

    @Managed
    public synchronized int getTotalSplits()
    {
        return allSplits.size();
    }

    @Managed
    public int getPendingSplits()
    {
        return pendingSplits.size();
    }

    @Managed
    public int getRunningSplits()
    {
        return runningSplits.size();
    }

    @Managed
    public int getBlockedSplits()
    {
        return blockedSplits.size();
    }

    @Managed
    public long getCompletedTasksLevel0()
    {
        return completedTasksPerLevel.get(0);
    }

    @Managed
    public long getCompletedTasksLevel1()
    {
        return completedTasksPerLevel.get(1);
    }

    @Managed
    public long getCompletedTasksLevel2()
    {
        return completedTasksPerLevel.get(2);
    }

    @Managed
    public long getCompletedTasksLevel3()
    {
        return completedTasksPerLevel.get(3);
    }

    @Managed
    public long getCompletedTasksLevel4()
    {
        return completedTasksPerLevel.get(4);
    }

    @Managed
    public long getRunningTasksLevel0()
    {
        return calculateRunningTasksForLevel(0);
    }

    @Managed
    public long getRunningTasksLevel1()
    {
        return calculateRunningTasksForLevel(1);
    }

    @Managed
    public long getRunningTasksLevel2()
    {
        return calculateRunningTasksForLevel(2);
    }

    @Managed
    public long getRunningTasksLevel3()
    {
        return calculateRunningTasksForLevel(3);
    }

    @Managed
    public long getRunningTasksLevel4()
    {
        return calculateRunningTasksForLevel(4);
    }

    @Managed
    @Nested
    public TimeStat getQueuedTime()
    {
        return queuedTime;
    }

    @Managed
    @Nested
    public TimeStat getWallTime()
    {
        return wallTime;
    }

    private synchronized int calculateRunningTasksForLevel(int level)
    {
        int count = 0;
        for (TaskHandle task : tasks) {
            if (calculatePriorityLevel(task.getThreadUsageNanos()) == level) {
                count++;
            }
        }
        return count;
    }

    @Managed(description = "Task processor executor")
    @Nested
    public ThreadPoolExecutorMBean getProcessorExecutor()
    {
        return executorMBean;
    }
}
