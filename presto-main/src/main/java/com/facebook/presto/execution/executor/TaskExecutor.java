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
package com.facebook.presto.execution.executor;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeDistribution;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.execution.SplitRunner;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking;
import com.facebook.presto.operator.scalar.JoniRegexpFunctions;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.version.EmbedVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.execution.executor.MultilevelSplitQueue.computeLevel;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

@ThreadSafe
public class TaskExecutor
{
    private static final Logger log = Logger.get(TaskExecutor.class);

    // print out split call stack if it has been running for a certain amount of time
    private static final Duration LONG_SPLIT_WARNING_THRESHOLD = new Duration(600, SECONDS);
    // Interrupt a split if it is running longer than this AND it's blocked on something known
    private static final Predicate<List<StackTraceElement>> DEFAULT_INTERRUPTIBLE_SPLIT_PREDICATE = elements ->
                    elements.stream()
                            .anyMatch(element -> element.getClassName().equals(JoniRegexpFunctions.class.getName()));
    private static final Duration DEFAULT_INTERRUPT_SPLIT_INTERVAL = new Duration(60, SECONDS);

    private static final AtomicLong NEXT_RUNNER_ID = new AtomicLong();

    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;

    private final int runnerThreads;
    private final int minimumNumberOfDrivers;
    private final int guaranteedNumberOfDriversPerTask;
    private final int maximumNumberOfDriversPerTask;
    private final EmbedVersion embedVersion;

    private final Ticker ticker;

    private final Duration interruptRunawaySplitsTimeout;
    private final Predicate<List<StackTraceElement>> interruptibleSplitPredicate;
    private final Duration interruptSplitInterval;

    private final ScheduledExecutorService splitMonitorExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("TaskExecutor"));
    private final SortedSet<RunningSplitInfo> runningSplitInfos = new ConcurrentSkipListSet<>();

    @GuardedBy("this")
    private final List<TaskHandle> tasks;

    /**
     * All splits registered with the task executor.
     */
    @GuardedBy("this")
    private final Set<PrioritizedSplitRunner> allSplits = new HashSet<>();

    /**
     * Intermediate splits (i.e. splits that should not be queued).
     */
    @GuardedBy("this")
    private final Set<PrioritizedSplitRunner> intermediateSplits = new HashSet<>();

    /**
     * Splits waiting for a runner thread.
     */
    private final MultilevelSplitQueue waitingSplits;

    /**
     * Per query priority trackers
     */
    private final Function<QueryId, TaskPriorityTracker> taskPriorityTrackerFactory;

    /**
     * Splits running on a thread.
     */
    private final Set<PrioritizedSplitRunner> runningSplits = newConcurrentHashSet();

    /**
     * Splits blocked by the driver.
     */
    private final Map<PrioritizedSplitRunner, Future<?>> blockedSplits = new ConcurrentHashMap<>();

    private final AtomicLongArray completedTasksPerLevel = new AtomicLongArray(5);
    private final AtomicLongArray completedSplitsPerLevel = new AtomicLongArray(5);

    private final TimeStat splitQueuedTime = new TimeStat(NANOSECONDS);
    private final TimeStat splitWallTime = new TimeStat(NANOSECONDS);

    private final TimeDistribution leafSplitWallTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitWallTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitScheduledTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitScheduledTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitWaitTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitWaitTime = new TimeDistribution(MICROSECONDS);

    private final TimeDistribution leafSplitCpuTime = new TimeDistribution(MICROSECONDS);
    private final TimeDistribution intermediateSplitCpuTime = new TimeDistribution(MICROSECONDS);

    // shared between SplitRunners
    private final CounterStat globalCpuTimeMicros = new CounterStat();
    private final CounterStat globalScheduledTimeMicros = new CounterStat();

    private final TimeStat blockedQuantaWallTime = new TimeStat(MICROSECONDS);
    private final TimeStat unblockedQuantaWallTime = new TimeStat(MICROSECONDS);

    private volatile boolean closed;

    @Inject
    public TaskExecutor(TaskManagerConfig config, EmbedVersion embedVersion, MultilevelSplitQueue splitQueue)
    {
        this(requireNonNull(config, "config is null").getMaxWorkerThreads(),
                config.getMinDrivers(),
                config.getMinDriversPerTask(),
                config.getMaxDriversPerTask(),
                config.getTaskPriorityTracking(),
                config.getInterruptRunawaySplitsTimeout(),
                DEFAULT_INTERRUPTIBLE_SPLIT_PREDICATE,
                DEFAULT_INTERRUPT_SPLIT_INTERVAL,
                embedVersion,
                splitQueue,
                Ticker.systemTicker());
    }

    @VisibleForTesting
    public TaskExecutor(
            int runnerThreads,
            int minDrivers,
            int guaranteedNumberOfDriversPerTask,
            int maximumNumberOfDriversPerTask,
            TaskPriorityTracking taskPriorityTracking,
            Ticker ticker)
    {
        this(
                runnerThreads,
                minDrivers,
                guaranteedNumberOfDriversPerTask,
                maximumNumberOfDriversPerTask,
                taskPriorityTracking,
                new TaskManagerConfig().getInterruptRunawaySplitsTimeout(),
                DEFAULT_INTERRUPTIBLE_SPLIT_PREDICATE,
                DEFAULT_INTERRUPT_SPLIT_INTERVAL,
                new EmbedVersion(new ServerConfig()),
                new MultilevelSplitQueue(2), ticker);
    }

    @VisibleForTesting
    public TaskExecutor(
            int runnerThreads,
            int minDrivers,
            int guaranteedNumberOfDriversPerTask,
            int maximumNumberOfDriversPerTask,
            TaskPriorityTracking taskPriorityTracking,
            MultilevelSplitQueue splitQueue,
            Ticker ticker)
    {
        this(
                runnerThreads,
                minDrivers,
                guaranteedNumberOfDriversPerTask,
                maximumNumberOfDriversPerTask,
                taskPriorityTracking,
                new TaskManagerConfig().getInterruptRunawaySplitsTimeout(),
                DEFAULT_INTERRUPTIBLE_SPLIT_PREDICATE,
                DEFAULT_INTERRUPT_SPLIT_INTERVAL,
                new EmbedVersion(new ServerConfig()),
                splitQueue,
                ticker);
    }

    @VisibleForTesting
    public TaskExecutor(
            int runnerThreads,
            int minDrivers,
            int guaranteedNumberOfDriversPerTask,
            int maximumNumberOfDriversPerTask,
            TaskPriorityTracking taskPriorityTracking,
            Duration interruptRunawaySplitsTimeout,
            Predicate<List<StackTraceElement>> interruptibleSplitPredicate,
            Duration interruptSplitInterval,
            EmbedVersion embedVersion,
            MultilevelSplitQueue splitQueue,
            Ticker ticker)
    {
        checkArgument(runnerThreads > 0, "runnerThreads must be at least 1");
        checkArgument(guaranteedNumberOfDriversPerTask > 0, "guaranteedNumberOfDriversPerTask must be at least 1");
        checkArgument(maximumNumberOfDriversPerTask > 0, "maximumNumberOfDriversPerTask must be at least 1");
        checkArgument(guaranteedNumberOfDriversPerTask <= maximumNumberOfDriversPerTask, "guaranteedNumberOfDriversPerTask cannot be greater than maximumNumberOfDriversPerTask");
        checkArgument(interruptRunawaySplitsTimeout.getValue(SECONDS) >= 1.0, "interruptRunawaySplitsTimeout must be at least 1 second");
        checkArgument(interruptSplitInterval.getValue(SECONDS) >= 1.0, "interruptSplitInterval must be at least 1 second");

        // we manage thread pool size directly, so create an unlimited pool
        this.executor = newCachedThreadPool(threadsNamed("task-processor-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        this.runnerThreads = runnerThreads;
        this.embedVersion = requireNonNull(embedVersion, "embedVersion is null");

        this.ticker = requireNonNull(ticker, "ticker is null");

        this.minimumNumberOfDrivers = minDrivers;
        this.guaranteedNumberOfDriversPerTask = guaranteedNumberOfDriversPerTask;
        this.maximumNumberOfDriversPerTask = maximumNumberOfDriversPerTask;
        this.waitingSplits = requireNonNull(splitQueue, "splitQueue is null");
        Function<QueryId, TaskPriorityTracker> taskPriorityTrackerFactory;
        switch (taskPriorityTracking) {
            case TASK_FAIR:
                taskPriorityTrackerFactory = (queryId) -> new TaskPriorityTracker(splitQueue);
                break;
            case QUERY_FAIR:
                LoadingCache<QueryId, TaskPriorityTracker> cache = CacheBuilder.newBuilder()
                        .weakValues()
                        .build(CacheLoader.from(queryId -> new TaskPriorityTracker(splitQueue)));
                taskPriorityTrackerFactory = cache::getUnchecked;
                break;
            default:
                throw new IllegalArgumentException("Unexpected taskPriorityTracking: " + taskPriorityTracking);
        }
        this.taskPriorityTrackerFactory = taskPriorityTrackerFactory;
        this.tasks = new LinkedList<>();
        this.interruptRunawaySplitsTimeout = interruptRunawaySplitsTimeout;
        this.interruptibleSplitPredicate = interruptibleSplitPredicate;
        this.interruptSplitInterval = interruptSplitInterval;
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < runnerThreads; i++) {
            addRunnerThread();
        }
        if (interruptRunawaySplitsTimeout != null) {
            long interval = (long) interruptSplitInterval.getValue(SECONDS);
            splitMonitorExecutor.scheduleAtFixedRate(this::interruptRunawaySplits, interval, interval, SECONDS);
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        closed = true;
        executor.shutdownNow();
        splitMonitorExecutor.shutdownNow();
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("runnerThreads", runnerThreads)
                .add("allSplits", allSplits.size())
                .add("intermediateSplits", intermediateSplits.size())
                .add("waitingSplits", waitingSplits.size())
                .add("runningSplits", runningSplits.size())
                .add("blockedSplits", blockedSplits.size())
                .toString();
    }

    private synchronized void addRunnerThread()
    {
        try {
            executor.execute(embedVersion.embedVersion(new TaskRunner()));
        }
        catch (RejectedExecutionException ignored) {
        }
    }

    public synchronized TaskHandle addTask(
            TaskId taskId,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(utilizationSupplier, "utilizationSupplier is null");
        checkArgument(!maxDriversPerTask.isPresent() || maxDriversPerTask.getAsInt() <= maximumNumberOfDriversPerTask,
                "maxDriversPerTask cannot be greater than the configured value");

        log.debug("Task scheduled " + taskId);

        TaskHandle taskHandle = new TaskHandle(
                taskId,
                taskPriorityTrackerFactory.apply(taskId.getQueryId()),
                utilizationSupplier,
                initialSplitConcurrency,
                splitConcurrencyAdjustFrequency,
                maxDriversPerTask);

        tasks.add(taskHandle);
        return taskHandle;
    }

    public void removeTask(TaskHandle taskHandle)
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskHandle.getTaskId())) {
            doRemoveTask(taskHandle);
        }

        // replace blocked splits that were terminated
        addNewEntrants();
    }

    private void doRemoveTask(TaskHandle taskHandle)
    {
        List<PrioritizedSplitRunner> splits;
        synchronized (this) {
            tasks.remove(taskHandle);
            splits = taskHandle.destroy();

            // stop tracking splits (especially blocked splits which may never unblock)
            allSplits.removeAll(splits);
            intermediateSplits.removeAll(splits);
            blockedSplits.keySet().removeAll(splits);
            waitingSplits.removeAll(splits);
        }

        // call destroy outside of synchronized block as it is expensive and doesn't need a lock on the task executor
        for (PrioritizedSplitRunner split : splits) {
            split.destroy();
        }

        // record completed stats
        long threadUsageNanos = taskHandle.getScheduledNanos();
        completedTasksPerLevel.incrementAndGet(computeLevel(threadUsageNanos));

        log.debug("Task finished or failed " + taskHandle.getTaskId());
    }

    public List<ListenableFuture<?>> enqueueSplits(TaskHandle taskHandle, boolean intermediate, List<? extends SplitRunner> taskSplits)
    {
        List<PrioritizedSplitRunner> splitsToDestroy = new ArrayList<>();
        List<ListenableFuture<?>> finishedFutures = new ArrayList<>(taskSplits.size());
        synchronized (this) {
            for (SplitRunner taskSplit : taskSplits) {
                PrioritizedSplitRunner prioritizedSplitRunner = new PrioritizedSplitRunner(
                        taskHandle,
                        taskSplit,
                        ticker,
                        globalCpuTimeMicros,
                        globalScheduledTimeMicros,
                        blockedQuantaWallTime,
                        unblockedQuantaWallTime);

                if (taskHandle.isDestroyed()) {
                    // If the handle is destroyed, we destroy the task splits to complete the future
                    splitsToDestroy.add(prioritizedSplitRunner);
                }
                else if (intermediate) {
                    // Note: we do not record queued time for intermediate splits
                    startIntermediateSplit(prioritizedSplitRunner);
                    // add the runner to the handle so it can be destroyed if the task is canceled
                    taskHandle.recordIntermediateSplit(prioritizedSplitRunner);
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
        completedSplitsPerLevel.incrementAndGet(split.getPriority().getLevel());
        synchronized (this) {
            allSplits.remove(split);

            long wallNanos = System.nanoTime() - split.getCreatedNanos();
            splitWallTime.add(Duration.succinctNanos(wallNanos));

            if (intermediateSplits.remove(split)) {
                intermediateSplitWallTime.add(wallNanos);
                intermediateSplitScheduledTime.add(split.getScheduledNanos());
                intermediateSplitWaitTime.add(split.getWaitNanos());
                intermediateSplitCpuTime.add(split.getCpuTimeNanos());
            }
            else {
                leafSplitWallTime.add(wallNanos);
                leafSplitScheduledTime.add(split.getScheduledNanos());
                leafSplitWaitTime.add(split.getWaitNanos());
                leafSplitCpuTime.add(split.getCpuTimeNanos());
            }

            TaskHandle taskHandle = split.getTaskHandle();
            taskHandle.splitComplete(split);

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
        if (taskHandle.getRunningLeafSplits() < min(guaranteedNumberOfDriversPerTask, taskHandle.getMaxDriversPerTask().orElse(Integer.MAX_VALUE))) {
            PrioritizedSplitRunner split = taskHandle.pollNextSplit();
            if (split != null) {
                startSplit(split);
                splitQueuedTime.add(Duration.nanosSince(split.getCreatedNanos()));
            }
        }
    }

    private synchronized void addNewEntrants()
    {
        // Ignore intermediate splits when checking minimumNumberOfDrivers.
        // Otherwise with (for example) minimumNumberOfDrivers = 100, 200 intermediate splits
        // and 100 leaf splits, depending on order of appearing splits, number of
        // simultaneously running splits may vary. If leaf splits start first, there will
        // be 300 running splits. If intermediate splits start first, there will be only
        // 200 running splits.
        int running = allSplits.size() - intermediateSplits.size();
        for (int i = 0; i < minimumNumberOfDrivers - running; i++) {
            PrioritizedSplitRunner split = pollNextSplitWorker();
            if (split == null) {
                break;
            }

            splitQueuedTime.add(Duration.nanosSince(split.getCreatedNanos()));
            startSplit(split);
        }
    }

    private synchronized void startIntermediateSplit(PrioritizedSplitRunner split)
    {
        startSplit(split);
        intermediateSplits.add(split);
    }

    private synchronized void startSplit(PrioritizedSplitRunner split)
    {
        allSplits.add(split);
        waitingSplits.offer(split);
    }

    private synchronized PrioritizedSplitRunner pollNextSplitWorker()
    {
        // todo find a better algorithm for this
        // find the first task that produces a split, then move that task to the
        // end of the task list, so we get round robin
        for (Iterator<TaskHandle> iterator = tasks.iterator(); iterator.hasNext(); ) {
            TaskHandle task = iterator.next();
            // skip tasks that are already running the configured max number of drivers
            if (task.getRunningLeafSplits() >= task.getMaxDriversPerTask().orElse(maximumNumberOfDriversPerTask)) {
                continue;
            }
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

    private void interruptRunawaySplits()
    {
        for (RunningSplitInfo splitInfo : runningSplitInfos) {
            Duration duration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
            if (duration.compareTo(interruptRunawaySplitsTimeout) < 0) {
                continue;
            }

            List<StackTraceElement> stack = asList(splitInfo.getThread().getStackTrace());
            if (interruptibleSplitPredicate.test(stack)) {
                String stackString = stack.stream()
                        .map(Object::toString)
                        .collect(joining(lineSeparator()));
                log.warn("Interrupting runaway split " + splitInfo.getSplitInfo() + lineSeparator() + stackString);
                splitInfo.getThread().interrupt();
            }
        }
    }

    private class TaskRunner
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
                        split = waitingSplits.take();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    String threadId = split.getTaskHandle().getTaskId() + "-" + split.getSplitId();
                    try (SetThreadName splitName = new SetThreadName(threadId)) {
                        RunningSplitInfo splitInfo = new RunningSplitInfo(ticker.read(), threadId, Thread.currentThread(), split);
                        runningSplitInfos.add(splitInfo);
                        runningSplits.add(split);

                        ListenableFuture<?> blocked;
                        try {
                            blocked = split.process();
                        }
                        finally {
                            runningSplitInfos.remove(splitInfo);
                            runningSplits.remove(split);
                        }

                        if (split.isFinished()) {
                            log.debug("%s is finished", split.getInfo());
                            splitFinished(split);
                        }
                        else {
                            if (blocked.isDone()) {
                                waitingSplits.offer(split);
                            }
                            else {
                                blockedSplits.put(split, blocked);
                                blocked.addListener(() -> {
                                    blockedSplits.remove(split);
                                    // reset the level priority to prevent previously-blocked splits from starving existing splits
                                    split.resetLevelPriority();
                                    waitingSplits.offer(split);
                                }, executor);
                            }
                        }
                    }
                    catch (Throwable t) {
                        // ignore random errors due to driver thread interruption
                        if (!split.isDestroyed()) {
                            if (t instanceof PrestoException) {
                                PrestoException e = (PrestoException) t;
                                log.error("Error processing %s: %s: %s", split.getInfo(), e.getErrorCode().getName(), e.getMessage());
                            }
                            else {
                                log.error(t, "Error processing %s", split.getInfo());
                            }
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
    public synchronized int getIntermediateSplits()
    {
        return intermediateSplits.size();
    }

    @Managed
    public int getWaitingSplits()
    {
        return waitingSplits.size();
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
    public long getCompletedSplitsLevel0()
    {
        return completedSplitsPerLevel.get(0);
    }

    @Managed
    public long getCompletedSplitsLevel1()
    {
        return completedSplitsPerLevel.get(1);
    }

    @Managed
    public long getCompletedSplitsLevel2()
    {
        return completedSplitsPerLevel.get(2);
    }

    @Managed
    public long getCompletedSplitsLevel3()
    {
        return completedSplitsPerLevel.get(3);
    }

    @Managed
    public long getCompletedSplitsLevel4()
    {
        return completedSplitsPerLevel.get(4);
    }

    @Managed
    public long getRunningTasksLevel0()
    {
        return getRunningTasksForLevel(0);
    }

    @Managed
    public long getRunningTasksLevel1()
    {
        return getRunningTasksForLevel(1);
    }

    @Managed
    public long getRunningTasksLevel2()
    {
        return getRunningTasksForLevel(2);
    }

    @Managed
    public long getRunningTasksLevel3()
    {
        return getRunningTasksForLevel(3);
    }

    @Managed
    public long getRunningTasksLevel4()
    {
        return getRunningTasksForLevel(4);
    }

    @Managed
    @Nested
    public TimeStat getSplitQueuedTime()
    {
        return splitQueuedTime;
    }

    @Managed
    @Nested
    public TimeStat getSplitWallTime()
    {
        return splitWallTime;
    }

    @Managed
    @Nested
    public TimeStat getBlockedQuantaWallTime()
    {
        return blockedQuantaWallTime;
    }

    @Managed
    @Nested
    public TimeStat getUnblockedQuantaWallTime()
    {
        return unblockedQuantaWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitScheduledTime()
    {
        return leafSplitScheduledTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitScheduledTime()
    {
        return intermediateSplitScheduledTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitWallTime()
    {
        return leafSplitWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitWallTime()
    {
        return intermediateSplitWallTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitWaitTime()
    {
        return leafSplitWaitTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitWaitTime()
    {
        return intermediateSplitWaitTime;
    }

    @Managed
    @Nested
    public TimeDistribution getLeafSplitCpuTime()
    {
        return leafSplitCpuTime;
    }

    @Managed
    @Nested
    public TimeDistribution getIntermediateSplitCpuTime()
    {
        return intermediateSplitCpuTime;
    }

    @Managed
    @Nested
    public CounterStat getGlobalScheduledTimeMicros()
    {
        return globalScheduledTimeMicros;
    }

    @Managed
    @Nested
    public CounterStat getGlobalCpuTimeMicros()
    {
        return globalCpuTimeMicros;
    }

    private synchronized int getRunningTasksForLevel(int level)
    {
        int count = 0;
        for (TaskHandle task : tasks) {
            if (task.getPriority().getLevel() == level) {
                count++;
            }
        }
        return count;
    }

    public String getMaxActiveSplitsInfo()
    {
        // Sample output:
        //
        // 2 splits have been continuously active for more than 600.00ms seconds
        //
        // "20180907_054754_00000_88xi4.1.0-2" tid=99 status=running detail=Split 20210429_154412_56509_f63i2.27.0.22-0 {path=hdfs://ns-prod/user/hive/freight_analytics.db/analytics_loads/000002_0, start=67108864, length=67108864, fileSize=531637256, hosts=[], database=freight_analytics, table=analytics_loads, forceLocalScheduling=false, partitionName=<UNPARTITIONED>, s3SelectPushdownEnabled=false} (start = 2.3639101134349113E10, wall = 34 ms, cpu = 0 ms, wait = 189 ms, calls = 1)
        // at java.util.Formatter$FormatSpecifier.<init>(Formatter.java:2708)
        // at java.util.Formatter.parse(Formatter.java:2560)
        // at java.util.Formatter.format(Formatter.java:2501)
        // at ... (more lines of stacktrace)
        //
        // "20180907_054754_00000_88xi4.1.0-3" tid=106 status=running detail=Error processing Split 20210423_234231_69820_3jigi.2.0.19-43 (start = 2.3149319001729774E10, wall = 143673 ms, cpu = 51060 ms, wait = 34701 ms, calls = 70)
        // at java.util.Formatter$FormatSpecifier.<init>(Formatter.java:2709)
        // at java.util.Formatter.parse(Formatter.java:2560)
        // at java.util.Formatter.format(Formatter.java:2501)
        // at ... (more line of stacktrace)
        StringBuilder stackTrace = new StringBuilder();
        int maxActiveSplitCount = 0;
        String message = "%s splits have been continuously active for more than %s seconds\n";
        for (RunningSplitInfo splitInfo : runningSplitInfos) {
            Duration duration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
            if (duration.compareTo(LONG_SPLIT_WARNING_THRESHOLD) >= 0) {
                maxActiveSplitCount++;
                stackTrace.append("\n");
                stackTrace.append(String.format("\"%s\" tid=%s status=%s detail=%s", splitInfo.getThreadId(), splitInfo.getThread().getId(), splitInfo.isFinished() ? "finished" : "running", splitInfo.getSplitInfo())).append("\n");
                for (StackTraceElement traceElement : splitInfo.getThread().getStackTrace()) {
                    stackTrace.append("\tat ").append(traceElement).append("\n");
                }
            }
        }

        return String.format(message, maxActiveSplitCount, LONG_SPLIT_WARNING_THRESHOLD).concat(stackTrace.toString());
    }

    @Managed
    public long getRunAwaySplitCount()
    {
        int count = 0;
        for (RunningSplitInfo splitInfo : runningSplitInfos) {
            Duration duration = Duration.succinctNanos(ticker.read() - splitInfo.getStartTime());
            if (duration.compareTo(LONG_SPLIT_WARNING_THRESHOLD) > 0) {
                count++;
            }
        }
        return count;
    }

    private static class RunningSplitInfo
            implements Comparable<RunningSplitInfo>
    {
        private final long startTime;
        private final String threadId;
        private final Thread thread;
        private final PrioritizedSplitRunner split;
        private boolean printed;

        public RunningSplitInfo(long startTime, String threadId, Thread thread, PrioritizedSplitRunner split)
        {
            this.startTime = startTime;
            this.threadId = threadId;
            this.thread = thread;
            this.split = split;
            this.printed = false;
        }

        public long getStartTime()
        {
            return startTime;
        }

        public String getThreadId()
        {
            return threadId;
        }

        public Thread getThread()
        {
            return thread;
        }

        public boolean isPrinted()
        {
            return printed;
        }

        public void setPrinted()
        {
            printed = true;
        }

        public boolean isFinished()
        {
            return split.isFinished();
        }

        public String getSplitInfo()
        {
            return split.getInfo();
        }

        @Override
        public int compareTo(RunningSplitInfo o)
        {
            return ComparisonChain.start()
                    .compare(startTime, o.getStartTime())
                    .compare(threadId, o.getThreadId())
                    .result();
        }
    }

    @Managed(description = "Task processor executor")
    @Nested
    public ThreadPoolExecutorMBean getProcessorExecutor()
    {
        return executorMBean;
    }
}
