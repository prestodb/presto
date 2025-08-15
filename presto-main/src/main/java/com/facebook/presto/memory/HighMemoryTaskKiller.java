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
package com.facebook.presto.memory;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.GarbageCollectionNotificationInfo;
import com.facebook.presto.execution.SqlTask;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ListMultimap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.management.JMException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.facebook.presto.memory.HighMemoryTaskKillerStrategy.FREE_MEMORY_ON_FREQUENT_FULL_GC;
import static com.facebook.presto.memory.HighMemoryTaskKillerStrategy.FREE_MEMORY_ON_FULL_GC;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_HEAP_MEMORY_LIMIT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HighMemoryTaskKiller
{
    private static final Logger log = Logger.get(HighMemoryTaskKiller.class);
    private static final String GC_NOTIFICATION_TYPE = "com.sun.management.gc.notification";
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final NotificationListener gcNotificationListener = (notification, ignored) -> onGCNotification(notification);
    private final SqlTaskManager sqlTaskManager;
    private final HighMemoryTaskKillerStrategy taskKillerStrategy;
    private final boolean taskKillerEnabled;
    private final Duration taskKillerFrequentFullGCDurationThreshold;
    private Duration lastFullGCTimestamp;
    private long lastFullGCCollectedBytes;
    private final long reclaimMemoryThreshold;
    private final long heapMemoryThreshold;
    Ticker ticker;

    @Inject
    public HighMemoryTaskKiller(SqlTaskManager sqlTaskManager, TaskManagerConfig taskManagerConfig)
    {
        requireNonNull(taskManagerConfig, "taskManagerConfig is null");

        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager must not be null");

        this.taskKillerStrategy = taskManagerConfig.getHighMemoryTaskKillerStrategy();
        this.taskKillerEnabled = taskManagerConfig.isHighMemoryTaskKillerEnabled();

        this.taskKillerFrequentFullGCDurationThreshold = taskManagerConfig.getHighMemoryTaskKillerFrequentFullGCDurationThreshold();
        this.reclaimMemoryThreshold = (long) (memoryMXBean.getHeapMemoryUsage().getMax() * taskManagerConfig.getHighMemoryTaskKillerGCReclaimMemoryThreshold());

        this.heapMemoryThreshold = (long) (memoryMXBean.getHeapMemoryUsage().getMax() * taskManagerConfig.getHighMemoryTaskKillerHeapMemoryThreshold());
        this.ticker = Ticker.systemTicker();
    }

    @PostConstruct
    public void start()
    {
        if (!taskKillerEnabled) {
            return;
        }

        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (mbean.getName().equals("TestingMBeanServer")) {
                continue;
            }

            ObjectName objectName = mbean.getObjectName();
            try {
                ManagementFactory.getPlatformMBeanServer().addNotificationListener(
                        objectName,
                        gcNotificationListener,
                        null,
                        null);
            }
            catch (JMException e) {
                throw new RuntimeException("Unable to add listener", e);
            }
        }
    }

    @PreDestroy
    public void stop()
    {
        if (!taskKillerEnabled) {
            return;
        }

        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            ObjectName objectName = mbean.getObjectName();
            try {
                ManagementFactory.getPlatformMBeanServer().removeNotificationListener(objectName, gcNotificationListener);
            }
            catch (JMException ignored) {
                log.error("Error removing notification: " + ignored);
            }
        }
    }

    private void onGCNotification(Notification notification)
    {
        if (GC_NOTIFICATION_TYPE.equals(notification.getType())) {
            GarbageCollectionNotificationInfo info = new GarbageCollectionNotificationInfo((CompositeData) notification.getUserData());
            if (info.isMajorGc()) {
                if (shouldTriggerTaskKiller(info)) {
                    //Kill task consuming most memory
                    List<SqlTask> activeTasks = getActiveTasks();
                    ListMultimap<QueryId, SqlTask> activeQueriesToTasksMap = activeTasks.stream()
                            .collect(toImmutableListMultimap(task -> task.getQueryContext().getQueryId(), Function.identity()));

                    Optional<QueryId> queryId = getMaxMemoryConsumingQuery(activeQueriesToTasksMap);

                    if (queryId.isPresent()) {
                        List<SqlTask> activeTasksToKill = activeQueriesToTasksMap.get(queryId.get());
                        for (SqlTask sqlTask : activeTasksToKill) {
                            TaskStats taskStats = sqlTask.getTaskInfo().getStats();
                            sqlTask.failed(new PrestoException(EXCEEDED_HEAP_MEMORY_LIMIT, format("Worker heap memory limit exceeded: User Memory: %d, System Memory: %d, Revocable Memory: %d", taskStats.getUserMemoryReservationInBytes(), taskStats.getSystemMemoryReservationInBytes(), taskStats.getRevocableMemoryReservationInBytes())));
                        }
                    }
                }
            }
        }
    }

    private boolean shouldTriggerTaskKiller(GarbageCollectionNotificationInfo info)
    {
        boolean triggerTaskKiller = false;
        DataSize beforeGcDataSize = info.getBeforeGcTotal();
        DataSize afterGcDataSize = info.getAfterGcTotal();

        if (taskKillerStrategy == FREE_MEMORY_ON_FREQUENT_FULL_GC) {
            long currentGarbageCollectedBytes = beforeGcDataSize.toBytes() - afterGcDataSize.toBytes();
            Duration currentFullGCTimestamp = new Duration(ticker.read(), TimeUnit.NANOSECONDS);

            if (isFrequentFullGC(lastFullGCTimestamp, currentFullGCTimestamp) && !hasFullGCFreedEnoughBytes(currentGarbageCollectedBytes)) {
                triggerTaskKiller = true;
            }

            lastFullGCTimestamp = currentFullGCTimestamp;
            lastFullGCCollectedBytes = currentGarbageCollectedBytes;
        }
        else if (taskKillerStrategy == FREE_MEMORY_ON_FULL_GC) {
            if (isLowMemory() && beforeGcDataSize.toBytes() - afterGcDataSize.toBytes() < reclaimMemoryThreshold) {
                triggerTaskKiller = true;
            }
        }
        log.debug("Task Killer Trigger: " + triggerTaskKiller + ", Before Full GC Head Size: " + beforeGcDataSize.toBytes() + " After Full GC Heap Size: " + afterGcDataSize.toBytes());

        return triggerTaskKiller;
    }

    private List<SqlTask> getActiveTasks()
    {
        return sqlTaskManager.getAllTasks().stream()
                .filter(task -> !task.getTaskState().isDone())
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public static Optional<QueryId> getMaxMemoryConsumingQuery(ListMultimap<QueryId, SqlTask> queryIDToSqlTaskMap)
    {
        if (queryIDToSqlTaskMap.isEmpty()) {
            return Optional.empty();
        }

        Comparator<Map.Entry<QueryId, Long>> comparator = Comparator.comparingLong(Map.Entry::getValue);

        Optional<QueryId> maxMemoryConsumpingQueryId = queryIDToSqlTaskMap.asMap().entrySet().stream()
                .map(entry ->
                        new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().stream()
                                .map(SqlTask::getTaskInfo)
                                .map(TaskInfo::getStats)
                                .mapToLong(stats -> stats.getUserMemoryReservationInBytes() + stats.getSystemMemoryReservationInBytes() + stats.getRevocableMemoryReservationInBytes())
                                .sum())
                ).max(comparator).map(Map.Entry::getKey);

        return maxMemoryConsumpingQueryId;
    }

    private boolean isFrequentFullGC(Duration lastFullGCTime, Duration currentFullGCTime)
    {
        long diffBetweenFullGCMilis = currentFullGCTime.toMillis() - lastFullGCTime.toMillis();
        log.debug("Time difference between last 2 full GC in miliseconds: " + diffBetweenFullGCMilis);
        if (diffBetweenFullGCMilis > taskKillerFrequentFullGCDurationThreshold.getValue(TimeUnit.MILLISECONDS)) {
            log.debug("Skip killing tasks Due to full GCs were not happening frequently.");
            return false;
        }
        return true;
    }

    private boolean hasFullGCFreedEnoughBytes(long currentGarbageCollectedBytes)
    {
        if (currentGarbageCollectedBytes < reclaimMemoryThreshold && lastFullGCCollectedBytes < reclaimMemoryThreshold) {
            log.debug("Full GC not able to free enough memory. Current freed bytes: " + currentGarbageCollectedBytes + " previously freed bytes: " + lastFullGCCollectedBytes);
            return false;
        }
        log.debug("Full GC able to free enough memory. Current freed bytes: " + currentGarbageCollectedBytes + " previously freed bytes: " + lastFullGCCollectedBytes);
        return true;
    }

    private boolean isLowMemory()
    {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

        if (memoryUsage.getUsed() > heapMemoryThreshold) {
            return true;
        }

        return false;
    }
}
