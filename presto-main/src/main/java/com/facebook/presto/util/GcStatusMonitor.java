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
package com.facebook.presto.util;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.GarbageCollectionNotificationInfo;
import com.facebook.presto.execution.SqlTask;
import com.facebook.presto.execution.SqlTaskIoStats;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

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
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.util.StringTableUtils.getTableStrings;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static java.util.AbstractMap.SimpleEntry;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;

public class GcStatusMonitor
{
    private static final Logger log = Logger.get(GcStatusMonitor.class);
    private static final String GC_NOTIFICATION_TYPE = "com.sun.management.gc.notification";
    private final NotificationListener notificationListener = (notification, ignored) -> onNotification(notification);
    private final SqlTaskManager sqlTaskManager;

    @Inject
    public GcStatusMonitor(SqlTaskManager sqlTaskManager)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager must not be null");
    }

    @PostConstruct
    public void start()
    {
        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (mbean.getName().equals("TestingMBeanServer")) {
                continue;
            }

            ObjectName objectName = mbean.getObjectName();
            try {
                ManagementFactory.getPlatformMBeanServer().addNotificationListener(
                        objectName,
                        notificationListener,
                        null,
                        null);
            }
            catch (JMException e) {
                throw new RuntimeException("Unable to add GC listener", e);
            }
        }
    }

    @PreDestroy
    public void stop()
    {
        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            ObjectName objectName = mbean.getObjectName();
            try {
                ManagementFactory.getPlatformMBeanServer().removeNotificationListener(objectName, notificationListener);
            }
            catch (JMException ignored) {
            }
        }
    }

    private void onNotification(Notification notification)
    {
        if (GC_NOTIFICATION_TYPE.equals(notification.getType())) {
            GarbageCollectionNotificationInfo info = new GarbageCollectionNotificationInfo((CompositeData) notification.getUserData());
            if (info.isMajorGc()) {
                onMajorGc();
            }
        }
    }

    private void onMajorGc()
    {
        try {
            logActiveTasks();
        }
        catch (Throwable throwable) {
            log.error(throwable);
        }
    }

    private void logActiveTasks()
    {
        // We only care about active tasks
        List<SqlTask> activeSqlTasks = getActiveSqlTasks();
        ListMultimap<QueryId, SqlTask> activeQueriesToTasksMap = activeSqlTasks.stream()
                .collect(toImmutableListMultimap(task -> task.getQueryContext().getQueryId(), Function.identity()));
        logQueriesAndTasks(activeQueriesToTasksMap);
    }

    private static void logQueriesAndTasks(ListMultimap<QueryId, SqlTask> queryIDToSqlTaskMap)
    {
        Comparator<Entry<QueryId, Long>> comparator = Comparator.comparingLong(Entry::getValue);
        //Print summary of queries sorted from most memory hungry to least memory hungry
        List<QueryId> queryIdsSortedByMemoryUsage = queryIDToSqlTaskMap.asMap().entrySet().stream()
                //Convert to Entry<QueryId, Long>, QueryId -> Total Memory Reservation
                .map(entry ->
                        new SimpleEntry<>(entry.getKey(), entry.getValue().stream()
                                .map(SqlTask::getTaskInfo)
                                .map(TaskInfo::getStats)
                                .mapToLong(stats -> stats.getUserMemoryReservationInBytes() + stats.getSystemMemoryReservationInBytes())
                                .sum())
                ).sorted(comparator.reversed())
                .map(Entry::getKey)
                .collect(toImmutableList());
        logQueriesAndTasks(queryIdsSortedByMemoryUsage, queryIDToSqlTaskMap);
        logTaskStats(queryIdsSortedByMemoryUsage, queryIDToSqlTaskMap);
    }

    private List<SqlTask> getActiveSqlTasks()
    {
        return sqlTaskManager.getAllTasks().stream()
                .filter(task -> !task.getTaskState().isDone())
                .collect(toImmutableList());
    }

    private static void logQueriesAndTasks(List<QueryId> queryIds, ListMultimap<QueryId, SqlTask> tasksByQueryId)
    {
        List<List<String>> rows = queryIds.stream().map(queryId -> {
            List<SqlTask> sqlTasks = tasksByQueryId.get(queryId);
            long userMemoryReservation = sqlTasks.stream()
                    .map(SqlTask::getTaskInfo)
                    .map(TaskInfo::getStats)
                    .mapToLong(TaskStats::getUserMemoryReservationInBytes)
                    .sum();
            long systemMemoryReservation = sqlTasks.stream()
                    .map(SqlTask::getTaskInfo)
                    .map(TaskInfo::getStats)
                    .mapToLong(TaskStats::getSystemMemoryReservationInBytes)
                    .sum();
            return ImmutableList.of(
                    queryId.toString(),
                    Long.toString(userMemoryReservation + systemMemoryReservation),
                    Long.toString(userMemoryReservation),
                    Long.toString(systemMemoryReservation));
        }).collect(toImmutableList());
        if (!rows.isEmpty()) {
            logInfoTable(ImmutableList.<List<String>>builder().add(
                    ImmutableList.of(
                            "Query ID",
                            "Total Memory Reservation",
                            "User Memory Reservation",
                            "System Memory Reservation"))
                    .addAll(rows)
                    .build());
        }
    }

    private static void logTaskStats(List<QueryId> queryIds, ListMultimap<QueryId, SqlTask> tasksByQueryId)
    {
        List<List<String>> rows = queryIds.stream().flatMap(queryId -> {
            List<SqlTask> sqlTasks = tasksByQueryId.get(queryId);
            Comparator<SqlTask> comparator = (first, second) -> {
                TaskStats aTaskStats = first.getTaskInfo().getStats();
                TaskStats bTaskStats = second.getTaskInfo().getStats();
                return Long.compare(aTaskStats.getUserMemoryReservationInBytes() + aTaskStats.getSystemMemoryReservationInBytes(),
                        bTaskStats.getUserMemoryReservationInBytes() + bTaskStats.getSystemMemoryReservationInBytes());
            };
            return sqlTasks.stream()
                    .sorted(comparator.reversed())
                    .map(task -> {
                        TaskInfo taskInfo = task.getTaskInfo();
                        SqlTaskIoStats taskIOStats = task.getIoStats();
                        TaskStatus taskStatus = taskInfo.getTaskStatus();
                        TaskStats taskStats = taskInfo.getStats();
                        return ImmutableList.of(
                                task.getQueryContext().getQueryId().toString(),
                                task.getTaskId().toString(),
                                taskStatus.getState().toString(),
                                taskStats.getCreateTime().toString(),
                                Long.toString(taskStats.getUserMemoryReservationInBytes()),
                                Long.toString(taskStats.getSystemMemoryReservationInBytes()),
                                Long.toString(taskIOStats.getInputDataSize().getTotalCount()),
                                Long.toString(taskIOStats.getOutputDataSize().getTotalCount()),
                                Long.toString(taskIOStats.getInputPositions().getTotalCount()),
                                Long.toString(taskIOStats.getOutputPositions().getTotalCount()));
                    });
        }).collect(toImmutableList());
        if (!rows.isEmpty()) {
            logInfoTable(ImmutableList.<List<String>>builder()
                    .add(ImmutableList.of(
                            "Query ID",
                            "Task ID",
                            "State",
                            "Created Ts",
                            "User Memory",
                            "System Memory",
                            "Input Bytes",
                            "Output Bytes",
                            "Input Row Count",
                            "Output Row Count"))
                    .addAll(rows)
                    .build());
        }
    }

    private static void logInfoTable(List<List<String>> table)
    {
        getTableStrings(table).stream().forEach(log::info);
    }
}
