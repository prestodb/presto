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

import com.facebook.presto.execution.SqlTask;
import com.facebook.presto.execution.SqlTaskIoStats;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.QueryId;
import io.airlift.log.Logger;
import io.airlift.stats.GarbageCollectionNotificationInfo;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GcStatusMonitor
{
    private final Logger log = Logger.get(GcStatusMonitor.class);
    private final NotificationListener notificationListener = (notification, ignored) -> onNotification(notification);
    private final SqlTaskManager sqlTaskManager;

    @Inject
    public GcStatusMonitor(
            SqlTaskManager taskManager)
    {
        this.sqlTaskManager = taskManager;
    }

    @PostConstruct
    public void start()
    {
        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
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

    private synchronized void onNotification(Notification notification)
    {
        if ("com.sun.management.gc.notification".equals(notification.getType())) {
            GarbageCollectionNotificationInfo info = new GarbageCollectionNotificationInfo((CompositeData) notification.getUserData());
            if (info.isMajorGc()) {
                onMajorGc();
            }
        }
    }

    private void onMajorGc()
    {
        try {
            onMajorGcLogging();
        }
        catch (Throwable throwable) {
            log.error(throwable);
        }
    }

    private void onMajorGcLogging()
    {
        // We only care about active tasks
        List<SqlTask> activeSqlTasks = getActiveSqlTasks();
        Map<QueryId, List<SqlTask>> activeQueriesToTasksMap = getQueriesToTaskMap(activeSqlTasks);
        logTaskInfosByQueryID(activeQueriesToTasksMap);
    }

    private Map<QueryId, List<SqlTask>> getQueriesToTaskMap(List<SqlTask> sqlTasks)
    {
        Map<QueryId, List<SqlTask>> queriesToActiveTasks = new HashMap<>();
        for (SqlTask task : sqlTasks) {
            QueryContext queryContext = task.getQueryContext();
            if (!queriesToActiveTasks.containsKey(queryContext.getQueryId())) {
                queriesToActiveTasks.put(queryContext.getQueryId(), new ArrayList<>());
            }

            queriesToActiveTasks.get(queryContext.getQueryId()).add(task);
        }

        return queriesToActiveTasks;
    }

    private List<SqlTask> getActiveSqlTasks()
    {
        ArrayList<SqlTask> sqlTasks = new ArrayList<>();
        for (SqlTask task : sqlTaskManager.getAllTasks()) {
            if (!task.getTaskInfo().getTaskStatus().getState().isDone()) {
                sqlTasks.add(task);
            }
        }

        return sqlTasks;
    }

    private void logInfoTable(List<List<String>> table)
    {
        for (String row : StringTableUtils.getTableStrings(table)) {
            log.info(row);
        }
    }

    private void logQueryInfo(QueryContext ctx)
    {
        logInfoTable(Arrays.asList(
                Arrays.asList(
                        "Query ID",
                        "Max User Bytes",
                        "Max Total Bytes"),
                Arrays.asList(
                        ctx.getQueryId().toString(),
                        Long.toString(ctx.getMaxUserMemory()),
                        Long.toString(ctx.getMaxTotalMemory()))));
    }

    private void logTaskInfos(List<SqlTask> sqlTasks)
    {
        ArrayList<List<String>> taskLogInfoList = new ArrayList<>();
        taskLogInfoList.add(Arrays.asList(
                "Task ID",
                "Query ID",
                "State",
                "Created Ts",
                "User Memory",
                "System Memory",
                "Input Bytes",
                "Output Bytes",
                "Input Row Count",
                "Output Row Count"));

        for (SqlTask task : sqlTasks) {
            TaskInfo taskInfo = task.getTaskInfo();
            SqlTaskIoStats taskIOStats = task.getIoStats();
            TaskStatus taskStatus = taskInfo.getTaskStatus();
            TaskStats taskStats = taskInfo.getStats();
            taskLogInfoList.add(Arrays.asList(
                    task.getTaskId().toString(),
                    task.getQueryContext().getQueryId().toString(),
                    taskStatus.getState().toString(),
                    taskStats.getCreateTime().toString(),
                    taskStats.getUserMemoryReservation().toString(),
                    taskStats.getSystemMemoryReservation().toString(),
                    Long.toString(taskIOStats.getInputDataSize().getTotalCount()),
                    Long.toString(taskIOStats.getOutputDataSize().getTotalCount()),
                    Long.toString(taskIOStats.getInputPositions().getTotalCount()),
                    Long.toString(taskIOStats.getOutputPositions().getTotalCount())));
        }

        logInfoTable(taskLogInfoList);
    }

    private void logTaskInfosByQueryID(Map<QueryId, List<SqlTask>> queryIDToSqlTaskMap)
    {
        for (Map.Entry<QueryId, List<SqlTask>> mapEntry : queryIDToSqlTaskMap.entrySet()) {
            // Log the Query
            logQueryInfo(sqlTaskManager.getQueryContext(mapEntry.getKey()));
            // Log the Tasks Associated with the Query
            logTaskInfos(mapEntry.getValue());
        }
    }
}
