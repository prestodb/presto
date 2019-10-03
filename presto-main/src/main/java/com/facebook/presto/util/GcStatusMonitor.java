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
import com.facebook.presto.operator.TaskStats;
import io.airlift.log.Logger;
import io.airlift.stats.GarbageCollectionNotificationInfo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.management.JMException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static java.lang.Math.max;

public class GcStatusMonitor
{
    private final Logger log = Logger.get(GcStatusMonitor.class);
    private final NotificationListener notificationListener = (notification, ignored) -> onNotification(notification);
    private final SqlTaskManager sqlTaskManager;

    @GuardedBy("this")
    private long lastGcEndTime = System.currentTimeMillis();

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
                onMajorGc(info);
            }
        }
    }

    private void onMajorGc(GarbageCollectionNotificationInfo info)
    {
        long applicationRuntime = max(0, info.getStartTime() - lastGcEndTime);
        lastGcEndTime = info.getEndTime();
        log.info(String.format(
                "%-40s %20s %15s %20s %20s %20s %20s %20s %20s",
                "Task ID",
                "App Agg Runtime",
                "State",
                "Created Ts",
                "Cumulative Mem",
                "Input Size",
                "Output Size",
                "Input Positions",
                "Output Positions"));

        try {
            for (SqlTask task : sqlTaskManager.getAllTasks()) {
                TaskInfo taskInfo = task.getTaskInfo();
                SqlTaskIoStats taskIOStats = task.getIoStats();
                TaskStatus taskStatus = taskInfo.getTaskStatus();
                TaskStats taskStats = taskInfo.getStats();
                // We only really care about inflight tasks
                if (!taskStatus.getState().isDone()) {
                    // In general the formatting follows the following:
                    // - longs: %20.20
                    // - doubles: %20.1f
                    // - strings: discretionary based on the object
                    log.info(String.format(
                            "%-40.40s %20.20s %15.15s %20.20s %20.1f %20.20s %20.20s %20.20s %20.20s",
                            task.getTaskId().toString(),
                            applicationRuntime,
                            taskStatus.getState().toString(),
                            taskStats.getCreateTime().millisOfSecond().get(),
                            taskStats.getCumulativeUserMemory(),
                            taskIOStats.getInputDataSize().getTotalCount(),
                            taskIOStats.getOutputDataSize().getTotalCount(),
                            taskIOStats.getInputPositions().getTotalCount(),
                            taskIOStats.getOutputPositions().getTotalCount()));
                }
            }
        }
        catch (Throwable throwable) {
            log.error(throwable);
        }
    }
}
