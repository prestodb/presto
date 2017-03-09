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
package com.facebook.presto.execution.controller;

import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TaskExecutorStatisticsFactory
{
    private static final String OPERATING_SYSTEM_OBJECT = "java.lang:type=OperatingSystem";
    private static final String PROCESS_CPU_TIME_ATTRIBUTE = "ProcessCpuTime";
    private static final Logger logger = Logger.get(TaskExecutorStatisticsFactory.class);

    private final MBeanServer mBeanServer;
    private final ObjectName operatingSystemObjectName;

    private long lastWallNanos;
    private long lastCpuNanos;

    public TaskExecutorStatisticsFactory()
    {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            operatingSystemObjectName = new ObjectName(OPERATING_SYSTEM_OBJECT);
        }
        catch (MalformedObjectNameException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot create ObjectName for " + OPERATING_SYSTEM_OBJECT);
        }
        OptionalLong cpuTime = getProcessCpuTime();
        if (cpuTime.isPresent()) {
            lastCpuNanos = cpuTime.getAsLong();
            lastWallNanos = System.nanoTime();
        }
    }

    public Optional<TaskExecutorStatistics> createTaskExectorStatistics(int runnerThreads, int allSplitCount)
    {
        OptionalLong cpuTime = getProcessCpuTime();
        if (!cpuTime.isPresent()) {
            return Optional.empty();
        }
        long currentCpuNanos = cpuTime.getAsLong();
        long currentWallNanos = System.nanoTime();
        TaskExecutorStatistics state = new TaskExecutorStatistics(
                new Duration(currentCpuNanos - lastCpuNanos, NANOSECONDS),
                new Duration(currentWallNanos - lastWallNanos, NANOSECONDS),
                runnerThreads,
                allSplitCount);
        lastCpuNanos = currentCpuNanos;
        lastWallNanos = currentWallNanos;
        return Optional.of(state);
    }

    private OptionalLong getProcessCpuTime()
    {
        try {
            return OptionalLong.of((long) mBeanServer.getAttribute(operatingSystemObjectName, PROCESS_CPU_TIME_ATTRIBUTE));
        }
        catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException e) {
            logger.error(
                    "Cannot get %s attribute from %s object when creating TaskExecutorStatistics due to: %s",
                    PROCESS_CPU_TIME_ATTRIBUTE,
                    OPERATING_SYSTEM_OBJECT,
                    e);
        }
        return OptionalLong.empty();
    }
}
