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
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.executor.TaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LowMemoryMonitor
{
    private static final Logger log = Logger.get(LowMemoryMonitor.class);
    private final ScheduledExecutorService lowMemoryExecutor = newScheduledThreadPool(1, daemonThreadsNamed("low-memory-monitor-executor"));
    private final TaskExecutor taskExecutor;
    private final double threshold;
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    @Inject
    public LowMemoryMonitor(TaskExecutor taskExecutor, TaskManagerConfig taskManagerConfig)
    {
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.threshold = taskManagerConfig.getMemoryBasedSlowDownThreshold();
    }

    @PostConstruct
    public void start()
    {
        if (threshold < 1.0) {
            lowMemoryExecutor.scheduleWithFixedDelay(() -> checkLowMemory(), 1, 1, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void stop()
    {
        lowMemoryExecutor.shutdown();
    }

    private void checkLowMemory()
    {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

        long usedMemory = memoryUsage.getUsed();
        long maxMemory = memoryUsage.getMax();
        long memoryThreshold = (long) (maxMemory * threshold);

        if (usedMemory > memoryThreshold) {
            if (!taskExecutor.isLowMemory()) {
                log.debug("Enabling Low Memory: Used: %s Max: %s Threshold: %s", usedMemory, maxMemory, memoryThreshold);
                taskExecutor.setLowMemory(true);
            }
        }
        else {
            if (taskExecutor.isLowMemory()) {
                log.debug("Disabling Low Memory: Used: %s Max: %s Threshold: %s", usedMemory, maxMemory, memoryThreshold);
                taskExecutor.setLowMemory(false);
            }
        }
    }
}
