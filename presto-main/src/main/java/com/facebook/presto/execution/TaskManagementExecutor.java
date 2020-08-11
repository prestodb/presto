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

import io.airlift.concurrent.ThreadPoolExecutorMBean;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Holder of {@link ScheduledExecutorService} used for task management.
 */
public final class TaskManagementExecutor
        implements AutoCloseable
{
    private final ScheduledExecutorService taskManagementExecutor;
    private final ThreadPoolExecutorMBean taskManagementExecutorMBean;

    public TaskManagementExecutor()
    {
        taskManagementExecutor = newScheduledThreadPool(5, threadsNamed("task-management-%s"));
        taskManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskManagementExecutor);
    }

    @PreDestroy
    public void close()
    {
        taskManagementExecutor.shutdownNow();
    }

    public ScheduledExecutorService getExecutor()
    {
        return taskManagementExecutor;
    }

    @Managed(description = "Task garbage collector executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskManagementExecutor()
    {
        return taskManagementExecutorMBean;
    }
}
