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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.sql.planner.PlanFragment;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class NativeExecutionTaskFactory
{
    // TODO add config
    private static final int MAX_THREADS = 1000;

    private final HttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;

    @Inject
    public NativeExecutionTaskFactory(
            @ForNativeExecutionTask HttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService updateScheduledExecutor)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(coreExecutor, "coreExecutor is null");
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");

        this.httpClient = httpClient;
        this.coreExecutor = coreExecutor;
        this.executor = new BoundedExecutor(coreExecutor, MAX_THREADS);
        this.updateScheduledExecutor = updateScheduledExecutor;
    }

    public NativeExecutionTask createNativeExecutionTask(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
    {
        return new NativeExecutionTask(
                session,
                location,
                taskId,
                fragment,
                sources,
                httpClient,
                tableWriteInfo,
                executor,
                updateScheduledExecutor);
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
    }
}
