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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.sql.planner.PlanFragment;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

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
    private final RemoteTaskStats stats;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    @Inject
    public NativeExecutionTaskFactory(
            @ForNativeExecutionTask HttpClient httpClient,
            RemoteTaskStats stats,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskInfo> taskInfoCodec,
            ExecutorService coreExecutor,
            ScheduledExecutorService updateScheduledExecutor)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(coreExecutor, "coreExecutor is null");
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");

        this.httpClient = httpClient;
        this.stats = stats;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.planFragmentCodec = planFragmentCodec;
        this.taskInfoCodec = taskInfoCodec;
        this.coreExecutor = coreExecutor;
        this.executor = new BoundedExecutor(coreExecutor, MAX_THREADS);
        this.updateScheduledExecutor = updateScheduledExecutor;
    }

    public NativeExecutionTask createNativeExecutionTask(
            Session session,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
    {
        return new NativeExecutionTask(
                session,
                taskId,
                fragment,
                sources,
                httpClient,
                tableWriteInfo,
                stats,
                executor,
                updateScheduledExecutor,
                taskUpdateRequestCodec,
                planFragmentCodec,
                taskInfoCodec);
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
    }
}
