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
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.sql.planner.PlanFragment;
import okhttp3.OkHttpClient;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class NativeExecutionTaskFactory
{
    // TODO add config
    private static final int MAX_THREADS = 1000;

    private final OkHttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec;
    private final TaskManagerConfig taskManagerConfig;
    private final QueryManagerConfig queryManagerConfig;

    @Inject
    public NativeExecutionTaskFactory(
            @ForNativeExecutionTask OkHttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService scheduledExecutorService,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec,
            TaskManagerConfig taskManagerConfig,
            QueryManagerConfig queryManagerConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coreExecutor = requireNonNull(coreExecutor, "coreExecutor is null");
        this.executor = new BoundedExecutor(coreExecutor, MAX_THREADS);
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
    }

    public NativeExecutionTask createNativeExecutionTask(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo,
            Optional<String> shuffleWriteInfo,
            Optional<String> broadcastBasePath)
    {
        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                httpClient,
                taskId,
                location,
                taskInfoCodec,
                planFragmentCodec,
                taskUpdateRequestCodec,
                taskManagerConfig.getInfoRefreshMaxWait(),
                executor,
                scheduledExecutorService,
                queryManagerConfig.getRemoteTaskMaxErrorDuration());
        return new NativeExecutionTask(
                session,
                workerClient,
                fragment,
                sources,
                tableWriteInfo,
                shuffleWriteInfo,
                broadcastBasePath,
                scheduledExecutorService,
                taskManagerConfig);
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }

    public OkHttpClient getHttpClient()
    {
        return httpClient;
    }

    public ExecutorService getCoreExecutor()
    {
        return coreExecutor;
    }

    public Executor getExecutor()
    {
        return executor;
    }

    public ScheduledExecutorService getScheduledExecutorService()
    {
        return scheduledExecutorService;
    }

    public JsonCodec<TaskInfo> getTaskInfoCodec()
    {
        return taskInfoCodec;
    }

    public JsonCodec<PlanFragment> getPlanFragmentCodec()
    {
        return planFragmentCodec;
    }

    public JsonCodec<BatchTaskUpdateRequest> getTaskUpdateRequestCodec()
    {
        return taskUpdateRequestCodec;
    }

    public TaskManagerConfig getTaskManagerConfig()
    {
        return taskManagerConfig;
    }

    public QueryManagerConfig getQueryManagerConfig()
    {
        return queryManagerConfig;
    }
}
