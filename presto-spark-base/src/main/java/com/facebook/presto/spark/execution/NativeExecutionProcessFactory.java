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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcessFactory
{
    // TODO add config
    private static final int MAX_THREADS = 1000;
    private static final Duration MAX_ERROR_DURATION = new Duration(2, TimeUnit.MINUTES);

    private final HttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final NativeExecutionTaskFactory taskFactory;
    private final TaskManagerConfig taskManagerConfig;
    private final NativeExecutionSystemConfig systemConfig;
    private final NativeExecutionNodeConfig nodeConfig;
    private final NativeExecutionConnectorConfig connectorConfig;

    @Inject
    public NativeExecutionProcessFactory(
            @ForNativeExecutionTask HttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            NativeExecutionTaskFactory taskFactory,
            TaskManagerConfig taskManagerConfig,
            NativeExecutionSystemConfig systemConfig,
            NativeExecutionNodeConfig nodeConfig,
            NativeExecutionConnectorConfig connectorConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coreExecutor = requireNonNull(coreExecutor, "coreExecutor is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.taskFactory = requireNonNull(taskFactory, "taskFactory is null");
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        this.systemConfig = requireNonNull(systemConfig, "systemConfig is null");
        this.nodeConfig = requireNonNull(nodeConfig, "nodeConfig is null");
        this.connectorConfig = requireNonNull(connectorConfig, "connectorConfig is null");
    }

    public NativeExecutionProcess createNativeExecutionProcess(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo)
            throws IOException
    {
        return createNativeExecutionProcess(session, location, taskId, fragment, sources, tableWriteInfo, MAX_ERROR_DURATION);
    }

    public NativeExecutionProcess createNativeExecutionProcess(
            Session session,
            URI location,
            TaskId taskId,
            PlanFragment fragment,
            List<TaskSource> sources,
            TableWriteInfo tableWriteInfo,
            Duration maxErrorDuration)
    {
        try {
            return new NativeExecutionProcess(
                    session,
                    location,
                    taskId,
                    fragment,
                    sources,
                    tableWriteInfo,
                    httpClient,
                    errorRetryScheduledExecutor,
                    serverInfoCodec,
                    taskFactory,
                    maxErrorDuration,
                    taskManagerConfig,
                    systemConfig,
                    nodeConfig,
                    connectorConfig);
        }
        catch (IOException e) {
            throw new PrestoException(NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR, format("Cannot start native process: %s", e.getMessage()), e);
        }
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        errorRetryScheduledExecutor.shutdownNow();
    }
}
