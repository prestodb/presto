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
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DetachedNativeExecutionProcessFactory
        extends NativeExecutionProcessFactory
{
    private final HttpClient httpClient;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final TaskManagerConfig taskManagerConfig;
    private final WorkerProperty<?, ?, ?> workerProperty;

    @Inject
    public DetachedNativeExecutionProcessFactory(
            @ForNativeExecutionTask HttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            TaskManagerConfig taskManagerConfig,
            WorkerProperty<?, ?, ?> workerProperty)
    {
        super(httpClient, coreExecutor, errorRetryScheduledExecutor, serverInfoCodec, taskManagerConfig, workerProperty);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        this.workerProperty = requireNonNull(workerProperty, "workerProperty is null");
    }

    @Override
    public NativeExecutionProcess get(
            Session session,
            URI location)
    {
        return get(session, location, new Duration(2, TimeUnit.MINUTES));
    }

    @Override
    public NativeExecutionProcess get(
            Session session,
            URI location,
            Duration maxErrorDuration)
    {
        try {
            return new DetachedNativeExecutionProcess(
                    session,
                    location,
                    httpClient,
                    errorRetryScheduledExecutor,
                    serverInfoCodec,
                    maxErrorDuration,
                    taskManagerConfig,
                    workerProperty);
        }
        catch (IOException e) {
            throw new PrestoException(NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR, format("Cannot start native process: %s", e.getMessage()), e);
        }
    }
}
