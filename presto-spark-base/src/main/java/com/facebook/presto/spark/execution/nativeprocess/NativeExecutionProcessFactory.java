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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spark.execution.task.ForNativeExecutionTask;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import okhttp3.OkHttpClient;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.isNativeExecutionProcessReuseEnabled;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcessFactory
{
    private static final Duration MAX_ERROR_DURATION = new Duration(2, TimeUnit.MINUTES);
    private final OkHttpClient httpClient;
    private final ExecutorService coreExecutor;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final WorkerProperty<?, ?, ?> workerProperty;
    private final String executablePath;
    private final String programArguments;

    private static NativeExecutionProcess process;

    @Inject
    public NativeExecutionProcessFactory(
            @ForNativeExecutionTask OkHttpClient httpClient,
            ExecutorService coreExecutor,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            WorkerProperty<?, ?, ?> workerProperty,
            FeaturesConfig featuresConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coreExecutor = requireNonNull(coreExecutor, "coreExecutor is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.workerProperty = requireNonNull(workerProperty, "workerProperty is null");
        this.executablePath = featuresConfig.getNativeExecutionExecutablePath();
        this.programArguments = featuresConfig.getNativeExecutionProgramArguments();
    }

    public synchronized NativeExecutionProcess getNativeExecutionProcess(Session session)
    {
        if (!isNativeExecutionProcessReuseEnabled(session) || process == null || !process.isAlive()) {
            process = createNativeExecutionProcess(session, MAX_ERROR_DURATION);
        }
        return process;
    }

    public NativeExecutionProcess createNativeExecutionProcess(Session session, Duration maxErrorDuration)
    {
        try {
            return new NativeExecutionProcess(
                    executablePath,
                    programArguments,
                    session,
                    httpClient,
                    coreExecutor,
                    errorRetryScheduledExecutor,
                    serverInfoCodec,
                    maxErrorDuration,
                    workerProperty);
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
        if (process != null) {
            process.close();
        }
    }

    protected String getExecutablePath()
    {
        return executablePath;
    }

    protected String getProgramArguments()
    {
        return programArguments;
    }
}
