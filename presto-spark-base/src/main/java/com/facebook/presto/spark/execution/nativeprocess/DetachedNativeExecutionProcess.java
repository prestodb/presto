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
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * This is a testing class that essentially does nothing. Its mere purpose is to disable the launching and killing of
 * native process by native execution. Instead it allows the native execution to reuse the same externally launched
 * process over and over again.
 */
public class DetachedNativeExecutionProcess
        extends NativeExecutionProcess
{
    private static final Logger log = Logger.get(DetachedNativeExecutionProcess.class);

    public DetachedNativeExecutionProcess(
            String executablePath,
            String programArguments,
            Session session,
            OkHttpClient httpClient,
            ExecutorService executorService,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            WorkerProperty<?, ?, ?> workerProperty)
            throws IOException
    {
        super(executablePath,
                programArguments,
                session,
                httpClient,
                executorService,
                errorRetryScheduledExecutor,
                serverInfoCodec,
                maxErrorDuration,
                workerProperty);
    }

    @Override
    public void start()
            throws ExecutionException, InterruptedException
    {
        log.info("Please use port " + getPort() + " for detached native process launching.");
        // getServerInfoWithRetry will return a Future on the getting the ServerInfo from the native process, we
        // intentionally block on the Future till the native process successfully response the ServerInfo to ensure the
        // process has been launched and initialized correctly.
        getServerInfoWithRetry().get();
    }

    /**
     * The port Spark native is going to use instead of dynamically generate. Since this class is for local debugging
     * only, there is no need to make this port configurable.
     *
     * @return a fixed port.
     */
    @Override
    public int getPort()
    {
        String configuredPort = requireNonNull(System.getProperty("NATIVE_PORT"), "NATIVE_PORT not set for interactive debugging");
        return Integer.valueOf(configuredPort);
    }
}
