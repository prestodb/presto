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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import io.airlift.units.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is a testing class that essentially does nothing. Its mere, purpose is to disable the launching and killing of
 * native process by native execution. Instead, it allows the native execution to reuse the same externally launched
 * process over and over again.
 */
public class DetachedNativeExecutionProcess
        extends NativeExecutionProcess
{
    private static final Logger log = Logger.get(DetachedNativeExecutionProcess.class);
    private static final int DEFAULT_PORT = 7777;

    public DetachedNativeExecutionProcess(
            String executablePath,
            URI uri,
            String catalogName,
            HttpClient httpClient,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            TaskManagerConfig taskManagerConfig,
            WorkerProperty<?, ?, ?> workerProperty) throws IOException
    {
        super(executablePath,
                uri,
                catalogName,
                httpClient,
                errorRetryScheduledExecutor,
                serverInfoCodec,
                maxErrorDuration,
                taskManagerConfig,
                workerProperty);
    }

    @Override
    public void start() throws ExecutionException, InterruptedException
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
     * @return a fixed port.
     */
    @Override
    public int getPort()
    {
        String configuredPort = System.getProperty("NATIVE_PORT");
        if (configuredPort != null) {
            return Integer.valueOf(configuredPort);
        }
        return DEFAULT_PORT;
    }
}
