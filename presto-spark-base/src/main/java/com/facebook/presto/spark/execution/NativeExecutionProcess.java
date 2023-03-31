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
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpServerClient;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.spark.SparkFiles;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_BINARY_NOT_EXIST;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcess
        implements AutoCloseable
{
    private static final Logger log = Logger.get(NativeExecutionProcess.class);
    private static final String NATIVE_EXECUTION_TASK_ERROR_MESSAGE = "Encountered too many errors talking to native process. The process may have crashed or be under too much load.";
    private static final String WORKER_CONFIG_FILE = "/config.properties";
    private static final String WORKER_NODE_CONFIG_FILE = "/node.properties";
    private static final String WORKER_CONNECTOR_CONFIG_FILE = "/catalog/";

    private final Session session;
    private final PrestoSparkHttpServerClient serverClient;
    private final URI location;
    private final int port;
    private final TaskManagerConfig taskManagerConfig;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final RequestErrorTracker errorTracker;
    private final HttpClient httpClient;
    private final WorkerProperty<?, ?, ?> workerProperty;

    private Process process;

    public NativeExecutionProcess(
            Session session,
            URI uri,
            HttpClient httpClient,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            TaskManagerConfig taskManagerConfig,
            WorkerProperty<?, ?, ?> workerProperty)
            throws IOException
    {
        this.port = getAvailableTcpPort();
        this.session = requireNonNull(session, "session is null");
        this.location = getBaseUriWithPort(requireNonNull(uri, "uri is null"), getPort());
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.serverClient = new PrestoSparkHttpServerClient(
                this.httpClient,
                location,
                serverInfoCodec);
        this.taskManagerConfig = requireNonNull(taskManagerConfig, "taskManagerConfig is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                uri,
                NATIVE_EXECUTION_TASK_ERROR,
                NATIVE_EXECUTION_TASK_ERROR_MESSAGE,
                maxErrorDuration,
                errorRetryScheduledExecutor,
                "getting native process status");
        this.workerProperty = requireNonNull(workerProperty, "workerProperty is null");
    }

    /**
     * Starts the external native execution process. The method will be blocked by connecting to the native process's /v1/info endpoint with backoff retries until timeout.
     */
    public void start()
            throws ExecutionException, InterruptedException, IOException
    {
        String executablePath = getProcessWorkingPath(SystemSessionProperties.getNativeExecutionExecutablePath(session));
        String configPath = Paths.get(getProcessWorkingPath("./"), String.valueOf(port)).toAbsolutePath().toString();

        populateConfigurationFiles(configPath);
        ProcessBuilder processBuilder = new ProcessBuilder(executablePath, "--v", "1", "--etc_dir", configPath);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            log.info("Launching %s \nConfig path: %s\n", executablePath, configPath);
            process = processBuilder.start();
        }
        catch (IOException e) {
            log.error(format("Cannot start %s, error message: %s", processBuilder.command(), e.getMessage()));
            throw new PrestoException(NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR, format("Cannot start %s", processBuilder.command()), e);
        }

        // getServerInfoWithRetry will return a Future on the getting the ServerInfo from the native process, we intentionally block on the Future till
        // the native process successfully response the ServerInfo to ensure the process has been launched and initialized correctly.
        getServerInfoWithRetry().get();
    }

    @VisibleForTesting
    public SettableFuture<ServerInfo> getServerInfoWithRetry()
    {
        SettableFuture<ServerInfo> future = SettableFuture.create();
        doGetServerInfo(future);
        return future;
    }

    @Override
    public void close()
    {
        if (process != null && process.isAlive()) {
            process.destroy();
            try {
                // For native process it takes 10s to initiate SHUTDOWN. The task cleanup interval is 60s. To be sure task cleanup is run at least once we just roughly double the
                // wait time.
                process.waitFor(120, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                if (process.isAlive()) {
                    log.warn("Graceful shutdown of native execution process failed. Force killing it.");
                    process.destroyForcibly();
                }
            }
        }
    }

    public int getPort()
    {
        return port;
    }

    public URI getLocation()
    {
        return location;
    }

    private static URI getBaseUriWithPort(URI baseUri, int port)
    {
        return uriBuilderFrom(baseUri)
                .port(port)
                .build();
    }

    private static int getAvailableTcpPort()
            throws IOException
    {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    private String getNativeExecutionCatalogName(Session session)
    {
        checkArgument(session.getCatalog().isPresent(), "Catalog isn't set in the session.");
        return session.getCatalog().get();
    }

    private void populateConfigurationFiles(String configBasePath)
            throws IOException
    {
        // The reason we have to pick and assign the port per worker is in our prod environment,
        // there is no port isolation among all the containers running on the same host, so we have
        // to pick unique port per worker to avoid port collision. This config will be passed down to
        // the native execution process eventually for process initialization.
        workerProperty.getSystemConfig().setHttpServerPort(port);
        workerProperty.populateAllProperties(
                Paths.get(configBasePath, WORKER_CONFIG_FILE),
                Paths.get(configBasePath, WORKER_NODE_CONFIG_FILE),
                Paths.get(configBasePath, format("%s%s.properties", WORKER_CONNECTOR_CONFIG_FILE, getNativeExecutionCatalogName(session))));
    }

    private void doGetServerInfo(SettableFuture<ServerInfo> future)
    {
        addCallback(serverClient.getServerInfo(), new FutureCallback<BaseResponse<ServerInfo>>()
        {
            @Override
            public void onSuccess(@Nullable BaseResponse<ServerInfo> response)
            {
                if (response.getStatusCode() != OK.code()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with HTTP status " + response.getStatusCode());
                }
                future.set(response.getValue());
            }

            @Override
            public void onFailure(Throwable failedReason)
            {
                if (failedReason instanceof RejectedExecutionException && httpClient.isClosed()) {
                    log.error(format("Unable to start the native process. HTTP client is closed. Reason: %s", failedReason.getMessage()));
                    future.setException(failedReason);
                    return;
                }
                // record failure
                try {
                    errorTracker.requestFailed(failedReason);
                }
                catch (PrestoException e) {
                    future.setException(e);
                    return;
                }
                // if throttled due to error, asynchronously wait for timeout and try again
                ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
                if (errorRateLimit.isDone()) {
                    doGetServerInfo(future);
                }
                else {
                    errorRateLimit.addListener(() -> doGetServerInfo(future), errorRetryScheduledExecutor);
                }
            }
        }, directExecutor());
    }

    private String getProcessWorkingPath(String path)
    {
        File absolutePath = new File(path);
        File workingDir = new File(SparkFiles.getRootDirectory());
        if (!absolutePath.isAbsolute()) {
            absolutePath = new File(workingDir, path);
        }

        if (!absolutePath.exists()) {
            log.error(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
            throw new PrestoException(NATIVE_EXECUTION_BINARY_NOT_EXIST, format("File doesn't exist %s", absolutePath.getAbsolutePath()));
        }

        return absolutePath.getAbsolutePath();
    }
}
