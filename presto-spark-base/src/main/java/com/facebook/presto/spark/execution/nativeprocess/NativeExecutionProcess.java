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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpServerClient;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.spark.SparkEnv$;
import org.apache.spark.SparkFiles;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
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
    private static final String NATIVE_EXECUTION_TASK_ERROR_MESSAGE = "Native process launch failed with multiple retries.";
    private static final String WORKER_CONFIG_FILE = "/config.properties";
    private static final String WORKER_NODE_CONFIG_FILE = "/node.properties";
    private static final String WORKER_VELOX_CONFIG_FILE = "/velox.properties";
    private static final String WORKER_CONNECTOR_CONFIG_FILE = "/catalog/";
    private static final int SIGSYS = 31;

    private final Session session;
    private final PrestoSparkHttpServerClient serverClient;
    private final URI location;
    private final int port;
    private final TaskManagerConfig taskManagerConfig;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final RequestErrorTracker errorTracker;
    private final HttpClient httpClient;
    private final WorkerProperty<?, ?, ?, ?> workerProperty;

    private volatile Process process;

    public NativeExecutionProcess(
            Session session,
            URI uri,
            HttpClient httpClient,
            ScheduledExecutorService errorRetryScheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            TaskManagerConfig taskManagerConfig,
            WorkerProperty<?, ?, ?, ?> workerProperty)
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
    public synchronized void start()
            throws ExecutionException, InterruptedException, IOException
    {
        if (process != null && process.isAlive()) {
            return;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(getLaunchCommand());
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            process = processBuilder.start();
        }
        catch (IOException e) {
            log.error(format("Cannot start %s, error message: %s", processBuilder.command(), e.getMessage()));
            throw new PrestoException(NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR, format("Cannot start %s", processBuilder.command()), e);
        }

        // getServerInfoWithRetry will return a Future on the getting the ServerInfo from the native process, we intentionally block on the Future till
        // the native process successfully response the ServerInfo to ensure the process has been launched and initialized correctly.
        try {
            getServerInfoWithRetry().get();
        }
        catch (Throwable t) {
            close();
            // If the native process launch failed, it usually indicates the current host machine is overloaded, we need to throw a fatal error (PrestoSparkFatalException is a
            // subclass of fatal error VirtualMachineError)to let Spark shutdown current executor and fail over to another one (Here is the definition of scala fatal error Spark
            // is relying on: https://www.scala-lang.org/api/2.13.3/scala/util/control/NonFatal$.html)
            throw new PrestoSparkFatalException(t.getMessage(), t.getCause());
        }
    }

    @VisibleForTesting
    public SettableFuture<ServerInfo> getServerInfoWithRetry()
    {
        SettableFuture<ServerInfo> future = SettableFuture.create();
        doGetServerInfo(future);
        return future;
    }

    /**
     * Triggers coredump (also terminates the process)
     */
    public void sendCoreSignal()
    {
        // chosen as the least likely core signal to occur naturally (invalid sys call)
        // https://man7.org/linux/man-pages/man7/signal.7.html
        sendSignal(SIGSYS);
    }

    public void sendSignal(int signal)
    {
        Process process = this.process;
        if (process == null) {
            log.warn("Failure sending signal, process does not exist");
            return;
        }
        long pid = getPid(process);
        if (!process.isAlive()) {
            log.warn("Failure sending signal, process is dead: %s", pid);
            return;
        }
        try {
            log.info("Sending signal to process %s: %s", pid, signal);
            Runtime.getRuntime().exec(format("kill -%s %s", signal, pid));
        }
        catch (IOException e) {
            log.warn(e, "Failure sending signal to process %s", pid);
        }
    }

    private static long getPid(Process p)
    {
        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                long pid = f.getLong(p);
                f.setAccessible(false);
                return pid;
            }
            return -1;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen
            throw new AssertionError(e);
        }
    }

    @Override
    public void close()
    {
        if (process != null && process.isAlive()) {
            long pid = getPid(process);
            log.info("Destroying process: %s", pid);
            process.destroy();
            try {
                // This 1 sec is arbitrary. Ideally, we do not need to be give any heads up
                // to CPP process on presto-on-spark native, because the resources
                // are reclaimed by the container manager.
                // For localmode, we still want to provide an opportunity for
                // graceful termination as there is no resource/container manager.
                process.waitFor(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                if (process.isAlive()) {
                    log.warn("Graceful shutdown of native execution process failed. Force killing it: %s", pid);
                    process.destroyForcibly();
                }
            }
        }
        else if (process != null) {
            log.info("Process is dead: %s", getPid(process));
            process = null;
        }
    }

    public boolean isAlive()
    {
        return process != null && process.isAlive();
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
    {
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        }
        catch (Exception ex) {
            // Something is wrong with the executor
            // Fail the executor
            throw new PrestoSparkFatalException("Failed to acquire port on host", ex);
        }
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
                Paths.get(configBasePath, WORKER_VELOX_CONFIG_FILE),
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
        // In the case of SparkEnv is not initialed (e.g. unit test), we just use current location instead of calling SparkFiles.getRootDirectory() to avoid error.
        String rootDirectory = SparkEnv$.MODULE$.get() != null ? SparkFiles.getRootDirectory() : ".";
        File workingDir = new File(rootDirectory);
        if (!absolutePath.isAbsolute()) {
            absolutePath = new File(workingDir, path);
        }

        if (!absolutePath.exists()) {
            log.error(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
            throw new PrestoException(NATIVE_EXECUTION_BINARY_NOT_EXIST, format("File doesn't exist %s", absolutePath.getAbsolutePath()));
        }

        return absolutePath.getAbsolutePath();
    }

    private List<String> getLaunchCommand()
            throws IOException
    {
        String executablePath = getProcessWorkingPath(SystemSessionProperties.getNativeExecutionExecutablePath(session));
        String programArgs = SystemSessionProperties.getNativeExecutionProgramArguments(session);
        String configPath = Paths.get(getProcessWorkingPath("./"), String.valueOf(port)).toAbsolutePath().toString();
        ImmutableList.Builder<String> command = ImmutableList.builder();
        List<String> argsList = Arrays.asList(programArgs.split("\\s+"));
        boolean etcDirSet = false;
        for (int i = 0; i < argsList.size(); i++) {
            String arg = argsList.get(i);
            if (arg.equals("--etc_dir")) {
                etcDirSet = true;
                configPath = argsList.get(i + 1);
                break;
            }
        }
        command.add(executablePath).addAll(argsList);
        if (!etcDirSet) {
            command.add("--etc_dir").add(configPath);
            populateConfigurationFiles(configPath);
        }
        ImmutableList<String> commandList = command.build();
        log.info("Launching native process using command: %s %s", executablePath, String.join(" ", commandList));
        return commandList;
    }
}
