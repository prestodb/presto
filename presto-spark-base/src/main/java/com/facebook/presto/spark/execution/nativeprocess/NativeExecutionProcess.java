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
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpServerClient;
import com.facebook.presto.spark.execution.http.server.RequestErrorTracker;
import com.facebook.presto.spark.execution.http.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.spark.SparkEnv$;
import org.apache.spark.SparkFiles;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_BINARY_NOT_EXIST;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.http.HttpStatus.SC_OK;

public class NativeExecutionProcess
        implements AutoCloseable
{
    private static final Logger log = Logger.get(NativeExecutionProcess.class);
    private static final String NATIVE_EXECUTION_TASK_ERROR_MESSAGE = "Native process launch failed with multiple retries.";
    private static final String WORKER_CONFIG_FILE = "/config.properties";
    private static final String WORKER_NODE_CONFIG_FILE = "/node.properties";
    private static final String WORKER_CONNECTOR_CONFIG_FILE = "/catalog/";
    private static final int SIGSYS = 31;

    private final String executablePath;
    private final String programArguments;
    private final Session session;
    private final PrestoSparkHttpServerClient serverClient;
    private final URI location;
    private final int port;
    private final Executor executor;
    private final RequestErrorTracker errorTracker;
    private final OkHttpClient httpClient;
    private final WorkerProperty<?, ?, ?> workerProperty;

    private volatile Process process;
    private volatile ProcessOutputPipe processOutputPipe;

    public NativeExecutionProcess(
            String executablePath,
            String programArguments,
            Session session,
            OkHttpClient httpClient,
            Executor executor,
            ScheduledExecutorService scheduledExecutorService,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            WorkerProperty<?, ?, ?> workerProperty)
            throws IOException
    {
        this.executablePath = requireNonNull(executablePath, "executablePath is null");
        this.programArguments = requireNonNull(programArguments, "programArguments is null");
        String nodeInternalAddress = workerProperty.getNodeConfig().getNodeInternalAddress();
        this.port = getAvailableTcpPort(nodeInternalAddress);
        this.session = requireNonNull(session, "session is null");
        this.location = HttpUrl.parse("http://" + nodeInternalAddress + ":" + getPort()).uri();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.serverClient = new PrestoSparkHttpServerClient(
                this.httpClient,
                location,
                serverInfoCodec);
        this.executor = requireNonNull(executor, "executor is null");
        this.errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                NATIVE_EXECUTION_TASK_ERROR_MESSAGE,
                maxErrorDuration,
                scheduledExecutorService,
                "getting native process status");
        this.workerProperty = requireNonNull(workerProperty, "workerProperty is null");
    }

    /**
     * Starts the external native execution process. The method will be blocked by connecting to the native process's /v1/info endpoint with backoff retries until timeout.
     */
    public synchronized void start()
            throws ExecutionException, InterruptedException, IOException
    {
        if (process != null) {
            return;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(getLaunchCommand());
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.environment().put("INIT_PRESTO_QUERY_ID", session.getQueryId().toString());
        try {
            process = processBuilder.start();
            processOutputPipe = new ProcessOutputPipe(
                    getPid(process),
                    process.getErrorStream(),
                    new FileOutputStream(FileDescriptor.err));
            processOutputPipe.start();
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
    public void terminateWithCore(Duration timeout)
    {
        // chosen as the least likely core signal to occur naturally (invalid sys call)
        // https://man7.org/linux/man-pages/man7/signal.7.html
        Process process = sendSignal(SIGSYS);
        if (process == null) {
            return;
        }
        try {
            long pid = getPid(process);
            log.info("Waiting %s for process %s to terminate", timeout, pid);
            if (!process.waitFor(timeout.toMillis(), MILLISECONDS)) {
                log.warn("Process %s did not terminate within %s", pid, timeout);
                process.destroyForcibly();
            }
            else {
                log.info("Process %s successfully terminated with status code %s", pid, process.exitValue());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Process sendSignal(int signal)
    {
        Process process = this.process;
        if (process == null) {
            log.warn("Failure sending signal, process does not exist");
            return null;
        }
        long pid = getPid(process);
        if (!process.isAlive()) {
            log.warn("Failure sending signal, process is dead: %s", pid);
            return null;
        }
        try {
            log.info("Sending signal to process %s: %s", pid, signal);
            Runtime.getRuntime().exec(format("kill -%s %s", signal, pid));
            return process;
        }
        catch (IOException e) {
            log.warn(e, "Failure sending signal to process %s", pid);
            return null;
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
        Process process = this.process;
        if (process == null) {
            return;
        }

        if (process.isAlive()) {
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
        else {
            log.info("Process is dead: %s", getPid(process));
        }
    }

    public boolean isAlive()
    {
        return process != null && process.isAlive();
    }

    public String getCrashReport()
    {
        ProcessOutputPipe pipe = processOutputPipe;
        if (pipe == null) {
            return "";
        }
        return pipe.getAbortMessage();
    }

    public int getPort()
    {
        return port;
    }

    public URI getLocation()
    {
        return location;
    }

    private static int getAvailableTcpPort(String nodeInternalAddress)
    {
        try {
            ServerSocket socket = new ServerSocket();
            socket.bind(new InetSocketAddress(nodeInternalAddress, 0));
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
        workerProperty.getSystemConfig()
                .update(NativeExecutionSystemConfig.HTTP_SERVER_HTTP_PORT, String.valueOf(port));
        workerProperty.populateAllProperties(
                Paths.get(configBasePath, WORKER_CONFIG_FILE),
                Paths.get(configBasePath, WORKER_NODE_CONFIG_FILE),
                Paths.get(configBasePath, format("%s%s.properties", WORKER_CONNECTOR_CONFIG_FILE,
                    getNativeExecutionCatalogName(session))));
    }

    private void doGetServerInfo(SettableFuture<ServerInfo> future)
    {
        addCallback(serverClient.getServerInfo(), new FutureCallback<BaseResponse<ServerInfo>>()
        {
            @Override
            public void onSuccess(@Nullable BaseResponse<ServerInfo> response)
            {
                if (response.getStatusCode() != SC_OK) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with HTTP status " + response.getStatusCode());
                }
                future.set(response.getValue());
            }

            @Override
            public void onFailure(Throwable failedReason)
            {
                if (failedReason instanceof RejectedExecutionException) {
                    log.error(format("Unable to start the native process. Reason: %s", failedReason.getMessage()));
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
                    errorRateLimit.addListener(() -> doGetServerInfo(future), executor);
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
        String configPath = Paths.get(getProcessWorkingPath("./"), String.valueOf(port)).toAbsolutePath().toString();
        ImmutableList.Builder<String> command = ImmutableList.builder();
        List<String> argsList = Arrays.asList(programArguments.split("\\s+"));
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
        log.info("Launching native process using command: %s", String.join(" ", commandList));
        return commandList;
    }

    private static class ProcessOutputPipe
            implements Runnable
    {
        private final long pid;
        private final InputStream inputStream;
        private final OutputStream outputStream;
        private final StringBuilder abortMessage = new StringBuilder();
        private final AtomicBoolean started = new AtomicBoolean();

        public ProcessOutputPipe(long pid, InputStream inputStream, OutputStream outputStream)
        {
            this.pid = pid;
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
            this.outputStream = requireNonNull(outputStream, "outputStream is null");
        }

        public void start()
        {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            Thread t = new Thread(this, format("NativeExecutionProcess#ProcessOutputPipe[%s]", pid));
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void run()
        {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, UTF_8))) {
                String line;
                boolean aborted = false;
                while ((line = reader.readLine()) != null) {
                    if (!aborted && line.startsWith("*** Aborted")) {
                        aborted = true;
                    }
                    if (aborted) {
                        synchronized (abortMessage) {
                            abortMessage.append(line).append("\n");
                        }
                    }
                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                }
            }
            catch (IOException e) {
                log.warn(e, "failure occurred when copying streams");
            }
        }

        public String getAbortMessage()
        {
            synchronized (abortMessage) {
                return abortMessage.toString();
            }
        }
    }
}
