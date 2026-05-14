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
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpServerClient;
import com.facebook.presto.spark.execution.http.server.RequestErrorTracker;
import com.facebook.presto.spark.execution.http.server.smile.BaseResponse;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

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
import java.nio.file.Path;
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
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * Base class for managing a forked native (C++) process that exposes an HTTP server.
 *
 * <p>Subclasses control: what configuration files are written into the etc-dir
 * ({@link #populateConfigurationFiles}), how the binary path is resolved
 * ({@link #resolveProcessWorkingPath}), and any environment customisation prior to
 * spawning ({@link #customizeProcessBuilder}). The base class owns the port,
 * the server-info HTTP client, the process lifecycle, and the stderr pipe.
 */
public abstract class AbstractNativeProcess
        implements AutoCloseable
{
    private static final Logger log = Logger.get(AbstractNativeProcess.class);
    private static final String NATIVE_EXECUTION_TASK_ERROR_MESSAGE = "Native process launch failed with multiple retries.";
    private static final String WORKER_CONFIG_FILE = "config.properties";
    private static final String WORKER_NODE_CONFIG_FILE = "node.properties";
    private static final String WORKER_CONNECTOR_CONFIG_DIR = "catalog";
    private static final int SIGSYS = 31;

    private final String executablePath;
    private final String programArguments;
    private final PrestoSparkHttpServerClient serverClient;
    private final URI location;
    private final int port;
    private final Executor executor;
    private final RequestErrorTracker errorTracker;

    private volatile Process process;
    private volatile ProcessOutputPipe processOutputPipe;

    protected AbstractNativeProcess(
            String executablePath,
            String programArguments,
            String nodeInternalAddress,
            OkHttpClient httpClient,
            Executor executor,
            ScheduledExecutorService scheduledExecutorService,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration)
    {
        this.executablePath = requireNonNull(executablePath, "executablePath is null");
        this.programArguments = requireNonNull(programArguments, "programArguments is null");
        requireNonNull(nodeInternalAddress, "nodeInternalAddress is null");
        this.port = getAvailableTcpPort(nodeInternalAddress);
        this.location = HttpUrl.parse("http://" + nodeInternalAddress + ":" + getPort()).uri();
        this.serverClient = new PrestoSparkHttpServerClient(
                requireNonNull(httpClient, "httpClient is null"),
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
    }

    /**
     * Starts the external native process. Blocks until the process responds at /v1/info
     * or until the error tracker exhausts its budget.
     */
    public synchronized void start()
            throws ExecutionException, InterruptedException, IOException
    {
        if (process != null) {
            return;
        }

        ProcessBuilder processBuilder = new ProcessBuilder(getLaunchCommand());
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        customizeProcessBuilder(processBuilder);
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

        try {
            getServerInfoWithRetry().get();
        }
        catch (Throwable t) {
            close();
            throw propagateStartFailure(t);
        }
    }

    /**
     * Hook for subclasses to mutate the {@link ProcessBuilder} before spawning.
     * Default implementation does nothing.
     */
    protected void customizeProcessBuilder(ProcessBuilder processBuilder)
    {
    }

    /**
     * Reports a fatal failure during {@link #start}; this method never returns normally.
     * Default implementation throws {@link PrestoSparkFatalException} (an {@code Error}
     * subclass) so Spark fails the executor and reschedules. Subclasses may override —
     * e.g. driver-side processes can throw a {@link PrestoException} to fail fast without
     * forcing executor failover.
     *
     * <p>The return type lets callers write {@code throw propagateStartFailure(t);} for
     * flow analysis, even though control never reaches the {@code throw}.
     */
    protected RuntimeException propagateStartFailure(Throwable t)
    {
        throw new PrestoSparkFatalException(t.getMessage(), t.getCause());
    }

    /**
     * Writes config.properties / node.properties / catalog/* into the supplied directory.
     * The directory exists when this is called.
     */
    protected abstract void populateConfigurationFiles(Path configBasePath)
            throws IOException;

    /**
     * Resolves a path used for binary or config-dir lookup. Default implementation
     * treats the path as an absolute filesystem path and verifies the file exists.
     * Subclasses may override (e.g. to resolve relative paths against
     * {@code SparkFiles.getRootDirectory()}).
     */
    protected String resolveProcessWorkingPath(String path)
    {
        File absolutePath = new File(path);
        if (!absolutePath.isAbsolute()) {
            absolutePath = new File(".", path);
        }
        if (!absolutePath.exists()) {
            log.error(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
            throw new PrestoException(NATIVE_EXECUTION_BINARY_NOT_EXIST, format("File doesn't exist %s", absolutePath.getAbsolutePath()));
        }
        return absolutePath.getAbsolutePath();
    }

    @VisibleForTesting
    public SettableFuture<ServerInfo> getServerInfoWithRetry()
    {
        SettableFuture<ServerInfo> future = SettableFuture.create();
        doGetServerInfo(future);
        return future;
    }

    /**
     * Triggers coredump (also terminates the process).
     */
    public void terminateWithCore(Duration timeout)
    {
        // chosen as the least likely core signal to occur naturally (invalid sys call)
        // https://man7.org/linux/man-pages/man7/signal.7.html
        Process running = sendSignal(SIGSYS);
        if (running == null) {
            return;
        }
        try {
            long pid = getPid(running);
            log.info("Waiting %s for process %s to terminate", timeout, pid);
            if (!running.waitFor(timeout.toMillis(), MILLISECONDS)) {
                log.warn("Process %s did not terminate within %s", pid, timeout);
                running.destroyForcibly();
            }
            else {
                log.info("Process %s successfully terminated with status code %s", pid, running.exitValue());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Process sendSignal(int signal)
    {
        Process running = this.process;
        if (running == null) {
            log.warn("Failure sending signal, process does not exist");
            return null;
        }
        long pid = getPid(running);
        if (!running.isAlive()) {
            log.warn("Failure sending signal, process is dead: %s", pid);
            return null;
        }
        try {
            log.info("Sending signal to process %s: %s", pid, signal);
            Runtime.getRuntime().exec(format("kill -%s %s", signal, pid));
            return running;
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
        Process running = this.process;
        if (running == null) {
            return;
        }

        if (running.isAlive()) {
            long pid = getPid(running);
            log.info("Destroying process: %s", pid);
            running.destroy();
            try {
                // This 1 sec is arbitrary. Ideally, we do not need to be give any heads up
                // to CPP process on presto-on-spark native, because the resources
                // are reclaimed by the container manager.
                // For localmode, we still want to provide an opportunity for
                // graceful termination as there is no resource/container manager.
                running.waitFor(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                if (running.isAlive()) {
                    log.warn("Graceful shutdown of native execution process failed. Force killing it: %s", pid);
                    running.destroyForcibly();
                }
            }
        }
        else {
            log.info("Process is dead: %s", getPid(running));
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

    protected static int getAvailableTcpPort(String nodeInternalAddress)
    {
        try {
            ServerSocket socket = new ServerSocket();
            socket.bind(new InetSocketAddress(nodeInternalAddress, 0));
            int p = socket.getLocalPort();
            socket.close();
            return p;
        }
        catch (Exception ex) {
            // Something is wrong with the executor — fail it.
            throw new PrestoSparkFatalException("Failed to acquire port on host", ex);
        }
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

    private List<String> getLaunchCommand()
            throws IOException
    {
        String configPath = Paths.get(resolveProcessWorkingPath("./"), String.valueOf(port)).toAbsolutePath().toString();
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
            populateConfigurationFiles(Paths.get(configPath));
        }
        ImmutableList<String> commandList = command.build();
        log.info("Launching native process using command: %s", String.join(" ", commandList));
        return commandList;
    }

    /**
     * Path of {@code config.properties} relative to a config base directory.
     */
    protected static Path workerConfigFile(Path configBasePath)
    {
        return configBasePath.resolve(WORKER_CONFIG_FILE);
    }

    /**
     * Path of {@code node.properties} relative to a config base directory.
     */
    protected static Path workerNodeConfigFile(Path configBasePath)
    {
        return configBasePath.resolve(WORKER_NODE_CONFIG_FILE);
    }

    /**
     * Path of the {@code catalog/} directory relative to a config base directory.
     */
    protected static Path workerCatalogDir(Path configBasePath)
    {
        return configBasePath.resolve(WORKER_CONNECTOR_CONFIG_DIR);
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
            Thread t = new Thread(this, format("AbstractNativeProcess#ProcessOutputPipe[%s]", pid));
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
