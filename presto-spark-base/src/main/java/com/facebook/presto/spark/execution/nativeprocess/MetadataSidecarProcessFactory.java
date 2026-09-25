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
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Inject;
import okhttp3.OkHttpClient;

import javax.annotation.PreDestroy;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Singleton factory for the driver-side {@link MetadataSidecarProcess}. Owns the lifecycle of
 * a single sidecar process per Spark driver JVM. The process is started lazily on the first
 * call to {@link #getOrStart()} and reused thereafter; {@link #close()} (also called via
 * {@code @PreDestroy}) tears it down.
 */
public class MetadataSidecarProcessFactory
{
    private static final Logger log = Logger.get(MetadataSidecarProcessFactory.class);

    private final OkHttpClient httpClient;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final MetadataSidecarConfig config;
    private final SidecarBinaryLocator binaryLocator;

    private volatile MetadataSidecarProcess process;

    @Inject
    public MetadataSidecarProcessFactory(
            @ForMetadataSidecar OkHttpClient httpClient,
            @ForMetadataSidecar ExecutorService executor,
            @ForMetadataSidecar ScheduledExecutorService scheduledExecutor,
            JsonCodec<ServerInfo> serverInfoCodec,
            MetadataSidecarConfig config,
            SidecarBinaryLocator binaryLocator)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.config = requireNonNull(config, "config is null");
        this.binaryLocator = requireNonNull(binaryLocator, "binaryLocator is null");
    }

    /**
     * Returns the running sidecar's local URI ({@code http://127.0.0.1:<port>}), starting it
     * if it has not been started yet.
     */
    public synchronized URI getOrStart()
    {
        if (process != null && process.isAlive()) {
            return process.getLocation();
        }
        if (process != null) {
            log.warn("Existing metadata sidecar process is not alive — replacing it");
            try {
                process.close();
            }
            catch (Exception e) {
                log.warn(e, "Error closing previous metadata sidecar process");
            }
        }
        process = new MetadataSidecarProcess(
                resolveExecutablePath(),
                config.getProgramArguments(),
                httpClient,
                executor,
                scheduledExecutor,
                serverInfoCodec,
                config.getStartupTimeout(),
                config.getStorageOncallName(),
                config.getStorageUserName(),
                config.getStorageServiceName());
        try {
            process.start();
        }
        catch (ExecutionException | InterruptedException | java.io.IOException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            try {
                process.close();
            }
            catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw new PrestoException(
                    NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR,
                    "Failed to start metadata sidecar",
                    e);
        }
        log.info("Metadata sidecar started at %s", process.getLocation());
        return process.getLocation();
    }

    @PreDestroy
    public synchronized void close()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        if (process != null) {
            try {
                log.info("Stopping metadata sidecar at %s", process.getLocation());
                process.close();
            }
            finally {
                process = null;
            }
        }
    }

    private String resolveExecutablePath()
    {
        if (config.getExecutablePath() != null && !config.getExecutablePath().isEmpty()) {
            return config.getExecutablePath();
        }
        return binaryLocator.locateNativeWorkerBinary()
                .orElseThrow(() -> new PrestoException(
                        NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR,
                        "metadata-sidecar.executable-path is unset and the SidecarBinaryLocator " +
                                "binding did not return a native worker binary"));
    }

    /**
     * Provides the absolute path to the native worker binary. The default binding returns
     * {@code Optional.empty()}; deployments that want automatic binary discovery should bind
     * a concrete implementation (e.g. one backed by an fbpkg lookup at the Presto-on-Spark
     * launcher layer). Kept as an interface here so {@code presto-spark-base} doesn't need to
     * depend on any deployment-specific launcher.
     */
    public interface SidecarBinaryLocator
    {
        java.util.Optional<String> locateNativeWorkerBinary();
    }
}
