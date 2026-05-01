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
import com.facebook.presto.spi.PrestoException;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;

/**
 * A driver-side {@code AbstractNativeProcess} that runs Velox in metadata-only sidecar mode
 * ({@code native-sidecar=true}). It exposes the standard Prestissimo HTTP endpoints
 * ({@code /v1/functions}, {@code /v1/expressions}, {@code /v1/velox/plan}, etc.) so that
 * the Presto-on-Spark driver can register native-only Velox functions into FunctionAndTypeManager
 * before query planning begins.
 *
 * <p>Differences from {@link NativeExecutionProcess}:
 * <ul>
 *   <li>No {@code Session}, {@code WorkerProperty}, {@code SparkConf}, or {@code SparkFiles}.
 *   <li>Resolves the binary as an absolute path (no {@code SparkFiles.getRootDirectory()}).
 *   <li>Writes a small fixed {@code config.properties}/{@code node.properties} pair with the
 *       FB-specific properties that {@code FacebookPrestoBase::registerFileSinks} requires
 *       even when sidecar mode is enabled (the WS storage triple and a spill-path
 *       short-circuit). No catalogs.
 *   <li>Failure to start throws a regular {@link PrestoException} rather than the
 *       {@code PrestoSparkFatalException} used by executor-side processes &mdash; this is the
 *       Spark driver, not an executor, so we want to fail the application instead of
 *       triggering executor failover.
 * </ul>
 */
public class MetadataSidecarProcess
        extends AbstractNativeProcess
{
    private static final Logger log = Logger.get(MetadataSidecarProcess.class);

    static final String SIDECAR_NODE_INTERNAL_ADDRESS = "127.0.0.1";

    private final String storageOncallName;
    private final String storageUserName;
    private final String storageServiceName;

    // Captured during populateConfigurationFiles so close() can clean up the etc tree the
    // sidecar process wrote at startup. Volatile because start() runs on the caller's
    // thread but close() may be invoked from a shutdown hook on a different thread.
    private volatile Path configBasePath;

    public MetadataSidecarProcess(
            String executablePath,
            String programArguments,
            OkHttpClient httpClient,
            Executor executor,
            ScheduledExecutorService scheduledExecutorService,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            String storageOncallName,
            String storageUserName,
            String storageServiceName)
    {
        super(executablePath,
                programArguments,
                SIDECAR_NODE_INTERNAL_ADDRESS,
                httpClient,
                executor,
                scheduledExecutorService,
                serverInfoCodec,
                maxErrorDuration);
        this.storageOncallName = storageOncallName;
        this.storageUserName = storageUserName;
        this.storageServiceName = storageServiceName;
    }

    @Override
    protected String resolveProcessWorkingPath(String path)
    {
        // Drop the etc-dir under the system tmpdir rather than the JVM's working directory.
        // The binary path itself is always absolute (config) so we don't need to resolve it.
        return new java.io.File(System.getProperty("java.io.tmpdir"), path).getAbsolutePath();
    }

    @Override
    protected void populateConfigurationFiles(Path configBasePath)
            throws IOException
    {
        Files.createDirectories(configBasePath);
        Files.write(workerConfigFile(configBasePath), buildSystemConfigLines());
        Files.write(workerNodeConfigFile(configBasePath), buildNodeConfigLines());
        // The catalog directory is intentionally empty (no connectors) but must exist —
        // PrestoServer iterates it during startup and aborts if it's missing.
        Files.createDirectories(workerCatalogDir(configBasePath));
        // Remember where we wrote so close() can clean it up; the parent abstract class
        // computes this path internally and never exposes it to the subclass otherwise.
        this.configBasePath = configBasePath;
    }

    @Override
    public void close()
    {
        try {
            super.close();
        }
        finally {
            // Etc-dir written by populateConfigurationFiles (may be null if start() never ran),
            // and the spill dir referenced from config.properties (may or may not exist
            // depending on whether the native process actually spilled).
            deleteQuietly(configBasePath);
            deleteQuietly(Paths.get(sidecarSpillDirectory(getPort())));
        }
    }

    private static void deleteQuietly(Path dir)
    {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try {
            deleteRecursively(dir, ALLOW_INSECURE);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete sidecar temp dir: %s", dir);
        }
    }

    private Iterable<String> buildSystemConfigLines()
    {
        // The native-sidecar=true flag turns on /v1/functions and friends.
        // The storage-* triple satisfies FacebookPrestoBase::registerFileSinks, which calls
        // requiredProperty() for these even though we never use WarmStorage in metadata-only
        // mode. The experimental.spiller-spill-path setting short-circuits the FB datacenter
        // detection in FacebookPrestoBase::getBaseSpillDirectory.
        return Arrays.asList(
                "discovery.uri=http://" + SIDECAR_NODE_INTERNAL_ADDRESS + ":" + getPort(),
                "presto.version=metadata-sidecar",
                "http-server.http.port=" + getPort(),
                "shutdown-onset-sec=1",
                "native-sidecar=true",
                "storage_oncall_name=" + storageOncallName,
                "storage_user_name=" + storageUserName,
                "storage_service_name=" + storageServiceName,
                "experimental.spiller-spill-path=" + sidecarSpillDirectory(getPort()));
    }

    private static String sidecarSpillDirectory(int port)
    {
        return System.getProperty("java.io.tmpdir") + "/presto-spark-metadata-sidecar-spill-" + port;
    }

    private Iterable<String> buildNodeConfigLines()
    {
        return Arrays.asList(
                "node.environment=presto-spark-driver-sidecar",
                "node.internal-address=" + SIDECAR_NODE_INTERNAL_ADDRESS,
                "node.location=presto-spark-driver-sidecar",
                "node.id=" + UUID.randomUUID());
    }

    @Override
    protected RuntimeException propagateStartFailure(Throwable t)
    {
        // Driver-side: fail the Spark application, don't trigger executor failover.
        throw new PrestoException(
                NATIVE_EXECUTION_PROCESS_LAUNCH_ERROR,
                format("Failed to start metadata sidecar at %s", getLocation()),
                t);
    }

    /**
     * Writes config.properties / node.properties into the given directory using the same
     * content {@link #populateConfigurationFiles} writes. Exposed for unit tests that want
     * to verify the file layout without spawning a process.
     */
    void writeConfigsForTest(Path configBasePath)
            throws IOException
    {
        populateConfigurationFiles(configBasePath);
    }
}
