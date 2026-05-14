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
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.spark.execution.property.WorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.storage.TempStorageHandle;
import okhttp3.OkHttpClient;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv$;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_BINARY_NOT_EXIST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NativeExecutionProcess
        extends AbstractNativeProcess
{
    private static final Logger log = Logger.get(NativeExecutionProcess.class);
    private static final String NATIVE_PROCESS_MEMORY_SPARK_CONF_NAME = "spark.memory.offHeap.size";

    private final Session session;
    private final WorkerProperty<?, ?, ?> workerProperty;
    // Temp storage used for spilling and broadcast join.
    private final Optional<TempStorageHandle> nativeTempStorageHandle;

    public NativeExecutionProcess(
            String executablePath,
            String programArguments,
            Session session,
            OkHttpClient httpClient,
            Executor executor,
            ScheduledExecutorService scheduledExecutorService,
            JsonCodec<ServerInfo> serverInfoCodec,
            Duration maxErrorDuration,
            WorkerProperty<?, ?, ?> workerProperty,
            Optional<TempStorageHandle> nativeTempStorageHandle)
            throws IOException
    {
        super(executablePath,
                programArguments,
                workerProperty.getNodeConfig().getNodeInternalAddress(),
                httpClient,
                executor,
                scheduledExecutorService,
                serverInfoCodec,
                maxErrorDuration);
        this.session = requireNonNull(session, "session is null");
        this.workerProperty = requireNonNull(workerProperty, "workerProperty is null");
        this.nativeTempStorageHandle = requireNonNull(nativeTempStorageHandle, "nativeTempStorageHandle is null");
        // Update any runtime configs to be used by presto native worker
        updateWorkerProperties();
    }

    @Override
    protected void customizeProcessBuilder(ProcessBuilder processBuilder)
    {
        processBuilder.environment().put("INIT_PRESTO_QUERY_ID", session.getQueryId().toString());
    }

    @Override
    protected void populateConfigurationFiles(Path configBasePath)
            throws IOException
    {
        workerProperty.populateAllProperties(
                workerConfigFile(configBasePath),
                workerNodeConfigFile(configBasePath),
                workerCatalogDir(configBasePath));
    }

    @Override
    protected String resolveProcessWorkingPath(String path)
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

    protected void updateWorkerProperties()
    {
        // Update memory properties
        updateWorkerMemoryProperties();

        // The reason we have to pick and assign the port per worker is in our prod environment,
        // there is no port isolation among all the containers running on the same host, so we have
        // to pick unique port per worker to avoid port collision. This config will be passed down to
        // the native execution process eventually for process initialization.
        workerProperty.getSystemConfig()
                .update(NativeExecutionSystemConfig.HTTP_SERVER_HTTP_PORT, String.valueOf(getPort()));

        // Update the temp storage configs for spilling and broadcast join.
        if (nativeTempStorageHandle.isPresent()) {
            workerProperty.updateTempStorageConfig(nativeTempStorageHandle.get());
        }
    }

    protected SparkConf getSparkConf()
    {
        return SparkEnv$.MODULE$.get() == null ? null : SparkEnv$.MODULE$.get().conf();
    }

    protected PrestoSparkWorkerProperty getWorkerProperty()
    {
        return (PrestoSparkWorkerProperty) workerProperty;
    }

    /**
     * Computes values for system-memory-gb and query-memory-gb to start the native worker
     * with.
     * This logic is mainly useful when spark has provisioned larger containers to run
     * previously OOMing tasks. Spark will provision larger container but without below
     * logic the cpp process will not be able to use it.
     *
     * Also, we write the logic in a way that same logic applies during first attempt v/s
     * subsequent OOMed larger container retry attempts
     *
     * The logic is simple and is as below
     * - New system-memory-gb = spark.memory.offHeap.size
     * - Then to calculate the new value of query-memory-gb we assume that
     *   the new query-memory to system-memory ratio should be same as old values.
     *   So we set newQueryMemory = newSystemMemory = (oldQueryMemory/oldSystemMemory)
     *
     *   TODO: In future make this algorithm more configurable. i.e. we might want a min/max
     *         cap on the systemMemoryGb-queryMemoryGb buffer. Currently we just assume ratio
     *         is good enough
     */
    protected void updateWorkerMemoryProperties()
    {
        // If sparkConf.NATIVE_PROCESS_MEMORY_SPARK_CONF_NAME is not set
        // skip making any updates
        SparkConf conf = getSparkConf();
        if (conf == null) {
            log.info("Not adjusting native process memory as conf is null");
            return;
        }
        if (!conf.contains(NATIVE_PROCESS_MEMORY_SPARK_CONF_NAME)) {
            log.info("Not adjusting native process memory as %s is not set", NATIVE_PROCESS_MEMORY_SPARK_CONF_NAME);
            return;
        }
        DataSize offHeapMemoryBytes = DataSize.succinctDataSize(
                conf.getSizeAsBytes(NATIVE_PROCESS_MEMORY_SPARK_CONF_NAME), BYTE);
        DataSize currentSystemMemory = DataSize.valueOf(workerProperty.getSystemConfig().getAllProperties()
                .get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB) + GIGABYTE.getUnitString());
        DataSize currentQueryMemory = DataSize.valueOf(workerProperty.getSystemConfig().getAllProperties()
                .get(NativeExecutionSystemConfig.QUERY_MEMORY_GB) + GIGABYTE.getUnitString());
        if (offHeapMemoryBytes.toBytes() == 0
                || currentSystemMemory.toBytes() == 0
                || offHeapMemoryBytes.toBytes() < currentSystemMemory.toBytes()) {
            log.info("Not adjusting native process memory as" +
                    " offHeapMemoryBytes=%s,currentSystemMemory=%s are invalid", offHeapMemoryBytes, currentSystemMemory.toBytes());
            return;
        }

        log.info("Setting Native Worker system-memory-gb to offHeap: %s", offHeapMemoryBytes);
        DataSize newSystemMemory = offHeapMemoryBytes.convertTo(GIGABYTE);

        double queryMemoryFraction = currentQueryMemory.toBytes() * 1.0 / currentSystemMemory.toBytes();
        DataSize newQueryMemoryBytes = DataSize.succinctDataSize(
                queryMemoryFraction * newSystemMemory.toBytes(), BYTE);
        log.info("Dynamically Tuning Presto Native Memory Configs. " +
                        "Configured SparkOffHeap: %s; " +
                        "[oldSystemMemory: %s, newSystemMemory: %s], queryMemoryFraction: %s, " +
                        "[oldQueryMemory: %s, newQueryMemory: %s]",
                offHeapMemoryBytes,
                currentSystemMemory,
                newSystemMemory,
                queryMemoryFraction,
                currentQueryMemory,
                newQueryMemoryBytes);

        workerProperty.getSystemConfig()
                .update(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB,
                        String.valueOf((int) newSystemMemory.getValue(GIGABYTE)));
        workerProperty.getSystemConfig()
                .update(NativeExecutionSystemConfig.QUERY_MEMORY_GB,
                        String.valueOf((int) newQueryMemoryBytes.getValue(GIGABYTE)));
        workerProperty.getSystemConfig()
                .update(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE,
                        newQueryMemoryBytes.convertTo(GIGABYTE).toString());
    }
}
