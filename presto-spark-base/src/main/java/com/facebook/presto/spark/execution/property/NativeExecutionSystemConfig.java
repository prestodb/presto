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
package com.facebook.presto.spark.execution.property;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkEnv$;
import org.apache.spark.SparkFiles;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This config class corresponds to config.properties for native execution process. Properties
 * inside will be used in Configs::SystemConfig in Configs.h/cpp
 */
public class NativeExecutionSystemConfig
{
    public static final String NATIVE_EXECUTION_SYSTEM_CONFIG = "native-execution-system-config";

    public static final String CONCURRENT_LIFESPANS_PER_TASK = "concurrent-lifespans-per-task";
    public static final String ENABLE_SERIALIZED_PAGE_CHECKSUM = "enable-serialized-page-checksum";
    public static final String ENABLE_VELOX_EXPRESSION_LOGGING = "enable_velox_expression_logging";
    public static final String ENABLE_VELOX_TASK_LOGGING = "enable_velox_task_logging";
    public static final String HTTP_SERVER_HTTP_PORT = "http-server.http.port";
    public static final String HTTP_SERVER_REUSE_PORT = "http-server.reuse-port";
    public static final String HTTP_SERVER_BIND_TO_NODE_INTERNAL_ADDRESS_ONLY_ENABLED = "http-server.bind-to-node-internal-address-only-enabled";
    public static final String HTTP_SERVER_HTTPS_PORT = "http-server.https.port";
    public static final String HTTP_SERVER_HTTPS_ENABLED = "http-server.https.enabled";
    public static final String HTTPS_SUPPORTED_CIPHERS = "https-supported-ciphers";
    public static final String HTTPS_CERT_PATH = "https-cert-path";
    public static final String HTTPS_KEY_PATH = "https-key-path";
    public static final String HTTP_SERVER_NUM_IO_THREADS_HW_MULTIPLIER = "http-server.num-io-threads-hw-multiplier";
    public static final String EXCHANGE_HTTP_CLIENT_NUM_IO_THREADS_HW_MULTIPLIER = "exchange.http-client.num-io-threads-hw-multiplier";
    public static final String ASYNC_DATA_CACHE_ENABLED = "async-data-cache-enabled";
    public static final String ASYNC_CACHE_SSD_GB = "async-cache-ssd-gb";
    public static final String CONNECTOR_NUM_IO_THREADS_HW_MULTIPLIER = "connector.num-io-threads-hw-multiplier";
    public static final String PRESTO_VERSION = "presto.version";
    public static final String SHUTDOWN_ONSET_SEC = "shutdown-onset-sec";
    public static final String SYSTEM_MEMORY_GB = "system-memory-gb";
    public static final String QUERY_MEMORY_GB = "query-memory-gb";
    public static final String QUERY_MAX_MEMORY_PER_NODE = "query.max-memory-per-node";
    public static final String USE_MMAP_ALLOCATOR = "use-mmap-allocator";
    public static final String MEMORY_ARBITRATOR_KIND = "memory-arbitrator-kind";
    public static final String SHARED_ARBITRATOR_RESERVED_CAPACITY = "shared-arbitrator.reserved-capacity";
    public static final String SHARED_ARBITRATOR_MEMORY_POOL_INITIAL_CAPACITY = "shared-arbitrator.memory-pool-initial-capacity";
    public static final String SHARED_ARBITRATOR_MAX_MEMORY_ARBITRATION_TIME = "shared-arbitrator.max-memory-arbitration-time";
    public static final String EXPERIMENTAL_SPILLER_SPILL_PATH = "experimental.spiller-spill-path";
    public static final String TASK_MAX_DRIVERS_PER_TASK = "task.max-drivers-per-task";
    public static final String ENABLE_OLD_TASK_CLEANUP = "enable-old-task-cleanup";
    public static final String SHUFFLE_NAME = "shuffle.name";
    public static final String HTTP_SERVER_ENABLE_ACCESS_LOG = "http-server.enable-access-log";
    public static final String CORE_ON_ALLOCATION_FAILURE_ENABLED = "core-on-allocation-failure-enabled";
    public static final String SPILL_ENABLED = "spill-enabled";
    public static final String AGGREGATION_SPILL_ENABLED = "aggregation-spill-enabled";
    public static final String JOIN_SPILL_ENABLED = "join-spill-enabled";
    public static final String ORDER_BY_SPILL_ENABLED = "order-by-spill-enabled";
    public static final String MAX_SPILL_BYTES = "max-spill-bytes";
    public static final String REMOTE_FUNCTION_SERVER_THRIFT_UDS_PATH = "remote-function-server.thrift.uds-path";
    public static final String REMOTE_FUNCTION_SERVER_SIGNATURE_FILES_DIRECTORY_PATH = "remote-function-server.signature.files.directory.path";
    public static final String REMOTE_FUNCTION_SERVER_SERDE = "remote-function-server.serde";
    public static final String REMOTE_FUNCTION_SERVER_CATALOG_NAME = "remote-function-server.catalog-name";

    private final String remoteFunctionServerSignatureFilesDirectoryPathDefault = "./functions/spark/";
    private final String remoteFunctionServerSerdeDefault = "presto_page";
    private final String remoteFunctionServerCatalogNameDefault = "";
    private final String concurrentLifespansPerTaskDefault = "5";
    private final String enableSerializedPageChecksumDefault = "true";
    private final String enableVeloxExpressionLoggingDefault = "false";
    private final String enableVeloxTaskLoggingDefault = "true";
    private final String httpServerHttpPortDefault = "7777";
    private final String httpServerReusePortDefault = "true";
    private final String httpServerBindToNodeInternalAddressOnlyEnabledDefault = "true";
    private final String httpServerHttpsPortDefault = "7778";
    private final String httpServerHttpsEnabledDefault = "false";
    private final String httpsSupportedCiphersDefault = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    private final String httpsCertPathDefault = "";
    private final String httpsKeyPathDefault = "";
    private final String httpServerNumIoThreadsHwMultiplierDefault = "1.0";
    private final String exchangeHttpClientNumIoThreadsHwMultiplierDefault = "1.0";
    private final String asyncDataCacheEnabledDefault = "false";
    private final String asyncCacheSsdGbDefault = "0";
    private final String connectorNumIoThreadsHwMultiplierDefault = "0";
    private final String prestoVersionDefault = "dummy.presto.version";
    private final String shutdownOnsetSecDefault = "10";
    private final String systemMemoryGbDefault = "10";
    private final String queryMemoryGbDefault = "8";
    private final String queryMaxMemoryPerNodeDefault = "8GB";
    private final String useMmapAllocatorDefault = "true";
    private final String memoryArbitratorKindDefault = "SHARED";
    private final String sharedArbitratorReservedCapacityDefault = "0GB";
    private final String sharedArbitratorMemoryPoolInitialCapacityDefault = "4GB";
    private final String sharedArbitratorMaxMemoryArbitrationTimeDefault = "5m";
    private final String experimentalSpillerSpillPathDefault = "";
    private final String taskMaxDriversPerTaskDefault = "15";
    private final String enableOldTaskCleanupDefault = "false";
    private final String shuffleNameDefault = "local";
    private final String httpServerEnableAccessLogDefault = "true";
    private final String coreOnAllocationFailureEnabledDefault = "false";
    private final String spillEnabledDefault = "true";
    private final String aggregationSpillEnabledDefault = "true";
    private final String joinSpillEnabledDefault = "true";
    private final String orderBySpillEnabledDefault = "true";
    private final String maxSpillBytesDefault = String.valueOf(600L << 30);

    private final Map<String, String> systemConfigs;
    private final Map<String, String> defaultSystemConfigs;

    @Inject
    public NativeExecutionSystemConfig(
            @Named(NATIVE_EXECUTION_SYSTEM_CONFIG) Map<String, String> systemConfigs)
    {
        this.systemConfigs = new HashMap<>(
                requireNonNull(systemConfigs, "systemConfigs is null"));

        ImmutableMap.Builder<String, String> defaultSystemConfigsBuilder = ImmutableMap.builder();

        String remoteFunctionServerThriftUdsPath = System.getProperty(REMOTE_FUNCTION_SERVER_THRIFT_UDS_PATH);
        if (remoteFunctionServerThriftUdsPath != null) {
            defaultSystemConfigsBuilder.put(REMOTE_FUNCTION_SERVER_THRIFT_UDS_PATH, remoteFunctionServerThriftUdsPath);
            defaultSystemConfigsBuilder.put(REMOTE_FUNCTION_SERVER_SIGNATURE_FILES_DIRECTORY_PATH, getAbsolutePath(remoteFunctionServerSignatureFilesDirectoryPathDefault));
            defaultSystemConfigsBuilder.put(REMOTE_FUNCTION_SERVER_SERDE, remoteFunctionServerSerdeDefault);
            defaultSystemConfigsBuilder.put(REMOTE_FUNCTION_SERVER_CATALOG_NAME, remoteFunctionServerCatalogNameDefault);
        }

        defaultSystemConfigs = defaultSystemConfigsBuilder
                .put(CONCURRENT_LIFESPANS_PER_TASK, concurrentLifespansPerTaskDefault)
                .put(ENABLE_SERIALIZED_PAGE_CHECKSUM, enableSerializedPageChecksumDefault)
                .put(ENABLE_VELOX_EXPRESSION_LOGGING, enableVeloxExpressionLoggingDefault)
                .put(ENABLE_VELOX_TASK_LOGGING, enableVeloxTaskLoggingDefault)
                .put(HTTP_SERVER_HTTP_PORT, httpServerHttpPortDefault)
                .put(HTTP_SERVER_REUSE_PORT, httpServerReusePortDefault)
                .put(HTTP_SERVER_BIND_TO_NODE_INTERNAL_ADDRESS_ONLY_ENABLED,
                    httpServerBindToNodeInternalAddressOnlyEnabledDefault)
                .put(HTTP_SERVER_HTTPS_PORT, httpServerHttpsPortDefault)
                .put(HTTP_SERVER_HTTPS_ENABLED, httpServerHttpsEnabledDefault)
                .put(HTTPS_SUPPORTED_CIPHERS, httpsSupportedCiphersDefault)
                .put(HTTPS_CERT_PATH, httpsCertPathDefault)
                .put(HTTPS_KEY_PATH, httpsKeyPathDefault)
                .put(HTTP_SERVER_NUM_IO_THREADS_HW_MULTIPLIER,
                    httpServerNumIoThreadsHwMultiplierDefault)
                .put(EXCHANGE_HTTP_CLIENT_NUM_IO_THREADS_HW_MULTIPLIER,
                    exchangeHttpClientNumIoThreadsHwMultiplierDefault)
                .put(ASYNC_DATA_CACHE_ENABLED, asyncDataCacheEnabledDefault)
                .put(ASYNC_CACHE_SSD_GB, asyncCacheSsdGbDefault)
                .put(CONNECTOR_NUM_IO_THREADS_HW_MULTIPLIER, connectorNumIoThreadsHwMultiplierDefault)
                .put(PRESTO_VERSION, prestoVersionDefault)
                .put(SHUTDOWN_ONSET_SEC, shutdownOnsetSecDefault)
                .put(SYSTEM_MEMORY_GB, systemMemoryGbDefault)
                .put(QUERY_MEMORY_GB, queryMemoryGbDefault)
                .put(QUERY_MAX_MEMORY_PER_NODE, queryMaxMemoryPerNodeDefault)
                .put(USE_MMAP_ALLOCATOR, useMmapAllocatorDefault)
                .put(MEMORY_ARBITRATOR_KIND, memoryArbitratorKindDefault)
                .put(SHARED_ARBITRATOR_RESERVED_CAPACITY, sharedArbitratorReservedCapacityDefault)
                .put(SHARED_ARBITRATOR_MEMORY_POOL_INITIAL_CAPACITY,
                    sharedArbitratorMemoryPoolInitialCapacityDefault)
                .put(SHARED_ARBITRATOR_MAX_MEMORY_ARBITRATION_TIME,
                    sharedArbitratorMaxMemoryArbitrationTimeDefault)
                .put(EXPERIMENTAL_SPILLER_SPILL_PATH, experimentalSpillerSpillPathDefault)
                .put(TASK_MAX_DRIVERS_PER_TASK, taskMaxDriversPerTaskDefault)
                .put(ENABLE_OLD_TASK_CLEANUP, enableOldTaskCleanupDefault)
                .put(SHUFFLE_NAME, shuffleNameDefault)
                .put(HTTP_SERVER_ENABLE_ACCESS_LOG, httpServerEnableAccessLogDefault)
                .put(CORE_ON_ALLOCATION_FAILURE_ENABLED, coreOnAllocationFailureEnabledDefault)
                .put(SPILL_ENABLED, spillEnabledDefault)
                .put(AGGREGATION_SPILL_ENABLED, aggregationSpillEnabledDefault)
                .put(JOIN_SPILL_ENABLED, joinSpillEnabledDefault)
                .put(ORDER_BY_SPILL_ENABLED, orderBySpillEnabledDefault)
                .put(MAX_SPILL_BYTES, maxSpillBytesDefault)
                .build();
    }

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(systemConfigs);
        defaultSystemConfigs.entrySet().stream()
                .filter(entry -> !systemConfigs.containsKey(entry.getKey()))
                .forEach(entry -> builder.put(entry.getKey(), entry.getValue()));
        return builder.build();
    }

    public NativeExecutionSystemConfig update(String key, String value)
    {
        systemConfigs.put(key, value);
        return this;
    }

    private String getAbsolutePath(String path)
    {
        File absolutePath = new File(path);
        if (!absolutePath.isAbsolute()) {
            // In the case of SparkEnv is not initialed (e.g. unit test), we just use current location instead of calling SparkFiles.getRootDirectory() to avoid error.
            String rootDirectory = SparkEnv$.MODULE$.get() != null ? SparkFiles.getRootDirectory() : ".";
            absolutePath = new File(rootDirectory, path);
        }

        if (!absolutePath.exists()) {
            throw new IllegalArgumentException(format("File doesn't exist %s", absolutePath.getAbsolutePath()));
        }

        return absolutePath.getAbsolutePath();
    }
}
