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

import com.facebook.airlift.configuration.Config;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This config class corresponds to config.properties for native execution process. Properties inside will be used in Configs::SystemConfig in Configs.h/cpp
 */
public class NativeExecutionSystemConfig
{
    private static final String CONCURRENT_LIFESPANS_PER_TASK = "concurrent-lifespans-per-task";
    private static final String ENABLE_SERIALIZED_PAGE_CHECKSUM = "enable-serialized-page-checksum";
    private static final String ENABLE_VELOX_EXPRESSION_LOGGING = "enable_velox_expression_logging";
    private static final String ENABLE_VELOX_TASK_LOGGING = "enable_velox_task_logging";
    // Port on which presto-native http server should run
    private static final String HTTP_SERVER_HTTP_PORT = "http-server.http.port";
    private static final String HTTP_SERVER_REUSE_PORT = "http-server.reuse-port";
    private static final String HTTP_SERVER_BIND_TO_NODE_INTERNAL_ADDRESS_ONLY_ENABLED = "http-server.bind-to-node-internal-address-only-enabled";
    private static final String REGISTER_TEST_FUNCTIONS = "register-test-functions";
    // Number of I/O thread to use for serving http request on presto-native (proxygen server)
    // this excludes worker thread used by velox
    private static final String HTTP_SERVER_HTTPS_PORT = "http-server.https.port";
    private static final String HTTP_SERVER_HTTPS_ENABLED = "http-server.https.enabled";

    // This config control what cipher suites are supported by Native workers for server and client.
    // Note Java and folly::SSLContext use different names to refer to the same cipher.
    // (guess for different name, Java specific authentication,key exchange and cipher together and folly just cipher).
    // For e.g. TLS_RSA_WITH_AES_256_GCM_SHA384 in Java and AES256-GCM-SHA384 in folly::SSLContext.
    // The ciphers need to enable worker to worker, worker to coordinator and coordinator to worker communication.
    // Have at least one cipher suite that is shared for the above 3, otherwise weird failures will result.
    private static final String HTTPS_CIPHERS = "https-supported-ciphers";

    // Note: Java packages cert and key in combined JKS file. But CPP requires them separately.
    // The HTTPS provides integrity and not security(authentication/authorization).
    // But the HTTPS will protect against data corruption by bad router and man in middle attacks.

    // The cert path for the https server
    private static final String HTTPS_CERT_PATH = "https-cert-path";
    // The key path for the https server
    private static final String HTTPS_KEY_PATH = "https-key-path";

    // TODO: others use "-" separator and this property use _ separator. Fix them.
    private static final String HTTP_SERVER_NUM_IO_THREADS_HW_MULTIPLIER = "http-server.num-io-threads-hw-multiplier";
    private static final String EXCHANGE_HTTP_CLIENT_NUM_IO_THREADS_HW_MULTIPLIER = "exchange.http-client.num-io-threads-hw-multiplier";
    private static final String ASYNC_DATA_CACHE_ENABLED = "async-data-cache-enabled";
    private static final String ASYNC_CACHE_SSD_GB = "async-cache-ssd-gb";
    private static final String CONNECTOR_NUM_IO_THREADS_HW_MULTIPLIER = "connector.num-io-threads-hw-multiplier";
    private static final String PRESTO_VERSION = "presto.version";
    private static final String SHUTDOWN_ONSET_SEC = "shutdown-onset-sec";
    // Memory related configurations.
    private static final String SYSTEM_MEMORY_GB = "system-memory-gb";
    private static final String QUERY_MEMORY_GB = "query.max-memory-per-node";
    private static final String USE_MMAP_ALLOCATOR = "use-mmap-allocator";
    // Memory arbitration related configurations.
    // Set the memory arbitrator kind. If it is empty, then there is no memory
    // arbitration, when a query runs out of its capacity, the query will fail.
    // If it set to "SHARED" (default), the shared memory arbitrator will be
    // used to conduct arbitration and try to trigger disk spilling to reclaim
    // memory so the query can run through completion.
    private static final String MEMORY_ARBITRATOR_KIND = "memory-arbitrator-kind";
    // Set memory arbitrator capacity to the same as per-query memory capacity
    // as there is only one query running at Presto-on-Spark at a time.
    private static final String MEMORY_ARBITRATOR_CAPACITY_GB = "query-memory-gb";
    // Set memory arbitrator reserved capacity. Since there is only one query
    // running at Presto-on-Spark at a time, then we shall set this to zero.
    private static final String MEMORY_ARBITRATOR_RESERVED_CAPACITY_GB = "query-reserved-memory-gb";
    // Set the initial memory capacity when we create a query memory pool. For
    // Presto-on-Spark, we set it to 'query-memory-gb' to allocate all the
    // memory arbitrator capacity to the query memory pool on its creation as
    // there is only one query running at a time.
    private static final String MEMORY_POOL_INIT_CAPACITY = "memory-pool-init-capacity";
    // Set the reserved memory capacity when we create a query memory pool. For
    // Presto-on-Spark, we set this to zero as there is only one query running
    // at a time.
    private static final String MEMORY_POOL_RESERVED_CAPACITY = "memory-pool-reserved-capacity";
    // Set the minimal memory capacity transfer between memory pools under
    // memory arbitration. For Presto-on-Spark, there is only one query running
    // so this specified how much memory to reclaim from a query when it runs
    // out of memory.
    private static final String MEMORY_POOL_TRANSFER_CAPACITY = "memory-pool-transfer-capacity";
    private static final String MEMORY_RECLAIM_WAIT_MS = "memory-reclaim-wait-ms";
    // Spilling related configs.
    private static final String SPILLER_SPILL_PATH = "experimental.spiller-spill-path";
    private static final String TASK_MAX_DRIVERS_PER_TASK = "task.max-drivers-per-task";
    // Tasks are considered old, when they are in not-running state and it ended more than
    // OLD_TASK_CLEANUP_MS ago or last heartbeat was more than OLD_TASK_CLEANUP_MS ago.
    // For Presto-On-Spark, this is not relevant as it runs tasks serially, and spark's speculative
    // execution takes care of zombie tasks.
    private static final String ENABLE_OLD_TASK_CLEANUP = "enable-old-task-cleanup";
    // Name of exchange client to use
    private static final String SHUFFLE_NAME = "shuffle.name";
    // Feature flag for access log on presto-native http server
    private static final String HTTP_SERVER_ACCESS_LOGS = "http-server.enable-access-log";
    // Terminates the native process and generates a core file on an allocation failure
    private static final String CORE_ON_ALLOCATION_FAILURE_ENABLED = "core-on-allocation-failure-enabled";
    private boolean enableSerializedPageChecksum = true;
    private boolean enableVeloxExpressionLogging;
    private boolean enableVeloxTaskLogging = true;
    private boolean httpServerReusePort = true;
    private boolean httpServerBindToNodeInternalAddressOnlyEnabled = true;
    private int httpServerPort = 7777;
    private double httpServerNumIoThreadsHwMultiplier = 1.0;
    private int httpsServerPort = 7778;
    private boolean enableHttpsCommunication;
    private String httpsCiphers = "AES128-SHA,AES128-SHA256,AES256-GCM-SHA384";
    private String httpsCertPath = "";
    private String httpsKeyPath = "";
    private double exchangeHttpClientNumIoThreadsHwMultiplier = 1.0;
    private boolean asyncDataCacheEnabled; // false
    private int asyncCacheSsdGb; // 0
    private double connectorNumIoThreadsHwMultiplier; // 0.0
    private int shutdownOnsetSec = 10;
    private int systemMemoryGb = 10;
    // Reserve 2GB from system memory for system operations such as disk
    // spilling and cache prefetch.
    private DataSize queryMemoryGb = new DataSize(8, DataSize.Unit.GIGABYTE);

    private DataSize queryReservedMemoryGb = new DataSize(0, DataSize.Unit.GIGABYTE);
    private boolean useMmapAllocator = true;
    private String memoryArbitratorKind = "SHARED";
    private int memoryArbitratorCapacityGb = 8;
    private int memoryArbitratorReservedCapacityGb;
    private long memoryPoolInitCapacity = 8L << 30;
    private long memoryPoolReservedCapacity;
    private long memoryPoolTransferCapacity = 2L << 30;
    private long memoryReclaimWaitMs = 300_000;
    private String spillerSpillPath = "";
    private int concurrentLifespansPerTask = 5;
    private int maxDriversPerTask = 15;
    private boolean enableOldTaskCleanUp; // false;
    private String prestoVersion = "dummy.presto.version";
    private String shuffleName = "local";
    private boolean registerTestFunctions;
    private boolean enableHttpServerAccessLog = true;
    private boolean coreOnAllocationFailureEnabled;

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(CONCURRENT_LIFESPANS_PER_TASK, String.valueOf(getConcurrentLifespansPerTask()))
                .put(ENABLE_SERIALIZED_PAGE_CHECKSUM, String.valueOf(isEnableSerializedPageChecksum()))
                .put(ENABLE_VELOX_EXPRESSION_LOGGING, String.valueOf(isEnableVeloxExpressionLogging()))
                .put(ENABLE_VELOX_TASK_LOGGING, String.valueOf(isEnableVeloxTaskLogging()))
                .put(HTTP_SERVER_HTTP_PORT, String.valueOf(getHttpServerPort()))
                .put(HTTP_SERVER_REUSE_PORT, String.valueOf(isHttpServerReusePort()))
                .put(HTTP_SERVER_BIND_TO_NODE_INTERNAL_ADDRESS_ONLY_ENABLED, String.valueOf(isHttpServerBindToNodeInternalAddressOnlyEnabled()))
                .put(REGISTER_TEST_FUNCTIONS, String.valueOf(isRegisterTestFunctions()))
                .put(HTTP_SERVER_HTTPS_PORT, String.valueOf(getHttpsServerPort()))
                .put(HTTP_SERVER_HTTPS_ENABLED, String.valueOf(isEnableHttpsCommunication()))
                .put(HTTPS_CIPHERS, String.valueOf(getHttpsCiphers()))
                .put(HTTPS_CERT_PATH, String.valueOf(getHttpsCertPath()))
                .put(HTTPS_KEY_PATH, String.valueOf(getHttpsKeyPath()))
                .put(HTTP_SERVER_NUM_IO_THREADS_HW_MULTIPLIER, String.valueOf(getHttpServerNumIoThreadsHwMultiplier()))
                .put(EXCHANGE_HTTP_CLIENT_NUM_IO_THREADS_HW_MULTIPLIER, String.valueOf(getExchangeHttpClientNumIoThreadsHwMultiplier()))
                .put(ASYNC_DATA_CACHE_ENABLED, String.valueOf(getAsyncDataCacheEnabled()))
                .put(ASYNC_CACHE_SSD_GB, String.valueOf(getAsyncCacheSsdGb()))
                .put(CONNECTOR_NUM_IO_THREADS_HW_MULTIPLIER, String.valueOf(getConnectorNumIoThreadsHwMultiplier()))
                .put(PRESTO_VERSION, getPrestoVersion())
                .put(SHUTDOWN_ONSET_SEC, String.valueOf(getShutdownOnsetSec()))
                .put(SYSTEM_MEMORY_GB, String.valueOf(getSystemMemoryGb()))
                .put(QUERY_MEMORY_GB, String.valueOf(getQueryMemoryGb()))
                .put(USE_MMAP_ALLOCATOR, String.valueOf(getUseMmapAllocator()))
                .put(MEMORY_ARBITRATOR_KIND, String.valueOf(getMemoryArbitratorKind()))
                .put(MEMORY_ARBITRATOR_CAPACITY_GB, String.valueOf(getMemoryArbitratorCapacityGb()))
                .put(MEMORY_ARBITRATOR_RESERVED_CAPACITY_GB, String.valueOf(getMemoryArbitratorReservedCapacityGb()))
                .put(MEMORY_POOL_INIT_CAPACITY, String.valueOf(getMemoryPoolInitCapacity()))
                .put(MEMORY_POOL_RESERVED_CAPACITY, String.valueOf(getMemoryPoolReservedCapacity()))
                .put(MEMORY_POOL_TRANSFER_CAPACITY, String.valueOf(getMemoryPoolTransferCapacity()))
                .put(MEMORY_RECLAIM_WAIT_MS, String.valueOf(getMemoryReclaimWaitMs()))
                .put(SPILLER_SPILL_PATH, String.valueOf(getSpillerSpillPath()))
                .put(TASK_MAX_DRIVERS_PER_TASK, String.valueOf(getMaxDriversPerTask()))
                .put(ENABLE_OLD_TASK_CLEANUP, String.valueOf(getOldTaskCleanupMs()))
                .put(SHUFFLE_NAME, getShuffleName())
                .put(HTTP_SERVER_ACCESS_LOGS, String.valueOf(isEnableHttpServerAccessLog()))
                .put(CORE_ON_ALLOCATION_FAILURE_ENABLED, String.valueOf(isCoreOnAllocationFailureEnabled()))
                .build();
    }

    @Config(SHUFFLE_NAME)
    public NativeExecutionSystemConfig setShuffleName(String shuffleName)
    {
        this.shuffleName = requireNonNull(shuffleName);
        return this;
    }

    public String getShuffleName()
    {
        return shuffleName;
    }

    @Config(ENABLE_SERIALIZED_PAGE_CHECKSUM)
    public NativeExecutionSystemConfig setEnableSerializedPageChecksum(boolean enableSerializedPageChecksum)
    {
        this.enableSerializedPageChecksum = enableSerializedPageChecksum;
        return this;
    }

    public boolean isEnableSerializedPageChecksum()
    {
        return enableSerializedPageChecksum;
    }

    @Config(ENABLE_VELOX_EXPRESSION_LOGGING)
    public NativeExecutionSystemConfig setEnableVeloxExpressionLogging(boolean enableVeloxExpressionLogging)
    {
        this.enableVeloxExpressionLogging = enableVeloxExpressionLogging;
        return this;
    }

    public boolean isEnableVeloxExpressionLogging()
    {
        return enableVeloxExpressionLogging;
    }

    @Config(ENABLE_VELOX_TASK_LOGGING)
    public NativeExecutionSystemConfig setEnableVeloxTaskLogging(boolean enableVeloxTaskLogging)
    {
        this.enableVeloxTaskLogging = enableVeloxTaskLogging;
        return this;
    }

    public boolean isEnableVeloxTaskLogging()
    {
        return enableVeloxTaskLogging;
    }

    @Config(HTTP_SERVER_HTTP_PORT)
    public NativeExecutionSystemConfig setHttpServerPort(int httpServerPort)
    {
        this.httpServerPort = httpServerPort;
        return this;
    }

    public int getHttpServerPort()
    {
        return httpServerPort;
    }

    @Config(HTTP_SERVER_REUSE_PORT)
    public NativeExecutionSystemConfig setHttpServerReusePort(boolean httpServerReusePort)
    {
        this.httpServerReusePort = httpServerReusePort;
        return this;
    }

    public boolean isHttpServerReusePort()
    {
        return httpServerReusePort;
    }

    public boolean isHttpServerBindToNodeInternalAddressOnlyEnabled()
    {
        return httpServerBindToNodeInternalAddressOnlyEnabled;
    }

    @Config(HTTP_SERVER_BIND_TO_NODE_INTERNAL_ADDRESS_ONLY_ENABLED)
    public NativeExecutionSystemConfig setHttpServerBindToNodeInternalAddressOnlyEnabled(boolean httpServerBindToNodeInternalAddressOnlyEnabled)
    {
        this.httpServerBindToNodeInternalAddressOnlyEnabled = httpServerBindToNodeInternalAddressOnlyEnabled;
        return this;
    }

    @Config(REGISTER_TEST_FUNCTIONS)
    public NativeExecutionSystemConfig setRegisterTestFunctions(boolean registerTestFunctions)
    {
        this.registerTestFunctions = registerTestFunctions;
        return this;
    }

    public boolean isRegisterTestFunctions()
    {
        return registerTestFunctions;
    }

    @Config(HTTP_SERVER_NUM_IO_THREADS_HW_MULTIPLIER)
    public NativeExecutionSystemConfig setHttpServerNumIoThreadsHwMultiplier(double httpServerNumIoThreadsHwMultiplier)
    {
        this.httpServerNumIoThreadsHwMultiplier = httpServerNumIoThreadsHwMultiplier;
        return this;
    }

    public double getHttpServerNumIoThreadsHwMultiplier()
    {
        return httpServerNumIoThreadsHwMultiplier;
    }

    public int getHttpsServerPort()
    {
        return httpsServerPort;
    }

    @Config(HTTP_SERVER_HTTPS_PORT)
    public NativeExecutionSystemConfig setHttpsServerPort(int httpsServerPort)
    {
        this.httpsServerPort = httpsServerPort;
        return this;
    }

    public boolean isEnableHttpsCommunication()
    {
        return enableHttpsCommunication;
    }

    @Config(HTTP_SERVER_HTTPS_ENABLED)
    public NativeExecutionSystemConfig setEnableHttpsCommunication(boolean enableHttpsCommunication)
    {
        this.enableHttpsCommunication = enableHttpsCommunication;
        return this;
    }

    public String getHttpsCiphers()
    {
        return httpsCiphers;
    }

    @Config(HTTPS_CIPHERS)
    public NativeExecutionSystemConfig setHttpsCiphers(String httpsCiphers)
    {
        this.httpsCiphers = httpsCiphers;
        return this;
    }

    public String getHttpsCertPath()
    {
        return httpsCertPath;
    }

    @Config(HTTPS_CERT_PATH)
    public NativeExecutionSystemConfig setHttpsCertPath(String httpsCertPath)
    {
        this.httpsCertPath = httpsCertPath;
        return this;
    }

    public String getHttpsKeyPath()
    {
        return httpsKeyPath;
    }

    @Config(HTTPS_KEY_PATH)
    public NativeExecutionSystemConfig setHttpsKeyPath(String httpsKeyPath)
    {
        this.httpsKeyPath = httpsKeyPath;
        return this;
    }

    @Config(EXCHANGE_HTTP_CLIENT_NUM_IO_THREADS_HW_MULTIPLIER)
    public NativeExecutionSystemConfig setExchangeHttpClientNumIoThreadsHwMultiplier(double exchangeHttpClientNumIoThreadsHwMultiplier)
    {
        this.exchangeHttpClientNumIoThreadsHwMultiplier = exchangeHttpClientNumIoThreadsHwMultiplier;
        return this;
    }

    public double getExchangeHttpClientNumIoThreadsHwMultiplier()
    {
        return exchangeHttpClientNumIoThreadsHwMultiplier;
    }

    @Config(ASYNC_DATA_CACHE_ENABLED)
    public NativeExecutionSystemConfig setAsyncDataCacheEnabled(boolean asyncDataCacheEnabled)
    {
        this.asyncDataCacheEnabled = asyncDataCacheEnabled;
        return this;
    }

    public boolean getAsyncDataCacheEnabled()
    {
        return asyncDataCacheEnabled;
    }

    @Config(ASYNC_CACHE_SSD_GB)
    public NativeExecutionSystemConfig setAsyncCacheSsdGb(int asyncCacheSsdGb)
    {
        this.asyncCacheSsdGb = asyncCacheSsdGb;
        return this;
    }

    public int getAsyncCacheSsdGb()
    {
        return asyncCacheSsdGb;
    }

    @Config(CONNECTOR_NUM_IO_THREADS_HW_MULTIPLIER)
    public NativeExecutionSystemConfig setConnectorNumIoThreadsHwMultiplier(double connectorNumIoThreadsHwMultiplier)
    {
        this.connectorNumIoThreadsHwMultiplier = connectorNumIoThreadsHwMultiplier;
        return this;
    }

    public double getConnectorNumIoThreadsHwMultiplier()
    {
        return connectorNumIoThreadsHwMultiplier;
    }

    @Config(SHUTDOWN_ONSET_SEC)
    public NativeExecutionSystemConfig setShutdownOnsetSec(int shutdownOnsetSec)
    {
        this.shutdownOnsetSec = shutdownOnsetSec;
        return this;
    }

    public int getShutdownOnsetSec()
    {
        return shutdownOnsetSec;
    }

    @Config(SYSTEM_MEMORY_GB)
    public NativeExecutionSystemConfig setSystemMemoryGb(int systemMemoryGb)
    {
        this.systemMemoryGb = systemMemoryGb;
        return this;
    }

    public int getSystemMemoryGb()
    {
        return systemMemoryGb;
    }

    @Config(QUERY_MEMORY_GB)
    public NativeExecutionSystemConfig setQueryMemoryGb(DataSize queryMemoryGb)
    {
        this.queryMemoryGb = queryMemoryGb;
        return this;
    }

    public DataSize getQueryMemoryGb()
    {
        return queryMemoryGb;
    }

    @Config(USE_MMAP_ALLOCATOR)
    public NativeExecutionSystemConfig setUseMmapAllocator(boolean useMmapAllocator)
    {
        this.useMmapAllocator = useMmapAllocator;
        return this;
    }

    public boolean getUseMmapAllocator()
    {
        return useMmapAllocator;
    }

    @Config(MEMORY_ARBITRATOR_KIND)
    public NativeExecutionSystemConfig setMemoryArbitratorKind(String memoryArbitratorKind)
    {
        this.memoryArbitratorKind = memoryArbitratorKind;
        return this;
    }

    public String getMemoryArbitratorKind()
    {
        return memoryArbitratorKind;
    }

    @Config(MEMORY_ARBITRATOR_CAPACITY_GB)
    public NativeExecutionSystemConfig setMemoryArbitratorCapacityGb(int memoryArbitratorCapacityGb)
    {
        this.memoryArbitratorCapacityGb = memoryArbitratorCapacityGb;
        return this;
    }

    public int getMemoryArbitratorCapacityGb()
    {
        return memoryArbitratorCapacityGb;
    }

    @Config(MEMORY_ARBITRATOR_RESERVED_CAPACITY_GB)
    public NativeExecutionSystemConfig setMemoryArbitratorReservedCapacityGb(int memoryArbitratorReservedCapacityGb)
    {
        this.memoryArbitratorReservedCapacityGb = memoryArbitratorReservedCapacityGb;
        return this;
    }

    public int getMemoryArbitratorReservedCapacityGb()
    {
        return memoryArbitratorReservedCapacityGb;
    }

    @Config(MEMORY_POOL_INIT_CAPACITY)
    public NativeExecutionSystemConfig setMemoryPoolInitCapacity(long memoryPoolInitCapacity)
    {
        this.memoryPoolInitCapacity = memoryPoolInitCapacity;
        return this;
    }

    public long getMemoryPoolInitCapacity()
    {
        return memoryPoolInitCapacity;
    }

    @Config(MEMORY_POOL_RESERVED_CAPACITY)
    public NativeExecutionSystemConfig setMemoryPoolReservedCapacity(long memoryPoolReservedCapacity)
    {
        this.memoryPoolReservedCapacity = memoryPoolReservedCapacity;
        return this;
    }

    public long getMemoryPoolReservedCapacity()
    {
        return memoryPoolReservedCapacity;
    }

    @Config(MEMORY_POOL_TRANSFER_CAPACITY)
    public NativeExecutionSystemConfig setMemoryPoolTransferCapacity(long memoryPoolTransferCapacity)
    {
        this.memoryPoolTransferCapacity = memoryPoolTransferCapacity;
        return this;
    }

    public long getMemoryPoolTransferCapacity()
    {
        return memoryPoolTransferCapacity;
    }

    @Config(MEMORY_RECLAIM_WAIT_MS)
    public NativeExecutionSystemConfig setMemoryReclaimWaitMs(long memoryReclaimWaitMs)
    {
        this.memoryReclaimWaitMs = memoryReclaimWaitMs;
        return this;
    }

    public long getMemoryReclaimWaitMs()
    {
        return memoryReclaimWaitMs;
    }

    @Config(SPILLER_SPILL_PATH)
    public NativeExecutionSystemConfig setSpillerSpillPath(String spillerSpillPath)
    {
        this.spillerSpillPath = spillerSpillPath;
        return this;
    }

    public String getSpillerSpillPath()
    {
        return spillerSpillPath;
    }

    @Config(CONCURRENT_LIFESPANS_PER_TASK)
    public NativeExecutionSystemConfig setConcurrentLifespansPerTask(int concurrentLifespansPerTask)
    {
        this.concurrentLifespansPerTask = concurrentLifespansPerTask;
        return this;
    }

    public int getConcurrentLifespansPerTask()
    {
        return concurrentLifespansPerTask;
    }

    @Config(TASK_MAX_DRIVERS_PER_TASK)
    public NativeExecutionSystemConfig setMaxDriversPerTask(int maxDriversPerTask)
    {
        this.maxDriversPerTask = maxDriversPerTask;
        return this;
    }

    public int getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    public boolean getOldTaskCleanupMs()
    {
        return enableOldTaskCleanUp;
    }

    @Config(ENABLE_OLD_TASK_CLEANUP)
    public NativeExecutionSystemConfig setOldTaskCleanupMs(boolean enableOldTaskCleanUp)
    {
        this.enableOldTaskCleanUp = enableOldTaskCleanUp;
        return this;
    }

    @Config(PRESTO_VERSION)
    public NativeExecutionSystemConfig setPrestoVersion(String prestoVersion)
    {
        this.prestoVersion = prestoVersion;
        return this;
    }

    public String getPrestoVersion()
    {
        return prestoVersion;
    }

    @Config(HTTP_SERVER_ACCESS_LOGS)
    public NativeExecutionSystemConfig setEnableHttpServerAccessLog(boolean enableHttpServerAccessLog)
    {
        this.enableHttpServerAccessLog = enableHttpServerAccessLog;
        return this;
    }

    public boolean isEnableHttpServerAccessLog()
    {
        return enableHttpServerAccessLog;
    }

    public boolean isCoreOnAllocationFailureEnabled()
    {
        return coreOnAllocationFailureEnabled;
    }

    @Config(CORE_ON_ALLOCATION_FAILURE_ENABLED)
    public NativeExecutionSystemConfig setCoreOnAllocationFailureEnabled(boolean coreOnAllocationFailureEnabled)
    {
        this.coreOnAllocationFailureEnabled = coreOnAllocationFailureEnabled;
        return this;
    }
}
