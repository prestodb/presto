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
    private static final String HTTP_SERVER_HTTP_PORT = "http-server.http.port";
    private static final String HTTP_SERVER_REUSE_PORT = "http-server.reuse-port";
    private static final String HTTP_EXEC_THREADS = "http_exec_threads";
    private static final String NUM_IO_THREADS = "num-io-threads";
    private static final String PRESTO_VERSION = "presto.version";
    private static final String SHUTDOWN_ONSET_SEC = "shutdown-onset-sec";
    private static final String SYSTEM_MEMORY_GB = "system-memory-gb";
    private static final String TASK_MAX_DRIVERS_PER_TASK = "task.max-drivers-per-task";
    private static final String DISCOVERY_URI = "discovery.uri";
    private static final String SHUFFLE_NAME = "shuffle.name";

    private boolean enableSerializedPageChecksum = true;
    private boolean enableVeloxExpressionLogging;
    private boolean enableVeloxTaskLogging = true;
    private boolean httpServerReusePort = true;
    private int httpServerPort = 7777;
    private int httpExecThreads = 32;
    private int numIoThreads = 30;
    private int shutdownOnsetSec = 10;
    private int systemMemoryGb = 10;
    private int concurrentLifespansPerTask = 5;
    private int maxDriversPerTask = 15;
    private String prestoVersion = "dummy.presto.version";
    private String discoveryUri = "http://127.0.0.1";
    private String shuffleName = "local";

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(CONCURRENT_LIFESPANS_PER_TASK, String.valueOf(getConcurrentLifespansPerTask()))
                .put(ENABLE_SERIALIZED_PAGE_CHECKSUM, String.valueOf(isEnableSerializedPageChecksum()))
                .put(ENABLE_VELOX_EXPRESSION_LOGGING, String.valueOf(isEnableVeloxExpressionLogging()))
                .put(ENABLE_VELOX_TASK_LOGGING, String.valueOf(isEnableVeloxTaskLogging()))
                .put(HTTP_SERVER_HTTP_PORT, String.valueOf(getHttpServerPort()))
                .put(HTTP_SERVER_REUSE_PORT, String.valueOf(isHttpServerReusePort()))
                .put(HTTP_EXEC_THREADS, String.valueOf(getHttpExecThreads()))
                .put(NUM_IO_THREADS, String.valueOf(getNumIoThreads()))
                .put(PRESTO_VERSION, getPrestoVersion())
                .put(SHUTDOWN_ONSET_SEC, String.valueOf(getShutdownOnsetSec()))
                .put(SYSTEM_MEMORY_GB, String.valueOf(getSystemMemoryGb()))
                .put(TASK_MAX_DRIVERS_PER_TASK, String.valueOf(getMaxDriversPerTask()))
                .put(DISCOVERY_URI, getDiscoveryUri())
                .put(SHUFFLE_NAME, getShuffleName())
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

    @Config(HTTP_EXEC_THREADS)
    public NativeExecutionSystemConfig setHttpExecThreads(int httpExecThreads)
    {
        this.httpExecThreads = httpExecThreads;
        return this;
    }

    public int getHttpExecThreads()
    {
        return httpExecThreads;
    }

    @Config(NUM_IO_THREADS)
    public NativeExecutionSystemConfig setNumIoThreads(int numIoThreads)
    {
        this.numIoThreads = numIoThreads;
        return this;
    }

    public int getNumIoThreads()
    {
        return numIoThreads;
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

    @Config(DISCOVERY_URI)
    public NativeExecutionSystemConfig setDiscoveryUri(String discoveryUri)
    {
        this.discoveryUri = discoveryUri;
        return this;
    }

    public String getDiscoveryUri()
    {
        return discoveryUri;
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
}
