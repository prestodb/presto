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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import io.prestosql.client.NodeVersion;
import io.prestosql.memory.MemoryInfo;

import static java.util.Objects.requireNonNull;

public class NodeStatus
{
    private final String nodeId;
    private final NodeVersion nodeVersion;
    private final String environment;
    private final boolean coordinator;
    private final Duration uptime;
    private final String externalAddress;
    private final String internalAddress;
    private final MemoryInfo memoryInfo;
    private final int processors;
    private final double processCpuLoad;
    private final double systemCpuLoad;
    private final long heapUsed;
    private final long heapAvailable;
    private final long nonHeapUsed;

    @JsonCreator
    public NodeStatus(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeVersion") NodeVersion nodeVersion,
            @JsonProperty("environment") String environment,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("uptime") Duration uptime,
            @JsonProperty("externalAddress") String externalAddress,
            @JsonProperty("internalAddress") String internalAddress,
            @JsonProperty("memoryInfo") MemoryInfo memoryInfo,
            @JsonProperty("processors") int processors,
            @JsonProperty("processCpuLoad") double processCpuLoad,
            @JsonProperty("systemCpuLoad") double systemCpuLoad,
            @JsonProperty("heapUsed") long heapUsed,
            @JsonProperty("heapAvailable") long heapAvailable,
            @JsonProperty("nonHeapUsed") long nonHeapUsed)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.coordinator = coordinator;
        this.uptime = requireNonNull(uptime, "uptime is null");
        this.externalAddress = requireNonNull(externalAddress, "externalAddress is null");
        this.internalAddress = requireNonNull(internalAddress, "internalAddress is null");
        this.memoryInfo = requireNonNull(memoryInfo, "memoryInfo is null");
        this.processors = processors;
        this.processCpuLoad = processCpuLoad;
        this.systemCpuLoad = systemCpuLoad;
        this.heapUsed = heapUsed;
        this.heapAvailable = heapAvailable;
        this.nonHeapUsed = nonHeapUsed;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @JsonProperty
    public Duration getUptime()
    {
        return uptime;
    }

    @JsonProperty
    public String getExternalAddress()
    {
        return externalAddress;
    }

    @JsonProperty
    public String getInternalAddress()
    {
        return internalAddress;
    }

    @JsonProperty
    public MemoryInfo getMemoryInfo()
    {
        return memoryInfo;
    }

    @JsonProperty
    public int getProcessors()
    {
        return processors;
    }

    @JsonProperty
    public double getProcessCpuLoad()
    {
        return processCpuLoad;
    }

    @JsonProperty
    public double getSystemCpuLoad()
    {
        return systemCpuLoad;
    }

    @JsonProperty
    public long getHeapUsed()
    {
        return heapUsed;
    }

    @JsonProperty
    public long getHeapAvailable()
    {
        return heapAvailable;
    }

    @JsonProperty
    public long getNonHeapUsed()
    {
        return nonHeapUsed;
    }
}
