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
package com.facebook.presto.server;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.memory.MemoryInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

@ThriftStruct
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

    @ThriftConstructor
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

    @ThriftField(1)
    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @ThriftField(2)
    @JsonProperty
    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @ThriftField(3)
    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @ThriftField(4)
    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @ThriftField(5)
    @JsonProperty
    public Duration getUptime()
    {
        return uptime;
    }

    @ThriftField(6)
    @JsonProperty
    public String getExternalAddress()
    {
        return externalAddress;
    }

    @ThriftField(7)
    @JsonProperty
    public String getInternalAddress()
    {
        return internalAddress;
    }

    @ThriftField(8)
    @JsonProperty
    public MemoryInfo getMemoryInfo()
    {
        return memoryInfo;
    }

    @ThriftField(9)
    @JsonProperty
    public int getProcessors()
    {
        return processors;
    }

    @ThriftField(10)
    @JsonProperty
    public double getProcessCpuLoad()
    {
        return processCpuLoad;
    }

    @ThriftField(11)
    @JsonProperty
    public double getSystemCpuLoad()
    {
        return systemCpuLoad;
    }

    @ThriftField(12)
    @JsonProperty
    public long getHeapUsed()
    {
        return heapUsed;
    }

    @ThriftField(13)
    @JsonProperty
    public long getHeapAvailable()
    {
        return heapAvailable;
    }

    @ThriftField(14)
    @JsonProperty
    public long getNonHeapUsed()
    {
        return nonHeapUsed;
    }
}
