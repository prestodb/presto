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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.memory.MemoryInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

public class NodeStatus
{
    private final String nodeId;
    private final NodeVersion nodeVersion;
    private final String environment;
    private final boolean coordinator;
    private final Duration uptime;
    private final MemoryInfo memoryInfo;

    @JsonCreator
    public NodeStatus(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeVersion") NodeVersion nodeVersion,
            @JsonProperty("environment") String environment,
            @JsonProperty("coordinator") boolean coordinator,
            @JsonProperty("uptime") Duration uptime,
            @JsonProperty("memoryInfo") MemoryInfo memoryInfo)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.coordinator = coordinator;
        this.uptime = requireNonNull(uptime, "uptime is null");
        this.memoryInfo = requireNonNull(memoryInfo, "memoryInfo is null");
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
    public MemoryInfo getMemoryInfo()
    {
        return memoryInfo;
    }
}
