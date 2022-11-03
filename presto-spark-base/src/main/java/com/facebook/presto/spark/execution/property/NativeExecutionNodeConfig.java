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

/**
 * This config class corresponds to node.properties for native execution process. Properties inside will be used in Configs::NodeConfig in Configs.h/cpp
 */
public class NativeExecutionNodeConfig
{
    private static final String NODE_ENVIRONMENT = "node.environment";
    private static final String NODE_ID = "node.id";
    private static final String NODE_LOCATION = "node.location";
    private static final String NODE_IP = "node.ip";
    private static final String NODE_MEMORY_GB = "node.memory_gb";

    private String nodeEnvironment = "spark-velox";
    private String nodeLocation = "/dummy/location";
    private String nodeIp = "0.0.0.0";
    private int nodeId;
    private int nodeMemoryGb = 10;

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(NODE_ENVIRONMENT, getNodeEnvironment())
                .put(NODE_ID, String.valueOf(getNodeId()))
                .put(NODE_LOCATION, getNodeLocation())
                .put(NODE_IP, getNodeIp())
                .put(NODE_MEMORY_GB, String.valueOf(getNodeMemoryGb())).build();
    }

    public String getNodeEnvironment()
    {
        return nodeEnvironment;
    }

    @Config(NODE_ENVIRONMENT)
    public NativeExecutionNodeConfig setNodeEnvironment(String nodeEnvironment)
    {
        this.nodeEnvironment = nodeEnvironment;
        return this;
    }

    public String getNodeLocation()
    {
        return nodeLocation;
    }

    @Config(NODE_LOCATION)
    public NativeExecutionNodeConfig setNodeLocation(String nodeLocation)
    {
        this.nodeLocation = nodeLocation;
        return this;
    }

    public String getNodeIp()
    {
        return nodeIp;
    }

    @Config(NODE_IP)
    public NativeExecutionNodeConfig setNodeIp(String nodeIp)
    {
        this.nodeIp = nodeIp;
        return this;
    }

    public int getNodeId()
    {
        return nodeId;
    }

    @Config(NODE_ID)
    public NativeExecutionNodeConfig setNodeId(int nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public int getNodeMemoryGb()
    {
        return nodeMemoryGb;
    }

    @Config(NODE_MEMORY_GB)
    public NativeExecutionNodeConfig setNodeMemoryGb(int nodeMemoryGb)
    {
        this.nodeMemoryGb = nodeMemoryGb;
        return this;
    }
}
