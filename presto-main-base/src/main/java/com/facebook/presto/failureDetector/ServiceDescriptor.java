/*
 * Copyright 2010 Proofpoint, Inc.
 *
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
package com.facebook.presto.failureDetector;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.failureDetector.FailureDetector.ServiceState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ServiceDescriptor
{
    private final UUID id;
    private final String nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final ServiceState state;
    private final Map<String, String> properties;

    @JsonCreator
    public ServiceDescriptor(
            @JsonProperty("id") UUID id,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("type") String type,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("state") ServiceState state,
            @JsonProperty("properties") Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");

        this.id = id;
        this.nodeId = nodeId;
        this.type = type;
        this.pool = pool;
        this.location = location;
        this.state = state;
        this.properties = ImmutableMap.copyOf(properties);
    }

    @JsonProperty
    public UUID getId()
    {
        return id;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getPool()
    {
        return pool;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public ServiceState getState()
    {
        return state;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceDescriptor that = (ServiceDescriptor) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("nodeId", nodeId)
                .add("type", type)
                .add("pool", pool)
                .add("location", location)
                .add("state", state)
                .add("properties", properties)
                .toString();
    }

    public static ServiceDescriptorBuilder serviceDescriptor(String type)
    {
        requireNonNull(type, "type is null");
        return new ServiceDescriptorBuilder(type);
    }

    public static class ServiceDescriptorBuilder
    {
        private UUID id;
        private String nodeId;
        private final String type;
        private String pool = "general";
        private String location;
        private ServiceState state;

        private final ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        private ServiceDescriptorBuilder(String type)
        {
            this.type = type;
        }

        public ServiceDescriptorBuilder setId(UUID id)
        {
            requireNonNull(id, "id is null");
            this.id = id;
            return this;
        }

        public ServiceDescriptorBuilder setNodeInfo(NodeInfo nodeInfo)
        {
            requireNonNull(nodeInfo, "nodeInfo is null");
            this.nodeId = nodeInfo.getNodeId();
            this.pool = nodeInfo.getPool();
            return this;
        }

        public ServiceDescriptorBuilder setNodeId(String nodeId)
        {
            requireNonNull(nodeId, "nodeId is null");
            this.nodeId = nodeId;
            return this;
        }

        public ServiceDescriptorBuilder setPool(String pool)
        {
            requireNonNull(pool, "pool is null");
            this.pool = pool;
            return this;
        }

        public ServiceDescriptorBuilder setLocation(String location)
        {
            requireNonNull(location, "location is null");
            this.location = location;
            return this;
        }

        public ServiceDescriptorBuilder setState(ServiceState state)
        {
            requireNonNull(state, "state is null");
            this.state = state;
            return this;
        }

        public ServiceDescriptorBuilder addProperty(String key, String value)
        {
            requireNonNull(key, "key is null");
            requireNonNull(value, "value is null");
            properties.put(key, value);
            return this;
        }

        public ServiceDescriptorBuilder addProperties(Map<String, String> properties)
        {
            requireNonNull(properties, "properties is null");
            this.properties.putAll(properties);
            return this;
        }

        public ServiceDescriptor build()
        {
            UUID id = (this.id == null) ? UUID.randomUUID() : this.id;
            return new ServiceDescriptor(id, nodeId, type, pool, location, state, properties.build());
        }
    }
}
