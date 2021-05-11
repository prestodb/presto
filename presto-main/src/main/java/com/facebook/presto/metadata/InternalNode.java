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
package com.facebook.presto.metadata;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;

import java.net.URI;
import java.util.OptionalInt;

import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
@ThriftStruct
public class InternalNode
        implements Node
{
    @ThriftEnum
    public enum NodeStatus
    {
        ALIVE(1),
        DEAD(2),
        /**/;

        private final int statusCode;

        NodeStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }

        @ThriftEnumValue
        public int getStatusCode()
        {
            return statusCode;
        }
    }

    private final String nodeIdentifier;
    private final URI internalUri;
    private final OptionalInt thriftPort;
    private final NodeVersion nodeVersion;
    private final boolean coordinator;
    private final boolean resourceManager;
    private final NodeStatus nodeStatus;

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator)
    {
        this(nodeIdentifier, internalUri, nodeVersion, coordinator, false);
    }

    public InternalNode(String nodeIdentifier, URI internalUri, NodeVersion nodeVersion, boolean coordinator, boolean resourceManager)
    {
        this(nodeIdentifier, internalUri, OptionalInt.empty(), nodeVersion, coordinator, resourceManager, ALIVE);
    }

    @ThriftConstructor
    public InternalNode(String nodeIdentifier, URI internalUri, OptionalInt thriftPort, String nodeVersion, boolean coordinator, boolean resourceManager)
    {
        this(nodeIdentifier, internalUri, thriftPort, new NodeVersion(nodeVersion), coordinator, resourceManager, ALIVE);
    }

    public InternalNode(String nodeIdentifier, URI internalUri, OptionalInt thriftPort, NodeVersion nodeVersion, boolean coordinator, boolean resourceManager, NodeStatus nodeStatus)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.internalUri = requireNonNull(internalUri, "internalUri is null");
        this.thriftPort = requireNonNull(thriftPort, "thriftPort is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.resourceManager = resourceManager;
        this.nodeStatus = nodeStatus;
    }

    @ThriftField(1)
    @Override
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String getHost()
    {
        return internalUri.getHost();
    }

    @Override
    @Deprecated
    public URI getHttpUri()
    {
        return getInternalUri();
    }

    @ThriftField(3)
    public OptionalInt getThriftPort()
    {
        return thriftPort;
    }

    @ThriftField(value = 2, name = "internalUri")
    public URI getInternalUri()
    {
        return internalUri;
    }

    @Override
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(internalUri);
    }

    @ThriftField(value = 4, name = "nodeVersion")
    @Override
    public String getVersion()
    {
        return nodeVersion.getVersion();
    }

    @ThriftField(5)
    @Override
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @ThriftField(6)
    @Override
    public boolean isResourceManager()
    {
        return resourceManager;
    }

    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
    }

    @ThriftField(7)
    public NodeStatus getNodeStatus()
    {
        return nodeStatus;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        InternalNode o = (InternalNode) obj;
        return nodeIdentifier.equals(o.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return nodeIdentifier.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("internalUri", internalUri)
                .add("thriftPort", thriftPort)
                .add("nodeVersion", nodeVersion)
                .add("coordinator", coordinator)
                .add("resourceManager", resourceManager)
                .toString();
    }
}
