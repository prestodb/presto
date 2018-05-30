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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;

import java.net.URI;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
public class PrestoNode
        implements Node
{
    private final String nodeIdentifier;
    private final URI httpUri;
    private final NodeVersion nodeVersion;
    private final boolean coordinator;

    public PrestoNode(String nodeIdentifier, URI httpUri, NodeVersion nodeVersion, boolean coordinator)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.httpUri = requireNonNull(httpUri, "httpUri is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
    }

    @Override
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public URI getHttpUri()
    {
        return httpUri;
    }

    @Override
    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(httpUri);
    }

    @Override
    public String getVersion()
    {
        return nodeVersion.getVersion();
    }

    @Override
    public boolean isCoordinator()
    {
        return coordinator;
    }

    public NodeVersion getNodeVersion()
    {
        return nodeVersion;
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
        PrestoNode o = (PrestoNode) obj;
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
                .add("httpUri", httpUri)
                .add("nodeVersion", nodeVersion)
                .toString();
    }
}
