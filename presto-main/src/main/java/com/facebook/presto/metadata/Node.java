package com.facebook.presto.metadata;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * A node is a server in a cluster than can process queries.
 */
public class Node
{
    private final String nodeIdentifier;
    private final URI httpUri;

    public Node(String nodeIdentifier, URI httpUri)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.httpUri = checkNotNull(httpUri, "httpUri");
    }

    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    public URI getHttpUri()
    {
        return httpUri;
    }

    public HostAddress getHostAndPort()
    {
        return HostAddress.fromUri(httpUri);
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
        Node o = (Node) obj;
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
        return Objects.toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("httpUri", httpUri)
                .toString();
    }

    public static Function<Node, String> getIdentifierFunction()
    {
        return new Function<Node, String>()
        {
            @Override
            public String apply(Node node)
            {
                return node.getNodeIdentifier();
            }
        };
    }

    public static Function<Node, HostAddress> hostAndPortGetter()
    {
        return new Function<Node, HostAddress>()
        {
            @Override
            public HostAddress apply(Node node)
            {
                return node.getHostAndPort();
            }
        };
    }
}
