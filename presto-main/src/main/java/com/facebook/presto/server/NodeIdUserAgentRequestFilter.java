package com.facebook.presto.server;

import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.Request.Builder.fromRequest;

public class NodeIdUserAgentRequestFilter
        implements HttpRequestFilter
{
    private final String nodeId;

    @Inject
    public NodeIdUserAgentRequestFilter(NodeInfo nodeInfo)
    {
        checkNotNull(nodeInfo, "nodeInfo is null");
        this.nodeId = checkNotNull(nodeInfo.getNodeId(), "nodeId is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        checkNotNull(request, "request is null");
        return fromRequest(request)
                .addHeader(USER_AGENT, nodeId)
                .build();
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
        return nodeId.equals(((NodeIdUserAgentRequestFilter) obj).nodeId);
    }

    @Override
    public int hashCode()
    {
        return nodeId.hashCode();
    }
}
