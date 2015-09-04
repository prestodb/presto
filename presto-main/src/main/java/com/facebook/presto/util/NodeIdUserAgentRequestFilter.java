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
package com.facebook.presto.util;

import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static java.util.Objects.requireNonNull;

public class NodeIdUserAgentRequestFilter
        implements HttpRequestFilter
{
    private final String nodeId;

    @Inject
    public NodeIdUserAgentRequestFilter(NodeInfo nodeInfo)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        this.nodeId = requireNonNull(nodeInfo.getNodeId(), "nodeId is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        requireNonNull(request, "request is null");
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
