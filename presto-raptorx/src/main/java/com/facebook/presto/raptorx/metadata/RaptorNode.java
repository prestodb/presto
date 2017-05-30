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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.spi.Node;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RaptorNode
{
    private final long nodeId;
    private final Node node;

    public RaptorNode(long nodeId, Node node)
    {
        this.nodeId = nodeId;
        this.node = requireNonNull(node, "node is null");
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public Node getNode()
    {
        return node;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeId", nodeId)
                .add("identifier", node.getNodeIdentifier())
                .add("address", node.getHostAndPort())
                .toString();
    }
}
