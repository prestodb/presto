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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.NodeSelectionHashFunction;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConsistentHashingNodeProvider;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.ModularHashingNodeProvider;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.PrestoException;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Comparator.comparing;

public class NodeSelectionUtils
{
    private NodeSelectionUtils() {}

    static List<HostAddress> sortedNodes(NodeMap nodeMap)
    {
        return nodeMap.getNodesByHostAndPort().values().stream()
                .sorted(comparing(InternalNode::getNodeIdentifier)).map(InternalNode::getHostAndPort)
                .collect(toImmutableList());
    }

    static NodeProvider getNodeProvider(NodeSelectionHashFunction nodeSelectionHashFunction, NodeMap nodeMap)
    {
        switch (nodeSelectionHashFunction) {
            case MODULAR:
                // todo identify if sorting will cause bottleneck
                return new ModularHashingNodeProvider<>(sortedNodes(nodeMap));
            case CONSISTENT_HASHING:
                return new ConsistentHashingNodeProvider<>(nodeMap.getHostAndAddress());
            default:
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection hash function %s", nodeSelectionHashFunction.name()));
        }
    }
}
