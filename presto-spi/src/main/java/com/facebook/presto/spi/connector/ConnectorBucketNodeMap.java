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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ConnectorBucketNodeMap
{
    private final int bucketCount;
    private final Optional<List<Node>> bucketToNode;
    private final NodeSelectionStrategy nodeSelectionStrategy;

    public static ConnectorBucketNodeMap createBucketNodeMap(int bucketCount)
    {
        return new ConnectorBucketNodeMap(bucketCount, Optional.empty(), NO_PREFERENCE);
    }

    public static ConnectorBucketNodeMap createBucketNodeMap(List<Node> bucketToNode, NodeSelectionStrategy nodeSelectionStrategy)
    {
        return new ConnectorBucketNodeMap(bucketToNode.size(), Optional.of(bucketToNode), nodeSelectionStrategy);
    }

    private ConnectorBucketNodeMap(int bucketCount, Optional<List<Node>> bucketToNode, NodeSelectionStrategy nodeSelectionStrategy)
    {
        this.nodeSelectionStrategy = requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        if (bucketCount <= 0) {
            throw new IllegalArgumentException("bucketCount must be positive");
        }
        if (bucketToNode.isPresent() && bucketToNode.get().size() != bucketCount) {
            throw new IllegalArgumentException(format("Mismatched bucket count in bucketToNode (%s) and bucketCount (%s)", bucketToNode.get().size(), bucketCount));
        }
        this.bucketCount = bucketCount;
        this.bucketToNode = bucketToNode.map(ArrayList::new).map(Collections::unmodifiableList);
    }

    public int getBucketCount()
    {
        return bucketCount;
    }

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    public List<Node> getFixedMapping()
    {
        return bucketToNode.orElseThrow(() -> new IllegalArgumentException("No fixed bucket to node mapping"));
    }
}
