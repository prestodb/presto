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

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public interface ConnectorNodePartitioningProvider
{
    // TODO: Use ConnectorPartitionHandle (instead of int) to represent individual buckets.
    // Currently, it's mixed. listPartitionHandles used CPartitionHandle whereas the other functions used int.

    /**
     * Returns a list of all partitions associated with the provided {@code partitioningHandle}.
     * <p>
     * This method must be implemented for connectors that support addressable split discovery.
     * The partitions return here will be used as address for the purpose of split discovery.
     */
    default List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return singletonList(NOT_PARTITIONED);
    }

    ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle);

    ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle);

    BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount);

    final class ConnectorBucketNodeMap
    {
        private final int bucketCount;
        private final Optional<Map<Integer, Node>> bucketToNode;

        public static ConnectorBucketNodeMap createBucketNodeMap(int bucketCount)
        {
            return new ConnectorBucketNodeMap(bucketCount, Optional.empty());
        }

        public static ConnectorBucketNodeMap createBucketNodeMap(Map<Integer, Node> bucketToNode)
        {
            requireNonNull(bucketToNode, "bucketToNode is null");
            if (bucketToNode.isEmpty()) {
                throw new IllegalArgumentException("bucketToNode is empty");
            }

            return new ConnectorBucketNodeMap(
                    bucketToNode.keySet().stream()
                            .mapToInt(Integer::intValue)
                            .max()
                            .getAsInt() + 1,
                    Optional.of(bucketToNode));
        }

        private ConnectorBucketNodeMap(int bucketCount, Optional<Map<Integer, Node>> bucketToNode)
        {
            if (bucketCount <= 0) {
                throw new IllegalArgumentException("bucketCount must be positive");
            }
            if (bucketToNode.isPresent() && bucketToNode.get().size() != bucketCount) {
                throw new IllegalArgumentException(format("Mismatched bucket count in bucketToNode (%s) and bucketCount (%s)", bucketToNode.get().size(), bucketCount));
            }
            this.bucketCount = bucketCount;
            this.bucketToNode = requireNonNull(bucketToNode, "bucketToNode is null")
                    .map(mapping -> unmodifiableMap(new HashMap<>(mapping)));
        }

        public int getBucketCount()
        {
            return bucketCount;
        }

        public boolean hasFixedMapping()
        {
            return bucketToNode.isPresent();
        }

        public Map<Integer, Node> getFixedMapping()
        {
            return bucketToNode.orElseThrow(() -> new IllegalArgumentException("No fixed bucket to node mapping"));
        }
    }
}
