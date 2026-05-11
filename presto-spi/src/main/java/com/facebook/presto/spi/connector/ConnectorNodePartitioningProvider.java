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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Collections.singletonList;

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
    default List<ConnectorPartitionHandle> listPartitionHandles(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return singletonList(NOT_PARTITIONED);
    }

    /**
     * Returns a list of partition handles for partition-aware grouped execution.
     * Each handle represents a (bucket, partitionValues) combination.
     * The connector creates compound handles and matching per-(bucket, partition) split queues.
     *
     * <p>The default falls back to the standard bucket-only handles when partitionValues is empty
     * (i.e., partition-aware scheduling is not active), and throws for non-empty partitionValues
     * because the connector must provide its own implementation to create per-(bucket, partition) handles.
     * Connectors supporting partition-aware grouped execution must override this method.
     *
     * @param partitionValues distinct partition value combinations for partition-aware scheduling;
     *                        empty when partition-aware scheduling is not enabled
     */
    default List<ConnectorPartitionHandle> listPartitionHandles(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Map<String, String>> partitionValues)
    {
        if (partitionValues.isEmpty()) {
            return listPartitionHandles(transactionHandle, session, partitioningHandle);
        }
        throw new UnsupportedOperationException("Connector does not support partition-aware grouped execution");
    }

    ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Node> sortedNodes);

    ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle);

    BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount);

    int getBucketCount(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle);
}
