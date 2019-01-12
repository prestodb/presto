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
package io.prestosql.spi.connector;

import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.function.ToIntFunction;

import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
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
}
