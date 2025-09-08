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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.iceberg.Schema;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static com.facebook.presto.iceberg.IcebergUtil.schemaFromHandles;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.google.common.collect.ImmutableList.toImmutableList;

// TODO #20578: WIP - Class implementation under development. Check Trino's IcebergNodePartitioningProvider for reference.

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Node> sortedNodes)
    {
        return Optional.empty();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        if (partitioningHandle instanceof IcebergUpdateHandle) {
            return new IcebergUpdateBucketFunction(bucketCount);
        }

        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;
        Schema schema = schemaFromHandles(handle.getPartitioningColumns());
        return new IcebergBucketFunction(
                // TODO #20578: Check if the value of the following line and compare it with the value contained in the parameter "partitionChannelTypes".
                handle.getPartitioningColumns().stream()
                        .map(IcebergColumnHandle::getType)
                        .collect(toImmutableList()),
                parsePartitionFields(schema, handle.getPartitioning()),
                handle.getPartitioningColumns(),
                bucketCount);
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return 0;
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return split -> {
            // Not currently used, likely because IcebergMetadata.getTableProperties currently does not expose partitioning.
            throw new UnsupportedOperationException();
        };
    }
}
