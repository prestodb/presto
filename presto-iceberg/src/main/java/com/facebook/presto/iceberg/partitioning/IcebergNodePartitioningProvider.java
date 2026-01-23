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
package com.facebook.presto.iceberg.partitioning;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    public static final int DEFAULT_BUCKET_COUNT = 16;

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // Partitioned read is not currently supported.
        return ImmutableList.of();
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Node> sortedNodes)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;

        List<IcebergPartitionFieldHandle> partitionFieldHandles = handle.getPartitionFieldHandles();
        int bucketCount = 1;
        for (IcebergPartitionFieldHandle partitionFieldHandle : partitionFieldHandles) {
            if (partitionFieldHandle.isBucketTransform()) {
                bucketCount *= partitionFieldHandle.getSize().orElseThrow();
            }
        }
        return createBucketNodeMap(Math.min(Math.max(bucketCount, DEFAULT_BUCKET_COUNT), sortedNodes.size()));
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;
        return new IcebergBucketFunction(handle, partitionChannelTypes, bucketCount);
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // Partitioned read is not currently supported.
        return 0;
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        // Partitioned read is not currently supported.
        return (split) -> 0;
    }
}
