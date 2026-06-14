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
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;

public class IcebergNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    public static final int DEFAULT_BUCKET_COUNT = 1024;

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Node> sortedNodes)
    {
        IcebergPartitioningHandle handle = (IcebergPartitioningHandle) partitioningHandle;

        List<IcebergPartitionFieldHandle> partitionFieldHandles = handle.getPartitionFieldHandles();

        // Heuristic bucket count estimation based on partition transforms and nodes size.
        boolean allBucketTransform = true;
        long bucketMultipleCount = 1L;
        for (IcebergPartitionFieldHandle partitionFieldHandle : partitionFieldHandles) {
            if (partitionFieldHandle.isBucketTransform()) {
                bucketMultipleCount *= partitionFieldHandle.getSize().orElseThrow();
            }
            else {
                allBucketTransform = false;
            }
        }

        int bucketCount = Ints.saturatedCast(bucketMultipleCount);
        int targetBucketCount = allBucketTransform ? bucketCount : Math.max(bucketCount, sortedNodes.size() * 2);

        return createBucketNodeMap(Math.min(targetBucketCount, DEFAULT_BUCKET_COUNT));
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
        // This return value will not actually be used, so return an invalid value to catch unintended usage.
        return -1;
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return (split) -> {
            throw new UnsupportedOperationException("This connector does not support partitioned read");
        };
    }
}
