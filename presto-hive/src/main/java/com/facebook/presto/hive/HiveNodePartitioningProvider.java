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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveBucketFunction.createHiveCompatibleBucketFunction;
import static com.facebook.presto.hive.HiveBucketFunction.createPrestoNativeBucketFunction;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.hive.HiveSessionProperties.isLegacyTimestampBucketing;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class HiveNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        BucketFunctionType bucketFunctionType = handle.getBucketFunctionType();
        switch (bucketFunctionType) {
            case HIVE_COMPATIBLE:
                return createHiveCompatibleBucketFunction(bucketCount, handle.getHiveTypes().get(), isLegacyTimestampBucketing(session));
            case PRESTO_NATIVE:
                return createPrestoNativeBucketFunction(bucketCount, handle.getTypes().get(), isLegacyTimestampBucketing(session));
            default:
                throw new IllegalArgumentException("Unsupported bucket function type " + bucketFunctionType);
        }
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Node> sortedNodes)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        NodeSelectionStrategy nodeSelectionStrategy = getNodeSelectionStrategy(session);
        int bucketCount = handle.getBucketCount();
        switch (nodeSelectionStrategy) {
            case HARD_AFFINITY:
            case SOFT_AFFINITY:
                return Optional.of(createBucketNodeMap(Stream.generate(() -> sortedNodes).flatMap(List::stream).limit(bucketCount).collect(toImmutableList()), nodeSelectionStrategy));
            case NO_PREFERENCE:
                return Optional.of(createBucketNodeMap(bucketCount));
            default:
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", nodeSelectionStrategy));
        }
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((HiveSplit) value).getReadBucketNumber()
                .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        return handle.getBucketCount();
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        int bucketCount = handle.getBucketCount();
        return IntStream.range(0, bucketCount).mapToObj(HivePartitionHandle::new).collect(toImmutableList());
    }
}
