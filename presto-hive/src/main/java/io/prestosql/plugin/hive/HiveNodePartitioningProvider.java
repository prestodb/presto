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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;

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
        List<HiveType> hiveTypes = handle.getHiveTypes();
        return new HiveBucketFunction(bucketCount, hiveTypes);
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        return createBucketNodeMap(handle.getBucketCount());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((HiveSplit) value).getBucketNumber()
                .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        int bucketCount = handle.getBucketCount();
        return IntStream.range(0, bucketCount).mapToObj(HivePartitionHandle::new).collect(toImmutableList());
    }
}
