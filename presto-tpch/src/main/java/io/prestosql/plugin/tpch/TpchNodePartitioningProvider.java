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
package io.prestosql.plugin.tpch;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class TpchNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchNodePartitioningProvider(NodeManager nodeManager, int splitsPerNode)
    {
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();

        // Split the data using split and skew by the number of nodes available.
        return createBucketNodeMap(toIntExact((long) nodes.size() * splitsPerNode));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((TpchSplit) value).getPartNumber();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        long totalRows = ((TpchPartitioningHandle) partitioningHandle).getTotalRows();
        long rowsPerBucket = totalRows / bucketCount;
        checkArgument(partitionChannelTypes.equals(ImmutableList.of(BIGINT)), "Expected one BIGINT parameter");
        return new TpchBucketFunction(bucketCount, rowsPerBucket);
    }
}
