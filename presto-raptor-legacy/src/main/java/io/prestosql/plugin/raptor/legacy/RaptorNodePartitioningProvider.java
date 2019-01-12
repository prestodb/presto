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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Node;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class RaptorNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeSupplier nodeSupplier;

    @Inject
    public RaptorNodePartitioningProvider(NodeSupplier nodeSupplier)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        RaptorPartitioningHandle handle = (RaptorPartitioningHandle) partitioning;

        Map<String, Node> nodesById = uniqueIndex(nodeSupplier.getWorkerNodes(), Node::getNodeIdentifier);

        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();
        for (String nodeIdentifier : handle.getBucketToNode()) {
            Node node = nodesById.get(nodeIdentifier);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeIdentifier);
            }
            bucketToNode.add(node);
        }
        return createBucketNodeMap(bucketToNode.build());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        return value -> ((RaptorSplit) value).getBucketNumber().getAsInt();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning, List<Type> partitionChannelTypes, int bucketCount)
    {
        return new RaptorBucketFunction(bucketCount, partitionChannelTypes);
    }
}
