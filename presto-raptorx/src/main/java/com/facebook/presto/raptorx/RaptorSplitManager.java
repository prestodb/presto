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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNode;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.AsyncSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class RaptorSplitManager
        implements ConnectorSplitManager
{
    private final NodeSupplier nodeSupplier;
    private final TransactionManager transactionManager;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("raptor-split-%s"));

    @Inject
    public RaptorSplitManager(NodeSupplier nodeSupplier, TransactionManager transactionManager)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        Transaction transaction = transactionManager.get(transactionHandle);
        RaptorTableLayoutHandle layout = (RaptorTableLayoutHandle) layoutHandle;
        RaptorTableHandle table = layout.getTable();
        boolean merged = !table.getTransactionId().isPresent();
        TupleDomain<RaptorColumnHandle> constraint = layout.getConstraint().transform(RaptorColumnHandle.class::cast);
        Map<Long, Type> chunkColumnTypes = getColumnTypes(transaction, table);
        Optional<CompressionType> compressionType = getCompressionType(transaction, table);
        List<HostAddress> bucketAddresses = getBucketAddresses(layout.getPartitioning().getBucketNodes());

        return new AsyncSplitSource<>(
                transaction.getBucketChunks(table.getTableId(), merged, constraint),
                bucket -> new RaptorSplit(
                        table.getTableId(),
                        bucket.getBucketNumber(),
                        bucket.getChunkIds(),
                        constraint,
                        bucketAddresses.get(bucket.getBucketNumber()),
                        table.getTransactionId(),
                        chunkColumnTypes,
                        compressionType),
                executor);
    }

    private List<HostAddress> getBucketAddresses(List<Long> bucketNodes)
    {
        ImmutableList.Builder<HostAddress> addresses = ImmutableList.builder();
        Map<Long, RaptorNode> nodes = uniqueIndex(nodeSupplier.getWorkerNodes(), RaptorNode::getNodeId);
        for (long nodeId : bucketNodes) {
            RaptorNode node = nodes.get(nodeId);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket went offline during query");
            }
            addresses.add(node.getNode().getHostAndPort());
        }
        return addresses.build();
    }

    private static Map<Long, Type> getColumnTypes(Transaction transaction, RaptorTableHandle table)
    {
        if (!table.getTransactionId().isPresent()) {
            return ImmutableMap.of();
        }

        if (table.getTransactionId().getAsLong() != transaction.getTransactionId()) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Mismatched transaction ID");
        }

        List<ColumnInfo> columns = transaction.getTableInfo(table.getTableId()).getColumns();
        if (columns.isEmpty()) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Table column types is empty");
        }
        return columns.stream().collect(toImmutableMap(ColumnInfo::getColumnId, ColumnInfo::getType));
    }

    private static Optional<CompressionType> getCompressionType(Transaction transaction, RaptorTableHandle table)
    {
        if (!table.getTransactionId().isPresent()) {
            return Optional.empty();
        }

        if (table.getTransactionId().getAsLong() != transaction.getTransactionId()) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Mismatched transaction ID");
        }

        return Optional.of(transaction.getTableInfo(table.getTableId()).getCompressionType());
    }
}
