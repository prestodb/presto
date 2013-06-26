package com.facebook.presto.connector.jmx;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JmxSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;

    @Inject
    public JmxSplitManager(JmxConnectorId jmxConnectorId, NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(jmxConnectorId, "jmxConnectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof JmxTableHandle && ((JmxTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(table, "table is null");
        checkNotNull(bindings, "bindings is null");

        checkArgument(table instanceof JmxTableHandle, "TableHandle must be an JmxTableHandle");
        JmxTableHandle jmxTableHandle = (JmxTableHandle) table;

        return ImmutableList.<Partition>of(new JmxPartition(jmxTableHandle));
    }

    @Override
    public Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof JmxPartition, "Partition must be an jmx partition");
        JmxPartition jmxPartition = (JmxPartition) partition;

        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        for (Node node : nodeManager.getAllNodes().getActiveNodes()) {
            splits.add(new JmxSplit(jmxPartition.tableHandle, ImmutableList.of(node.getHostAndPort())));
        }
        return splits.build();
    }

    public static class JmxPartition
            implements Partition
    {
        private final JmxTableHandle tableHandle;

        public JmxPartition(JmxTableHandle tableHandle)
        {
            this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        }

        public JmxTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @Override
        public String getPartitionId()
        {
            return "jmx";
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return ImmutableMap.of();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("tableHandle", tableHandle)
                    .toString();
        }
    }
}
