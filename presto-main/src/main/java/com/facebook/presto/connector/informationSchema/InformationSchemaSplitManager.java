package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InformationSchemaSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public InformationSchemaSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getConnectorId()
    {
        // information schema is not a connector
        return null;
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof InformationSchemaTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkNotNull(table, "table is null");
        checkNotNull(bindings, "bindings is null");

        checkArgument(table instanceof InformationSchemaTableHandle, "TableHandle must be an InformationSchemaTableHandle");
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) table;

        return ImmutableList.<Partition>of(new InformationSchemaPartition(informationSchemaTableHandle, bindings));
    }

    @Override
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Partition partition = Iterables.getOnlyElement(partitions);
        checkArgument(partition instanceof InformationSchemaPartition, "Partition must be an informationSchema partition");
        InformationSchemaPartition informationSchemaPartition = (InformationSchemaPartition) partition;

        Optional<Node> currentNode = nodeManager.getCurrentNode();
        Preconditions.checkState(currentNode.isPresent(), "current node is not in the active set");
        ImmutableList<HostAddress> localAddress = ImmutableList.of(currentNode.get().getHostAndPort());

        ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();
        for (Entry<ColumnHandle, Object> entry : informationSchemaPartition.getFilters().entrySet()) {
            InformationSchemaColumnHandle informationSchemaColumnHandle = (InformationSchemaColumnHandle) entry.getKey();
            filters.put(informationSchemaColumnHandle.getColumnName(), entry.getValue());
        }

        Split split = new InformationSchemaSplit(informationSchemaPartition.table, filters.build(), localAddress);

        return ImmutableList.of(split);
    }

    public static class InformationSchemaPartition
            implements Partition
    {
        private final InformationSchemaTableHandle table;
        private final Map<ColumnHandle, Object> filters;

        public InformationSchemaPartition(InformationSchemaTableHandle table, Map<ColumnHandle, Object> filters)
        {
            this.table = table;
            this.filters = ImmutableMap.copyOf(checkNotNull(filters, "filters is null"));
        }

        public InformationSchemaTableHandle getTable()
        {
            return table;
        }

        @Override
        public String getPartitionId()
        {
            return table.getSchemaTableName().toString();
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return ImmutableMap.of();
        }

        public Map<ColumnHandle, Object> getFilters()
        {
            return filters;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("table", table)
                    .add("filters", filters)
                    .toString();
        }
    }
}
