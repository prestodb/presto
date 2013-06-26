package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.spi.ColumnType.BOOLEAN;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class NodesSystemTable
        implements SystemTable
{
    public static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("sys", "node");

    public static final TableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", STRING)
            .column("http_uri", STRING)
            .column("node_version", STRING)
            .column("is_active", BOOLEAN)
            .build();

    private final NodeManager nodeManager;

    @Inject
    public NodesSystemTable(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public boolean isDistributed()
    {
        return false;
    }

    @Override
    public TableMetadata getTableMetadata()
    {
        return NODES_TABLE;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(NODES_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(NODES_TABLE);
        AllNodes allNodes = nodeManager.getAllNodes();
        for (Node node : allNodes.getActiveNodes()) {
            table.addRow(node.getNodeIdentifier(), node.getHttpUri().toString(), node.getNodeVersion().toString(), Boolean.TRUE);
        }
        for (Node node : allNodes.getInactiveNodes()) {
            table.addRow(node.getNodeIdentifier(), node.getHttpUri().toString(), node.getNodeVersion().toString(), Boolean.FALSE);
        }
        return table.build().cursor();
    }
}
