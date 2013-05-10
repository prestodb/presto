package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class SlowDatanodeSystemTable
        implements SystemTable
{
    public static final SchemaTableName SLOW_DATANODES_TABLE_NAME = new SchemaTableName("sys", "slow_datanode");

    public static final TableMetadata NODES_TABLE = new TableMetadata(SLOW_DATANODES_TABLE_NAME, ImmutableList.of(
            new ColumnMetadata("node_id", STRING, 0, false),
            new ColumnMetadata("connector_id", STRING, 1, false),
            new ColumnMetadata("host_name", STRING, 2, false)));


    private final ConcurrentMap<String, SlowDatanodeSwitcher> slowDatanodeSwitchers = new ConcurrentHashMap<>();
    private final String nodeId;

    public SlowDatanodeSystemTable(String nodeId)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
    }

    public void addConnectorSlowDatanodeSwitcher(String connectorId, SlowDatanodeSwitcher slowDatanodeSwitcher)
    {
        slowDatanodeSwitchers.put(connectorId, slowDatanodeSwitcher);
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
        return ImmutableList.copyOf(transform(NODES_TABLE.getColumns(), new Function<ColumnMetadata, ColumnType>()
        {
            @Override
            public ColumnType apply(ColumnMetadata columnMetadata)
            {
                return columnMetadata.getType();
            }
        }));
    }

    @Override
    public RecordCursor cursor()
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(NODES_TABLE);
        for (Entry<String, SlowDatanodeSwitcher> entry : slowDatanodeSwitchers.entrySet()) {
            String connectorId = entry.getKey();
            SlowDatanodeSwitcher slowDatanodeSwitcher = entry.getValue();
            for (DatanodeInfo node : slowDatanodeSwitcher.getUnfavoredDatanodeInfo()) {
                table.addRow(ImmutableList.of(
                        nodeId,
                        connectorId,
                        node.getHostName()
                ));
            }
        }
        return table.build().cursor();
    }

}
