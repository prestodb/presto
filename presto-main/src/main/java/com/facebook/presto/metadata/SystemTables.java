package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class SystemTables
        extends AbstractInformationSchemaMetadata
{
    public static final String SYSTEM_SCHEMA = "sys";

    public static final String TABLE_NODES = "nodes";

    private static final Map<String, List<ColumnMetadata>> METADATA = ImmutableMap.<String, List<ColumnMetadata>>builder()
            .put(TABLE_NODES, columnsBuilder()
                    .column("node_identifier", VARIABLE_BINARY)
                    .column("http_uri", VARIABLE_BINARY)
                    .column("is_active", VARIABLE_BINARY)
                    .build())
            .build();

    private final NodeManager nodeManager;

    @Inject
    public SystemTables(NodeManager nodeManager)
    {
        super(SYSTEM_SCHEMA, METADATA);
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    public InternalTable getInternalTable(QualifiedTableName table)
    {
        checkTable(table);
        checkArgument(table.getSchemaName().equals(SYSTEM_SCHEMA), "schema is not %s", SYSTEM_SCHEMA);

        switch (table.getTableName()) {
            case TABLE_NODES:
                return buildNodes();
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildNodes()
    {
        TupleInfo tupleInfo = systemTupleInfo(TABLE_NODES);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (Node node : nodeManager.getActiveNodes()) {
            table.add(tupleInfo.builder()
                    .append(node.getNodeIdentifier())
                    .append(node.getHttpUri().toString())
                    .append("YES")
                    .build());
        }
        return table.build();
    }

    private static TupleInfo systemTupleInfo(String tableName)
    {
        checkArgument(METADATA.containsKey(tableName), "table does not exist: %s", tableName);
        return new TupleInfo(transform(METADATA.get(tableName), MetadataUtil.getType()));
    }
}
