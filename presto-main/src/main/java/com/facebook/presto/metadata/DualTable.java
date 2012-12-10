package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;

public class DualTable
{
    public static final String NAME = "dual";

    private static final List<ColumnMetadata> METADATA = columnsBuilder().column("dummy", VARIABLE_BINARY).build();

    public static TableMetadata getMetadata(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        checkArgument(tableName.equals(NAME), "table is not %s", NAME);

        InternalTableHandle handle = new InternalTableHandle(catalogName, schemaName, NAME);
        return new TableMetadata(catalogName, schemaName, NAME, METADATA, handle);
    }

    public static InternalTable getInternalTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        checkArgument(tableName.equals(NAME), "table is not %s", NAME);

        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        table.add(tupleInfo.builder().append("X").build());
        return table.build();
    }
}
