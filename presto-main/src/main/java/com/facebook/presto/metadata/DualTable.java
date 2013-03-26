package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;

import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;

public class DualTable
{
    public static final String NAME = "dual";

    private static final List<ColumnMetadata> METADATA = columnsBuilder().column("dummy", VARIABLE_BINARY).build();

    public static TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);
        checkArgument(table.getTableName().equals(NAME), "table is not %s", NAME);

        InternalTableHandle handle = InternalTableHandle.forQualifiedTableName(table);
        return new TableMetadata(table, METADATA, handle);
    }

    public static InternalTable getInternalTable(QualifiedTableName table)
    {
        checkTable(table);
        checkArgument(table.getTableName().equals(NAME), "table is not %s", NAME);

        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY);
        InternalTable.Builder tableBuilder = InternalTable.builder(tupleInfo);
        tableBuilder.add(tupleInfo.builder().append("X").build());
        return tableBuilder.build();
    }
}
