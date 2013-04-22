package com.facebook.presto.metadata;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DualTable
        implements InternalSchemaMetadata
{
    public static final String NAME = "dual";

    private static final String COLUMN_NAME = "dummy";

    private static final int ORDINAL_POSITION = 0;

    private static final ColumnMetadata COLUMN_METADATA = new ColumnMetadata(COLUMN_NAME, VARIABLE_BINARY, ORDINAL_POSITION);

    private static final InternalTable DATA;

    static {
        DATA = InternalTable.builder(SINGLE_VARBINARY)
                .add(SINGLE_VARBINARY.builder().append("X").build())
                .build();
    }

    public static boolean isDualTable(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        return tableHandle instanceof InternalTableHandle && isDualTable(((InternalTableHandle) tableHandle).getTableName());
    }

    public static boolean isDualTable(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");
        return table.getTableName().equals(NAME);
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedTableName table)
    {
        if (!isDualTable(table)) {
            return Optional.absent();
        }
        return Optional.<TableHandle>of(new InternalTableHandle(table));
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TableHandle tableHandle)
    {
        if (!isDualTable(tableHandle)) {
            return Optional.absent();
        }
        return Optional.of(new TableMetadata(((InternalTableHandle) tableHandle).getTableName(), ImmutableList.of(COLUMN_METADATA)));
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        // dual can not be a listed table because it is in all possible schemas
        if (!prefix.getTableName().isPresent() || !prefix.getTableName().get().equals(NAME)) {
            return ImmutableList.of();
        }

        return ImmutableList.of(new QualifiedTableName(prefix.getCatalogName(), prefix.getSchemaName().get(), prefix.getTableName().get()));
    }

    @Override
    public Optional<ColumnMetadata> getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        if (!isDualTable(tableHandle)) {
            return Optional.absent();
        }

        checkArgument(columnHandle instanceof InternalColumnHandle, "columnHandle is not an instance of InternalColumnHandle");
        InternalColumnHandle internalColumnHandle = (InternalColumnHandle) columnHandle;
        checkArgument(internalColumnHandle.getColumnIndex() == ORDINAL_POSITION, "column handle is not for DUAL");

        return Optional.of(COLUMN_METADATA);
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        if (!prefix.getTableName().isPresent() || !prefix.getTableName().get().equals(NAME)) {
            return ImmutableMap.of();
        }

        QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), prefix.getSchemaName().get(), prefix.getTableName().get());
        return ImmutableMap.<QualifiedTableName, List<ColumnMetadata>>of(tableName, ImmutableList.of(COLUMN_METADATA));
    }

    public InternalTable getInternalTable(QualifiedTableName table)
    {
        checkArgument(isDualTable(table), "table is not %s", NAME);
        return DATA;
    }

    public List<ColumnMetadata> listTableColumns(TableHandle tableHandle)
    {
        if (tableHandle instanceof InternalTableHandle) {
            InternalTableHandle internalTableHandle = (InternalTableHandle) tableHandle;
            if (isDualTable(internalTableHandle.getTableName())) {
                return ImmutableList.of(COLUMN_METADATA);
            }
        }
        return null;
    }

    public Optional<Map<String, ColumnHandle>> getColumnHandles(TableHandle tableHandle)
    {
        if (!isDualTable(tableHandle)) {
            return Optional.absent();
        }
        return Optional.<Map<String, ColumnHandle>>of(ImmutableMap.<String, ColumnHandle>of(COLUMN_NAME, new InternalColumnHandle(ORDINAL_POSITION)));
    }
}
