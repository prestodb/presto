package com.facebook.presto.connector.dual;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DualMetadata
        implements ConnectorMetadata
{
    @VisibleForTesting
    public static final MetadataManager DUAL_METADATA_MANAGER;

    static {
        DUAL_METADATA_MANAGER = new MetadataManager();
        DUAL_METADATA_MANAGER.addInternalSchemaMetadata(new DualMetadata());
    }

    public static final String NAME = "dual";

    public static final String COLUMN_NAME = "dummy";

    public static final ColumnMetadata COLUMN_METADATA = new ColumnMetadata(COLUMN_NAME, STRING, 0, false);

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof DualTableHandle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of();
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName table)
    {
        checkNotNull(table, "table is null");
        if (!table.getTableName().equals(NAME)) {
            return null;
        }
        return new DualTableHandle(table.getSchemaName());
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof DualTableHandle, "tableHandle is not a dual table handle");

        SchemaTableName tableName = new SchemaTableName(((DualTableHandle) tableHandle).getSchemaName(), NAME);
        return new TableMetadata(tableName, ImmutableList.of(COLUMN_METADATA));
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        // don't list dual
        return ImmutableList.of();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof DualTableHandle, "tableHandle is not a dual table handle");
        checkNotNull(columnName, "columnName is null");
        if (!columnName.equals(COLUMN_NAME)) {
            return null;
        }
        return new DualColumnHandle(COLUMN_NAME);
    }

    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof DualTableHandle, "tableHandle is not a dual table handle");
        return ImmutableMap.<String, ColumnHandle>of(COLUMN_NAME, new DualColumnHandle(COLUMN_NAME));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof DualTableHandle, "tableHandle is not a dual table handle");

        checkArgument(columnHandle instanceof DualColumnHandle, "columnHandle is not an instance of DualColumnHandle");
        DualColumnHandle dualColumnHandle = (DualColumnHandle) columnHandle;
        checkArgument(dualColumnHandle.getColumnName().equals(COLUMN_NAME), "column handle is not for DUAL");

        return COLUMN_METADATA;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        // dual can not be a listed at catalog level because dual is in all possible schemas
        if (prefix.getSchemaName() == null) {
            return ImmutableMap.of();
        }

        if (prefix.getTableName() != null && !prefix.getTableName().equals(NAME)) {
            return ImmutableMap.of();
        }

        SchemaTableName tableName = new SchemaTableName(prefix.getSchemaName(), NAME);
        return ImmutableMap.<SchemaTableName, List<ColumnMetadata>>of(tableName, ImmutableList.of(COLUMN_METADATA));
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    public List<ColumnMetadata> listTableColumns(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof DualTableHandle, "tableHandle is not a dual table handle");

        return ImmutableList.of(COLUMN_METADATA);
    }

}
