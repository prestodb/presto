package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.MetadataUtil.toQualifiedTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public abstract class AbstractInformationSchemaMetadata
        implements InternalSchemaMetadata
{
    protected final String schemaName;
    protected final Map<String, List<ColumnMetadata>> metadata;

    public AbstractInformationSchemaMetadata(String informationSchema,
            Map<String, List<ColumnMetadata>> metadata)
    {
        this.schemaName = informationSchema;
        this.metadata = metadata;
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedTableName tableName)
    {
        checkTable(tableName);
        if (!tableName.getSchemaName().equals(schemaName)) {
            return Optional.absent();
        }

        List<ColumnMetadata> metadata = this.metadata.get(tableName.getTableName());
        if (metadata == null) {
            return Optional.absent();
        }
        return Optional.<TableHandle>of(new InternalTableHandle(tableName));
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        if (!(tableHandle instanceof InternalTableHandle)) {
            return Optional.absent();
        }

        QualifiedTableName tableName = ((InternalTableHandle) tableHandle).getTableName();
        if (!tableName.getSchemaName().equals(schemaName)) {
            return Optional.absent();
        }

        List<ColumnMetadata> metadata = this.metadata.get(tableName.getTableName());
        checkArgument(metadata != null, "Unknown table %s", tableName);
        return Optional.of(new TableMetadata(tableName, metadata));
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        if (prefix.getSchemaName().isPresent() && !prefix.getSchemaName().get().equals(schemaName)) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(transform(metadata.keySet(), toQualifiedTableName(prefix.getCatalogName(), schemaName)));
    }

    @Override
    public Optional<ColumnMetadata> getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        Optional<TableMetadata> tableMetadata = getTableMetadata(tableHandle);
        if (!tableMetadata.isPresent()) {
            return Optional.absent();
        }
        List<ColumnMetadata> columns = tableMetadata.get().getColumns();

        checkArgument(columnHandle instanceof InternalColumnHandle, "columnHandle is not an instance of InternalColumnHandle");
        String columnName = ((InternalColumnHandle) columnHandle).getColumnName();
        for (ColumnMetadata column : columns) {
            if (column.getName().equals(columnName)) {
                return Optional.of(column);
            }
        }
        return Optional.absent();
    }

    @Override
    public Optional<Map<String, ColumnHandle>> getColumnHandles(TableHandle tableHandle)
    {
        Optional<TableMetadata> tableMetadata = getTableMetadata(tableHandle);
        if (!tableMetadata.isPresent()) {
            return Optional.absent();
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : tableMetadata.get().getColumns()) {
            columnHandles.put(columnMetadata.getName(), new InternalColumnHandle(columnMetadata.getName()));
        }

        return Optional.<Map<String, ColumnHandle>>of(columnHandles.build());
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        Optional<String> schemaName = prefix.getSchemaName();
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<String, List<ColumnMetadata>> entry : metadata.entrySet()) {
            Optional<String> tableName = prefix.getSchemaName();
            if (!tableName.isPresent() || tableName.get().equals(entry.getKey())) {
                builder.put(new QualifiedTableName(prefix.getCatalogName(), this.schemaName, entry.getKey()), entry.getValue());
            }
        }
        return builder.build();
    }
}
