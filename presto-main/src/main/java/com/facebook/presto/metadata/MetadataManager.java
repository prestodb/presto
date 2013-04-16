package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkColumnName;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

@Singleton
public class MetadataManager
        implements Metadata
{
    // Note this myst be a list to assure dual is always checked first
    private final List<InternalSchemaMetadata> internalSchemas;
    private final Map<String, ConnectorMetadata> connectors;
    private final FunctionRegistry functions = new FunctionRegistry();

    @VisibleForTesting
    public MetadataManager()
    {
        this.internalSchemas = ImmutableList.<InternalSchemaMetadata>of(new DualTable());
        this.connectors = ImmutableMap.of();
    }

    @Inject
    public MetadataManager(Set<InternalSchemaMetadata> internalSchemas, Map<String, ConnectorMetadata> connectors)
    {
        this.internalSchemas = ImmutableList.<InternalSchemaMetadata>builder()
                .add(new DualTable())
                .addAll(checkNotNull(internalSchemas, "internalSchemas is null"))
                .build();
        this.connectors = ImmutableMap.copyOf((checkNotNull(connectors, "connectors is null")));
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return functions.get(handle);
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return functions.isAggregationFunction(name);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return functions.list();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkCatalogName(catalogName);
        ConnectorMetadata connectorMetadata = connectors.get(catalogName);
        if (connectorMetadata == null) {
            return ImmutableList.of();
        }
        return connectorMetadata.listSchemaNames();
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedTableName table)
    {
        checkTable(table);

        // internal schemas like information_schema and sys are in every catalog
        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            Optional<TableHandle> tableHandle = internalSchemaMetadata.getTableHandle(table);
            if (tableHandle.isPresent()) {
                return tableHandle;
            }
        }

        ConnectorMetadata connectorMetadata = connectors.get(table.getCatalogName());
        if (connectorMetadata == null) {
            return Optional.absent();
        }
        return Optional.fromNullable(connectorMetadata.getTableHandle(table.asSchemaTableName()));
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            Optional<TableMetadata> tableMetadata = internalSchemaMetadata.getTableMetadata(tableHandle);
            if (tableMetadata.isPresent()) {
                return tableMetadata.get();
            }
        }
        for (Entry<String, ConnectorMetadata> entry : connectors.entrySet()) {
            ConnectorMetadata metadata = entry.getValue();
            if (metadata.canHandle(tableHandle)) {
                SchemaTableMetadata tableMetadata = lookupDataSource(tableHandle).getTableMetadata(tableHandle);
                QualifiedTableName qualifiedTableName = new QualifiedTableName(entry.getKey(), tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
                return new TableMetadata(qualifiedTableName, tableMetadata.getColumns(), tableMetadata.getPartitionKeys());
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            Optional<Map<String, ColumnHandle>> columnHandles = internalSchemaMetadata.getColumnHandles(tableHandle);
            if (columnHandles.isPresent()) {
                return columnHandles.get();
            }
        }
        return lookupDataSource(tableHandle).getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            Optional<ColumnMetadata> column = internalSchemaMetadata.getColumnMetadata(tableHandle, columnHandle);
            if (column.isPresent()) {
                return column.get();
            }
        }

        return lookupDataSource(tableHandle).getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableList.Builder<QualifiedTableName> tables = ImmutableList.builder();
        ConnectorMetadata connectorMetadata = connectors.get(prefix.getCatalogName());
        if (connectorMetadata != null) {
            tables.addAll(transform(connectorMetadata.listTables(prefix.getSchemaName()), convertFromSchemaTableName(prefix.getCatalogName())));
        }

        // internal schemas like information_schema and sys are in every catalog
        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            tables.addAll(internalSchemaMetadata.listTables(prefix));
        }

        return tables.build();
    }

    @Override
    public Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkColumnName(columnName);


        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            Optional<Map<String, ColumnHandle>> columnHandles = internalSchemaMetadata.getColumnHandles(tableHandle);
            if (columnHandles.isPresent()) {
                return Optional.fromNullable(columnHandles.get().get(columnName));
            }
        }

        return Optional.fromNullable(lookupDataSource(tableHandle).getColumnHandle(tableHandle, columnName));
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (InternalSchemaMetadata internalSchemaMetadata : internalSchemas) {
            builder.putAll(internalSchemaMetadata.listTableColumns(prefix));
        }
        ConnectorMetadata connector = connectors.get(prefix.getCatalogName());
        if (connector != null) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : connector.listTableColumns(prefix.asSchemaTablePrefix()).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), entry.getKey().getSchemaName(), entry.getKey().getTableName());
                builder.put(tableName, entry.getValue());
            }
        }
        return builder.build();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        SchemaTablePrefix schemaTablePrefix = new SchemaTablePrefix(prefix.getSchemaName().orNull(), prefix.getTableName().orNull());

        ConnectorMetadata connectorMetadata = connectors.get(prefix.getCatalogName());
        if (connectorMetadata == null) {
            return ImmutableList.of();
        }
        return connectorMetadata.listTablePartitionValues(schemaTablePrefix);
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        ConnectorMetadata connectorMetadata = connectors.get(tableMetadata.getTable().getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", tableMetadata.getTable().getCatalogName());
        return connectorMetadata.createTable(new SchemaTableMetadata(tableMetadata.getTable().asSchemaTableName(), tableMetadata.getColumns(), tableMetadata.getPartitionKeys()));
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupDataSource(tableHandle).dropTable(tableHandle);
    }

    private ConnectorMetadata lookupDataSource(TableHandle tableHandle)
    {
        for (ConnectorMetadata metadata : connectors.values()) {
            if (metadata.canHandle(tableHandle)) {
                return metadata;
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }
}
