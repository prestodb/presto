package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;

import javax.inject.Singleton;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkColumnName;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class MetadataManager
        implements Metadata
{
    // Note this myst be a list to assure dual is always checked first
    private final List<InternalSchemaMetadata> internalSchemas;
    private final SortedSet<ConnectorMetadata> connectors;
    private final FunctionRegistry functions = new FunctionRegistry();

    @VisibleForTesting
    public MetadataManager()
    {
        this.internalSchemas = ImmutableList.<InternalSchemaMetadata>of(new DualTable());
        this.connectors = ImmutableSortedSet.of();
    }

    @Inject
    public MetadataManager(Set<InternalSchemaMetadata> internalSchemas, Set<ConnectorMetadata> connectors)
    {
        this.internalSchemas = ImmutableList.<InternalSchemaMetadata>builder()
                .add(new DualTable())
                .addAll(checkNotNull(internalSchemas, "internalSchemas is null"))
                .build();
        this.connectors = ImmutableSortedSet.copyOf(new PriorityComparator(), checkNotNull(connectors, "metadataProviders is null"));
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
        QualifiedTablePrefix prefix = QualifiedTablePrefix.builder(catalogName).build();
        return lookupDataSource(prefix).listSchemaNames(catalogName);
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

        return Optional.fromNullable(lookupDataSource(table).getTableHandle(table));
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

        return lookupDataSource(tableHandle).getTableMetadata(tableHandle);
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
        tables.addAll(lookupDataSource(prefix).listTables(prefix));

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
        for (ConnectorMetadata connector : connectors) {
            builder.putAll(connector.listTableColumns(prefix));
        }
        return builder.build();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return lookupDataSource(prefix).listTablePartitionValues(prefix);
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        return lookupDataSource(tableMetadata.getTable()).createTable(tableMetadata);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupDataSource(tableHandle).dropTable(tableHandle);
    }

    private ConnectorMetadata lookupDataSource(QualifiedTableName table)
    {
        checkTable(table);
        return lookupDataSource(QualifiedTablePrefix.builder(table.getCatalogName())
                .schemaName(table.getSchemaName())
                .tableName(table.getTableName())
                .build());
    }

    private ConnectorMetadata lookupDataSource(QualifiedTablePrefix prefix)
    {
        for (ConnectorMetadata metadata : connectors) {
            if (metadata.canHandle(prefix)) {
                return metadata;
            }
        }

        // TODO: need a proper way to report that catalog does not exist
        throw new IllegalArgumentException("No metadata provider for: " + prefix);
    }

    private ConnectorMetadata lookupDataSource(TableHandle tableHandle)
    {
        for (ConnectorMetadata metadata : connectors) {
            if (metadata.canHandle(tableHandle)) {
                return metadata;
            }
        }

        // TODO: need a proper way to report that catalog does not exist
        throw new IllegalArgumentException("No metadata provider for: " + tableHandle);
    }

    private static class PriorityComparator implements Comparator<ConnectorMetadata>
    {
        @Override
        public int compare(ConnectorMetadata o1, ConnectorMetadata o2)
        {
            // reverse sort order
            return Ints.compare(o2.priority(), o1.priority());
        }
    }
}
