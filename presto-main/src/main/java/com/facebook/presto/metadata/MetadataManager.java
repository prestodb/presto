package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.annotations.VisibleForTesting;
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

import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTableColumns;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTables;
import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.SystemTables.listSystemTableColumns;
import static com.facebook.presto.metadata.SystemTables.listSystemTables;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;

@Singleton
public class MetadataManager
        implements Metadata
{
    private final Map<String, InternalSchemaMetadata> internalSchemas;
    private final SortedSet<ConnectorMetadata> connectors;
    private final FunctionRegistry functions = new FunctionRegistry();

    @VisibleForTesting
    public MetadataManager()
    {
        this.internalSchemas = ImmutableMap.of();
        this.connectors = ImmutableSortedSet.of();
    }

    @Inject
    public MetadataManager(Map<String, InternalSchemaMetadata> internalSchemas, Set<ConnectorMetadata> connectors)
    {
        this.internalSchemas = ImmutableMap.copyOf(checkNotNull(internalSchemas, "internalSchemas is null"));
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
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);

        // "DUAL" is a table is in every schema
        if (table.getTableName().equals(DualTable.NAME)) {
            return DualTable.getTable(table);
        }

        // internal schemas like information_schema and sys are in every catalog
        InternalSchemaMetadata internalSchemaMetadata = internalSchemas.get(table.getSchemaName());
        if (internalSchemaMetadata != null) {
            return internalSchemaMetadata.getTable(table);
        }

        return lookupDataSource(table).getTable(table);
    }

    @Override
    public QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        return lookupDataSource(tableHandle).getTableName(tableHandle);
    }

    @Override
    public TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        return lookupDataSource(tableHandle).getTableColumn(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        if (!prefix.hasSchemaName()) {
            // blank catalog must also return the system and information schema tables.
            List<QualifiedTableName> catalogTables = lookupDataSource(prefix).listTables(prefix);
            List<QualifiedTableName> informationSchemaTables = listInformationSchemaTables(prefix.getCatalogName());
            List<QualifiedTableName> systemTables = listSystemTables(prefix.getCatalogName());
            return ImmutableList.copyOf(concat(catalogTables, informationSchemaTables, systemTables));
        }

        // internal schemas like information_schema and sys are in every catalog
        InternalSchemaMetadata internalSchemaMetadata = internalSchemas.get(prefix.getSchemaName().get());
        if (internalSchemaMetadata != null) {
            return internalSchemaMetadata.listTables(prefix.getCatalogName());
        }

        return lookupDataSource(prefix).listTables(prefix);
    }

    @Override
    public List<TableColumn> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return getTableColumns(prefix.getCatalogName(), lookupDataSource(prefix).listTableColumns(prefix));
    }

    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        checkTable(table);
        return lookupDataSource(table).listTablePartitionKeys(table);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        return lookupDataSource(prefix).listTablePartitionValues(prefix);
    }

    @Override
    public void createTable(TableMetadata table)
    {
        checkTable(table.getTable());
        lookupDataSource(table.getTable()).createTable(table);
    }

    @Override
    public void dropTable(TableMetadata table)
    {
        checkTable(table.getTable());
        lookupDataSource(table.getTable()).dropTable(table);
    }

    private List<TableColumn> getTableColumns(String catalogName, List<TableColumn> catalogColumns)
    {
        List<TableColumn> informationSchemaColumns = listInformationSchemaTableColumns(catalogName);
        List<TableColumn> systemColumns = listSystemTableColumns(catalogName);
        return ImmutableList.copyOf(concat(catalogColumns, informationSchemaColumns, systemColumns));
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
