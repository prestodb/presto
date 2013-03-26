package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Singleton;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTableColumns;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTables;
import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.metadata.SystemTables.SYSTEM_SCHEMA;
import static com.facebook.presto.metadata.SystemTables.listSystemTableColumns;
import static com.facebook.presto.metadata.SystemTables.listSystemTables;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;

@Singleton
public class MetadataManager
        implements Metadata
{
    private final Map<DataSourceType, Metadata> metadataSourceMap;
    private final ImportMetadata importMetadata;

    @Inject
    public MetadataManager(NativeMetadata nativeMetadata,
            InternalMetadata internalMetadata,
            ImportMetadata importMetadata)
    {
        this.importMetadata = importMetadata;

        metadataSourceMap = ImmutableMap.<DataSourceType, Metadata>builder()
                .put(DataSourceType.NATIVE, checkNotNull(nativeMetadata, "nativeMetadata is null"))
                .put(DataSourceType.INTERNAL, checkNotNull(internalMetadata, "internalMetadata is null"))
                .put(DataSourceType.IMPORT, checkNotNull(importMetadata, "importMetadata is null"))
                .build();
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return lookup(DataSourceType.NATIVE).getFunction(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return lookup(DataSourceType.NATIVE).getFunction(handle);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return lookup(DataSourceType.NATIVE).listFunctions();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkCatalogName(catalogName);
        DataSourceType dataSourceType = lookupDataSource(catalogName, null, null);
        return lookup(dataSourceType).listSchemaNames(catalogName);
    }

    @Override
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);
        DataSourceType dataSourceType = lookupDataSource(table);
        return lookup(dataSourceType).getTable(table);
    }

    @Override
    public QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        return lookup(tableHandle.getDataSourceType()).getTableName(tableHandle);
    }

    @Override
    public TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        return lookup(columnHandle.getDataSourceType()).getTableColumn(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, Optional<String> schemaName)
    {
        checkSchemaName(catalogName, schemaName);

        if (schemaName.isPresent()) {
            DataSourceType dataSourceType = lookupDataSource(catalogName, schemaName.get(), null);
            return lookup(dataSourceType).listTables(catalogName, schemaName);
        }
        else {
            // blank catalog must also return the system and information schema tables.
            DataSourceType dataSourceType = lookupDataSource(catalogName, null, null);
            List<QualifiedTableName> catalogTables = lookup(dataSourceType).listTables(catalogName, Optional.<String>absent());
            List<QualifiedTableName> informationSchemaTables = listInformationSchemaTables(catalogName);
            List<QualifiedTableName> systemTables = listSystemTables(catalogName);
            return ImmutableList.copyOf(concat(catalogTables, informationSchemaTables, systemTables));
        }
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        DataSourceType dataSourceType = lookupDataSource(catalogName, schemaName.orNull(), tableName.orNull());
        return getTableColumns(catalogName, lookup(dataSourceType).listTableColumns(catalogName, schemaName, tableName));
    }

    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        checkTable(table);
        DataSourceType dataSourceType = lookupDataSource(table);
        return lookup(dataSourceType).listTablePartitionKeys(table);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        DataSourceType dataSourceType = lookupDataSource(catalogName, schemaName.orNull(), tableName.orNull());
        return lookup(dataSourceType).listTablePartitionValues(catalogName, schemaName, tableName);
    }

    @Override
    public void createTable(TableMetadata table)
    {
        DataSourceType dataSourceType = lookupDataSource(table.getTable());
        checkArgument(dataSourceType == DataSourceType.NATIVE, "table creation is only supported for native tables");
        metadataSourceMap.get(dataSourceType).createTable(table);
    }

    private List<TableColumn> getTableColumns(String catalogName, List<TableColumn> catalogColumns)
    {
        List<TableColumn> informationSchemaColumns = listInformationSchemaTableColumns(catalogName);
        List<TableColumn> systemColumns = listSystemTableColumns(catalogName);
        return ImmutableList.copyOf(concat(catalogColumns, informationSchemaColumns, systemColumns));
    }

    public DataSourceType lookupDataSource(QualifiedTableName table)
    {
        checkTable(table);
        return lookupDataSource(table.getCatalogName(), table.getSchemaName(), table.getTableName());
    }

    private DataSourceType lookupDataSource(@Nonnull String catalogName, @Nullable String schemaName, @Nullable String tableName)
    {
        if (DualTable.NAME.equals(tableName)) {
            return DataSourceType.INTERNAL;
        }

        if (INFORMATION_SCHEMA.equals(schemaName) || SYSTEM_SCHEMA.equals(schemaName)) {
            return DataSourceType.INTERNAL;
        }

        // TODO: use some sort of catalog registry for this
        if (catalogName.equals("default")) {
            return DataSourceType.NATIVE;
        }

        // TODO: this is a hack until we have the ability to create and manage catalogs from sql
        if (importMetadata.hasCatalog(catalogName)) {
            return DataSourceType.IMPORT;
        }

        // TODO: need a proper way to report that catalog does not exist
        throw new IllegalArgumentException("catalog does not exist: " + catalogName);
    }

    private Metadata lookup(DataSourceType dataSourceType)
    {
        Metadata metadata = metadataSourceMap.get(dataSourceType);
        checkArgument(metadata != null, "tableMetadataSource does not exist: %s", dataSourceType);
        return metadata;
    }
}
