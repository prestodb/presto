package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTableColumns;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTables;
import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;

public class MetadataManager
        implements Metadata
{
    private final Map<DataSourceType, Metadata> metadataSourceMap;

    @Inject
    public MetadataManager(NativeMetadata nativeMetadata, InternalMetadata internalMetadata, ImportMetadata importMetadata)
    {
        metadataSourceMap = ImmutableMap.<DataSourceType, Metadata>builder()
                .put(DataSourceType.NATIVE, checkNotNull(nativeMetadata, "nativeMetadata is null"))
                .put(DataSourceType.INTERNAL, checkNotNull(internalMetadata, "internalMetadata is null"))
                .put(DataSourceType.IMPORT, checkNotNull(importMetadata, "importMetadata is null"))
                .build();
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return metadataSourceMap.get(DataSourceType.NATIVE).getFunction(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return metadataSourceMap.get(DataSourceType.NATIVE).getFunction(handle);
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        DataSourceType dataSourceType = lookupDataSource(catalogName, schemaName, tableName);
        return lookup(dataSourceType).getTable(catalogName, schemaName, tableName);
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName)
    {
        checkCatalogName(catalogName);
        DataSourceType dataSourceType = lookupDataSource(catalogName);
        List<QualifiedTableName> catalogTables = lookup(dataSourceType).listTables(catalogName);
        List<QualifiedTableName> informationSchemaTables = listInformationSchemaTables(catalogName);
        return ImmutableList.copyOf(concat(catalogTables, informationSchemaTables));
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        DataSourceType dataSourceType = lookupDataSource(catalogName);
        return lookup(dataSourceType).listTables(catalogName);
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName)
    {
        checkCatalogName(catalogName);
        DataSourceType dataSourceType = lookupDataSource(catalogName);
        List<TableColumn> catalogColumns = lookup(dataSourceType).listTableColumns(catalogName);
        List<TableColumn> informationSchemaColumns = listInformationSchemaTableColumns(catalogName);
        return ImmutableList.copyOf(concat(catalogColumns, informationSchemaColumns));
    }

    @Override
    public void createTable(TableMetadata table)
    {
        DataSourceType dataSourceType = lookupDataSource(table.getCatalogName(), table.getSchemaName());
        checkArgument(dataSourceType == DataSourceType.NATIVE, "table creation is only supported for native tables");
        metadataSourceMap.get(dataSourceType).createTable(table);
    }

    private static DataSourceType lookupDataSource(String catalogName)
    {
        // use a schema name that won't match any real or special schemas
        return lookupDataSource(catalogName, "$dummy_schema$");
    }

    private static DataSourceType lookupDataSource(String catalogName, String schemaName)
    {
        // use a table name that won't match any real or special tables
        return lookupDataSource(catalogName, schemaName, "$dummy_table$");
    }

    private static DataSourceType lookupDataSource(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);

        if (tableName.equals(DualTable.NAME)) {
            return DataSourceType.INTERNAL;
        }

        if (schemaName.equals(INFORMATION_SCHEMA)) {
            return DataSourceType.INTERNAL;
        }

        // TODO: use some sort of catalog registry for this
        if (catalogName.equals("default")) {
            return DataSourceType.NATIVE;
        }
        if (catalogName.equals("hive")) {
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
