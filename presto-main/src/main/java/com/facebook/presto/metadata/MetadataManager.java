package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MetadataManager
    implements Metadata
{
    private final Metadata localMetadata;
    private final Map<DataSourceType, MetadataReader> tableMetadataSourceMap;

    @Inject
    public MetadataManager(DatabaseMetadata localMetadata, ImportMetadataReader importTableMetadataSource)
    {
        this.localMetadata = checkNotNull(localMetadata, "localMetadata is null");

        tableMetadataSourceMap = ImmutableMap.<DataSourceType, MetadataReader>builder()
                .put(DataSourceType.NATIVE, localMetadata)
                .put(DataSourceType.IMPORT, importTableMetadataSource)
                .build();
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        checkNotNull(name, "name is null");
        checkNotNull(parameterTypes, "parameterTypes is null");
        checkArgument(!parameterTypes.isEmpty(), "must provide at least one paramaterType");
        return localMetadata.getFunction(name, parameterTypes);
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(schemaName, "schemaName is null");
        checkNotNull(tableName, "tableName is null");

        DataSourceType dataSourceType = convertCatalogToDataSourceType(catalogName);
        return lookup(dataSourceType).getTable(catalogName, schemaName, tableName);
    }

    @Override
    public TableMetadata getTable(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        return lookup(tableHandle.getDataSourceType()).getTable(tableHandle);
    }

    @Override
    public ColumnMetadata getColumn(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        return lookup(columnHandle.getDataSourceType()).getColumn(columnHandle);
    }

    @Override
    public void createTable(TableMetadata table)
    {
        checkNotNull(table, "table is null");
        localMetadata.createTable(table);
    }

    // Temporary placeholder to determine data source from catalog
    private DataSourceType convertCatalogToDataSourceType(String catalogName)
    {
        if (catalogName.equalsIgnoreCase("default")) {
            return DataSourceType.NATIVE;
        }
        else {
            return DataSourceType.IMPORT;
        }
    }

    public MetadataReader lookup(DataSourceType dataSourceType)
    {
        checkNotNull(dataSourceType, "dataSourceHandle is null");

        MetadataReader metadataReader = tableMetadataSourceMap.get(dataSourceType);
        checkArgument(metadataReader != null, "tableMetadataSource does not exist: %s", dataSourceType);
        return metadataReader;
    }
}
