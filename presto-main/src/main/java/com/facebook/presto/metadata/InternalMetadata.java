package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTables;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.metadata.SystemTables.SYSTEM_SCHEMA;
import static com.facebook.presto.metadata.SystemTables.listSystemTables;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalMetadata
        extends AbstractMetadata
{
    private final InformationSchemaMetadata informationSchema;
    private final SystemTables systemTables;

    @Inject
    public InternalMetadata(InformationSchemaMetadata informationSchema, SystemTables systemTables)
    {
        this.informationSchema = checkNotNull(informationSchema, "informationSchema is null");
        this.systemTables = checkNotNull(systemTables, "systemTables is null");
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);

        if (tableName.equals(DualTable.NAME)) {
            return DualTable.getMetadata(catalogName, schemaName, tableName);
        }

        switch (schemaName) {
            case INFORMATION_SCHEMA:
                return informationSchema.getTableMetadata(catalogName, schemaName, tableName);
            case SYSTEM_SCHEMA:
                return systemTables.getTableMetadata(catalogName, schemaName, tableName);
        }

        return null;
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        switch (schemaName) {
            case INFORMATION_SCHEMA:
                return listInformationSchemaTables(catalogName);
            case SYSTEM_SCHEMA:
                return listSystemTables(catalogName);
        }
        return ImmutableList.of();
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName, String tableName)
    {
        // hack for SHOW COLUMNS
        return ImmutableList.of();
    }
}
