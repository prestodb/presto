package com.facebook.presto.metadata;

import javax.inject.Inject;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalMetadata
        extends AbstractMetadata
{
    private final InformationSchemaMetadata informationSchema;

    @Inject
    public InternalMetadata(InformationSchemaMetadata informationSchema)
    {
        this.informationSchema = checkNotNull(informationSchema, "informationSchema is null");
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);

        if (tableName.equals(DualTable.NAME)) {
            return DualTable.getMetadata(catalogName, schemaName, tableName);
        }

        if (schemaName.equals(INFORMATION_SCHEMA)) {
            return informationSchema.getTableMetadata(catalogName, schemaName, tableName);
        }

        return null;
    }
}
