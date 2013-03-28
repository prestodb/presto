package com.facebook.presto.metadata;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.listInformationSchemaTables;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
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
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);

        if (table.getTableName().equals(DualTable.NAME)) {
            return DualTable.getTable(table);
        }

        switch (table.getSchemaName()) {
            case INFORMATION_SCHEMA:
                return informationSchema.getTable(table);
            case SYSTEM_SCHEMA:
                return systemTables.getTable(table);
        }

        return null;
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        Optional<String> schemaName = prefix.getSchemaName();
        if (schemaName.isPresent()) {
            switch (schemaName.get()) {
                case INFORMATION_SCHEMA:
                    return listInformationSchemaTables(prefix.getCatalogName());
                case SYSTEM_SCHEMA:
                    return listSystemTables(prefix.getCatalogName());
            }
        }
        return ImmutableList.of();
    }

    @Override
    public List<TableColumn> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        // hack for SHOW COLUMNS
        return ImmutableList.of();
    }
}
