package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Inject;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.metadata.InformationSchemaMetadata.informationSchemaTupleInfo;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class InformationSchemaData
{
    private final Metadata metadata;

    @Inject
    public InformationSchemaData(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public InternalTable getInternalTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        checkArgument(schemaName.equals(INFORMATION_SCHEMA), "schema is not %s", INFORMATION_SCHEMA);

        switch (tableName) {
            case TABLE_COLUMNS:
                return buildColumns(catalogName);
            case TABLE_TABLES:
                return buildTables(catalogName);
        }

        throw new IllegalArgumentException(format("table does not exist: %s.%s.%s", catalogName, schemaName, tableName));
    }

    private InternalTable buildColumns(String catalogName)
    {
        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_COLUMNS);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (TableColumn column : metadata.listTableColumns(catalogName)) {
            table.add(tupleInfo.builder()
                    .append(column.getCatalogName())
                    .append(column.getSchemaName())
                    .append(column.getTableName())
                    .append(column.getColumnName())
                    .append(column.getOrdinalPosition())
                    .appendNull()
                    .append("YES")
                    .append(column.getDataType().getName())
                    .build());
        }
        return table.build();
    }

    private InternalTable buildTables(String catalogName)
    {
        TupleInfo tupleInfo = informationSchemaTupleInfo(TABLE_TABLES);
        InternalTable.Builder table = InternalTable.builder(tupleInfo);
        for (QualifiedTableName name : metadata.listTables(catalogName)) {
            table.add(tupleInfo.builder()
                    .append(name.getCatalogName())
                    .append(name.getSchemaName())
                    .append(name.getTableName())
                    .append("BASE TABLE")
                    .build());
        }
        return table.build();
    }
}
