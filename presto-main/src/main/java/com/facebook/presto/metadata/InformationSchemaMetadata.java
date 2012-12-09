package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;

public class InformationSchemaMetadata
{
    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final String TABLE_COLUMNS = "columns";
    public static final String TABLE_TABLES = "tables";

    private static final Map<String, List<ColumnMetadata>> METADATA = ImmutableMap.<String, List<ColumnMetadata>>builder()
            .put(TABLE_COLUMNS, columnsBuilder()
                    .column("table_catalog", VARIABLE_BINARY)
                    .column("table_schema", VARIABLE_BINARY)
                    .column("table_name", VARIABLE_BINARY)
                    .column("column_name", VARIABLE_BINARY)
                    .column("ordinal_position", FIXED_INT_64)
                    .column("column_default", VARIABLE_BINARY)
                    .column("is_nullable", VARIABLE_BINARY)
                    .column("data_type", VARIABLE_BINARY)
                    .build())
            .put(TABLE_TABLES, columnsBuilder()
                    .column("table_catalog", VARIABLE_BINARY)
                    .column("table_schema", VARIABLE_BINARY)
                    .column("table_name", VARIABLE_BINARY)
                    .column("table_type", VARIABLE_BINARY)
                    .build())
            .build();

    public TableMetadata getTableMetadata(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        checkArgument(schemaName.equals(INFORMATION_SCHEMA), "schema is not %s", INFORMATION_SCHEMA);

        List<ColumnMetadata> metadata = METADATA.get(tableName);
        if (metadata != null) {
            InternalTableHandle handle = new InternalTableHandle(catalogName, schemaName, tableName);
            return new TableMetadata(catalogName, schemaName, tableName, metadata, handle);
        }

        return null;
    }

    static TupleInfo informationSchemaTupleInfo(String tableName)
    {
        checkArgument(METADATA.containsKey(tableName), "table does not exist: %s", tableName);
        return new TupleInfo(Iterables.transform(METADATA.get(tableName), getType()));
    }

    private static Function<ColumnMetadata, TupleInfo.Type> getType()
    {
        return new Function<ColumnMetadata, TupleInfo.Type>()
        {
            @Override
            public TupleInfo.Type apply(ColumnMetadata column)
            {
                return column.getType();
            }
        };
    }
}
