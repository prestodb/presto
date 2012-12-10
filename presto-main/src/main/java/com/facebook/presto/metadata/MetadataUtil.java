package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class MetadataUtil
{
    public static void checkTableName(String catalogName, String schemaName, String tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(tableName, "tableName");
    }

    public static void checkSchemaName(String catalogName, String schemaName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
    }

    public static void checkCatalogName(String catalogName)
    {
        checkLowerCase(catalogName, "catalogName");
    }

    private static void checkLowerCase(String s, String name)
    {
        checkNotNull(s, "%s is null", name);
        checkArgument(s.equals(s.toLowerCase()), "%s is not lowercase", name);
    }

    public static Function<ColumnMetadata, TupleInfo.Type> getType()
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

    public static Function<Map.Entry<String, List<ColumnMetadata>>, List<TableColumn>> getColumns(
            final String catalogName, final String schemaName)
    {
        return new Function<Map.Entry<String, List<ColumnMetadata>>, List<TableColumn>>()
        {
            @Override
            public List<TableColumn> apply(Map.Entry<String, List<ColumnMetadata>> entry)
            {
                String tableName = entry.getKey();
                ImmutableList.Builder<TableColumn> list = ImmutableList.builder();
                int position = 1;
                for (ColumnMetadata column : entry.getValue()) {
                    list.add(new TableColumn(
                            catalogName, schemaName, tableName,
                            column.getName(), position, column.getType()));
                    position++;
                }
                return list.build();
            }
        };
    }

    public static Function<String, QualifiedTableName> getTable(final String catalogName, final String schemaName)
    {
        return new Function<String, QualifiedTableName>()
        {
            @Override
            public QualifiedTableName apply(String tableName)
            {
                return new QualifiedTableName(catalogName, schemaName, tableName);
            }
        };
    }

    public static class ColumnMetadataListBuilder
    {
        private final List<ColumnMetadata> columns = new ArrayList<>();

        public ColumnMetadataListBuilder column(String columnName, TupleInfo.Type type)
        {
            ColumnHandle handle = new InternalColumnHandle(columns.size());
            columns.add(new ColumnMetadata(columnName, type, handle));
            return this;
        }

        public List<ColumnMetadata> build()
        {
            return ImmutableList.copyOf(columns);
        }

        public static ColumnMetadataListBuilder columnsBuilder()
        {
            return new ColumnMetadataListBuilder();
        }
    }
}
