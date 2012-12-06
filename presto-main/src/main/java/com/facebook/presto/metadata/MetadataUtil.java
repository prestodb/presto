package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

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
