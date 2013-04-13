package com.facebook.presto.metadata;

import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public class MetadataUtil
{
    public static QualifiedTableName checkTable(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");
        checkLowerCase(table.getCatalogName(), "catalogName");
        checkLowerCase(table.getSchemaName(), "schemaName");
        checkLowerCase(table.getTableName(), "tableName");
        return table;
    }

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkNotNull(schemaName, "schemaName is null");
        checkNotNull(tableName, "tableName is null");

        if (schemaName.isPresent()) {
            checkLowerCase(schemaName.get(), "schemaName");

            if (tableName.isPresent()) {
                checkLowerCase(tableName.get(), "tableName");
            }
        }
        else {
            checkState(!tableName.isPresent(), "schemaName is absent!");
        }
    }

    public static void checkCatalogName(String catalogName)
    {
        checkLowerCase(catalogName, "catalogName");
    }

    public static void checkTableName(String catalogName, String schemaName, String tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(tableName, "tableName");
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

    public static List<TableColumn> getTableColumns(String catalogName, String schemaName, Map<String, List<ColumnMetadata>> tables)
    {
        return ImmutableList.copyOf(concat(transform(tables.entrySet(), getColumns(catalogName, schemaName))));
    }

    public static List<QualifiedTableName> getTableNames(String catalogName, String schemaName, Map<String, List<ColumnMetadata>> tables)
    {
        return ImmutableList.copyOf(transform(tables.keySet(), getTable(catalogName, schemaName)));
    }

    private static Function<Map.Entry<String, List<ColumnMetadata>>, List<TableColumn>> getColumns(
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
                    list.add(new TableColumn(new QualifiedTableName(catalogName, schemaName, tableName),
                            column.getName(), position, column.getType()));
                    position++;
                }
                return list.build();
            }
        };
    }

    private static Function<String, QualifiedTableName> getTable(final String catalogName, final String schemaName)
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

    public static QualifiedTableName createQualifiedTableName(Session session, QualifiedName name)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkArgument(name.getParts().size() <= 3, "Too many dots in table name: %s", name);

        List<String> parts = Lists.reverse(name.getParts());
        String tableName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema();
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog();

        return new QualifiedTableName(catalogName, schemaName, tableName);
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

    public static TableMetadata findOrCreateTable(Metadata metadata, QualifiedTableName table, List<ColumnMetadata> columns)
    {
        TableMetadata tableMetadata = metadata.getTable(table);
        if (tableMetadata == null || !tableMetadata.getTableHandle().isPresent()) {

            tableMetadata = new TableMetadata(table, columns);
            metadata.createTable(tableMetadata);

            tableMetadata = metadata.getTable(table);
        }

        return tableMetadata;
    }
}
