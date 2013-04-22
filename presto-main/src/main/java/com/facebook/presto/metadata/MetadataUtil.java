package com.facebook.presto.metadata;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MetadataUtil
{
    public static QualifiedTableName checkTable(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");
        checkCatalogName(table.getCatalogName());
        checkSchemaName(table.getSchemaName());
        checkTableName(table.getTableName());
        return table;
    }

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkCatalogName(catalogName);
        checkSchemaName(schemaName);
        checkTableName(tableName);

        if (!schemaName.isPresent()) {
            checkState(!tableName.isPresent(), "schemaName is absent!");
        }
    }

    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static Optional<String> checkSchemaName(Optional<String> schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static Optional<String> checkTableName(Optional<String> tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static String checkColumnName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static void checkTableName(String catalogName, String schemaName, String tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(tableName, "tableName");
    }

    public static Optional<String> checkLowerCase(Optional<String> value, String name)
    {
        if (value.isPresent()) {
            checkLowerCase(value.get(), name);
        }
        return value;
    }

    public static String checkLowerCase(String value, String name)
    {
        checkNotNull(value, "%s is null", name);
        checkArgument(value.equals(value.toLowerCase()), "%s is not lowercase", name);
        return value;
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

    public static Function<String, QualifiedTableName> toQualifiedTableName(final String catalogName, final String schemaName)
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
        private int ordinalPosition;

        public ColumnMetadataListBuilder column(String columnName, TupleInfo.Type type)
        {
            columns.add(new ColumnMetadata(columnName, type, ordinalPosition++));
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
