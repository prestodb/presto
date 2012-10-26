package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SessionMetadata
{
    private static final String DEFAULT_CATALOG = "default";
    private static final String DEFAULT_SCHEMA = "default";

    private final Metadata metadata;

    public SessionMetadata(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public FunctionInfo getFunction(QualifiedName name)
    {
        checkArgument(name.getParts().size() == 1, "qualified functions not supported");
        FunctionInfo functionInfo = metadata.getFunction(name.getSuffix());
        checkArgument(functionInfo != null, "Function '%s' not defined", name);
        return functionInfo;
    }

    public TableMetadata getTable(QualifiedName name)
    {
        checkArgument(name.getParts().size() <= 3, "too many dots in name: %s", name);

        List<String> parts = Lists.reverse(name.getParts());
        String tableName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : DEFAULT_SCHEMA;
        String catalogName = (parts.size() > 2) ? parts.get(2) : DEFAULT_CATALOG;

        TableMetadata table = metadata.getTable(catalogName, schemaName, tableName);
        checkArgument(table != null, "Table '%s.%s.%s' not defined", catalogName, schemaName, tableName);
        return table;
    }

    public List<QualifiedName> getTableSchema(QualifiedName name)
    {
        TableMetadata table = getTable(name);
        if (table == null) {
            return null;
        }

        List<ColumnMetadata> columns = table.getColumns();
        ImmutableList.Builder<QualifiedName> names = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            names.add(QualifiedName.of(qualifiedTableName(table), column.getName()));
        }
        return names.build();
    }

    private static QualifiedName qualifiedTableName(TableMetadata table)
    {
        if (!table.getCatalogName().equals(DEFAULT_CATALOG)) {
            return QualifiedName.of(table.getCatalogName(), table.getSchemaName(), table.getTableName());
        }
        if (!table.getSchemaName().equals(DEFAULT_SCHEMA)) {
            return QualifiedName.of(table.getSchemaName(), table.getTableName());
        }
        return QualifiedName.of(table.getTableName());
    }
}
