package com.facebook.presto.sql.compiler;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.sql.compiler.Type.toRaw;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SessionMetadata
{
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_SCHEMA = "default";

    private final Metadata metadata;
    private String currentCatalog;
    private String currentSchema;

    public SessionMetadata(Metadata metadata, String currentCatalog, String currentSchema)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.currentCatalog = checkNotNull(currentCatalog, "currentCatalog is null");
        this.currentSchema = checkNotNull(currentSchema, "currentSchema is null");
    }

    public SessionMetadata(Metadata metadata)
    {
        this(metadata, DEFAULT_CATALOG, DEFAULT_SCHEMA);
    }

    public void using(String catalogName, String schemaName)
    {
        currentCatalog = catalogName;
        currentSchema = schemaName;
    }

    public FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes)
    {
        return metadata.getFunction(name, Lists.transform(parameterTypes, toRaw()));
    }

    public TableMetadata getTable(QualifiedName name)
    {
        checkArgument(name.getParts().size() <= 3, "too many dots in name: %s", name);

        List<String> parts = Lists.reverse(name.getParts());
        String tableName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : currentSchema;
        String catalogName = (parts.size() > 2) ? parts.get(2) : currentCatalog;

        TableMetadata table = metadata.getTable(catalogName, schemaName, tableName);
        checkArgument(table != null, "Table '%s.%s.%s' not defined", catalogName, schemaName, tableName);
        return table;
    }

    private QualifiedName qualifiedTableName(TableMetadata table)
    {
        if (!table.getCatalogName().equals(currentCatalog)) {
            return QualifiedName.of(table.getCatalogName(), table.getSchemaName(), table.getTableName());
        }
        if (!table.getSchemaName().equals(currentSchema)) {
            return QualifiedName.of(table.getSchemaName(), table.getTableName());
        }
        return QualifiedName.of(table.getTableName());
    }
}
