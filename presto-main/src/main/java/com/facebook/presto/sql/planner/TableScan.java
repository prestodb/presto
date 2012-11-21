package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class TableScan
    extends PlanNode
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final Map<String, Symbol> attributes; // attribute -> symbol

    public TableScan(String catalogName, String schemaName, String tableName, Map<String, Symbol> attributes)
    {
        Preconditions.checkNotNull(catalogName, "catalogName is null");
        Preconditions.checkNotNull(schemaName, "schemaName is null");
        Preconditions.checkNotNull(tableName, "tableName is null");
        Preconditions.checkNotNull(attributes, "attributes is null");
        Preconditions.checkArgument(!attributes.isEmpty(), "attributes is empty");

        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public Map<String, Symbol> getAttributes()
    {
        return attributes;
    }

    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(attributes.values());
    }

    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }
}
