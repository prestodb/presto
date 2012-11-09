package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
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
    private final Map<String, Slot> attributes;

    public TableScan(String catalogName, String schemaName, String tableName, Map<String, Slot> attributes)
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

    public Map<String, Slot> getAttributes()
    {
        return attributes;
    }

    public List<Slot> getOutputs()
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
