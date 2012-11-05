package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ColumnScan
    extends PlanNode
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String attribute;
    private final Slot slot;

    public ColumnScan(String catalogName, String schemaName, String tableName, String attribute, Slot slot)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attribute = attribute;
        this.slot = slot;
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

    public String getAttributeName()
    {
        return attribute;
    }

    @Override
    public List<Slot> getOutputs()
    {
        return ImmutableList.of(slot);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitColumnScan(this, context);
    }

}
