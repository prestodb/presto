package com.facebook.presto.sql.planner;

public class PlanVisitor<C, R>
{
    protected R visitPlan(PlanNode node, C context)
    {
        return null;
    }

    public R visitAggregation(AggregationNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAlign(AlignNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitColumnScan(ColumnScan node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitFilter(FilterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(ProjectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOutput(OutputPlan node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLimit(LimitNode node, C context)
    {
        return visitPlan(node, context);
    }
}
