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

    public R visitFilter(FilterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(ProjectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopN(TopNNode node, C context)
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

    public R visitTableScan(TableScan node, C context)
    {
        return visitPlan(node, context);
    }
}
