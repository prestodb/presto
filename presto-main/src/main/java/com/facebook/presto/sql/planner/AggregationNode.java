package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.concat;

public class AggregationNode
    extends PlanNode
{
    private final PlanNode source;
    private final List<Slot> groupByKeys;
    private final Map<Slot, FunctionCall> aggregations;
    private final Map<Slot, FunctionInfo> infos;

    public AggregationNode(PlanNode source, List<Slot> groupByKeys, Map<Slot, FunctionCall> aggregations, Map<Slot, FunctionInfo> infos)
    {
        this.source = source;
        this.groupByKeys = groupByKeys;
        this.aggregations = aggregations;
        this.infos = infos;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Slot> getOutputs()
    {
        return ImmutableList.copyOf(concat(groupByKeys, aggregations.keySet()));
    }

    public Map<Slot, FunctionCall> getAggregations()
    {
        return aggregations;
    }

    public Map<Slot, FunctionInfo> getFunctionInfos()
    {
        return infos;
    }

    public List<Slot> getGroupBy()
    {
        return groupByKeys;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }
}
