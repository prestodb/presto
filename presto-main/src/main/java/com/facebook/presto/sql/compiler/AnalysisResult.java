package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class AnalysisResult
{
    private final SlotAllocator slotAllocator;
    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews;
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors;

    private final AnalyzedExpression predicate;
    private final AnalyzedOutput output;
    private final List<AnalyzedExpression> groupBy;
    private final List<AnalyzedAggregation> aggregations;

    public static AnalysisResult newInstance(AnalysisContext context,
            AnalyzedOutput output,
            AnalyzedExpression predicate,
            List<AnalyzedExpression> groupBy,
            List<AnalyzedAggregation> aggregations)
    {
        return new AnalysisResult(
                context.getSlotAllocator(),
                context.getTableDescriptors(),
                context.getInlineViews(),
                aggregations,
                predicate,
                output,
                groupBy);
    }

    private AnalysisResult(SlotAllocator slotAllocator,
            IdentityHashMap<Relation, TupleDescriptor> tableDescriptors,
            IdentityHashMap<Subquery, AnalysisResult> inlineViews,
            List<AnalyzedAggregation> aggregations,
            AnalyzedExpression predicate,
            AnalyzedOutput output,
            List<AnalyzedExpression> groupBy)
    {
        this.slotAllocator = slotAllocator;
        this.tableDescriptors = new IdentityHashMap<>(tableDescriptors);
        this.inlineViews = new IdentityHashMap<>(inlineViews);
        this.aggregations = ImmutableList.copyOf(aggregations);
        this.predicate = predicate;
        this.output = output;
        this.groupBy = ImmutableList.copyOf(groupBy);
    }

    public TupleDescriptor getOutputDescriptor()
    {
        return output.getDescriptor();
    }

    public Map<Slot, AnalyzedExpression> getOutputExpressions()
    {
        return output.getExpressions();
    }

    public AnalyzedExpression getPredicate()
    {
        return predicate;
    }

    public TupleDescriptor getTableDescriptor(Table table)
    {
        return tableDescriptors.get(table);
    }

    public AnalysisResult getAnalysis(Subquery inlineView)
    {
        Preconditions.checkArgument(inlineViews.containsKey(inlineView), "Analysis for inlineView is missing. Broken analysis?");
        return inlineViews.get(inlineView);
    }

    public List<AnalyzedAggregation> getAggregations()
    {
        return aggregations;
    }

    public SlotAllocator getSlotAllocator()
    {
        return slotAllocator;
    }

    public List<AnalyzedExpression> getGroupByExpressions()
    {
        return groupBy;
    }
}
