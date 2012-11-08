package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
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
    private final Long limit;

    public static AnalysisResult newInstance(AnalysisContext context,
            AnalyzedOutput output,
            AnalyzedExpression predicate,
            List<AnalyzedExpression> groupBy,
            List<AnalyzedAggregation> aggregations,
            @Nullable Long limit)
    {
        return new AnalysisResult(
                context.getSlotAllocator(),
                context.getTableDescriptors(),
                context.getInlineViews(),
                aggregations,
                predicate,
                output,
                groupBy,
                limit);
    }

    private AnalysisResult(SlotAllocator slotAllocator,
            IdentityHashMap<Relation, TupleDescriptor> tableDescriptors,
            IdentityHashMap<Subquery, AnalysisResult> inlineViews,
            List<AnalyzedAggregation> aggregations,
            @Nullable AnalyzedExpression predicate,
            AnalyzedOutput output,
            List<AnalyzedExpression> groupBy,
            @Nullable Long limit)
    {
        Preconditions.checkNotNull(slotAllocator, "slotAllocator is null");
        Preconditions.checkNotNull(tableDescriptors, "tableDescriptors is null");
        Preconditions.checkNotNull(inlineViews, "inlineViews is null");
        Preconditions.checkNotNull(aggregations, "aggregations is null");
        Preconditions.checkNotNull(output, "output is null");
        Preconditions.checkNotNull(groupBy, "groupBy is null");

        this.slotAllocator = slotAllocator;
        this.tableDescriptors = new IdentityHashMap<>(tableDescriptors);
        this.inlineViews = new IdentityHashMap<>(inlineViews);
        this.aggregations = ImmutableList.copyOf(aggregations);
        this.predicate = predicate;
        this.output = output;
        this.groupBy = ImmutableList.copyOf(groupBy);
        this.limit = limit;
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
        Preconditions.checkArgument(tableDescriptors.containsKey(table), "Analysis for table is missing. Broken analysis?");
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

    public Long getLimit()
    {
        return limit;
    }
}
