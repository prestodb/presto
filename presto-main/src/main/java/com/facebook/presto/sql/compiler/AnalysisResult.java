package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalysisResult
{
    private final SlotAllocator slotAllocator;
    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews;
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors;

    private final AnalyzedExpression predicate;
    private final AnalyzedOutput output;
    private final List<AnalyzedExpression> groupBy;
    private final Set<AnalyzedAggregation> aggregations;
    private final Long limit;
    private final List<AnalyzedOrdering> orderBy;

    public static AnalysisResult newInstance(AnalysisContext context,
            AnalyzedOutput output,
            AnalyzedExpression predicate,
            List<AnalyzedExpression> groupBy,
            Set<AnalyzedAggregation> aggregations,
            @Nullable Long limit,
            List<AnalyzedOrdering> orderBy)
    {
        return new AnalysisResult(
                context.getSlotAllocator(),
                context.getTableDescriptors(),
                context.getInlineViews(),
                aggregations,
                predicate,
                output,
                groupBy,
                orderBy,
                limit
        );
    }

    private AnalysisResult(SlotAllocator slotAllocator,
            IdentityHashMap<Relation, TupleDescriptor> tableDescriptors,
            IdentityHashMap<Subquery, AnalysisResult> inlineViews,
            Set<AnalyzedAggregation> aggregations,
            @Nullable AnalyzedExpression predicate,
            AnalyzedOutput output,
            List<AnalyzedExpression> groupBy,
            List<AnalyzedOrdering> orderBy,
            @Nullable Long limit)
    {
        Preconditions.checkNotNull(slotAllocator, "slotAllocator is null");
        Preconditions.checkNotNull(tableDescriptors, "tableDescriptors is null");
        Preconditions.checkNotNull(inlineViews, "inlineViews is null");
        Preconditions.checkNotNull(aggregations, "aggregations is null");
        Preconditions.checkNotNull(output, "output is null");
        Preconditions.checkNotNull(groupBy, "groupBy is null");
        Preconditions.checkNotNull(orderBy, "orderBy is null");

        this.slotAllocator = slotAllocator;
        this.tableDescriptors = new IdentityHashMap<>(tableDescriptors);
        this.inlineViews = new IdentityHashMap<>(inlineViews);
        this.aggregations = ImmutableSet.copyOf(aggregations);
        this.predicate = predicate;
        this.output = output;
        this.groupBy = ImmutableList.copyOf(groupBy);
        this.limit = limit;
        this.orderBy = ImmutableList.copyOf(orderBy);
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

    public Set<AnalyzedAggregation> getAggregations()
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

    public List<AnalyzedOrdering> getOrderBy()
    {
        return orderBy;
    }
}
