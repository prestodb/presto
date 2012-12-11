package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.tree.Join;
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
    private final SymbolAllocator symbolAllocator;
    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews;
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors;
    private final IdentityHashMap<Relation, TableMetadata> tableMetadata;

    private final AnalyzedExpression predicate;
    private final AnalyzedOutput output;
    private final List<AnalyzedExpression> groupBy;
    private final Set<AnalyzedAggregation> aggregations;
    private final Long limit;
    private final List<AnalyzedOrdering> orderBy;
    private final IdentityHashMap<Join, AnalyzedExpression> joinCriteria;
    private final boolean distinct;

    public static AnalysisResult newInstance(AnalysisContext context,
            boolean distinct,
            AnalyzedOutput output,
            AnalyzedExpression predicate,
            List<AnalyzedExpression> groupBy,
            Set<AnalyzedAggregation> aggregations,
            @Nullable Long limit,
            List<AnalyzedOrdering> orderBy)
    {
        return new AnalysisResult(
                context.getSymbolAllocator(),
                context.getTableDescriptors(),
                context.getTableMetadata(),
                context.getInlineViews(),
                context.getJoinCriteria(),
                distinct,
                aggregations,
                predicate,
                output,
                groupBy,
                orderBy,
                limit
        );
    }

    private AnalysisResult(SymbolAllocator symbolAllocator,
            IdentityHashMap<Relation, TupleDescriptor> tableDescriptors,
            IdentityHashMap<Relation, TableMetadata> tableMetadata, 
            IdentityHashMap<Subquery, AnalysisResult> inlineViews,
            IdentityHashMap<Join, AnalyzedExpression> joinCriteria,
            boolean distinct,
            Set<AnalyzedAggregation> aggregations,
            @Nullable AnalyzedExpression predicate,
            AnalyzedOutput output,
            List<AnalyzedExpression> groupBy,
            List<AnalyzedOrdering> orderBy,
            @Nullable Long limit)
    {
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");
        Preconditions.checkNotNull(tableDescriptors, "tableDescriptors is null");
        Preconditions.checkNotNull(tableMetadata, "tableMetadata is null");
        Preconditions.checkNotNull(inlineViews, "inlineViews is null");
        Preconditions.checkNotNull(joinCriteria, "joinCriteria is null");
        Preconditions.checkNotNull(aggregations, "aggregations is null");
        Preconditions.checkNotNull(output, "output is null");
        Preconditions.checkNotNull(groupBy, "groupBy is null");
        Preconditions.checkNotNull(orderBy, "orderBy is null");

        this.symbolAllocator = symbolAllocator;
        this.tableDescriptors = new IdentityHashMap<>(tableDescriptors);
        this.tableMetadata = new IdentityHashMap<>(tableMetadata);
        this.inlineViews = new IdentityHashMap<>(inlineViews);
        this.joinCriteria = new IdentityHashMap<>(joinCriteria);
        this.distinct = distinct;
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

    public Map<Symbol, AnalyzedExpression> getOutputExpressions()
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

    public TableMetadata getTableMetadata(Table table)
    {
        Preconditions.checkArgument(tableMetadata.containsKey(table), "Analysis for table is missing. Broken analysis?");
        return tableMetadata.get(table);
    }

    public AnalysisResult getAnalysis(Subquery inlineView)
    {
        Preconditions.checkArgument(inlineViews.containsKey(inlineView), "Analysis for inlineView is missing. Broken analysis?");
        return inlineViews.get(inlineView);
    }

    public AnalyzedExpression getJoinCriteria(Join join)
    {
        Preconditions.checkArgument(joinCriteria.containsKey(join), "Analysis for join is missing. Broken analysis?");
        return joinCriteria.get(join);
    }

    public Set<AnalyzedAggregation> getAggregations()
    {
        return aggregations;
    }

    public SymbolAllocator getSymbolAllocator()
    {
        return symbolAllocator;
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

    public Type getType(Symbol symbol)
    {
        return symbolAllocator.getType(symbol);
    }

    public Map<Symbol, Type> getTypes()
    {
        return symbolAllocator.getTypes();
    }

    public boolean isDistinct()
    {
        return distinct;
    }
}
