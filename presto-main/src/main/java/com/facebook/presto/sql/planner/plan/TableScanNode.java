package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
public class TableScanNode
    extends PlanNode
{
    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column
    private final Expression partitionPredicate;
    private final Expression upstreamPredicateHint; // TODO: hack to support lack of connector predicate negotiation (fix this)

    @JsonCreator
    public TableScanNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("partitionPredicate") Expression partitionPredicate,
            @JsonProperty("upstreamPredicateHint") Expression upstreamPredicateHint)
    {
        super(id);

        Preconditions.checkNotNull(table, "table is null");
        Preconditions.checkNotNull(outputSymbols, "outputSymbols is null");
        Preconditions.checkArgument(!outputSymbols.isEmpty(), "outputSymbols is empty");
        Preconditions.checkNotNull(assignments, "assignments is null");
        Preconditions.checkArgument(!assignments.isEmpty(), "assignments is empty");
        Preconditions.checkNotNull(partitionPredicate, "partitionPredicate is null");
        Preconditions.checkNotNull(upstreamPredicateHint, "upstreamPredicateHint is null");

        this.table = table;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.assignments = ImmutableMap.copyOf(assignments);
        this.partitionPredicate = partitionPredicate;
        this.upstreamPredicateHint = upstreamPredicateHint;

        Preconditions.checkArgument(assignments.keySet().containsAll(outputSymbols), "Assignments must provide mappings for all output symbols");
        Preconditions.checkArgument(assignments.keySet().containsAll(DependencyExtractor.extractUnique(partitionPredicate)), "Assignments must provide mappings for all partition predicate symbols");
        Preconditions.checkArgument(outputSymbols.containsAll(DependencyExtractor.extractUnique(upstreamPredicateHint)), "Upstream predicate hint must be in terms of output symbols");
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }


    @JsonProperty("partitionPredicate")
    public Expression getPartitionPredicate()
    {
        return partitionPredicate;
    }

    @JsonProperty("upstreamPredicateHint")
    public Expression getUpstreamPredicateHint()
    {
        return upstreamPredicateHint;
    }

    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
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
