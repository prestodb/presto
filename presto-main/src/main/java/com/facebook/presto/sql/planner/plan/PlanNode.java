package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OutputNode.class, name = "output"),
        @JsonSubTypes.Type(value = ProjectNode.class, name = "project"),
        @JsonSubTypes.Type(value = TableScanNode.class, name = "tablescan"),
        @JsonSubTypes.Type(value = AggregationNode.class, name = "aggregation"),
        @JsonSubTypes.Type(value = FilterNode.class, name = "filter"),
        @JsonSubTypes.Type(value = LimitNode.class, name = "limit"),
        @JsonSubTypes.Type(value = TopNNode.class, name = "topn"),
        @JsonSubTypes.Type(value = SortNode.class, name = "sort"),
        @JsonSubTypes.Type(value = ExchangeNode.class, name = "exchange"),
        @JsonSubTypes.Type(value = SinkNode.class, name = "sink"),
        @JsonSubTypes.Type(value = JoinNode.class, name = "join")})
public abstract class PlanNode
{
    private final PlanNodeId id;

    protected PlanNode(PlanNodeId id)
    {
        checkNotNull(id, "id is null");
        this.id = id;
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }

    public abstract List<PlanNode> getSources();
    public abstract List<Symbol> getOutputSymbols();

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
