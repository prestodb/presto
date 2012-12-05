package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class FilterNode
    extends PlanNode
{
    private final PlanNode source;
    private final Expression predicate;
    private final List<Symbol> outputs;

    @JsonCreator
    public FilterNode(@JsonProperty("source") PlanNode source, @JsonProperty("predicate") Expression predicate, @JsonProperty("outputs") List<Symbol> outputs)
    {
        this.source = source;
        this.predicate = predicate;
        this.outputs = outputs;
    }

    @JsonProperty("predicate")
    public Expression getPredicate()
    {
        return predicate;
    }

    @Override
    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

}
