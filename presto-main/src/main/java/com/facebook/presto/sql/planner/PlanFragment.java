package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

public class PlanFragment
{
    private final int id;
    private final PlanNode root;
    private final boolean partitioned;
    private final Map<Symbol, Type> symbols;

    public PlanFragment(@JsonProperty("id") int id, @JsonProperty("partitioned") boolean isPartitioned, @JsonProperty("symbols") Map<Symbol, Type> symbols, @JsonProperty("root") PlanNode root)
    {
        Preconditions.checkArgument(id >= 0, "id must be positive");
        Preconditions.checkNotNull(symbols, "symbols is null");
        Preconditions.checkNotNull(root, "root is null");

        this.id = id;
        this.root = root;
        partitioned = isPartitioned;
        this.symbols = symbols;
    }

    @JsonProperty("id")
    public int getId()
    {
        return id;
    }

    @JsonProperty("partitioned")
    public boolean isPartitioned()
    {
        return partitioned;
    }

    @JsonProperty("root")
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty("symbols")
    public Map<Symbol, Type> getSymbols()
    {
        return symbols;
    }

    public List<PlanNode> getSources()
    {
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        findSources(root, sources);
        return sources.build();
    }

    private void findSources(PlanNode node, ImmutableList.Builder<PlanNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findSources(source, builder);
        }

        if (node.getSources().isEmpty()) {
            builder.add(node);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("partitioned", partitioned)
                .toString();
    }
}
