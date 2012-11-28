package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
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
}
