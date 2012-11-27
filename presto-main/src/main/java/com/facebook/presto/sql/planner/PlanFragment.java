package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class PlanFragment
{
    private final int id;
    private final PlanNode root;
    private final boolean partitioned;
    private final Map<Symbol, Type> symbols;

    public PlanFragment(int id, boolean isPartitioned, Map<Symbol, Type> symbols, PlanNode root)
    {
        this.id = id;
        this.root = root;
        partitioned = isPartitioned;
        this.symbols = symbols;
    }

    public int getId()
    {
        return id;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    public PlanNode getRoot()
    {
        return root;
    }

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
