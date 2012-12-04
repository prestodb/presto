package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;

public class PlanFragmentBuilder
{
    private final int id;
    private PlanNode root;
    private boolean isPartitioned;

    public PlanFragmentBuilder(int id)
    {
        this.id = id;
    }

    public int getId()
    {
        return id;
    }

    public PlanFragmentBuilder setRoot(PlanNode newRoot)
    {
        root = newRoot;
        return this;
    }

    public PlanFragmentBuilder setPartitioned(boolean partitioned)
    {
        isPartitioned = partitioned;
        return this;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public boolean isPartitioned()
    {
        return isPartitioned;
    }

    public PlanFragment build(Map<Symbol, Type> symbols)
    {
        Set<Symbol> dependencies = SymbolExtractor.extract(root);

        return new PlanFragment(id, isPartitioned, Maps.filterKeys(symbols, in(dependencies)), root);
    }

    public static Function<PlanFragmentBuilder, PlanFragment> buildFragmentFunction(final Map<Symbol, Type> symbols)
    {
        return new Function<PlanFragmentBuilder, PlanFragment>()
        {
            @Override
            public PlanFragment apply(PlanFragmentBuilder input)
            {
                return input.build(symbols);
            }
        };
    }
}
