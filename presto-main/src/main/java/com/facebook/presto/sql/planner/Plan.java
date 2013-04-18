package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;

import java.util.Map;

public class Plan
{
    private final PlanNode root;
    private final SymbolAllocator symbolAllocator;

    public Plan(PlanNode root, SymbolAllocator symbolAllocator)
    {
        Preconditions.checkNotNull(root, "root is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");

        this.root = root;
        this.symbolAllocator = symbolAllocator;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public Map<Symbol, Type> getTypes()
    {
        return symbolAllocator.getTypes();
    }

    public SymbolAllocator getSymbolAllocator()
    {
        return symbolAllocator;
    }
}
