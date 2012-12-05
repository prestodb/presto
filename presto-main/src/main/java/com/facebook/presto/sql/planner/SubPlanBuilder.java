package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.in;

public class SubPlanBuilder
{
    private final int id;
    private PlanNode root;
    private boolean isPartitioned;
    private List<SubPlan> children = ImmutableList.of();

    private final SymbolAllocator allocator;

    public SubPlanBuilder(int id, SymbolAllocator allocator, PlanNode root)
    {
        Preconditions.checkArgument(id >= 0, "id must be >= 0");
        Preconditions.checkNotNull(allocator, "symbols is null");
        Preconditions.checkNotNull(root, "root is null");

        this.allocator = allocator;
        this.id = id;
        this.root = root;
    }

    public int getId()
    {
        return id;
    }

    public SubPlanBuilder setRoot(PlanNode root)
    {
        Preconditions.checkNotNull(root, "root is null");
        this.root = root;
        return this;
    }

    public SubPlanBuilder setPartitioned(boolean partitioned)
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

    public SubPlanBuilder setChildren(Iterable<SubPlan> children)
    {
        this.children = ImmutableList.copyOf(children);
        return this;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public SubPlan build()
    {
        Set<Symbol> dependencies = SymbolExtractor.extract(root);

        PlanFragment fragment = new PlanFragment(id, isPartitioned, Maps.filterKeys(allocator.getTypes(), in(dependencies)), root);

        return new SubPlan(fragment, children);
    }
}
