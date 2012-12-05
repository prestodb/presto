package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static com.google.common.base.Predicates.instanceOf;

@Immutable
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> children;

    public SubPlan(PlanFragment fragment, List<SubPlan> children)
    {
        Preconditions.checkNotNull(fragment, "fragment is null");
        Preconditions.checkNotNull(children, "children is null");

        this.fragment = fragment;
        this.children = ImmutableList.copyOf(children);
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public void sanityCheck()
    {
        if (Iterables.any(fragment.getSources(), instanceOf(ExchangeNode.class)) && children.isEmpty()) {
            throw new IllegalStateException("Subplan has remote exchanges but no child subplans");
        }
        else if (!Iterables.any(fragment.getSources(), instanceOf(ExchangeNode.class)) && !children.isEmpty()) {
            throw new IllegalStateException("Subplan has no remote exchanges but has child subplans");
        }

        for (SubPlan child : children) {
            child.sanityCheck();
        }
    }
}
