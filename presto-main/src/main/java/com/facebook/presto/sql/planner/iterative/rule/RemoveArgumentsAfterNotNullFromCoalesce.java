package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.sql.planner.plan.Patterns.project;

public class RemoveArgumentsAfterNotNullFromCoalesce
    implements Rule<ProjectNode>
{

    @Override
    public Pattern<ProjectNode> getPattern() {
        return project();
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context) {
        PlanNode current = node;
        current.getSources();
        return null;
    }
}
