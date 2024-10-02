package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

public class RemoveArgumentsAfterNotNullFromCoalesce
    implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(join().capturedAs(JOIN)));

    @Override
    public Pattern<ProjectNode> getPattern() {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context) {
        JoinNode join = captures.get(JOIN);

        if (!join.getFilter().isPresent()) {
            return Result.empty();
        }

        List<RowExpression> notNulls = join.getFilter().get().getChildren();
        List<VariableReferenceExpression> coalesceArgs = projectNode.getOutputVariables();

        return Result.empty();
    }
}
