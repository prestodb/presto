package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.SystemSessionProperties.isPushDownSemiJoinThroughUnnestEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoinSource;
import static com.facebook.presto.sql.planner.plan.Patterns.unnest;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Pushdown semi join through Unnest node
 * <pre>
 *     - SemiJoin
 *          - Unnest
 *              replicate: key
 *              unnest: array -> field
 *              - Scan
 *                  array, key
 *          - Scan
 *              
 * </pre>
 */
public class PushDownSemiJoinThroughUnnest
        implements Rule<SemiJoinNode>
{
    private static final Capture<UnnestNode> CHILD = newCapture();
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(
            semiJoinSource().matching(unnest().capturedAs(CHILD)));

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushDownSemiJoinThroughUnnestEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        checkArgument(!node.getDistributionType().isPresent(), "The optimization does not support dynamic filter");
        UnnestNode unnestNode = captures.get(CHILD);
        VariableReferenceExpression sourceVariable = node.getSourceJoinVariable();
        if (unnestNode.getReplicateVariables().contains(sourceVariable)) {
            Assignments identityAssignment = identityAssignments(node.getOutputVariables());
            SemiJoinNode newSemiJoin = (SemiJoinNode) node.replaceChildren(ImmutableList.of(unnestNode.getSource(), node.getFilteringSource()));
            UnnestNode newUnnest = new UnnestNode(
                    unnestNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newSemiJoin,
                    ImmutableList.<VariableReferenceExpression>builder().addAll(unnestNode.getReplicateVariables()).add(node.getSemiJoinOutput()).build(),
                    unnestNode.getUnnestVariables(),
                    unnestNode.getOrdinalityVariable()
            );
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newUnnest, identityAssignment));
        }
        return Result.empty();
    }
}
