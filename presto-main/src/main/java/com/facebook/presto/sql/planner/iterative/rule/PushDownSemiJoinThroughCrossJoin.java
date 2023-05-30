package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isPushDownSemiJoinThroughCrossJoinEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoinSource;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Pushdown semi join below cross join
 * <pre>
 *     - SemiJoin
 *          sourceVar = filterVar
 *          - CrossJoin
 *              - scan l
 *                  sourceVar
 *              - scan r
 *                  r.k1, r.k2
 *          - scan filterSource
 *              filterVar
 * </pre>
 * into
 * <pre>
 *     - CrossJoin
 *          - SemiJoin
 *              sourceVar = filterVar
 *              - scan l
 *                  sourceVar
 *              - scan filterSource
 *                  filterVar
 *          - scan r
 *              r.k1, r.k2
 * </pre>
 */
public class PushDownSemiJoinThroughCrossJoin
        implements Rule<SemiJoinNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(
            semiJoinSource().matching(join().matching(x -> x.getCriteria().isEmpty() && x.getType().equals(JoinNode.Type.INNER)).capturedAs(CHILD)));

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushDownSemiJoinThroughCrossJoinEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        checkArgument(!node.getDistributionType().isPresent(), "The optimization does not support dynamic filter");
        JoinNode joinNode = captures.get(CHILD);
        VariableReferenceExpression sourceVariable = node.getSourceJoinVariable();
        if (joinNode.getLeft().getOutputVariables().contains(sourceVariable)) {
            return Result.ofPlanNode(pushDownSemiJoin(node, joinNode, true, context.getIdAllocator()));
        }
        else if (joinNode.getRight().getOutputVariables().contains(sourceVariable)) {
            return Result.ofPlanNode(pushDownSemiJoin(node, joinNode, false, context.getIdAllocator()));
        }
        return Result.empty();
    }

    private PlanNode pushDownSemiJoin(SemiJoinNode semiJoinNode, JoinNode joinNode, boolean pushDownLeft, PlanNodeIdAllocator idAllocator)
    {
        PlanNode joinInput = pushDownLeft ? joinNode.getLeft() : joinNode.getRight();
        Assignments identityAssignment = identityAssignments(semiJoinNode.getOutputVariables());
        SemiJoinNode newSemiJoinNode = (SemiJoinNode) semiJoinNode.replaceChildren(ImmutableList.of(joinInput, semiJoinNode.getFilteringSource()));
        ImmutableList.Builder<VariableReferenceExpression> joinOutput = ImmutableList.builder();
        joinOutput.addAll(pushDownLeft ? newSemiJoinNode.getOutputVariables() : joinNode.getLeft().getOutputVariables());
        joinOutput.addAll(pushDownLeft ? joinNode.getRight().getOutputVariables() : newSemiJoinNode.getOutputVariables());
        JoinNode newJoinNode = new JoinNode(
                joinNode.getSourceLocation(),
                idAllocator.getNextId(),
                joinNode.getType(),
                pushDownLeft ? newSemiJoinNode : joinNode.getLeft(),
                pushDownLeft ? joinNode.getRight() : newSemiJoinNode,
                joinNode.getCriteria(),
                joinOutput.build(),
                joinNode.getFilter(),
                Optional.empty(),
                Optional.empty(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters());
        return new ProjectNode(idAllocator.getNextId(), newJoinNode, identityAssignment);
    }
}
