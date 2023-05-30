package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushDownSemiJoinThroughCrossJoinEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoinSource;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * Pushdown semi join below cross join
 * <pre>
 *     - SemiJoin
 *          sourceVar = filterVar
 *          - Project
 *              sourceVar := l1.k1+l.k2
 *              - CrossJoin
 *                  - scan l
 *                      l.k1, l.k2
 *                  - scan r
 *                      r.k1, r.k2
 *          - scan filterSource
 *              filterVar
 * </pre>
 * into
 * <pre>
 *     - CrossJoin
 *          - SemiJoin
 *              sourceVar = filterVar
 *              - Project
 *                  sourceVar := l1.k1+l.k2
 *                  - scan l
 *                      l.k1, l.k2
 *              - scan filterSource
 *                  filterVar
 *          - scan r
 *              r.k1, r.k2
 * </pre>
 */
public class PushDownSemiJoinThroughProjectCrossJoin
        implements Rule<SemiJoinNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<JoinNode> JOIN = newCapture();
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(semiJoinSource().matching(project().with(source().
            matching(join().matching(x -> x.getCriteria().isEmpty() && x.getType().equals(JoinNode.Type.INNER)).capturedAs(JOIN))).capturedAs(PROJECT)));

    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    public PushDownSemiJoinThroughProjectCrossJoin(Metadata metadata)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
    }

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
        ProjectNode projectNode = captures.get(PROJECT);
        JoinNode joinNode = captures.get(JOIN);
        VariableReferenceExpression sourceVariable = node.getSourceJoinVariable();
        RowExpression sourceExpression = projectNode.getAssignments().getMap().getOrDefault(sourceVariable, null);
        if (!sourceExpression.equals(sourceVariable) && !determinismEvaluator.isDeterministic(sourceExpression))
        {
            return Result.empty();
        }

        Set<VariableReferenceExpression> variables = extractUnique(sourceExpression);
        if (joinNode.getLeft().getOutputVariables().containsAll(variables)) {
            return Result.ofPlanNode(pushDownSemiJoin(projectNode, joinNode, node, true, context.getIdAllocator()));
        }
        else if (joinNode.getRight().getOutputVariables().containsAll(variables)) {
            return Result.ofPlanNode(pushDownSemiJoin(projectNode, joinNode, node, false, context.getIdAllocator()));
        }

        return Result.empty();
    }

    private PlanNode pushDownSemiJoin(ProjectNode projectNode, JoinNode joinNode, SemiJoinNode semiJoinNode, boolean pushDownLeft, PlanNodeIdAllocator idAllocator)
    {
        VariableReferenceExpression sourceVariable = semiJoinNode.getSourceJoinVariable();
        RowExpression sourceExpression = projectNode.getAssignments().getMap().get(sourceVariable);
        PlanNode newJoinInput = pushDownLeft ? joinNode.getLeft() : joinNode.getRight();
        if (!sourceExpression.equals(sourceVariable)) {
            newJoinInput = PlannerUtils.addProjections(newJoinInput, idAllocator, ImmutableMap.of(sourceVariable, sourceExpression));
        }
        SemiJoinNode newSemiJoinNode = (SemiJoinNode) semiJoinNode.replaceChildren(ImmutableList.of(newJoinInput, semiJoinNode.getFilteringSource()));
        ImmutableList.Builder<VariableReferenceExpression> joinOutput = ImmutableList.builder();
        joinOutput.addAll(pushDownLeft ? newSemiJoinNode.getOutputVariables() : joinNode.getLeft().getOutputVariables());
        joinOutput.addAll(pushDownLeft ? joinNode.getRight().getOutputVariables() : newSemiJoinNode.getOutputVariables());
        JoinNode newJoinNode = new JoinNode(
                joinNode.getSourceLocation(), idAllocator.getNextId(),
                joinNode.getType(),
                pushDownLeft ? newSemiJoinNode : joinNode.getLeft(),
                pushDownLeft ? joinNode.getRight() : newSemiJoinNode,
                joinNode.getCriteria(),
                joinOutput.build(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters()
        );
        PlanNode remainingProject = PlannerUtils.addProjections(newJoinNode, idAllocator,
                projectNode.getAssignments().entrySet().stream().filter(x -> !x.getKey().equals(sourceVariable)).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        return new ProjectNode(idAllocator.getNextId(), remainingProject, identityAssignments(semiJoinNode.getOutputVariables()));
    }
}
