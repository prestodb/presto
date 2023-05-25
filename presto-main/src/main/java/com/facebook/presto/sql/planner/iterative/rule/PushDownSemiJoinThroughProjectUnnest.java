package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushDownSemiJoinThroughCrossJoinEnabled;
import static com.facebook.presto.SystemSessionProperties.isPushDownSemiJoinThroughUnnestEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoinSource;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.unnest;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class PushDownSemiJoinThroughProjectUnnest
        implements Rule<SemiJoinNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<UnnestNode> UNNEST = newCapture();
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(
            semiJoinSource().matching(project().with(source().matching(unnest().capturedAs(UNNEST))).capturedAs(PROJECT)));

    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    public PushDownSemiJoinThroughProjectUnnest(Metadata metadata)
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
        return isPushDownSemiJoinThroughUnnestEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT);
        UnnestNode unnestNode = captures.get(UNNEST);
        VariableReferenceExpression sourceVariable = node.getSourceJoinVariable();
        RowExpression sourceExpression = projectNode.getAssignments().getMap().get(sourceVariable);
        if (!sourceExpression.equals(sourceVariable) && !determinismEvaluator.isDeterministic(sourceExpression))
        {
            return Result.empty();
        }

        Set<VariableReferenceExpression> variables = extractUnique(sourceExpression);
        if (unnestNode.getReplicateVariables().containsAll(variables)) {
            PlanNode newProject = unnestNode.getSource();
            if (!sourceExpression.equals(sourceVariable)) {
                newProject = PlannerUtils.addProjections(unnestNode.getSource(), context.getIdAllocator(), ImmutableMap.of(sourceVariable, sourceExpression));
            }
            SemiJoinNode newSemiJoinNode = (SemiJoinNode) node.replaceChildren(ImmutableList.of(newProject, node.getFilteringSource()));
            UnnestNode newUnnest = new UnnestNode(
                    unnestNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newSemiJoinNode,
                    ImmutableList.<VariableReferenceExpression>builder().addAll(unnestNode.getReplicateVariables()).add(newSemiJoinNode.getSemiJoinOutput()).build(),
                    unnestNode.getUnnestVariables(), unnestNode.getOrdinalityVariable()
            );
            PlanNode remainingProject = PlannerUtils.addProjections(newUnnest, context.getIdAllocator(),
                    projectNode.getAssignments().entrySet().stream().filter(x -> !x.getKey().equals(sourceVariable)).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), remainingProject, identityAssignments(node.getOutputVariables())));
        }

        return Result.empty();
    }
}
