/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushProjectionThroughCrossJoin;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.plan.Patterns.project;

/**
 * Pushes projections that reference only one side of a cross join
 * below that join, so that expressions are evaluated on fewer rows.
 *
 * <p>Handles cascading projections: walks through intermediate
 * ProjectNodes between the matched project and the cross join,
 * pushing single-side assignments from all levels below the join.
 *
 * <p>Transforms:
 * <pre>
 * Project(a_expr = f(a), b_expr = g(b), mixed = h(a, b))
 *   CrossJoin
 *     Left(a)
 *     Right(b)
 * </pre>
 * to:
 * <pre>
 * Project(a_expr, b_expr, mixed = h(a, b))
 *   CrossJoin
 *     Project(a_expr = f(a), a)
 *       Left(a)
 *     Project(b_expr = g(b), b)
 *       Right(b)
 * </pre>
 *
 * <p>Also handles cascading projections:
 * <pre>
 * Project(y = g(x))
 *   Project(x = f(a), b = b)
 *     CrossJoin
 *       Left(a)
 *       Right(b)
 * </pre>
 * to:
 * <pre>
 * Project(y = y)
 *   CrossJoin
 *     Project(y = g(x))
 *       Project(x = f(a))
 *         Left(a)
 *     Right(b)
 * </pre>
 *
 * <p>Only fires when there is at least one non-identity, deterministic
 * assignment that exclusively references variables from a single side.
 */
public class PushProjectionThroughCrossJoin
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final DeterminismEvaluator determinismEvaluator;

    public PushProjectionThroughCrossJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushProjectionThroughCrossJoin(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        // Walk through intermediate ProjectNodes to find a CrossJoin
        List<ProjectNode> chain = new ArrayList<>();
        chain.add(project);
        PlanNode current = context.getLookup().resolve(project.getSource());
        while (current instanceof ProjectNode) {
            chain.add((ProjectNode) current);
            current = context.getLookup().resolve(((ProjectNode) current).getSource());
        }

        if (!(current instanceof JoinNode) || !((JoinNode) current).isCrossJoin()) {
            return Result.empty();
        }

        JoinNode crossJoin = (JoinNode) current;

        Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(
                crossJoin.getLeft().getOutputVariables());
        Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(
                crossJoin.getRight().getOutputVariables());

        // Track which variables are effectively from each side as we push assignments
        Set<VariableReferenceExpression> effectiveLeft = new HashSet<>(leftVariables);
        Set<VariableReferenceExpression> effectiveRight = new HashSet<>(rightVariables);

        // Process levels from bottom (closest to cross join) to top
        // chain is [top, ..., bottom], so iterate in reverse
        int totalLevels = chain.size();
        List<Assignments> leftPushLevels = new ArrayList<>();
        List<Assignments> rightPushLevels = new ArrayList<>();
        List<Assignments> residualLevels = new ArrayList<>();
        boolean anyPushed = false;

        for (int chainIdx = totalLevels - 1; chainIdx >= 0; chainIdx--) {
            ProjectNode proj = chain.get(chainIdx);

            Assignments.Builder leftPush = Assignments.builder();
            Assignments.Builder rightPush = Assignments.builder();
            Assignments.Builder residualBuild = Assignments.builder();

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : proj.getAssignments().entrySet()) {
                VariableReferenceExpression outputVar = entry.getKey();
                RowExpression expression = entry.getValue();

                // Identity assignments: propagate side membership but don't push
                if (expression instanceof VariableReferenceExpression) {
                    VariableReferenceExpression inputVar = (VariableReferenceExpression) expression;
                    if (effectiveLeft.contains(inputVar)) {
                        effectiveLeft.add(outputVar);
                    }
                    if (effectiveRight.contains(inputVar)) {
                        effectiveRight.add(outputVar);
                    }
                    residualBuild.put(outputVar, expression);
                    continue;
                }

                // Non-deterministic expressions must not be pushed below the cross join
                if (!determinismEvaluator.isDeterministic(expression)) {
                    residualBuild.put(outputVar, expression);
                    continue;
                }

                Set<VariableReferenceExpression> referencedVars = extractUnique(expression);
                boolean refsLeft = referencedVars.stream().anyMatch(effectiveLeft::contains);
                boolean refsRight = referencedVars.stream().anyMatch(effectiveRight::contains);
                // If any referenced variable is defined above the cross join (not from either side),
                // the expression cannot be pushed below
                boolean refsAbove = referencedVars.stream()
                        .anyMatch(v -> !effectiveLeft.contains(v) && !effectiveRight.contains(v));

                if (refsAbove) {
                    residualBuild.put(outputVar, expression);
                }
                else if (refsLeft && !refsRight) {
                    leftPush.put(outputVar, expression);
                    residualBuild.put(outputVar, outputVar);
                    effectiveLeft.add(outputVar);
                    anyPushed = true;
                }
                else if (refsRight && !refsLeft) {
                    rightPush.put(outputVar, expression);
                    residualBuild.put(outputVar, outputVar);
                    effectiveRight.add(outputVar);
                    anyPushed = true;
                }
                else {
                    // References both sides or is a constant: keep above
                    residualBuild.put(outputVar, expression);
                }
            }

            leftPushLevels.add(leftPush.build());
            rightPushLevels.add(rightPush.build());
            residualLevels.add(residualBuild.build());
        }

        if (!anyPushed) {
            return Result.empty();
        }

        // Collect all variables needed by residual projects (for pass-throughs)
        Set<VariableReferenceExpression> allResidualNeeds = new HashSet<>();
        for (Assignments res : residualLevels) {
            for (RowExpression expression : res.getExpressions()) {
                allResidualNeeds.addAll(extractUnique(expression));
            }
        }

        // Build left chain: stack pushed projects on CrossJoin.Left
        PlanNode newLeft = crossJoin.getLeft();
        for (Assignments pushed : leftPushLevels) {
            if (pushed.isEmpty()) {
                continue;
            }
            Assignments.Builder all = Assignments.builder();
            all.putAll(pushed);
            for (VariableReferenceExpression v : newLeft.getOutputVariables()) {
                if (!pushed.getMap().containsKey(v)) {
                    all.put(v, v);
                }
            }
            newLeft = new ProjectNode(
                    newLeft.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newLeft,
                    all.build(),
                    ProjectNode.Locality.LOCAL);
        }

        // Build right chain: stack pushed projects on CrossJoin.Right
        PlanNode newRight = crossJoin.getRight();
        for (Assignments pushed : rightPushLevels) {
            if (pushed.isEmpty()) {
                continue;
            }
            Assignments.Builder all = Assignments.builder();
            all.putAll(pushed);
            for (VariableReferenceExpression v : newRight.getOutputVariables()) {
                if (!pushed.getMap().containsKey(v)) {
                    all.put(v, v);
                }
            }
            newRight = new ProjectNode(
                    newRight.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newRight,
                    all.build(),
                    ProjectNode.Locality.LOCAL);
        }

        // Build new cross join
        ImmutableList.Builder<VariableReferenceExpression> newJoinOutputs = ImmutableList.builder();
        newJoinOutputs.addAll(newLeft.getOutputVariables());
        newJoinOutputs.addAll(newRight.getOutputVariables());

        JoinNode newCrossJoin = new JoinNode(
                crossJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                crossJoin.getType(),
                newLeft,
                newRight,
                crossJoin.getCriteria(),
                newJoinOutputs.build(),
                crossJoin.getFilter(),
                crossJoin.getLeftHashVariable(),
                crossJoin.getRightHashVariable(),
                crossJoin.getDistributionType(),
                crossJoin.getDynamicFilters());

        // Build residual chain above cross join
        PlanNode result = newCrossJoin;
        for (int i = 0; i < residualLevels.size(); i++) {
            Assignments res = residualLevels.get(i);
            boolean isTopLevel = (i == residualLevels.size() - 1);

            // Skip intermediate levels that are all identity (no-op pass-through)
            if (!isTopLevel && isAllIdentity(res)) {
                continue;
            }

            // For non-top-level residuals, add pass-throughs for source variables
            // not already in the residual so that upper levels can reference them
            // (e.g. variables pushed at higher levels that flow through the cross join)
            if (!isTopLevel) {
                Assignments.Builder augmented = Assignments.builder();
                augmented.putAll(res);
                for (VariableReferenceExpression v : result.getOutputVariables()) {
                    if (!res.getMap().containsKey(v)) {
                        augmented.put(v, v);
                    }
                }
                res = augmented.build();
            }

            result = new ProjectNode(
                    project.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    result,
                    res,
                    project.getLocality());
        }

        return Result.ofPlanNode(result);
    }

    private static boolean isAllIdentity(Assignments assignments)
    {
        return assignments.entrySet().stream().allMatch(
                e -> e.getValue() instanceof VariableReferenceExpression
                        && e.getValue().equals(e.getKey()));
    }
}
