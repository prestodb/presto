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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isSimplifyCoalesceOverJoinKeys;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

/**
 * Simplifies redundant COALESCE expressions over equi-join keys based on join type.
 * <p>
 * For equi-join l.x = r.y:
 * <ul>
 *   <li>LEFT JOIN:  COALESCE(l.x, r.y) or COALESCE(r.y, l.x) → l.x (left key is never null)</li>
 *   <li>RIGHT JOIN: COALESCE(l.x, r.y) or COALESCE(r.y, l.x) → r.y (right key is never null)</li>
 *   <li>INNER JOIN: COALESCE(l.x, r.y) → l.x, COALESCE(r.y, l.x) → r.y (both non-null, pick first arg)</li>
 *   <li>FULL JOIN:  cannot simplify</li>
 * </ul>
 * <p>
 * This is important because tool-generated queries often produce patterns like
 * {@code SELECT COALESCE(l.x, r.y) FROM l LEFT JOIN r ON l.x = r.y} which
 * interferes with bucketed join planning.
 */
public class SimplifyCoalesceOverJoinKeys
        implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(join().capturedAs(JOIN)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isSimplifyCoalesceOverJoinKeys(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN);
        JoinType joinType = joinNode.getType();

        // FULL JOIN: both sides may be null, cannot simplify
        if (joinType == JoinType.FULL) {
            return Result.empty();
        }

        List<EquiJoinClause> criteria = joinNode.getCriteria();
        if (criteria.isEmpty()) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(joinNode.getLeft().getOutputVariables());
        Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(joinNode.getRight().getOutputVariables());

        // Build a map of join key pairs for quick lookup
        // Maps (leftVar, rightVar) pairs from equi-join criteria
        Map<VariableReferenceExpression, VariableReferenceExpression> leftToRight = new HashMap<>();
        Map<VariableReferenceExpression, VariableReferenceExpression> rightToLeft = new HashMap<>();
        for (EquiJoinClause clause : criteria) {
            leftToRight.put(clause.getLeft(), clause.getRight());
            rightToLeft.put(clause.getRight(), clause.getLeft());
        }

        Assignments assignments = project.getAssignments();
        boolean anySimplified = false;
        Assignments.Builder newAssignments = Assignments.builder();

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.getMap().entrySet()) {
            RowExpression expression = entry.getValue();
            RowExpression simplified = trySimplifyCoalesce(expression, joinType, leftVariables, rightVariables, leftToRight, rightToLeft);
            if (simplified != expression) {
                anySimplified = true;
            }
            newAssignments.put(entry.getKey(), simplified);
        }

        if (!anySimplified) {
            return Result.empty();
        }

        return Result.ofPlanNode(new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                joinNode,
                newAssignments.build(),
                project.getLocality()));
    }

    private static RowExpression trySimplifyCoalesce(
            RowExpression expression,
            JoinType joinType,
            Set<VariableReferenceExpression> leftVariables,
            Set<VariableReferenceExpression> rightVariables,
            Map<VariableReferenceExpression, VariableReferenceExpression> leftToRight,
            Map<VariableReferenceExpression, VariableReferenceExpression> rightToLeft)
    {
        if (!(expression instanceof SpecialFormExpression)) {
            return expression;
        }

        SpecialFormExpression specialForm = (SpecialFormExpression) expression;
        if (specialForm.getForm() != COALESCE) {
            return expression;
        }

        List<RowExpression> arguments = specialForm.getArguments();
        if (arguments.size() != 2) {
            return expression;
        }

        // Both arguments must be variable references
        if (!(arguments.get(0) instanceof VariableReferenceExpression) ||
                !(arguments.get(1) instanceof VariableReferenceExpression)) {
            return expression;
        }

        VariableReferenceExpression first = (VariableReferenceExpression) arguments.get(0);
        VariableReferenceExpression second = (VariableReferenceExpression) arguments.get(1);

        // Check if these two variables form an equi-join key pair
        boolean isLeftRight = leftVariables.contains(first) && rightVariables.contains(second) &&
                leftToRight.containsKey(first) && leftToRight.get(first).equals(second);
        boolean isRightLeft = rightVariables.contains(first) && leftVariables.contains(second) &&
                rightToLeft.containsKey(first) && rightToLeft.get(first).equals(second);

        if (!isLeftRight && !isRightLeft) {
            return expression;
        }

        VariableReferenceExpression leftKey = isLeftRight ? first : second;
        VariableReferenceExpression rightKey = isLeftRight ? second : first;

        switch (joinType) {
            case INNER:
                // Both sides non-null on join keys; pick the first argument
                return first;
            case LEFT:
                // Left key guaranteed non-null
                return leftKey;
            case RIGHT:
                // Right key guaranteed non-null
                return rightKey;
            default:
                return expression;
        }
    }
}
