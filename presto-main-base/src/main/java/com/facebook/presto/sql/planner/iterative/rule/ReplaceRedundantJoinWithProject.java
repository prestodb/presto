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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

public class ReplaceRedundantJoinWithProject
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();

        List<VariableReferenceExpression> leftOutputVariables = node.getOutputVariables().stream()
                .filter(variable -> left.getOutputVariables().contains(variable))
                .collect(Collectors.toList());

        List<VariableReferenceExpression> rightOutputVariables = node.getOutputVariables().stream()
                .filter(variable -> right.getOutputVariables().contains(variable))
                .collect(Collectors.toList());

        switch (node.getType()) {
            case INNER:
                return Result.empty();
            case LEFT:
                return !isEmpty(left, lookup) && isEmpty(right, lookup) ?
                        Result.ofPlanNode(appendNulls(
                                left,
                                leftOutputVariables,
                                rightOutputVariables,
                                context.getIdAllocator()
                        )) :
                        Result.empty();
            case RIGHT:
                return isEmpty(left, lookup) && !isEmpty(right, lookup) ?
                        Result.ofPlanNode(appendNulls(
                                right,
                                rightOutputVariables,
                                leftOutputVariables,
                                context.getIdAllocator()
                        )) :
                        Result.empty();
            case FULL:
                if (isEmpty(left, lookup) && !isEmpty(right, lookup)) {
                    return Result.ofPlanNode(appendNulls(
                            right,
                            rightOutputVariables,
                            leftOutputVariables,
                            context.getIdAllocator()));
                }
                if (!isEmpty(left, lookup) && isEmpty(right, lookup)) {
                    return Result.ofPlanNode(appendNulls(
                            left,
                            leftOutputVariables,
                            rightOutputVariables,
                            context.getIdAllocator()));
                }
                return Result.empty();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static ProjectNode appendNulls(PlanNode source, List<VariableReferenceExpression> sourceOutputs, List<VariableReferenceExpression> nullVariables, PlanNodeIdAllocator idAllocator)
    {
        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(sourceOutputs);
        nullVariables
                .forEach(variable -> assignments.put(variable, new ConstantExpression(null, variable.getType())));

        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }
}
