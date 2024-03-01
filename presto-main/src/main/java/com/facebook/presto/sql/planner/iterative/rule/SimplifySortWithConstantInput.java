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
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SortNode;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isRewriteExpressionWithConstantEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.sort;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class SimplifySortWithConstantInput
        implements Rule<SortNode>
{
    private static final Capture<ProjectNode> SOURCE = Capture.newCapture();

    private static final Pattern<SortNode> PATTERN = sort().with(source().matching(
            project().matching(project -> project.getAssignments().getMap().values().stream().anyMatch(x -> x instanceof ConstantExpression)).capturedAs(SOURCE)));

    @Override
    public Pattern<SortNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteExpressionWithConstantEnabled(session);
    }

    @Override
    public Result apply(SortNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(SOURCE);
        Map<VariableReferenceExpression, ConstantExpression> constantExpressionMap = projectNode.getAssignments().entrySet().stream().filter(x -> x.getValue() instanceof ConstantExpression)
                .collect(toImmutableMap(Map.Entry::getKey, x -> (ConstantExpression) x.getValue()));
        if (node.getOrderingScheme().getOrderByVariables().stream().anyMatch(x -> constantExpressionMap.containsKey(x))) {
            List<Ordering> newOrderBy = node.getOrderingScheme().getOrderBy().stream()
                    .filter(x -> !constantExpressionMap.containsKey(x.getVariable())).collect(toImmutableList());
            if (newOrderBy.isEmpty()) {
                return Result.ofPlanNode(projectNode);
            }
            OrderingScheme orderExcludeConstantVariable = new OrderingScheme(newOrderBy);
            return Result.ofPlanNode(new SortNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), projectNode, orderExcludeConstantVariable, node.isPartial()));
        }
        return Result.empty();
    }
}
