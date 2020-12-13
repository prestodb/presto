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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getFirst;

public class PruneRedundantProjectionAssignments
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        Map<Boolean, List<Map.Entry<VariableReferenceExpression, RowExpression>>> projections = node.getAssignments().entrySet().stream()
                .collect(Collectors.partitioningBy(entry -> entry.getValue() instanceof VariableReferenceExpression || entry.getValue() instanceof ConstantExpression));
        Map<RowExpression, ImmutableMap<VariableReferenceExpression, RowExpression>> uniqueProjections = projections.get(false).stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue, toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        if (uniqueProjections.size() == projections.get(false).size()) {
            return Result.empty();
        }
        Assignments.Builder childAssignments = Assignments.builder();
        Assignments.Builder parentAssignments = Assignments.builder();
        projections.get(true).forEach(entry -> childAssignments.put(entry.getKey(), entry.getValue()));
        projections.get(true).forEach(entry -> parentAssignments.put(entry.getKey(), entry.getKey()));
        for (Map.Entry<RowExpression, ImmutableMap<VariableReferenceExpression, RowExpression>> entry : uniqueProjections.entrySet()) {
            VariableReferenceExpression variable = getFirst(entry.getValue().keySet(), null);
            checkState(variable != null, "variable should not be null");
            childAssignments.put(variable, entry.getKey());
            entry.getValue().keySet().forEach(v -> parentAssignments.put(v, variable));
        }
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(node.getId(), node.getSource(), childAssignments.build(), node.getLocality()),
                        parentAssignments.build(),
                        LOCAL));
    }
}
