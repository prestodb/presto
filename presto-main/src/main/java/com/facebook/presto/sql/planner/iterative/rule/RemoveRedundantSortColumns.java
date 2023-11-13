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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SortNode;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.sort;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Removes sort columns from input if the source has a Key that refers to the ordering columns
 */
public class RemoveRedundantSortColumns
        implements Rule<SortNode>
{
    private static final Logger log = Logger.get(RemoveRedundantSortColumns.class);
    private static final Pattern<SortNode> PATTERN = sort();

    @Override
    public Pattern<SortNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SortNode node, Captures captures, Context context)
    {
        OrderingScheme orderingScheme = node.getOrderingScheme();
        PlanNode source = node.getSource();
        log.debug("[%s] SortNode : %s", node.getId(), node);

        Optional<LogicalProperties> sourceLogicalProperties = ((GroupReference) source).getLogicalProperties();
        OrderingScheme newOrderingScheme = pruneOrderingColumns(orderingScheme, source, sourceLogicalProperties);

        if (newOrderingScheme.equals(orderingScheme)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new SortNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, newOrderingScheme, node.isPartial()));
    }

    public static OrderingScheme pruneOrderingColumns(OrderingScheme nodeOrderingScheme, PlanNode source, Optional<LogicalProperties> sourceLogicalProperties)
    {
        if (sourceLogicalProperties.isPresent()) {
            LogicalProperties logicalProperties = sourceLogicalProperties.get();
            Set<VariableReferenceExpression> orderingVariables = nodeOrderingScheme.getOrderingsMap().keySet();
            log.debug("Current Node order variables: %s%nLogical properties for source [%s] : %s", orderingVariables, source.getId(), logicalProperties);

            Set<VariableReferenceExpression> smallestKeyVariables = logicalProperties.getSmallestKeyVariablesSet(orderingVariables);
            if (smallestKeyVariables.isEmpty()) {
                log.debug("No key variables found");
                return nodeOrderingScheme;
            }

            return new OrderingScheme(nodeOrderingScheme.getOrderBy().stream()
                    .filter(ordering -> smallestKeyVariables.contains(ordering.getVariable()))
                    .collect(toImmutableList()));
        }

        return nodeOrderingScheme;
    }
}
