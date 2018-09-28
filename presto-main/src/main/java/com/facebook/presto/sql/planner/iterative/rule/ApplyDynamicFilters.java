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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SystemSessionProperties.isEnableDynamicFiltering;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.Property.property;
import static com.facebook.presto.sql.DynamicFilters.createDynamicFilterExpression;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class ApplyDynamicFilters
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = typeOf(JoinNode.class).with(property("type", JoinNode::getType).matching(INNER::equals));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnableDynamicFiltering(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (!node.getDynamicFilters().isEmpty() || node.getCriteria().isEmpty()) {
            return Result.empty();
        }

        ImmutableMap.Builder<String, Symbol> dynamicFilters = ImmutableMap.builder();
        ImmutableList.Builder<Expression> predicates = ImmutableList.builder();

        for (EquiJoinClause clause : node.getCriteria()) {
            Symbol probeSymbol = clause.getLeft();
            Symbol buildSymbol = clause.getRight();
            String id = context.getIdAllocator().getNextId().toString();
            predicates.add(createDynamicFilterExpression(id, probeSymbol.toSymbolReference()));
            dynamicFilters.put(id, buildSymbol);
        }

        return Result.ofPlanNode(
                new JoinNode(
                        node.getId(),
                        node.getType(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                node.getLeft(),
                                combineConjuncts(predicates.build())),
                        node.getRight(),
                        node.getCriteria(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        dynamicFilters.build()));
    }
}
