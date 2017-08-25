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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDynamicPartitionPruningEnabled;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.Property.property;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ApplyDynamicFilters
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = typeOf(JoinNode.class).with(property("type", JoinNode::getType).matching(INNER::equals));

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDynamicPartitionPruningEnabled(session);
    }

    @Override
    public Optional<PlanNode> apply(JoinNode node, Captures captures, Context context)
    {
        List<EquiJoinClause> criteria = node.getCriteria();
        Assignments existingAssignments = node.getDynamicFilterAssignments();

        List<EquiJoinClause> unprocessedClauses = criteria.stream()
                .filter(c -> !existingAssignments.getExpressions().contains(c.getRight().toSymbolReference()))
                .collect(toImmutableList());

        if (unprocessedClauses.isEmpty()) {
            return Optional.empty();
        }

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(existingAssignments);
        ImmutableList.Builder<Expression> predicates = ImmutableList.builder();

        for (EquiJoinClause clause : unprocessedClauses) {
            Symbol probeSymbol = clause.getLeft();
            Symbol buildSymbol = clause.getRight();
            Type buildSymbolType = requireNonNull(context.getSymbolAllocator().getTypes().get(buildSymbol));
            Symbol assignedSymbol = context.getSymbolAllocator().newSymbol("df", buildSymbolType);
            DeferredSymbolReference dynamicFilterReference = new DeferredSymbolReference(node.getId().toString(), assignedSymbol.getName());
            predicates.add(new ComparisonExpression(EQUAL, probeSymbol.toSymbolReference(), dynamicFilterReference));
            assignments.put(assignedSymbol, buildSymbol.toSymbolReference());
        }

        return Optional.of(
                new JoinNode(
                        node.getId(),
                        node.getType(),
                        new FilterNode(context.getIdAllocator().getNextId(), node.getLeft(), combineConjuncts(predicates.build())),
                        node.getRight(),
                        node.getCriteria(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        assignments.build()));
    }
}
