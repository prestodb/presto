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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.DependencyExtractor.extractUnique;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;

public class PushFilter
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof FilterNode)) {
            return Optional.empty();
        }

        FilterNode parent = (FilterNode) node;

        PlanNode source = lookup.resolve(parent.getSource());

        Set<Symbol> pushableSymbols = getPushableSymbols(source);
        if (pushableSymbols.isEmpty()) {
            return Optional.empty();
        }

        return pushFilter(parent, source, pushableSymbols, idAllocator);
    }

    private static Optional<PlanNode> pushFilter(FilterNode parent, PlanNode child, Set<Symbol> pushableSymbols, PlanNodeIdAllocator idAllocator)
    {
        List<Expression> pushableConjuncts = extractConjuncts(parent.getPredicate())
                .stream()
                .filter(conjunct -> difference(extractUnique(conjunct), pushableSymbols).isEmpty())
                .collect(toImmutableList());

        List<Expression> unpushableConjucts = extractConjuncts(parent.getPredicate())
                .stream()
                .filter(conjunct -> !difference(extractUnique(conjunct), pushableSymbols).isEmpty())
                .collect(toImmutableList());

        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }

        PlanNode result = child.replaceChildren(ImmutableList.of(new FilterNode(
                idAllocator.getNextId(),
                getOnlyElement(child.getSources()),
                combineConjuncts(pushableConjuncts))));

        if (!unpushableConjucts.isEmpty()) {
            result = new FilterNode(idAllocator.getNextId(), result, combineConjuncts(unpushableConjucts));
        }

        return Optional.of(result);
    }

    private static Set<Symbol> getPushableSymbols(PlanNode planNode)
    {
        if (planNode instanceof SortNode || planNode instanceof SampleNode) {
            return ImmutableSet.copyOf(planNode.getOutputSymbols());
        }
        else if (planNode instanceof AssignUniqueId || planNode instanceof MarkDistinctNode) {
            return ImmutableSet.copyOf(getOnlyElement(planNode.getSources()).getOutputSymbols());
        }
        else {
            return ImmutableSet.of();
        }
    }
}
