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
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.DependencyExtractor.extractUnique;
import static com.facebook.presto.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static com.facebook.presto.sql.planner.optimizations.Predicates.predicate;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
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

        List<Expression> conjuncts = extractConjuncts(parent.getPredicate());

        List<Expression> pushableConjuncts = filterPushableConjuncts(source, conjuncts);

        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }

        List<Expression> unpushableConjuncts = conjuncts.stream()
                .filter(predicate(pushableConjuncts::contains).negate())
                .collect(toImmutableList());

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (int i = 0; i < source.getSources().size(); i++) {
            Map<Symbol, ? extends Expression> mappings = getMapping(source, i);
            List<Expression> rewrittenConjuncts = pushableConjuncts.stream()
                    .map(conjunct -> inlineSymbols(mappings, conjunct))
                    .collect(toImmutableList());

            builder.add(filter(idAllocator, rewrittenConjuncts, source.getSources().get(i)));
        }

        PlanNode rewrittenSource = source.replaceChildren(builder.build());
        if (unpushableConjuncts.isEmpty()) {
            return Optional.of(rewrittenSource);
        }
        else {
            return Optional.of(filter(idAllocator, unpushableConjuncts, rewrittenSource));
        }
    }

    @NotNull
    private static FilterNode filter(PlanNodeIdAllocator idAllocator, List<Expression> conjuncts, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, combineConjuncts(conjuncts));
    }

    private static Map<Symbol, ? extends Expression> getMapping(PlanNode planNode, int index)
    {
        if (planNode instanceof AssignUniqueId
                || planNode instanceof MarkDistinctNode
                || planNode instanceof SampleNode
                || planNode instanceof SortNode) {
            return planNode.getOutputSymbols().stream()
                    .collect(toImmutableMap(Function.identity(), Symbol::toSymbolReference));
        }
        else if (planNode instanceof SetOperationNode) {
            return ((SetOperationNode) planNode).sourceSymbolMap(index);
        }
        else if (planNode instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) planNode;
            List<Symbol> output = exchangeNode.getOutputSymbols();
            List<Symbol> input = exchangeNode.getInputs().get(index);
            checkState(input.size() == output.size(), "Exchange input and output does not match");
            return IntStream.range(0, input.size())
                    .mapToObj(Integer::valueOf)
                    .collect(toImmutableMap(output::get, i -> input.get(i).toSymbolReference()));
        }
        else if (planNode instanceof ProjectNode) {
            return ((ProjectNode) planNode).getAssignments().getMap();
        }
        throw new IllegalStateException("Unsupported node: " + planNode);
    }

    private static List<Expression> filterPushableConjuncts(PlanNode planNode, List<Expression> conjuncts)
    {
        if (planNode instanceof UnionNode
                || planNode instanceof ExchangeNode
                || planNode instanceof SortNode
                || planNode instanceof SampleNode) {
            return conjuncts;
        }
        else if (planNode instanceof SetOperationNode) {
            return conjuncts.stream()
                    .filter(DeterminismEvaluator::isDeterministic)
                    .collect(toImmutableList());
        }
        else if (planNode instanceof AssignUniqueId || planNode instanceof MarkDistinctNode) {
            PlanNode source = getOnlyElement(planNode.getSources());
            Set<Symbol> pushableSymbols = ImmutableSet.copyOf(source.getOutputSymbols());
            return conjuncts.stream()
                    .filter(conjunct -> difference(extractUnique(conjunct), pushableSymbols).isEmpty())
                    .collect(toImmutableList());
        }
        else if (planNode instanceof ProjectNode) {
            Set<Symbol> deterministicSymbols = ((ProjectNode) planNode).getAssignments().entrySet().stream()
                    .filter(entry -> DeterminismEvaluator.isDeterministic(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());

            return conjuncts.stream()
                    .filter(conjunct -> extractUnique(conjunct).stream().allMatch(deterministicSymbols::contains))
                    .collect(toImmutableList());
        }
        else {
            return ImmutableList.of();
        }
    }
}
