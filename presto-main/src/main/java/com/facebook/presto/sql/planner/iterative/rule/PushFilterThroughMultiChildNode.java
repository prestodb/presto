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

import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static com.facebook.presto.sql.planner.optimizations.Predicates.predicate;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;

public class PushFilterThroughMultiChildNode
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof FilterNode)) {
            return Optional.empty();
        }

        FilterNode parent = (FilterNode) node;

        PlanNode source = lookup.resolve(parent.getSource());

        List<Expression> conjuncts = extractConjuncts(parent.getPredicate());

        List<Expression> pushableConjuncts = pushableConjuncts(source, conjuncts);

        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }

        List<Expression> unpushableConjuncts = unpushableConjuncts(source, conjuncts);

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (int i = 0; i < source.getSources().size(); i++) {
            Map<Symbol, SymbolReference> mappings = getMapping(source, i);
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

    private static Map<Symbol, SymbolReference> getMapping(PlanNode planNode, int index)
    {
        if (planNode instanceof SetOperationNode) {
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

        throw new IllegalStateException("Unsupported node: " + planNode);
    }

    private static List<Expression> pushableConjuncts(PlanNode planNode, List<Expression> conjuncts)
    {
        if (planNode instanceof UnionNode || planNode instanceof ExchangeNode) {
            return conjuncts;
        }
        else if (planNode instanceof SetOperationNode) {
            return conjuncts.stream()
                    .filter(DeterminismEvaluator::isDeterministic)
                    .collect(toImmutableList());
        }
        else {
            return ImmutableList.of();
        }
    }

    private static List<Expression> unpushableConjuncts(PlanNode planNode, List<Expression> conjuncts)
    {
        if (planNode instanceof UnionNode || planNode instanceof ExchangeNode) {
            return emptyList();
        }

        return conjuncts.stream()
                .filter(predicate(DeterminismEvaluator::isDeterministic).negate())
                .collect(toImmutableList());
    }
}
