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
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static com.facebook.presto.sql.planner.optimizations.Predicates.predicate;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
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

        if (!(source instanceof SetOperationNode)) {
            return Optional.empty();
        }

        List<Expression> conjuncts = extractConjuncts(parent.getPredicate());
        SetOperationNode child = (SetOperationNode) source;

        List<Expression> pushableConjuncts = pushableConjuncts(child, conjuncts);

        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }

        List<Expression> unpushableConjuncts = unpushableConjuncts(child, conjuncts);

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (int i = 0; i < child.getSources().size(); i++) {
            Map<Symbol, SymbolReference> mappings = child.sourceSymbolMap(i);
            List<Expression> rewrittenConjuncts = pushableConjuncts.stream()
                    .map(conjunct -> inlineSymbols(mappings, conjunct))
                    .collect(toImmutableList());

            builder.add(new FilterNode(idAllocator.getNextId(), child.getSources().get(i), combineConjuncts(rewrittenConjuncts)));
        }

        PlanNode rewrittenChild = child.replaceChildren(builder.build());
        if (unpushableConjuncts.isEmpty()) {
            return Optional.of(rewrittenChild);
        }
        else {
            return Optional.of(new FilterNode(idAllocator.getNextId(), rewrittenChild, combineConjuncts(unpushableConjuncts)));
        }
    }

    private List<Expression> pushableConjuncts(SetOperationNode child, List<Expression> conjuncts)
    {
        if (child instanceof UnionNode) {
            return conjuncts;
        }

        return conjuncts.stream()
                .filter(DeterminismEvaluator::isDeterministic)
                .collect(toImmutableList());
    }

    private List<Expression> unpushableConjuncts(SetOperationNode child, List<Expression> conjuncts)
    {
        if (child instanceof UnionNode) {
            return emptyList();
        }

        return conjuncts.stream()
                .filter(predicate(DeterminismEvaluator::isDeterministic).negate())
                .collect(toImmutableList());
    }
}
