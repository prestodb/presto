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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Inlines expressions from a child project node into a parent project node
 * as long as they are referenced only once (to avoid introducing duplicate
 * computation), and that the references don't appear within a TRY block
 * (to avoid changing semantics).
 */
public class InlineProjections
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode parent = (ProjectNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) source;

        // find the references that appear just once across all
        // expressions in the parent project node
        Set<Symbol> singletonInputs = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(e -> DependencyExtractor.extractAll(e).stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() == 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (singletonInputs.isEmpty()) {
            return Optional.empty();
        }

        // exclude any inputs to TRY expression. Inlining them would potentially
        // change the semantics of those expressions
        Set<Symbol> tryArguments = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(e -> extractTryArguments(e).stream())
                .collect(Collectors.toSet());

        Set<Symbol> candidates = Sets.difference(singletonInputs, tryArguments);

        // see if any assignments for those inputs is *not* an identity projection
        boolean allIdentities = child.getAssignments()
                .entrySet().stream()
                .filter(e -> candidates.contains(e.getKey()))
                .allMatch(e -> e.getValue() instanceof SymbolReference &&
                        e.getKey().getName().equals(((SymbolReference) e.getValue()).getName()));
        if (allIdentities) {
            return Optional.empty();
        }

        // inline the expressions
        Assignments.Builder parentAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> entry : parent.getAssignments().entrySet()) {
            Expression inlined = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Object>()
            {
                @Override
                public Expression rewriteSymbolReference(SymbolReference reference, Object context, ExpressionTreeRewriter treeRewriter)
                {
                    Symbol symbol = Symbol.from(reference);
                    if (candidates.contains(symbol)) {
                        return child.getAssignments().get(symbol);
                    }

                    return reference;
                }
            }, entry.getValue());

            parentAssignments.put(entry.getKey(), inlined);
        }

        // derive identity assignments for the inputs of expressions that were inlined
        Set<Symbol> inputs = child.getAssignments()
                .entrySet().stream()
                .filter(e -> candidates.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(e -> DependencyExtractor.extractAll(e).stream())
                .collect(Collectors.toSet());

        Assignments.Builder childAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : child.getAssignments().entrySet()) {
            if (!candidates.contains(assignment.getKey())) {
                childAssignments.put(assignment.getKey(), assignment.getValue());
            }
        }
        for (Symbol input : inputs) {
            childAssignments.put(input, input.toSymbolReference());
        }

        return Optional.of(
                new ProjectNode(
                        parent.getId(),
                        new ProjectNode(
                                child.getId(),
                                child.getSource(),
                                childAssignments.build()),
                        parentAssignments.build()));
    }

    private Set<Symbol> extractTryArguments(Expression expression)
    {
        Set<Symbol> result = new HashSet<>();

        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitTryExpression(TryExpression node, Void context)
            {
                result.addAll(DependencyExtractor.extractUnique(node));
                return null;
            }
        }.process(expression, null);

        return result;
    }
}
