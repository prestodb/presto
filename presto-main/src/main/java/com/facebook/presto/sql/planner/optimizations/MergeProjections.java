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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.TryExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

/**
 * Merges chains of consecutive projections
 */
public class MergeProjections
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            if (source instanceof ProjectNode) {
                ProjectNode sourceProject = (ProjectNode) source;
                if (isDeterministic(sourceProject) && !containsTry(node)) {
                    ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                    for (Map.Entry<Symbol, Expression> projection : node.getAssignments().entrySet()) {
                        Expression inlined = ExpressionTreeRewriter.rewriteWith(
                                new ExpressionSymbolInliner(sourceProject.getAssignments()), projection.getValue());
                        projections.put(projection.getKey(), inlined);
                    }

                    return new ProjectNode(node.getId(), sourceProject.getSource(), projections.build());
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        private static boolean isDeterministic(ProjectNode node)
        {
            return node.getAssignments().values().stream().allMatch(DeterminismEvaluator::isDeterministic);
        }

        private static boolean containsTry(ProjectNode node)
        {
            return node.getAssignments().values().stream().anyMatch(TryExpression.class::isInstance);
        }
    }
}
