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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Evaluates a constant Apply expression. For example:
 * <p>
 * apply(values(a, b), r -> values(x, y))
 * <p>
 * into
 * <p>
 * values((a, x), (a, y), (b, x), (b, y))
 */
public class EvaluateConstantApply
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            if (!node.getCorrelation().isEmpty()) {
                // TODO: we should be able to support this by "evaluating" the
                // expressions on the inner side after binding the values from
                // the outer side
                return context.defaultRewrite(node);
            }

            if (!(node.getInput() instanceof ValuesNode) || !(node.getSubquery() instanceof ValuesNode)) {
                return context.defaultRewrite(node);
            }

            ValuesNode outer = (ValuesNode) node.getInput();
            ValuesNode inner = (ValuesNode) node.getSubquery();

            // semantics of apply are similar to a cross join
            ImmutableList.Builder<List<Expression>> result = ImmutableList.builder();

            for (List<Expression> outerRow : outer.getRows()) {
                for (List<Expression> innerRow : inner.getRows()) {
                    result.add(ImmutableList.<Expression>builder()
                            .addAll(outerRow)
                            .addAll(innerRow)
                            .build());
                }
            }

            return new ValuesNode(node.getId(), node.getOutputSymbols(), result.build());
        }
    }
}
