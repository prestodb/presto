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
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;

import java.util.Map;

/**
 * Removes unnecessary apply nodes of the form
 * <p>
 * apply(x, r -> ())
 * <p>
 * or
 * <p>
 * apply(x, r -> scalar(...)), where the scalar subquery produces no columns
 * <p>
 * by rewriting them to x
 */
public class RemoveRedundantApply
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
            // if the subquery produces no columns...
            if (node.getSubquery().getOutputSymbols().isEmpty()) {
                // and it's guaranteed to produce a single row
                if (node.getSubquery() instanceof EnforceSingleRowNode) {
                    return node.getInput();
                }
            }

            // or it's a VALUES with a single row
            if ((node.getSubquery() instanceof ValuesNode)) {
                if (((ValuesNode) node.getSubquery()).getRows().size() == 1) {
                    return node.getInput();
                }
            }

            return context.defaultRewrite(node);
        }
    }
}
