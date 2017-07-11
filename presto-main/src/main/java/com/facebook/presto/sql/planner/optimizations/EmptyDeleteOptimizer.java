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
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * If the predicate for a delete is optimized to false, the target table scan
 * of the delete will be replaced with an empty values node. This type of
 * plan cannot be executed and is meaningless anyway, so we replace the
 * entire thing with a values node.
 */
@Deprecated
public class EmptyDeleteOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitTableFinish(TableFinishNode finish, RewriteContext<Void> context)
        {
            if (finish.getSource() instanceof ExchangeNode) {
                ExchangeNode exchange = (ExchangeNode) finish.getSource();
                if (exchange.getSources().size() == 1 && (getOnlyElement(exchange.getSources()) instanceof DeleteNode)) {
                    DeleteNode delete = (DeleteNode) getOnlyElement(exchange.getSources());
                    if (delete.getSource() instanceof ValuesNode) {
                        ValuesNode values = (ValuesNode) delete.getSource();
                        if (values.getRows().isEmpty()) {
                            return new ValuesNode(
                                    finish.getId(),
                                    finish.getOutputSymbols(),
                                    ImmutableList.of(ImmutableList.of(new LongLiteral("0"))));
                        }
                    }
                }
            }
            return finish;
        }
    }
}
