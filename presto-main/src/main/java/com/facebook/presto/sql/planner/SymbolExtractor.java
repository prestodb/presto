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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Computes all symbols declared by a logical plan
 */
public final class SymbolExtractor
{
    private SymbolExtractor() {}

    public static Set<Symbol> extract(PlanNode node)
    {
        Visitor visitor = new Visitor();
        node.accept(visitor, null);

        return visitor.getSymbols();
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final ImmutableSet.Builder<Symbol> symbols = ImmutableSet.builder();

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            symbols.addAll(node.getOutputSymbols());
            return super.visitPlan(node, context);
        }

        public Set<Symbol> getSymbols()
        {
            return symbols.build();
        }
    }
}
