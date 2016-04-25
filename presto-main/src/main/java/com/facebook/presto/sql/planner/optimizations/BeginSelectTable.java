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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class BeginSelectTable
        implements PlanOptimizer
{
    private final Metadata metadata;

    public BeginSelectTable(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan, new Context());
    }

    private class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Context> context)
        {
            node.getSource().accept(this, context);
            context.get().getNodes().stream().forEach((n) -> metadata.beginSelect(session, n.getTable(), n.getLayout(), n.getAssignments().values()));
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            context.get().registerTableScan(node);
            node.getSources().stream().forEach((n) -> n.accept(this, context));
            return node;
        }
    }

    private static class Context
    {
        private ImmutableSet.Builder<TableScanNode> nodesBuilder = ImmutableSet.builder();

        public void registerTableScan(TableScanNode node)
        {
            nodesBuilder.add(node);
        }

        public Set<TableScanNode> getNodes()
        {
            return nodesBuilder.build();
        }
    }
}
