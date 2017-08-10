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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Pick an arbitrary layout if none has been chosen
 */
@Deprecated
public class PickLayout
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PickLayout(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");

        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, session, symbolAllocator, idAllocator, this.sqlParser), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final TableLayoutRewriter tableLayoutRewriter;

        public Rewriter(Metadata metadata, Session session, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, SqlParser sqlParser)
        {
            this.tableLayoutRewriter = new TableLayoutRewriter(metadata, session, symbolAllocator, idAllocator, sqlParser);
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (node.getSource() instanceof TableScanNode && !((TableScanNode) node.getSource()).getLayout().isPresent()) {
                return tableLayoutRewriter.planTableScan((TableScanNode) node.getSource(), node.getPredicate()).orElse(node);
            }

            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            if (node.getLayout().isPresent()) {
                return node;
            }

            return tableLayoutRewriter.planTableScan(node, BooleanLiteral.TRUE_LITERAL).orElse(node);
        }
    }
}
