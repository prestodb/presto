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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.IdentityHashMap;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.base.Preconditions.checkNotNull;

public class SimplifyExpressions
        extends PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public SimplifyExpressions(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, ConnectorSession session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(metadata, sqlParser, session, types), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final ConnectorSession session;
        private final Map<Symbol, Type> types;

        public Rewriter(Metadata metadata, SqlParser sqlParser, ConnectorSession session, Map<Symbol, Type> types)
        {
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.session = session;
            this.types = types;
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            Map<Symbol, Expression> assignments = ImmutableMap.copyOf(Maps.transformValues(node.getOutputMap(), simplifyExpressionFunction()));
            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            Expression simplified = simplifyExpression(node.getPredicate());
            if (simplified.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(node.getId(), source, simplified);
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = simplifyExpression(node.getOriginalConstraint());
            }
            return new TableScanNode(node.getId(), node.getTable(), node.getOutputSymbols(), node.getAssignments(), originalConstraint, node.getSummarizedPartition());
        }

        private Function<Expression, Expression> simplifyExpressionFunction()
        {
            return new Function<Expression, Expression>()
            {
                @Override
                public Expression apply(Expression input)
                {
                    return simplifyExpression(input);
                }
            };
        }

        private Expression simplifyExpression(Expression input)
        {
            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, types, input);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(input, metadata, session, expressionTypes);
            return LiteralInterpreter.toExpression(interpreter.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(input));
        }
    }
}
