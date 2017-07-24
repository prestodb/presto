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
import com.facebook.presto.sql.planner.DesugarAtTimeZoneRewriter;
import com.facebook.presto.sql.planner.DesugarTryExpressionRewriter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DesugaringOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public DesugaringOptimizer(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, sqlParser, session, types, symbolAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final Session session;
        private final Map<Symbol, Type> types;
        private final SymbolAllocator symbolAllocator;

        public Rewriter(Metadata metadata, SqlParser sqlParser, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator)
        {
            this.metadata = metadata;
            this.sqlParser = sqlParser;
            this.session = session;
            this.types = types;
            this.symbolAllocator = symbolAllocator;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            checkState(extractExpressionsNonRecursive(node).isEmpty(), "Unhandled plan node with expressions");
            return super.visitPlan(node, context);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Map<Symbol, Aggregation> aggregations = node.getAggregations().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> {
                        Aggregation aggregation = entry.getValue();
                        return new Aggregation((FunctionCall) desugar(aggregation.getCall()), aggregation.getSignature(), aggregation.getMask(), aggregation.getOrderBy(), aggregation.getOrdering());
                    }));
            return new AggregationNode(
                    node.getId(),
                    source,
                    aggregations,
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Assignments assignments = node.getAssignments().rewrite(this::desugar);
            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Expression cleaned = desugar(node.getPredicate());
            return new FilterNode(node.getId(), source, cleaned);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = desugar(node.getOriginalConstraint());
            }
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    originalConstraint);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = context.rewrite(node.getLeft());
            PlanNode right = context.rewrite(node.getRight());
            Optional<Expression> filter = node.getFilter().map(this::desugar);
            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    node.getOutputSymbols(),
                    filter,
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            return new ValuesNode(
                    node.getId(),
                    node.getOutputSymbols(),
                    node.getRows().stream()
                            .map(row -> row.stream()
                                    .map(this::desugar)
                                    .collect(toImmutableList()))
                            .collect(toImmutableList()));
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            PlanNode input = context.rewrite(node.getInput());
            PlanNode subquery = context.rewrite(node.getSubquery());
            // ApplyNode.Assignments are synthetic expressions which are meaningful for ApplyNode transformations.
            // They cannot contain any lambda or "sugared" expression
            return new ApplyNode(node.getId(), input, subquery, node.getSubqueryAssignments(), node.getCorrelation(), node.getOriginSubquery());
        }

        private Expression desugar(Expression expression)
        {
            expression = DesugarTryExpressionRewriter.rewrite(expression);
            expression = LambdaCaptureDesugaringRewriter.rewrite(expression, symbolAllocator.getTypes(), symbolAllocator);
            expression = DesugarAtTimeZoneRewriter.rewrite(expression, session, metadata, sqlParser, symbolAllocator);

            return expression;
        }
    }
}
