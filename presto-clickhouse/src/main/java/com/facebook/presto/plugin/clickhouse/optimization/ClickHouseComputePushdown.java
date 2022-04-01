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
package com.facebook.presto.plugin.clickhouse.optimization;

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.clickhouse.ClickHouseTableHandle;
import com.facebook.presto.plugin.clickhouse.ClickHouseTableLayoutHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ClickHouseComputePushdown
        implements ConnectorPlanOptimizer
{
    private final ExpressionOptimizer expressionOptimizer;
    private final ClickHouseFilterToSqlTranslator clickHouseFilterToSqlTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public ClickHouseComputePushdown(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution functionResolution,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            String identifierQuote,
            Set<Class<?>> functionTranslators)
    {
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(identifierQuote, "identifierQuote is null");
        requireNonNull(functionTranslators, "functionTranslators is null");
        requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        requireNonNull(functionResolution, "functionResolution is null");

        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.clickHouseFilterToSqlTranslator = new ClickHouseFilterToSqlTranslator(
                functionMetadataManager,
                buildFunctionTranslator(functionTranslators),
                identifierQuote);
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionMetadataManager);
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return maxSubplan.accept(new Visitor(session, idAllocator), null);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, null);
                if (newChild != child) {
                    changed = true;
                }
                children.add(newChild);
            }

            if (!changed) {
                return node;
            }
            return node.replaceChildren(children.build());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            TableScanNode oldTableScanNode = null;
            if (node.getSource() instanceof TableScanNode) {
                oldTableScanNode = (TableScanNode) node.getSource();
            }
            else if (node.getSource() instanceof FilterNode) {
                FilterNode oldTableFilterNode = (FilterNode) node.getSource();
                oldTableScanNode = (TableScanNode) oldTableFilterNode.getSource();
            }
            else {
                return node;
            }
            //TableScanNode oldTableScanNode = (TableScanNode) node.getSource();
            TableHandle oldTableHandle = oldTableScanNode.getTable();
            ClickHouseTableHandle oldConnectorTable = (ClickHouseTableHandle) oldTableHandle.getConnectorHandle();

            String simpleExpression = " limit " + node.getCount() + " ";

            ClickHouseTableLayoutHandle oldTableLayoutHandle = (ClickHouseTableLayoutHandle) oldTableHandle.getLayout().get();
            ClickHouseTableLayoutHandle newTableLayoutHandle = new ClickHouseTableLayoutHandle(
                    oldConnectorTable,
                    oldTableLayoutHandle.getTupleDomain(),
                    Optional.empty(), Optional.of(simpleExpression));

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getConnectorHandle(),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            TableScanNode newTableScanNode = new TableScanNode(
                    null,
                    idAllocator.getNextId(),
                    tableHandle,
                    oldTableScanNode.getOutputVariables(),
                    oldTableScanNode.getAssignments(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint());

            return new LimitNode(null, idAllocator.getNextId(), newTableScanNode, node.getCount(), node.getStep());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            TableScanNode oldTableScanNode = (TableScanNode) node.getSource();
            TableHandle oldTableHandle = oldTableScanNode.getTable();
            ClickHouseTableHandle oldConnectorTable = (ClickHouseTableHandle) oldTableHandle.getConnectorHandle();

            RowExpression predicate = expressionOptimizer.optimize(node.getPredicate(), OPTIMIZED, session);
            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);
            TranslatedExpression<ClickHouseExpression> clickHouseExpression = translateWith(
                    predicate,
                    clickHouseFilterToSqlTranslator,
                    oldTableScanNode.getAssignments());

            // TODO if jdbcExpression is not present, walk through translated subtree to find out which parts can be pushed down
            if (!oldTableHandle.getLayout().isPresent() || !clickHouseExpression.getTranslated().isPresent()) {
                return node;
            }

            ClickHouseTableLayoutHandle oldTableLayoutHandle = (ClickHouseTableLayoutHandle) oldTableHandle.getLayout().get();
            ClickHouseTableLayoutHandle newTableLayoutHandle = new ClickHouseTableLayoutHandle(
                    oldConnectorTable,
                    oldTableLayoutHandle.getTupleDomain(),
                    clickHouseExpression.getTranslated(),
                    Optional.empty());

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getConnectorHandle(),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            TableScanNode newTableScanNode = new TableScanNode(
                    null,
                    idAllocator.getNextId(),
                    tableHandle,
                    oldTableScanNode.getOutputVariables(),
                    oldTableScanNode.getAssignments(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint());

            return new FilterNode(null, idAllocator.getNextId(), newTableScanNode, node.getPredicate());
        }
    }
    private static class LimitContext
    {
        private final long count;
        private final LimitNode.Step step;

        public LimitContext(long count, LimitNode.Step step)
        {
            this.count = count;
            this.step = step;
        }

        public long getCount()
        {
            return count;
        }

        public LimitNode.Step getStep()
        {
            return step;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("count", count)
                    .add("step", step)
                    .toString();
        }
    }
}
