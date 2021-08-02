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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeSqlBodies;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeVariableBindings;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcComputePushdown
        implements ConnectorPlanOptimizer
{
    private final ExpressionOptimizer expressionOptimizer;
    private final JdbcFilterToSqlTranslator jdbcFilterToSqlTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public JdbcComputePushdown(
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
        this.jdbcFilterToSqlTranslator = new JdbcFilterToSqlTranslator(
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
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            TableScanNode oldTableScanNode = (TableScanNode) node.getSource();
            TableHandle oldTableHandle = oldTableScanNode.getTable();
            if (!oldTableHandle.getLayout().isPresent()) {
                return node;
            }

            RowExpression predicate = expressionOptimizer.optimize(node.getPredicate(), OPTIMIZED, session);
            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);
            TranslatedExpression<JdbcExpression> jdbcExpression = translateWith(
                    predicate,
                    jdbcFilterToSqlTranslator,
                    oldTableScanNode.getAssignments());

            Optional<JdbcExpression> translated = jdbcExpression.getTranslated();
            JdbcTableHandle oldConnectorTable = (JdbcTableHandle) oldTableHandle.getConnectorHandle();
            // All filter can be pushed down
            if (translated.isPresent()) {
                return createNewTableScanNode(oldTableScanNode, oldTableHandle, oldConnectorTable, translated);
            }

            // Find out which parts can be pushed down
            List<RowExpression> remainingExpressions = new ArrayList<>();
            List<JdbcExpression> translatedExpressions = new ArrayList<>();

            List<RowExpression> rowExpressions = LogicalRowExpressions.extractConjuncts(predicate);
            for (RowExpression expression : rowExpressions) {
                TranslatedExpression<JdbcExpression> translatedExpression = translateWith(
                        expression,
                        jdbcFilterToSqlTranslator,
                        oldTableScanNode.getAssignments());

                if (!translatedExpression.getTranslated().isPresent()) {
                    remainingExpressions.add(expression);
                }
                else {
                    translatedExpressions.add(translatedExpression.getTranslated().get());
                }
            }

            // no filter can be pushed down
            if (!remainingExpressions.isEmpty() && translatedExpressions.isEmpty()) {
                return node;
            }

            List<String> sqlBodies = mergeSqlBodies(translatedExpressions);
            List<ConstantExpression> variableBindings = mergeVariableBindings(translatedExpressions);
            translated = Optional.of(new JdbcExpression(format("%s", Joiner.on(" AND ").join(sqlBodies)), variableBindings));
            TableScanNode newTableScanNode = createNewTableScanNode(oldTableScanNode, oldTableHandle, oldConnectorTable, translated);

            return new FilterNode(idAllocator.getNextId(), newTableScanNode, logicalRowExpressions.combineConjuncts(remainingExpressions));
        }

        private TableScanNode createNewTableScanNode(
                TableScanNode oldTableScanNode,
                TableHandle oldTableHandle,
                JdbcTableHandle oldConnectorTable,
                Optional<JdbcExpression> additionalPredicate)
        {
            JdbcTableLayoutHandle oldTableLayoutHandle = (JdbcTableLayoutHandle) oldTableHandle.getLayout().get();
            JdbcTableLayoutHandle newTableLayoutHandle = new JdbcTableLayoutHandle(
                    oldConnectorTable,
                    oldTableLayoutHandle.getTupleDomain(),
                    additionalPredicate);

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getConnectorHandle(),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            return new TableScanNode(
                    idAllocator.getNextId(),
                    tableHandle,
                    oldTableScanNode.getOutputVariables(),
                    oldTableScanNode.getAssignments(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint());
        }
    }
}
