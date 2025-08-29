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
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcSessionProperties;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.optimization.function.JoinOperatorTranslators;
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
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeSqlBodies;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeVariableBindings;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcComputePushdown
        implements ConnectorPlanOptimizer
{
    private final ExpressionOptimizerProvider expressionOptimizerProvider;
    private final JdbcFilterToSqlTranslator jdbcFilterToSqlTranslator;
    private final JdbcJoinPredicateToSqlTranslator jdbcJoinPredicateToSqlTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public JdbcComputePushdown(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution functionResolution,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizerProvider expressionOptimizerProvider,
            String identifierQuote,
            Set<Class<?>> functionTranslators)
    {
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(identifierQuote, "identifierQuote is null");
        requireNonNull(functionTranslators, "functionTranslators is null");
        requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        requireNonNull(functionResolution, "functionResolution is null");

        this.expressionOptimizerProvider = requireNonNull(expressionOptimizerProvider, "expressionOptimizerProvider is null");
        this.jdbcFilterToSqlTranslator = new JdbcFilterToSqlTranslator(
                functionMetadataManager,
                buildFunctionTranslator(functionTranslators),
                identifierQuote);
        this.jdbcJoinPredicateToSqlTranslator = new JdbcJoinPredicateToSqlTranslator(
                functionMetadataManager,
                buildFunctionTranslator(ImmutableSet.of(JoinOperatorTranslators.class)),
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
            RowExpression predicate = expressionOptimizerProvider.getExpressionOptimizer(session).optimize(node.getPredicate(), OPTIMIZED, session);
            predicate = logicalRowExpressions.convertToConjunctiveNormalForm(predicate);

            JdbcTableHandle tableHandle = (JdbcTableHandle) oldTableScanNode.getTable().getConnectorHandle();
            RowExpressionTranslator translator = tableHandle.hasJoinTables() ? jdbcJoinPredicateToSqlTranslator : jdbcFilterToSqlTranslator;
            TranslatedExpression<JdbcExpression> jdbcExpression = translateWith(
                    predicate,
                    translator,
                    oldTableScanNode.getAssignments());
            Optional<JdbcExpression> translated = jdbcExpression.getTranslated();
            JdbcTableLayoutHandle oldTableLayoutHandle = (JdbcTableLayoutHandle) oldTableHandle.getLayout().get();
            // All filter can be pushed down
            if (translated.isPresent()) {
                return createNewTableScanNode(oldTableScanNode, oldTableHandle, oldTableLayoutHandle, translated);
            }
            if (JdbcSessionProperties.isPartialPredicatePushDownEnabled(this.session)) {
//                Copied from the PR to enhance pushing part filters down to the tablehandle
//                - https://github.com/prestodb/presto/pull/16412/files.
//                This will find out which parts can be pushed down and able to translate by connector,
//                and the remaining part will keep in a new FilterNode.

                // Find out which parts can be pushed down
                ImmutableList.Builder<RowExpression> remainingExpressionsBuilder = ImmutableList.builder();
                ImmutableList.Builder<JdbcExpression> translatedExpressionsBuilder = ImmutableList.builder();

                predicate = expressionOptimizerProvider.getExpressionOptimizer(session).optimize(node.getPredicate(), SERIALIZABLE, session);
                List<RowExpression> rowExpressions = LogicalRowExpressions.extractConjuncts(predicate);
                for (RowExpression expression : rowExpressions) {
                    TranslatedExpression<JdbcExpression> translatedExpression = translateWith(
                            expression,
                            translator,
                            oldTableScanNode.getAssignments());

                    if (!translatedExpression.getTranslated().isPresent()) {
                        remainingExpressionsBuilder.add(expression);
                    }
                    else {
                        translatedExpressionsBuilder.add(translatedExpression.getTranslated().get());
                    }
                }

                List<RowExpression> remainingExpressions = remainingExpressionsBuilder.build();
                List<JdbcExpression> translatedExpressions = translatedExpressionsBuilder.build();

                // no filter can be pushed down
                if (!remainingExpressions.isEmpty() && translatedExpressions.isEmpty()) {
                    return node;
                }

                List<String> sqlBodies = mergeSqlBodies(translatedExpressions);
                List<ConstantExpression> variableBindings = mergeVariableBindings(translatedExpressions);
                translated = Optional.of(new JdbcExpression(format("%s", Joiner.on(" AND ").join(sqlBodies)), variableBindings));
                TableScanNode newTableScanNode = createNewTableScanNode(oldTableScanNode, oldTableHandle, oldTableLayoutHandle, translated);

                RowExpression remainingPredicates = logicalRowExpressions.combineConjuncts(remainingExpressions);
                return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), newTableScanNode, remainingPredicates);
            }
            return node;
        }

        private TableScanNode createNewTableScanNode(
                TableScanNode oldTableScanNode,
                TableHandle oldTableHandle,
                JdbcTableLayoutHandle oldTableLayoutHandle,
                Optional<JdbcExpression> additionalPredicate)
        {
            JdbcTableLayoutHandle newTableLayoutHandle = new JdbcTableLayoutHandle(
                    oldTableLayoutHandle.getTable(),
                    oldTableLayoutHandle.getTupleDomain(),
                    additionalPredicate,
                    oldTableLayoutHandle.getLayoutString());

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getConnectorHandle(),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            return new TableScanNode(
                    oldTableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableHandle,
                    oldTableScanNode.getOutputVariables(),
                    oldTableScanNode.getAssignments(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint(),
                    oldTableScanNode.getCteMaterializationInfo());
        }
    }
}
