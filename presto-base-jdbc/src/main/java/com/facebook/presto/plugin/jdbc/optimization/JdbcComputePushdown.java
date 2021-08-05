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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeSqlBodies;
import static com.facebook.presto.plugin.jdbc.optimization.function.JdbcTranslationUtil.mergeVariableBindings;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Maps.immutableEntry;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcComputePushdown
        implements ConnectorPlanOptimizer
{
    private final JdbcClient jdbcClient;
    private final ExpressionOptimizer expressionOptimizer;
    private final JdbcFilterToSqlTranslator jdbcFilterToSqlTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public JdbcComputePushdown(
            JdbcClient jdbcClient,
            TypeManager typeManager,
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
        requireNonNull(typeManager, "typeManager is null");

        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.jdbcFilterToSqlTranslator = new JdbcFilterToSqlTranslator(
                typeManager,
                functionMetadataManager,
                buildFunctionTranslator(functionTranslators),
                functionResolution,
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
        JdbcQueryGeneratorContext context = new JdbcQueryGeneratorContext();
        return maxSubplan.accept(new Visitor(session, idAllocator), context);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, JdbcQueryGeneratorContext>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, JdbcQueryGeneratorContext context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, context);
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
        public PlanNode visitProject(ProjectNode node, JdbcQueryGeneratorContext context)
        {
            PlanNode planNode = node.getSource().accept(this, context);
            if (!(planNode instanceof TableScanNode)) {
                if (node.getSource() == planNode) {
                    return node;
                }
                else {
                    return node.replaceChildren(ImmutableList.of(planNode));
                }
            }

            Assignments assignments = node.getAssignments();
            boolean allVariableReferenceExpression = assignments.entrySet().stream()
                    .allMatch(assignment -> assignment.getValue() instanceof VariableReferenceExpression);

            if (allVariableReferenceExpression) {
                TableScanNode oldTableScanNode = (TableScanNode) planNode;
                LinkedHashMap<VariableReferenceExpression, ColumnHandle> newAssignments = node.getAssignments().getMap().entrySet()
                        .stream()
                        .map(assignment -> {
                            VariableReferenceExpression value = (VariableReferenceExpression) assignment.getValue();
                            return immutableEntry(assignment.getKey(), oldTableScanNode.getAssignments().get(value));
                        }).collect(Collectors.toMap(Map.Entry::getKey,
                                Map.Entry::getValue,
                                (val1, val2) -> val1,
                                LinkedHashMap::new));

                return new TableScanNode(
                        idAllocator.getNextId(),
                        oldTableScanNode.getTable(),
                        node.getOutputVariables(),
                        newAssignments,
                        oldTableScanNode.getCurrentConstraint(),
                        oldTableScanNode.getEnforcedConstraint());
            }

            return node;
        }

        @Override
        public PlanNode visitLimit(LimitNode node, JdbcQueryGeneratorContext context)
        {
            PlanNode planNode = node.getSource().accept(this, context);
            if (!(planNode instanceof TableScanNode)) {
                if (node.getSource() == planNode) {
                    return node;
                }
                else {
                    return node.replaceChildren(ImmutableList.of(planNode));
                }
            }

            if (!jdbcClient.supportsLimit()) {
                return new LimitNode(idAllocator.getNextId(), planNode, node.getCount(), node.getStep());
            }

            long count = node.getCount();
            TableScanNode tableScanNode = (TableScanNode) planNode;
            TableHandle handle = tableScanNode.getTable();
            Optional<ConnectorTableLayoutHandle> oldLayout = handle.getLayout();
            if (oldLayout.isPresent()) {
                JdbcTableHandle oldConnectorTable = (JdbcTableHandle) handle.getConnectorHandle();
                return createNewTableScanNode(tableScanNode, handle, oldConnectorTable, context.withLimit(OptionalLong.of(count)));
            }

            return node;
        }

        @Override
        public PlanNode visitTopN(TopNNode node, JdbcQueryGeneratorContext context)
        {
            PlanNode planNode = node.getSource().accept(this, context);
            if (!(planNode instanceof TableScanNode)) {
                if (node.getSource() == planNode) {
                    return node;
                }
                else {
                    return node.replaceChildren(ImmutableList.of(planNode));
                }
            }

            TableScanNode tableScanNode = (TableScanNode) planNode;
            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            List<JdbcSortItem> sortItems = node.getOrderingScheme().getOrderByVariables().stream()
                    .map(orderBy -> {
                        verify(assignments.containsKey(orderBy), "assignments does not contain order by item: %s", orderBy.getName());
                        JdbcColumnHandle columnHandle = (JdbcColumnHandle) assignments.get(orderBy);
                        return new JdbcSortItem(columnHandle, node.getOrderingScheme().getOrdering(orderBy));
                    }).collect(Collectors.toList());

            if (!jdbcClient.supportsTopN(sortItems)) {
                return new TopNNode(idAllocator.getNextId(), planNode, node.getCount(), node.getOrderingScheme(), node.getStep());
            }

            long count = node.getCount();
            TableHandle handle = tableScanNode.getTable();
            Optional<ConnectorTableLayoutHandle> oldLayout = handle.getLayout();
            if (oldLayout.isPresent()) {
                JdbcTableHandle oldConnectorTable = (JdbcTableHandle) handle.getConnectorHandle();
                return createNewTableScanNode(tableScanNode, handle, oldConnectorTable, context.withTopN(Optional.of(sortItems), OptionalLong.of(count)));
            }

            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, JdbcQueryGeneratorContext context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            TableScanNode oldTableScanNode = (TableScanNode) node.getSource();
            TableHandle oldTableHandle = oldTableScanNode.getTable();
            if (!oldTableHandle.getLayout().isPresent()) {
                return node;
            }

            RowExpression predicate = logicalRowExpressions.convertToConjunctiveNormalForm(node.getPredicate());
            TranslatedExpression<JdbcExpression> jdbcExpression = translateWith(
                    predicate,
                    jdbcFilterToSqlTranslator,
                    oldTableScanNode.getAssignments());

            Optional<JdbcExpression> translated = jdbcExpression.getTranslated();
            JdbcTableHandle oldConnectorTable = (JdbcTableHandle) oldTableHandle.getConnectorHandle();
            // All filter can be pushed down
            if (translated.isPresent()) {
                return createNewTableScanNode(oldTableScanNode, oldTableHandle, oldConnectorTable, context.withFilter(translated));
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
            TableScanNode newTableScanNode = createNewTableScanNode(oldTableScanNode, oldTableHandle,
                    oldConnectorTable, context.withFilter(translated));

            return new FilterNode(idAllocator.getNextId(), newTableScanNode, logicalRowExpressions.combineConjuncts(remainingExpressions));
        }

        private TableScanNode createNewTableScanNode(
                TableScanNode oldTableScanNode,
                TableHandle oldTableHandle,
                JdbcTableHandle oldConnectorTable,
                JdbcQueryGeneratorContext context)
        {
            JdbcTableHandle newJdbcTableHandle = new JdbcTableHandle(
                    oldConnectorTable.getConnectorId(),
                    oldConnectorTable.getSchemaTableName(),
                    oldConnectorTable.getCatalogName(),
                    oldConnectorTable.getSchemaName(),
                    oldConnectorTable.getTableName(),
                    Optional.of(context));

            JdbcTableLayoutHandle oldTableLayoutHandle = (JdbcTableLayoutHandle) oldTableHandle.getLayout().get();
            JdbcTableLayoutHandle newTableLayoutHandle = new JdbcTableLayoutHandle(
                    newJdbcTableHandle,
                    oldTableLayoutHandle.getTupleDomain());

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    newJdbcTableHandle,
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
