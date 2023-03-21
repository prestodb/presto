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
import com.facebook.presto.plugin.clickhouse.ClickHouseColumnHandle;
import com.facebook.presto.plugin.clickhouse.ClickHouseTableHandle;
import com.facebook.presto.plugin.clickhouse.ClickHouseTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.CLICKHOUSE_QUERY_GENERATOR_FAILURE;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class ClickHouseComputePushdown
        implements ConnectorPlanOptimizer
{
    private final ExpressionOptimizer expressionOptimizer;
    private final ClickHouseFilterToSqlTranslator clickHouseFilterToSqlTranslator;
    private final LogicalRowExpressions logicalRowExpressions;
    private final ClickHouseQueryGenerator clickhouseQueryGenerator;
    private static final String PushdownException = "avg";

    public ClickHouseComputePushdown(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution functionResolution,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            String identifierQuote,
            Set<Class<?>> functionTranslators,
            ClickHouseQueryGenerator clickhouseQueryGenerator)
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
        this.clickhouseQueryGenerator = requireNonNull(clickhouseQueryGenerator, "pinot query generator is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        Map<PlanNodeId, TableScanNode> scanNodes = maxSubplan.accept(new TableFindingVisitor(), null);
        return maxSubplan.accept(new Visitor(scanNodes, session, idAllocator), null);
    }

    private static class TableFindingVisitor
            extends PlanVisitor<Map<PlanNodeId, TableScanNode>, Void>
    {
        @Override
        public Map<PlanNodeId, TableScanNode> visitPlan(PlanNode node, Void context)
        {
            Map<PlanNodeId, TableScanNode> result = new IdentityHashMap<>();
            node.getSources().forEach(source -> result.putAll(source.accept(this, context)));
            return result;
        }

        @Override
        public Map<PlanNodeId, TableScanNode> visitTableScan(TableScanNode node, Void context)
        {
            Map<PlanNodeId, TableScanNode> result = new IdentityHashMap<>();
            result.put(node.getId(), node);
            return result;
        }
    }

    private static Optional<ClickHouseTableHandle> getClickHouseTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof ClickHouseTableHandle) {
                return Optional.of((ClickHouseTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        for (int i = 0; i < node.getSources().size(); i++) {
            if (children.get(i) != node.getSources().get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;
        private final Map<PlanNodeId, TableScanNode> tableScanNodes;

        public Visitor(Map<PlanNodeId, TableScanNode> tableScanNodes, ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.tableScanNodes = tableScanNodes;
            tableScanNodes.forEach((key, value) -> getClickHouseTableHandle(value).get().getTableName());
        }

        private Optional<PlanNode> tryCreatingNewScanNode(PlanNode plan)
        {
            Optional<ClickHouseQueryGenerator.ClickHouseQueryGeneratorResult> clickhouseSQL = clickhouseQueryGenerator.generate(plan, session);
            if (!clickhouseSQL.isPresent()) {
                return Optional.empty();
            }
            ClickHouseQueryGeneratorContext context = clickhouseSQL.get().getContext();
            final PlanNodeId tableScanNodeId = context.getTableScanNodeId().orElseThrow(() -> new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Expected to find a clickhouse table scan node id"));
            if (!tableScanNodes.containsKey(tableScanNodeId)) {
                throw new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Expected to find a clickhouse table scan node");
            }

            final TableScanNode tableScanNode = tableScanNodes.get(tableScanNodeId);
            ClickHouseTableHandle clickHouseTableHandle = getClickHouseTableHandle(tableScanNode).orElseThrow(() -> new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Expected to find a clickhouse table handle"));
            TableHandle oldTableHandle = tableScanNode.getTable();
            Map<VariableReferenceExpression, ClickHouseColumnHandle> assignments = context.getAssignments();

            ClickHouseTableHandle oldConnectorTable = (ClickHouseTableHandle) oldTableHandle.getConnectorHandle();
            ClickHouseTableLayoutHandle oldTableLayoutHandle = (ClickHouseTableLayoutHandle) oldTableHandle.getLayout().get();
            ClickHouseTableLayoutHandle newTableLayoutHandle = new ClickHouseTableLayoutHandle(
                    oldConnectorTable,
                    oldTableLayoutHandle.getTupleDomain(),
                    Optional.empty(), Optional.empty(), Optional.of(clickhouseSQL.get().getGeneratedClickhouseSQL()));

            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new ClickHouseTableHandle(clickHouseTableHandle.getConnectorId(),
                            new SchemaTableName(clickHouseTableHandle.getSchemaName(), clickHouseTableHandle.getTableName()),
                            null,
                            clickHouseTableHandle.getSchemaName(),
                            clickHouseTableHandle.getTableName()),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            return Optional.of(
                    new TableScanNode(
                            tableScanNode.getSourceLocation(),
                            idAllocator.getNextId(),
                            newTableHandle,
                            ImmutableList.copyOf(assignments.keySet()),
                            assignments.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, (e) -> (ColumnHandle) (e.getValue()))),
                            tableScanNode.getCurrentConstraint(),
                            tableScanNode.getEnforcedConstraint()));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushedDownPlan = tryCreatingNewScanNode(node);

            boolean hasAvg = false;
            if (pushedDownPlan.isPresent()) {
                for (int variableIndex = 0; variableIndex < pushedDownPlan.get().getOutputVariables().size(); variableIndex++) {
                    // Filter nodes that may contain aggregate functions
                    if (pushedDownPlan.get().getOutputVariables().get(variableIndex).getName().length() > 3) {
                        // Determine whether the node is an avg function. The avg function currently does not support pushdown.
                        if (pushedDownPlan.get().getOutputVariables().get(variableIndex).getName().substring(0, 3).equals(PushdownException)) {
                            hasAvg = true;
                            break;
                        }
                    }
                }
            }

            if (!pushedDownPlan.isPresent() || hasAvg) {
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
            return pushedDownPlan.orElseGet(() -> replaceChildren(
                    node,
                    node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
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

            if (!oldTableHandle.getLayout().isPresent() || !clickHouseExpression.getTranslated().isPresent()) {
                return node;
            }

            ClickHouseTableLayoutHandle oldTableLayoutHandle = (ClickHouseTableLayoutHandle) oldTableHandle.getLayout().get();
            ClickHouseTableLayoutHandle newTableLayoutHandle = new ClickHouseTableLayoutHandle(
                    oldConnectorTable,
                    oldTableLayoutHandle.getTupleDomain(),
                    clickHouseExpression.getTranslated(),
                    Optional.empty(),
                    Optional.empty());

            TableHandle tableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    oldTableHandle.getConnectorHandle(),
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            TableScanNode newTableScanNode = new TableScanNode(
                    oldTableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableHandle,
                    oldTableScanNode.getOutputVariables(),
                    oldTableScanNode.getAssignments(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint());

            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), newTableScanNode, node.getPredicate());
        }
    }
}
