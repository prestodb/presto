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
package com.facebook.presto.druid;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
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
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_QUERY_GENERATOR_FAILURE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DruidPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private final DruidQueryGenerator druidQueryGenerator;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public DruidPlanOptimizer(
            DruidQueryGenerator druidQueryGenerator,
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.druidQueryGenerator = requireNonNull(druidQueryGenerator, "pinot query generator is null");
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standard function resolution is null");
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                standardFunctionResolution,
                functionMetadataManager);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        Map<PlanNodeId, TableScanNode> scanNodes = maxSubplan.accept(new TableFindingVisitor(), null);
        return maxSubplan.accept(new Visitor(scanNodes, session, idAllocator), null);
    }

    private static Optional<DruidTableHandle> getDruidTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof DruidTableHandle) {
                return Optional.of((DruidTableHandle) connectorHandle);
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

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final Map<PlanNodeId, TableScanNode> tableScanNodes;
        private final IdentityHashMap<FilterNode, Void> filtersSplitUp = new IdentityHashMap<>();

        public Visitor(Map<PlanNodeId, TableScanNode> tableScanNodes, ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.tableScanNodes = tableScanNodes;
            // Just making sure that the table exists
            tableScanNodes.forEach((key, value) -> getDruidTableHandle(value).get().getTableName());
        }

        private Optional<PlanNode> tryCreatingNewScanNode(PlanNode plan)
        {
            Optional<DruidQueryGenerator.DruidQueryGeneratorResult> dql = druidQueryGenerator.generate(plan, session);
            if (!dql.isPresent()) {
                return Optional.empty();
            }
            DruidQueryGeneratorContext context = dql.get().getContext();
            final PlanNodeId tableScanNodeId = context.getTableScanNodeId().orElseThrow(() -> new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Expected to find a druid table scan node id"));
            if (!tableScanNodes.containsKey(tableScanNodeId)) {
                throw new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Expected to find a druid table scan node");
            }
            final TableScanNode tableScanNode = tableScanNodes.get(tableScanNodeId);
            DruidTableHandle druidTableHandle = getDruidTableHandle(tableScanNode).orElseThrow(() -> new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Expected to find a druid table handle"));
            TableHandle oldTableHandle = tableScanNode.getTable();
            Map<VariableReferenceExpression, DruidColumnHandle> assignments = context.getAssignments();
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new DruidTableHandle(druidTableHandle.getSchemaName(), druidTableHandle.getTableName(), Optional.of(dql.get().getGeneratedDql())),
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());
            return Optional.of(
                    new TableScanNode(
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
            return pushedDownPlan.orElseGet(() -> replaceChildren(
                    node,
                    node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (filtersSplitUp.containsKey(node)) {
                return this.visitPlan(node, context);
            }
            filtersSplitUp.put(node, null);
            FilterNode nodeToRecurseInto = node;
            List<RowExpression> pushable = new ArrayList<>();
            List<RowExpression> nonPushable = new ArrayList<>();
            DruidFilterExpressionConverter druidFilterExpressionConverter = new DruidFilterExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution, session);
            for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(node.getPredicate())) {
                try {
                    conjunct.accept(druidFilterExpressionConverter, (var) -> new DruidQueryGeneratorContext.Selection(var.getName(), DruidQueryGeneratorContext.Origin.DERIVED));
                    pushable.add(conjunct);
                }
                catch (PrestoException e) {
                    nonPushable.add(conjunct);
                }
            }
            if (!pushable.isEmpty()) {
                FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), logicalRowExpressions.combineConjuncts(pushable));
                Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, logicalRowExpressions.combineConjuncts(nonPushable)));

                filtersSplitUp.put(pushableFilter, null);
                if (nonPushableFilter.isPresent()) {
                    FilterNode nonPushableFilterNode = nonPushableFilter.get();
                    filtersSplitUp.put(nonPushableFilterNode, null);
                    nodeToRecurseInto = nonPushableFilterNode;
                }
                else {
                    nodeToRecurseInto = pushableFilter;
                }
            }
            return this.visitFilter(nodeToRecurseInto, context);
        }
    }
}
