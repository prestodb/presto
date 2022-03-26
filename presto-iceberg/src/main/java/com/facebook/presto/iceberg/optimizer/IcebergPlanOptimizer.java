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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static java.util.Objects.requireNonNull;

public class IcebergPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;

    @Inject
    IcebergPlanOptimizer(StandardFunctionResolution functionResolution, RowExpressionService rowExpressionService, TypeManager typeManager)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return maxSubplan.accept(new FilterPushdownVisitor(functionResolution, rowExpressionService, typeManager, idAllocator, session), null);
    }

    private static class FilterPushdownVisitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final RowExpressionService rowExpressionService;
        private final StandardFunctionResolution functionResolution;
        private final TypeManager typeManager;
        private final PlanNodeIdAllocator idAllocator;

        public FilterPushdownVisitor(
                StandardFunctionResolution functionResolution,
                RowExpressionService rowExpressionService,
                TypeManager typeManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session)
        {
            this.functionResolution = functionResolution;
            this.rowExpressionService = rowExpressionService;
            this.typeManager = typeManager;
            this.idAllocator = idAllocator;
            this.session = session;
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
        public PlanNode visitFilter(FilterNode filter, Void context)
        {
            if (!(filter.getSource() instanceof TableScanNode)) {
                return visitPlan(filter, context);
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();

            Map<String, IcebergColumnHandle> nameToColumnHandlesMapping = tableScan.getAssignments().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().getName(), e -> (IcebergColumnHandle) e.getValue()));

            RowExpression filterPredicate = filter.getPredicate();

            //TODO we should optimize the filter expression
            DomainTranslator.ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                    .fromPredicate(session, filterPredicate, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session).toColumnExtractor());

            // Only pushdown the range filters which apply to entire columns, because iceberg does not accept the filters on the subfields in nested structures
            TupleDomain<IcebergColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                    .transform(subfield -> subfield.getPath().isEmpty() ? subfield.getRootName() : null)
                    .transform(nameToColumnHandlesMapping::get);

            // Simplify call is required because iceberg does not support a large value list for IN predicate
            TupleDomain<IcebergColumnHandle> simplifiedColumnDomain = entireColumnDomain.simplify();

            TableHandle handle = tableScan.getTable();
            IcebergTableHandle oldTableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            IcebergTableHandle newTableHandle = new IcebergTableHandle(
                    oldTableHandle.getSchemaName(),
                    oldTableHandle.getTableName(),
                    oldTableHandle.getTableType(),
                    oldTableHandle.getSnapshotId(),
                    simplifiedColumnDomain);
            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), newTableHandle, handle.getTransaction(), handle.getLayout()),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getCurrentConstraint(),
                    TupleDomain.all());

            if (TRUE_CONSTANT.equals(filterPredicate)) {
                return newTableScan;
            }
            return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), newTableScan, filterPredicate);
        }
    }
}
