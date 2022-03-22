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
package com.facebook.presto.hive;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveSessionProperties.isPartialAggregationPushdownEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPartialAggregationPushdownForVariableLengthDatatypesEnabled;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HivePartialAggregationPushdown
        implements ConnectorPlanOptimizer
{
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final Supplier<TransactionalMetadata> metadataFactory;

    private static final int DUMMY_AGGREGATED_COLUMN_INDEX = -20;

    @Inject
    public HivePartialAggregationPushdown(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            Supplier<TransactionalMetadata> metadataFactory)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standard function resolution is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadata factory is null");
    }

    private static Optional<HiveTableHandle> getHiveTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof HiveTableHandle) {
                return Optional.of((HiveTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        return children.containsAll(node.getSources()) ? node : node.replaceChildren(children);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!isPartialAggregationPushdownEnabled(session)) {
            return maxSubplan;
        }
        return maxSubplan.accept(new Visitor(variableAllocator, session, idAllocator), null);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final VariableAllocator variableAllocator;

        public Visitor(VariableAllocator variableAllocator, ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
        }

        private boolean isAggregationPushdownSupported(AggregationNode partialAggregationNode)
        {
            if (partialAggregationNode.hasNonEmptyGroupingSet()) {
                return false;
            }

            TableScanNode tableScanNode = (TableScanNode) partialAggregationNode.getSource();
            ConnectorTableMetadata connectorTableMetadata = metadataFactory.get().getTableMetadata(session, tableScanNode.getTable().getConnectorHandle());
            Optional<Object> rawFormat = Optional.ofNullable(connectorTableMetadata.getProperties().get(HiveTableProperties.STORAGE_FORMAT_PROPERTY));
            if (!rawFormat.isPresent()) {
                return false;
            }

            final HiveStorageFormat hiveStorageFormat = HiveStorageFormat.valueOf(rawFormat.get().toString());
            if (hiveStorageFormat != ORC && hiveStorageFormat != PARQUET) {
                return false;
            }

            if (tableScanNode.getTable().getLayout().isPresent()) {
                HiveTableLayoutHandle hiveTableLayoutHandle = (HiveTableLayoutHandle) tableScanNode.getTable().getLayout().get();
                if (!hiveTableLayoutHandle.getPredicateColumns().isEmpty()) {
                    return false;
                }
            }

            /**
             * Aggregation push downs are supported only on primitive types and supported aggregation functions are:
             * count(*), count(columnName), min(columnName), max(columnName)
             */
            for (AggregationNode.Aggregation aggregation : partialAggregationNode.getAggregations().values()) {
                FunctionHandle functionHandle = aggregation.getFunctionHandle();
                if (!(standardFunctionResolution.isCountFunction(functionHandle) ||
                        standardFunctionResolution.isMaxFunction(functionHandle) ||
                        standardFunctionResolution.isMinFunction(functionHandle))) {
                    return false;
                }

                if (aggregation.getArguments().isEmpty() && !standardFunctionResolution.isCountFunction(functionHandle)) {
                    return false;
                }

                List<RowExpression> arguments = aggregation.getArguments();
                if (arguments.size() > 1) {
                    return false;
                }

                if (standardFunctionResolution.isMinFunction(functionHandle) || standardFunctionResolution.isMaxFunction(functionHandle)) {
                    // Only allow supported datatypes for min/max
                    Type type = arguments.get(0).getType();
                    if (BOOLEAN.equals(type) ||
                            type.getJavaType() == boolean.class ||
                            isRowType(type) ||
                            isArrayType(type) ||
                            isMapType(type)) {
                        return false;
                    }

                    if (hiveStorageFormat == ORC) {
                        if (TINYINT.equals(type) ||
                                VARBINARY.equals(type) ||
                                TIMESTAMP.equals(type)) {
                            return false;
                        }
                    }

                    if ((VARBINARY.equals(type) || VARCHAR.equals(type)) &&
                            !isPartialAggregationPushdownForVariableLengthDatatypesEnabled(session)) {
                        return false;
                    }
                }
            }
            return true;
        }

        private Optional<PlanNode> tryPartialAggregationPushdown(PlanNode plan)
        {
            if (!(plan instanceof AggregationNode
                    && ((AggregationNode) plan).getStep().equals(PARTIAL)
                    && ((AggregationNode) plan).getSource() instanceof TableScanNode)) {
                return Optional.empty();
            }

            AggregationNode partialAggregationNode = (AggregationNode) plan;

            TableScanNode oldTableScanNode = (TableScanNode) partialAggregationNode.getSource();
            TableHandle oldTableHandle = oldTableScanNode.getTable();
            HiveTableHandle hiveTableHandle = getHiveTableHandle(oldTableScanNode).orElseThrow(() -> new PrestoException(NOT_FOUND, "Hive table handle not found"));

            if (!isAggregationPushdownSupported(partialAggregationNode)) {
                return Optional.empty();
            }

            HiveTypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregationEntry : partialAggregationNode.getAggregations().entrySet()) {
                CallExpression callExpression = aggregationEntry.getValue().getCall();
                String columnName;
                int columnIndex;
                HiveType hiveType = HiveType.toHiveType(hiveTypeTranslator, callExpression.getType());
                if (callExpression.getArguments().isEmpty()) {
                    columnName = "count_star";
                    columnIndex = DUMMY_AGGREGATED_COLUMN_INDEX;
                }
                else {
                    RowExpression column = callExpression.getArguments().get(0);
                    columnName = column.toString();
                    HiveColumnHandle oldColumnHandle = (HiveColumnHandle) oldTableScanNode.getAssignments().get(column);
                    columnIndex = oldColumnHandle.getHiveColumnIndex();
                    hiveType = oldColumnHandle.getHiveType();
                }

                ColumnHandle newColumnHandle = new HiveColumnHandle(
                        columnName,
                        hiveType,
                        callExpression.getType().getTypeSignature(),
                        columnIndex,
                        HiveColumnHandle.ColumnType.AGGREGATED,
                        Optional.of("partial aggregation pushed down"),
                        Optional.of(aggregationEntry.getValue()));
                assignments.put(aggregationEntry.getKey(), newColumnHandle);
            }

            HiveTableLayoutHandle oldTableLayoutHandle = (HiveTableLayoutHandle) oldTableHandle.getLayout().get();
            HiveTableLayoutHandle newTableLayoutHandle = new HiveTableLayoutHandle(
                    oldTableLayoutHandle.getSchemaTableName(),
                    oldTableLayoutHandle.getTablePath(),
                    oldTableLayoutHandle.getPartitionColumns(),
                    oldTableLayoutHandle.getDataColumns(),
                    oldTableLayoutHandle.getTableParameters(),
                    oldTableLayoutHandle.getPartitions().get(),
                    oldTableLayoutHandle.getDomainPredicate(),
                    oldTableLayoutHandle.getRemainingPredicate(),
                    oldTableLayoutHandle.getPredicateColumns(),
                    oldTableLayoutHandle.getPartitionColumnPredicate(),
                    oldTableLayoutHandle.getBucketHandle(),
                    oldTableLayoutHandle.getBucketFilter(),
                    oldTableLayoutHandle.isPushdownFilterEnabled(),
                    oldTableLayoutHandle.getLayoutString(),
                    oldTableLayoutHandle.getRequestedColumns(),
                    true);

            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    hiveTableHandle,
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            return Optional.of(new
                    TableScanNode(
                    idAllocator.getNextId(),
                    newTableHandle,
                    ImmutableList.copyOf(partialAggregationNode.getOutputVariables()),
                    ImmutableMap.copyOf(assignments),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint()));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushedDownPlan = tryPartialAggregationPushdown(node);
            return pushedDownPlan.orElseGet(() -> replaceChildren(
                    node,
                    node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
        }
    }
}
