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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.TransactionalMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveSessionProperties.isPartialAggregationPushdownEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPartialAggregationPushdownForVariableLengthDatatypesEnabled;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public class HivePartialAggregationPushdown
        implements ConnectorPlanOptimizer
{
    private final StandardFunctionResolution standardFunctionResolution;
    private final Supplier<TransactionalMetadata> metadataFactory;

    private static final int DUMMY_AGGREGATED_COLUMN_INDEX = -20;

    public HivePartialAggregationPushdown(
            StandardFunctionResolution standardFunctionResolution,
            Supplier<TransactionalMetadata> metadataFactory)
    {
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

    @Override
    public PlanNode optimize(PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!isPartialAggregationPushdownEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new Rewriter(session, idAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;

        public Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
        }

        private boolean isAggregationPushdownSupported(AggregationNode partialAggregationNode, Map<VariableReferenceExpression, ColumnHandle> assignments)
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
            if (hiveStorageFormat != ORC && hiveStorageFormat != PARQUET && hiveStorageFormat != DWRF) {
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
            for (Aggregation aggregation : partialAggregationNode.getAggregations().values()) {
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
                else if (arguments.size() == 1) {
                    RowExpression column = aggregation.getCall().getArguments().get(0);
                    HiveColumnHandle columnHandle = (HiveColumnHandle) assignments.get(column);
                    // These columns get 'PREFILLED' with values in the corresponding page sources
                    if (columnHandle.getColumnType() != REGULAR) {
                        return false;
                    }
                }

                if (standardFunctionResolution.isMinFunction(functionHandle) || standardFunctionResolution.isMaxFunction(functionHandle)) {
                    // Only allow supported datatypes for min/max
                    Type type = arguments.get(0).getType();
                    switch (hiveStorageFormat) {
                        case ORC:
                        case DWRF:
                            if (isNotSupportedOrcTypeForMinMax(type)) {
                                return false;
                            }
                            break;
                        case PARQUET:
                            if (isNotSupportedParquetTypeForMinMax(type)) {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                }
            }
            return true;
        }

        private boolean isNotSupportedOrcTypeForMinMax(Type type)
        {
            return BOOLEAN.equals(type) ||
                    type.getJavaType() == boolean.class ||
                    isRowType(type) ||
                    isArrayType(type) ||
                    isMapType(type) ||
                    TINYINT.equals(type) ||
                    VARBINARY.equals(type) ||
                    TIMESTAMP.equals(type) ||
                    isNotSupportedOrcTypeForVariableLengthDataType(type);
        }

        private boolean isNotSupportedParquetTypeForMinMax(Type type)
        {
            return BOOLEAN.equals(type) ||
                    type.getJavaType() == boolean.class ||
                    isRowType(type) ||
                    isArrayType(type) ||
                    isMapType(type) ||
                    isNotSupportedParquetTypeForVariableLengthDataType(type);
        }

        private boolean isNotSupportedOrcTypeForVariableLengthDataType(Type type)
        {
            return VARCHAR.equals(type) && !isPartialAggregationPushdownForVariableLengthDatatypesEnabled(session);
        }

        private boolean isNotSupportedParquetTypeForVariableLengthDataType(Type type)
        {
            boolean isVariableLengthType = VARBINARY.equals(type) || VARCHAR.equals(type);
            return isVariableLengthType && !isPartialAggregationPushdownForVariableLengthDatatypesEnabled(session);
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

            if (!isAggregationPushdownSupported(partialAggregationNode, oldTableScanNode.getAssignments())) {
                return Optional.empty();
            }

            HiveTypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, Aggregation> aggregationEntry : partialAggregationNode.getAggregations().entrySet()) {
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
            HiveTableLayoutHandle newTableLayoutHandle = oldTableLayoutHandle.builder().setPartialAggregationsPushedDown(true).build();

            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    hiveTableHandle,
                    oldTableHandle.getTransaction(),
                    Optional.of(newTableLayoutHandle));

            return Optional.of(new TableScanNode(
                    oldTableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    newTableHandle,
                    ImmutableList.copyOf(partialAggregationNode.getOutputVariables()),
                    ImmutableMap.copyOf(assignments),
                    oldTableScanNode.getTableConstraints(),
                    oldTableScanNode.getCurrentConstraint(),
                    oldTableScanNode.getEnforcedConstraint(),
                    oldTableScanNode.getCteMaterializationInfo()));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            return tryPartialAggregationPushdown(node).orElse(node);
        }
    }
}
