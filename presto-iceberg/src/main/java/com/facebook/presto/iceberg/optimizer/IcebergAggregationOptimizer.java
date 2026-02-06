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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.util.AggregateConverter;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeValue;
import static com.facebook.presto.iceberg.IcebergUtil.getNonMetadataColumnConstraints;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IcebergAggregationOptimizer
        implements ConnectorPlanOptimizer
{
    public static final Logger log = Logger.get(IcebergAggregationOptimizer.class);
    private final IcebergTransactionManager icebergTransactionManager;
    private final StandardFunctionResolution functionResolution;

    public IcebergAggregationOptimizer(
            IcebergTransactionManager icebergTransactionManager,
            StandardFunctionResolution functionResolution)
    {
        this.icebergTransactionManager = requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (isPushdownFilterEnabled(session)) {
            return maxSubplan;
        }
        Optimizer optimizer = new Optimizer(session, idAllocator, icebergTransactionManager, functionResolution);
        return ConnectorPlanRewriter.rewriteWith(optimizer, maxSubplan, null);
    }

    private static class Optimizer
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession connectorSession;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager icebergTransactionManager;
        private final AggregateConverter aggregateConverter;
        private final Map<Predicate<FunctionHandle>, Expression.Operation> allowedFunctions;

        private Optimizer(ConnectorSession connectorSession,
                          PlanNodeIdAllocator idAllocator,
                          IcebergTransactionManager icebergTransactionManager,
                          StandardFunctionResolution functionResolution)
        {
            this.connectorSession = connectorSession;
            this.idAllocator = idAllocator;
            this.icebergTransactionManager = icebergTransactionManager;
            this.allowedFunctions = ImmutableMap.of(
                    functionResolution::isCountFunction, Expression.Operation.COUNT,
                    functionHandle -> functionHandle.getArgumentTypes().size() == 1 && functionResolution.isMinFunction(functionHandle), Expression.Operation.MIN,
                    functionHandle -> functionHandle.getArgumentTypes().size() == 1 && functionResolution.isMaxFunction(functionHandle), Expression.Operation.MAX);
            this.aggregateConverter = new AggregateConverter(allowedFunctions);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            Optional<TableScanNode> result = findTableScan(node.getSource());
            if (result.isEmpty()) {
                return context.defaultRewrite(node);
            }

            // verify all outputs of table scan are partition keys
            TableScanNode tableScan = result.get();
            IcebergTableHandle tableHandle = (IcebergTableHandle) tableScan.getTable().getConnectorHandle();
            Table table = IcebergUtil.getIcebergTable(getConnectorMetadata(tableScan.getTable()),
                    connectorSession, tableHandle.getSchemaTableName());
            TupleDomain<IcebergColumnHandle> predicate = getNonMetadataColumnConstraints(((IcebergTableLayoutHandle) tableScan.getTable().getLayout().get())
                    .getValidPredicate());

            if (!isReducible(table, node)) {
                return context.defaultRewrite(node);
            }

            Expression filter = toIcebergExpression(predicate);
            // Fold min/max aggregations to a constant value
            return reduce(node, table.schema(), table, tableHandle.getIcebergTableName().getSnapshotId(), filter);
        }

        private static Optional<TableScanNode> findTableScan(PlanNode source)
        {
            while (true) {
                if (source instanceof ProjectNode) {
                    source = ((ProjectNode) source).getSource();
                }
                else if (source instanceof TableScanNode) {
                    return Optional.of((TableScanNode) source);
                }
                else {
                    return Optional.empty();
                }
            }
        }

        private boolean isReducible(Table table, AggregationNode node)
        {
            if (!(table instanceof BaseTable)) {
                return false;
            }

            // The aggregation is reducible when there is no group by key
            if (node.getAggregations().isEmpty() || !node.getGroupingKeys().isEmpty()) {
                return false;
            }

            // supported functions are only MIN/MAX/COUNT aggregates
            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                if (aggregation.isDistinct() || aggregation.getMask().isPresent() || allowedFunctions.keySet().stream().noneMatch(
                        pred -> pred.test(aggregation.getFunctionHandle())) && !aggregation.isDistinct()) {
                    return false;
                }
            }

            return true;
        }

        private PlanNode reduce(
                AggregationNode node,
                Schema schema,
                Table table,
                Optional<Long> snapshotId,
                Expression filter)
        {
            AggregateEvaluator aggregateEvaluator;
            List<BoundAggregate<?, ?>> expressions =
                    Lists.newArrayListWithExpectedSize(node.getAggregations().size());

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                try {
                    AggregationNode.Aggregation aggregation = node.getAggregations().get(variable);
                    Expression expr = aggregateConverter.convert(aggregation);
                    if (expr != null) {
                        Expression bound = Binder.bind(schema.asStruct(), expr, false);
                        expressions.add((BoundAggregate<?, ?>) bound);
                    }
                }
                catch (UnsupportedOperationException e) {
                    log.info(
                            "Skipping aggregate pushdown: AggregateFunc {} can't be converted to iceberg Expression",
                            variable,
                            e);
                    return node;
                }
                catch (IllegalArgumentException | ValidationException e) {
                    log.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", variable, e);
                    return node;
                }
            }

            aggregateEvaluator = AggregateEvaluator.create(expressions);
            if (!metricsModeSupportsAggregatePushDown(table, aggregateEvaluator.aggregates())) {
                return node;
            }
            TableScan scan = table.newScan().includeColumnStats();
            Snapshot snapshot = snapshotId.map(table::snapshot).orElseGet(table::currentSnapshot);
            if (snapshot == null) {
                log.info("Skipping aggregate pushdown: table snapshot is null");
                return node;
            }
            scan = scan.useSnapshot(snapshot.snapshotId());
            scan = scan.filter(filter);

            try (CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles()) {
                List<FileScanTask> tasks = ImmutableList.copyOf(fileScanTasks);
                for (FileScanTask task : tasks) {
                    if (!task.deletes().isEmpty()) {
                        log.info("Skipping aggregate pushdown: detected row level deletes");
                        return node;
                    }

                    aggregateEvaluator.update(task.file());
                }
            }
            catch (IOException e) {
                log.info("Skipping aggregate pushdown: ", e);
                return node;
            }

            if (!aggregateEvaluator.allAggregatorsValid()) {
                return node;
            }

            StructLike structLike = aggregateEvaluator.result();
            System.out.println("====> " + structLike);
            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                VariableReferenceExpression outputVariable = node.getOutputVariables().get(i);
                RowExpression expression = new ConstantExpression(getNativeValue(outputVariable.getType(), structLike.get(i, Object.class)), outputVariable.getType());
                assignmentsBuilder.put(outputVariable, expression);
            }
            Assignments assignments = assignmentsBuilder.build();
            ValuesNode valuesNode = new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(new ArrayList<>(assignments.getExpressions())), Optional.empty());
            return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), valuesNode, assignments, LOCAL);
        }

        private ConnectorMetadata getConnectorMetadata(TableHandle tableHandle)
        {
            requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
            ConnectorMetadata metadata = icebergTransactionManager.get(tableHandle.getTransaction());
            checkState(metadata instanceof IcebergAbstractMetadata, "metadata must be IcebergAbstractMetadata");
            return metadata;
        }

        private boolean metricsModeSupportsAggregatePushDown(Table table, List<BoundAggregate<?, ?>> aggregates)
        {
            MetricsConfig config = MetricsConfig.forTable(table);
            for (BoundAggregate aggregate : aggregates) {
                String colName = aggregate.columnName();
                if (!colName.equals("*")) {
                    MetricsModes.MetricsMode mode = config.columnMode(colName);
                    if (mode instanceof MetricsModes.None) {
                        log.info("Skipping aggregate pushdown: No metrics for column {}", colName);
                        return false;
                    }
                    else if (mode instanceof MetricsModes.Counts) {
                        if (aggregate.op() == Expression.Operation.MAX
                                || aggregate.op() == Expression.Operation.MIN) {
                            log.info(
                                    "Skipping aggregate pushdown: Cannot produce min or max from count for column {}",
                                    colName);
                            return false;
                        }
                    }
                    else if (mode instanceof MetricsModes.Truncate) {
                        // lower_bounds and upper_bounds may be truncated, so disable push down
                        if (aggregate.type().typeId() == Type.TypeID.STRING) {
                            if (aggregate.op() == Expression.Operation.MAX
                                    || aggregate.op() == Expression.Operation.MIN) {
                                log.info(
                                        "Skipping aggregate pushdown: Cannot produce min or max from truncated values for column {}",
                                        colName);
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }
    }
}
