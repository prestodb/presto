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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts cardinality-insensitive aggregations (max, min, "distinct") over partition keys
 * into simple metadata queries
 */
public class MetadataQueryOptimizer
        extends PlanOptimizer
{
    private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of("max", "min", "approx_distinct");

    private final Metadata metadata;
    private final SplitManager splitManager;

    public MetadataQueryOptimizer(Metadata metadata, SplitManager splitManager)
    {
        checkNotNull(metadata, "metadata is null");
        checkNotNull(splitManager, "splitManager is null");

        this.metadata = metadata;
        this.splitManager = splitManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return PlanRewriter.rewriteWith(new Optimizer(metadata, splitManager, idAllocator), plan, null);
    }

    private static class Optimizer
            extends PlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final SplitManager splitManager;

        private Optimizer(Metadata metadata, SplitManager splitManager, PlanNodeIdAllocator idAllocator)
        {
            this.metadata = metadata;
            this.splitManager = splitManager;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            // supported functions are only MIN/MAX/APPROX_DISTINCT or distinct aggregates
            for (FunctionCall call : node.getAggregations().values()) {
                if (!ALLOWED_FUNCTIONS.contains(call.getName().toString()) && !call.isDistinct()) {
                    return context.defaultRewrite(node);
                }
            }

            Optional<TableScanNode> result = findTableScan(node.getSource());
            if (!result.isPresent()) {
                return context.defaultRewrite(node);
            }

            // verify all outputs of table scan are partition keys
            TableScanNode tableScan = result.get();

            ImmutableMap.Builder<Symbol, Type> typesBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, ColumnHandle> columnBuilder = ImmutableMap.builder();

            List<Symbol> inputs = tableScan.getOutputSymbols();
            for (Symbol symbol : inputs) {
                ColumnHandle column = tableScan.getAssignments().get(symbol);
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableScan.getTable(), column);

                if (!columnMetadata.isPartitionKey()) {
                    // the optimization is only valid if the aggregation node only
                    // relies on partition keys
                    return context.defaultRewrite(node);
                }

                typesBuilder.put(symbol, columnMetadata.getType());
                columnBuilder.put(symbol, column);
            }

            Map<Symbol, ColumnHandle> columns = columnBuilder.build();
            Map<Symbol, Type> types = typesBuilder.build();

            // Materialize the list of partitions and replace the TableScan node
            // with a Values node
            List<Partition> partitions;
            if (tableScan.getGeneratedPartitions().isPresent()) {
                partitions = tableScan.getGeneratedPartitions().get().getPartitions();
            }
            else {
                partitions = splitManager.getPartitions(result.get().getTable(), Optional.of(tableScan.getPartitionsDomainSummary()))
                        .getPartitions();
            }

            ImmutableList.Builder<List<Expression>> rowsBuilder = ImmutableList.builder();
            for (Partition partition : partitions) {
                Map<ColumnHandle, SerializableNativeValue> entries = partition.getTupleDomain().extractNullableFixedValues();

                ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
                // for each input column, add a literal expression using the entry value
                for (Symbol input : inputs) {
                    ColumnHandle column = columns.get(input);
                    Type type = types.get(input);
                    SerializableNativeValue value = entries.get(column);
                    if (value == null) {
                        // partition key does not have a single value, so bail out to be safe
                        return context.defaultRewrite(node);
                    }
                    else {
                        rowBuilder.add(LiteralInterpreter.toExpression(value.getValue(), type));
                    }
                }
                rowsBuilder.add(rowBuilder.build());
            }

            // replace the tablescan node with a values node
            ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), inputs, rowsBuilder.build());
            return PlanRewriter.rewriteWith(new Replacer(valuesNode), node);
        }

        private Optional<TableScanNode> findTableScan(PlanNode source)
        {
            while (true) {
                // allow any chain of linear transformations
                if (source instanceof MarkDistinctNode ||
                        source instanceof FilterNode ||
                        source instanceof LimitNode ||
                        source instanceof TopNNode ||
                        source instanceof SortNode) {
                    source = source.getSources().get(0);
                }
                else if (source instanceof ProjectNode) {
                    // verify projections are deterministic
                    ProjectNode project = (ProjectNode) source;
                    if (!Iterables.all(project.getExpressions(), DeterminismEvaluator::isDeterministic)) {
                        return Optional.empty();
                    }
                    source = project.getSource();
                }
                else if (source instanceof TableScanNode) {
                    return Optional.of((TableScanNode) source);
                }
                else {
                    return Optional.empty();
                }
            }
        }
    }

    private static class Replacer
            extends PlanRewriter<Void>
    {
        private final ValuesNode replacement;

        private Replacer(ValuesNode replacement)
        {
            this.replacement = replacement;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return replacement;
        }
    }
}
