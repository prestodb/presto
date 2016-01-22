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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
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
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
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

import static java.util.Objects.requireNonNull;

/**
 * Converts cardinality-insensitive aggregations (max, min, "distinct") over partition keys
 * into simple metadata queries
 */
public class MetadataQueryOptimizer
        implements PlanOptimizer
{
    private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of("max", "min", "approx_distinct");

    private final Metadata metadata;

    public MetadataQueryOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isOptimizeMetadataQueries(session)) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, idAllocator), plan, null);
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Metadata metadata;

        private Optimizer(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.metadata = metadata;
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
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScan.getTable(), column);

                typesBuilder.put(symbol, columnMetadata.getType());
                columnBuilder.put(symbol, column);
            }

            Map<Symbol, ColumnHandle> columns = columnBuilder.build();
            Map<Symbol, Type> types = typesBuilder.build();

            // Materialize the list of partitions and replace the TableScan node
            // with a Values node
            TableLayout layout = null;
            if (!tableScan.getLayout().isPresent()) {
                List<TableLayoutResult> layouts = metadata.getLayouts(session, tableScan.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty());
                if (layouts.size() == 1) {
                    layout = Iterables.getOnlyElement(layouts).getLayout();
                }
            }
            else {
                layout = metadata.getLayout(session, tableScan.getLayout().get());
            }

            if (layout == null || !layout.getDiscretePredicates().isPresent()) {
                return context.defaultRewrite(node);
            }
            DiscretePredicates predicates = layout.getDiscretePredicates().get();

            // the optimization is only valid if the aggregation node only relies on partition keys
            if (!predicates.getColumns().containsAll(columns.values())) {
                return context.defaultRewrite(node);
            }

            ImmutableList.Builder<List<Expression>> rowsBuilder = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> domain : predicates.getPredicates()) {
                if (!domain.isNone()) {
                    Map<ColumnHandle, NullableValue> entries = TupleDomain.extractFixedValues(domain).get();

                    ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
                    // for each input column, add a literal expression using the entry value
                    for (Symbol input : inputs) {
                        ColumnHandle column = columns.get(input);
                        Type type = types.get(input);
                        NullableValue value = entries.get(column);
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
            }

            // replace the tablescan node with a values node
            ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), inputs, rowsBuilder.build());
            return SimplePlanRewriter.rewriteWith(new Replacer(valuesNode), node);
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
                    if (!Iterables.all(project.getAssignments().values(), DeterminismEvaluator::isDeterministic)) {
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
            extends SimplePlanRewriter<Void>
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
