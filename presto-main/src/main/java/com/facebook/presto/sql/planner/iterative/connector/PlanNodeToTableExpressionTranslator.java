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
package com.facebook.presto.sql.planner.iterative.connector;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.relation.AggregateExpression;
import com.facebook.presto.spi.relation.FilterExpression;
import com.facebook.presto.spi.relation.ProjectExpression;
import com.facebook.presto.spi.relation.TableExpression;
import com.facebook.presto.spi.relation.TableScanExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.SqlToColumnExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence.canonicalizeColumnExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PlanNodeToTableExpressionTranslator
{
    private final Metadata metadata;
    private final TypeProvider types;
    private final Session session;
    private final SqlParser parser;
    private final Lookup lookup;

    public PlanNodeToTableExpressionTranslator(Metadata metadata, TypeProvider types, Session session, SqlParser parser, Lookup lookup)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.types = requireNonNull(types, "types is null");
        this.session = requireNonNull(session, "session is null");
        this.parser = requireNonNull(parser, "parser is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<TableExpression> translate(PlanNode plan)
    {
        return plan.accept(new PlanRewriter(), null);
    }

    private class PlanRewriter
            extends PlanVisitor<Optional<TableExpression>, Void>
    {
        @Override
        protected Optional<TableExpression> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<TableExpression> visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            ColumnExpression predicate = toColumnExpression(node.getPredicate(), source.getOutputSymbols(), SCALAR);
            Optional<TableExpression> sourceTableExpression = source instanceof GroupReference ?
                    lookup.resolveGroup(source).findAny().flatMap(planNode -> planNode.accept(this, context))
                    : source.accept(this, context);
            if (sourceTableExpression.isPresent()) {
                return Optional.of(new FilterExpression(predicate, sourceTableExpression.get()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<TableExpression> visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            List<Symbol> inputs = source.getOutputSymbols();
            Map<Symbol, ColumnExpression> assignmentColumnExpressions = node.getAssignments()
                    .getMap()
                    .entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, entry -> toColumnExpression(entry.getValue(), inputs, SCALAR)));

            Optional<TableExpression> child = source instanceof GroupReference ?
                    lookup.resolveGroup(source).findAny().flatMap(planNode -> planNode.accept(this, context))
                    : source.accept(this, context);
            if (!child.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new ProjectExpression(node.getOutputSymbols().stream().map(symbol -> assignmentColumnExpressions.get(symbol)).collect(Collectors.toList()), child.get()));
        }

        @Override
        public Optional<TableExpression> visitTableScan(TableScanNode node, Void context)
        {
            return Optional.of(new TableScanExpression(
                    node.getTable().getConnectorHandle(),
                    node.getOutputSymbols().stream()
                            .map(symbol -> new ColumnReferenceExpression(node.getAssignments().get(symbol), types.get(symbol)))
                            .collect(Collectors.toList()),
                    node.getLayout().map(TableLayoutHandle::getConnectorHandle)));
        }

        @Override
        public Optional<TableExpression> visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                return Optional.empty();
            }
            Optional<PlanNode> source = node.getSource() instanceof GroupReference ? lookup.resolveGroup(node.getSource()).findAny()
                    : Optional.of(node.getSource());
            if (!source.isPresent()) {
                return Optional.empty();
            }
            List<List<Symbol>> groupingSets;
            Map<Symbol, Symbol> groupingKeyInputSymbol;

            if (source.get() instanceof GroupIdNode) {
                // grouping sets columns on aggregation Node has |groupKey1$gid|groupKey2$gid|groupId|
                // GroupId has |groupKey1$gid -> groupKeyInput1, groupKey2$gid -> groupKeyInput 2, ...| aggregation columns | groupId|
                GroupIdNode groupIdNode = (GroupIdNode) source.get();
                source = groupIdNode.getSource() instanceof GroupReference ? lookup.resolveGroup(groupIdNode.getSource()).findAny() : Optional.of(groupIdNode.getSource());
                groupingSets = groupIdNode.getGroupingSets();
                groupingKeyInputSymbol = groupIdNode.getGroupingColumns();
            }
            else {
                groupingSets = ImmutableList.of(node.getGroupingKeys());
                groupingKeyInputSymbol = node.getGroupingKeys().stream().collect(toMap(k -> k, v -> v));
            }

            if (!source.isPresent()) {
                return Optional.empty();
            }
            Optional<TableExpression> sourceTableExpression = source.get().accept(this, context);
            if (!sourceTableExpression.isPresent()) {
                return Optional.empty();
            }
            List<Symbol> inputs = source.get().getOutputSymbols();

            List<List<Integer>> groupingSetSpec = groupingSets
                    .stream()
                    .map(set -> set
                            .stream()
                            .map(groupingKeyInputSymbol::get)
                            .map(inputs::indexOf)
                            .collect(toImmutableList())
                    ).collect(toImmutableList());
            if (groupingSetSpec.stream()
                    .flatMap(List::stream)
                    .anyMatch(i -> i < 0)) {
                // Failed to convert grouping set to input channels
                return Optional.empty();
            }

            // TODO support function decorator in ColumnExpression
            if (node.getAggregations().values()
                    .stream()
                    .map(AggregationNode.Aggregation::getCall)
                    .anyMatch(aggregation -> aggregation.isDistinct() == true || aggregation.getWindow().isPresent() ||
                            aggregation.getFilter().isPresent() || aggregation.getOrderBy().isPresent())) {
                return Optional.empty();
            }

            Map<Symbol, ColumnExpression> aggregations = node.getAggregations().entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, entry -> toColumnExpression(entry.getValue().getCall(), inputs, AGGREGATE)));

            List<ColumnExpression> aggregationColumns = node.getOutputSymbols().stream()
                    .filter(symbol -> aggregations.containsKey(symbol))
                    .map(symbol -> aggregations.get(symbol))
                    .collect(toImmutableList());

            checkArgument(aggregationColumns.size() == aggregations.size(), "aggregation of %s size not equals %s", aggregationColumns, node.getAggregations().size());

            List<ColumnExpression> groupingColumns = Stream.concat(
                    node.getOutputSymbols().stream()
                            .filter(groupingKeyInputSymbol::containsKey)
                            .map(groupingKeyInputSymbol::get)
                            .map(symbol -> new InputReferenceExpression(inputs.indexOf(symbol), types.get(symbol)))
                            .filter(input -> input.getField() >= 0),
                    groupingSets.size() > 1 ? Stream.of(new VariableReferenceExpression("groupId", BIGINT)) : Stream.empty())
                    .collect(toImmutableList());
            checkArgument(groupingColumns.size() == node.getGroupingKeys().size(), "grouping key %s size not equals that of %s", groupingColumns, node.getGroupingKeys());

            return Optional.of(
                    new AggregateExpression(
                            aggregationColumns,
                            groupingColumns,
                            groupingSetSpec,
                            sourceTableExpression.get()));
        }

        private ColumnExpression toColumnExpression(Expression expression, List<Symbol> inputs, FunctionKind type)
        {
            Map<NodeRef<Expression>, Type> expressionTypes =
                    getExpressionTypes(session, metadata, parser, types, expression, ImmutableList.of(), WarningCollector.NOOP, false);
            return canonicalizeColumnExpression(SqlToColumnExpressionTranslator.translate(expression, type, expressionTypes,
                    ImmutableMap.of(), inputs.stream().map(Symbol::getName).collect(toImmutableList()), metadata.getFunctionRegistry(), metadata.getTypeManager(), session, false));
        }
    }
}
