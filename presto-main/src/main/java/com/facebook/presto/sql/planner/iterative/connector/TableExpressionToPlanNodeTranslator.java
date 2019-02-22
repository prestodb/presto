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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.AggregateExpression;
import com.facebook.presto.spi.relation.FilterExpression;
import com.facebook.presto.spi.relation.ProjectExpression;
import com.facebook.presto.spi.relation.TableExpression;
import com.facebook.presto.spi.relation.TableScanExpression;
import com.facebook.presto.spi.relation.UnaryTableExpression;
import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.connector.rewriter.TableExpressionRewriter;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.relational.ColumnExpressionToSqlTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.sql.relational.SqlToColumnExpressionTranslator.InputCollector.listColumnHandles;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class TableExpressionToPlanNodeTranslator
{
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final LiteralEncoder literalEncoder;
    private final FunctionRegistry functionRegistry;
    private final Metadata metadata;

    public TableExpressionToPlanNodeTranslator(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, LiteralEncoder literalEncoder, Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
        this.functionRegistry = requireNonNull(metadata.getFunctionRegistry(), "functionRegistry is null");
        this.metadata = metadata;
    }

    public Optional<PlanNode> translate(Session session, ConnectorId connectorId, TableExpression tableExpression, List<Symbol> outputSymbols)
    {
        return new Visitor(session, connectorId).accept(tableExpression, new Context(outputSymbols));
    }

    private static class Context
    {
        private final List<Symbol> outputChannels;

        public Context(List<Symbol> outputChannels)
        {
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        }

        public List<Symbol> getOutput()
        {
            return outputChannels;
        }
    }

    private class Visitor
    {
        private final Session session;
        private final ConnectorId connectorId;
        private final TableExpressionRewriter<Optional<PlanNode>, Context> rewriter;

        public Visitor(Session session, ConnectorId connectorId)
        {
            this.session = requireNonNull(session, "session is null");
            this.connectorId = requireNonNull(connectorId, "session is null");
            this.rewriter = new TableExpressionRewriter<Optional<PlanNode>, Context>()
                    .addRule(ProjectExpression.class, this::visitProject)
                    .addRule(FilterExpression.class, this::visitFilter)
                    .addRule(AggregateExpression.class, this::visitAggregate)
                    .addRule(TableScanExpression.class, this::visitTableScan);
        }

        public Optional<PlanNode> accept(TableExpression node, Context context)
        {
            return rewriter.accept(node, context);
        }

        private Optional<PlanNode> visitProject(TableExpressionRewriter<Optional<PlanNode>, Context> currentRewriter, ProjectExpression project, Context context)
        {
            checkNoColumnInputs(project);
            List<Symbol> outputChannelNames = context.getOutput();
            List<Symbol> inputChannelNames = getInputChannels(project);
            checkArgument(outputChannelNames.size() == project.getOutput().size(), "project does not have expected size as output");

            Optional<PlanNode> source = currentRewriter.accept(project.getSource(), new Context(inputChannelNames));

            if (source.isPresent()) {
                List<Expression> assignmentExpressions = project.getOutput().stream()
                        .map(rowExpression -> translate(rowExpression, inputChannelNames))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(toImmutableList());

                if (assignmentExpressions.size() != project.getOutput().size()) {
                    return Optional.empty();
                }
                Assignments.Builder assignments = Assignments.builder();
                for (int i = 0; i < assignmentExpressions.size(); i++) {
                    assignments.put(outputChannelNames.get(i), assignmentExpressions.get(i));
                }
                return Optional.of(new ProjectNode(idAllocator.getNextId(), source.get(), assignments.build()));
            }
            return Optional.empty();
        }

        private Optional<PlanNode> visitFilter(TableExpressionRewriter<Optional<PlanNode>, Context> currentRewriter, FilterExpression filter, Context context)
        {
            checkArgument(listColumnHandles(filter.getPredicate()).size() == 0, "Filter node predicate has column reference");
            Optional<PlanNode> source = currentRewriter.accept(filter.getSource(), context);
            if (source.isPresent()) {
                Optional<Expression> predicate = translate(filter.getPredicate(), source.get().getOutputSymbols());
                if (predicate.isPresent()) {
                    return Optional.of(new FilterNode(idAllocator.getNextId(), source.get(), predicate.get()));
                }
            }
            return Optional.empty();
        }

        private Optional<PlanNode> visitAggregate(TableExpressionRewriter<Optional<PlanNode>, Context> currentRewriter, AggregateExpression aggregate, Context context)
        {
            checkNoColumnInputs(aggregate);
            int numGroups = aggregate.getGroups().size();
            List<Symbol> outputChannelNames = context.getOutput();
            checkArgument(outputChannelNames.size() == aggregate.getOutput().size(), "aggregate %s does not have expected size as output", aggregate.getOutput());
            // grouping column name won't change and it will always be the first columns of output
            Map<Integer, Symbol> groupingSymbol = new HashMap<>();
            for (int i = 0; i < aggregate.getGroups().size(); i++) {
                ColumnExpression column = aggregate.getGroups().get(i);
                if (column instanceof InputReferenceExpression) {
                    groupingSymbol.put(((InputReferenceExpression) column).getField(), outputChannelNames.get(i));
                }
            }
            List<Symbol> inputChannelNames = aggregate.getGroupSets().size() > 1 ? getInputChannels(aggregate) : getInputChannels(aggregate, groupingSymbol);

            Optional<PlanNode> source = currentRewriter.accept(aggregate.getSource(), new Context(inputChannelNames));
            if (!source.isPresent()) {
                return Optional.empty();
            }
            AggregationNode.GroupingSetDescriptor groupingSetDescriptor;
            Optional<Symbol> groupIdSymbol = Optional.empty();
            if (aggregate.getGroupSets().size() > 1) {
                groupIdSymbol = Optional.of(outputChannelNames.get(numGroups - 1));
                List<List<Symbol>> groupingSets = aggregate.getGroupSets()
                        .stream()
                        .map(set -> set.stream()
                                .map(i -> outputChannelNames.get(i))
                                .collect(toImmutableList()))
                        .collect(toImmutableList());

                List<Symbol> aggregateArguments = IntStream.range(numGroups, outputChannelNames.size())
                        .boxed()
                        .map(i -> aggregate.getOutput().get(i))
                        .filter(CallExpression.class::isInstance)
                        .map(CallExpression.class::cast)
                        .map(CallExpression::getArguments)
                        .flatMap(List::stream)
                        .filter(InputReferenceExpression.class::isInstance)
                        .map(InputReferenceExpression.class::cast)
                        .map(InputReferenceExpression::getField)
                        .map(i -> inputChannelNames.get(i))
                        .collect(toImmutableList());

                Map<Symbol, Symbol> groupingColumnNames = IntStream.range(0, numGroups - 1)
                        .boxed()
                        .collect(toMap(i -> outputChannelNames.get(i), i -> inputChannelNames.get(((InputReferenceExpression) (aggregate.getGroups().get(i))).getField())));

                source = Optional.of(
                        new GroupIdNode(
                                idAllocator.getNextId(),
                                source.get(),
                                groupingSets,
                                groupingColumnNames,
                                aggregateArguments,
                                groupIdSymbol.get()));
                groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                        Stream.concat(
                                aggregate.getGroups().stream()
                                        .filter(InputReferenceExpression.class::isInstance)
                                        .map(InputReferenceExpression.class::cast)
                                        .map(InputReferenceExpression::getField)
                                        .map(i -> outputChannelNames.get(i)),
                                Stream.of(groupIdSymbol.get())
                        ).collect(toImmutableList()),
                        aggregate.getGroupSets().size(),
                        ImmutableSet.of());
            }
            else {
                groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                        aggregate.getGroups().stream()
                                .filter(InputReferenceExpression.class::isInstance)
                                .map(InputReferenceExpression.class::cast)
                                .map(InputReferenceExpression::getField)
                                .map(i -> inputChannelNames.get(i))
                                .collect(toImmutableList()),
                        aggregate.getGroupSets().size(),
                        ImmutableSet.of());
            }

            // Need to keeps the key order same as output
            Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>();
            for (int i = 0; i < aggregate.getAggregations().size(); i++) {
                aggregations.put(
                        outputChannelNames.get(i + numGroups),
                        new AggregationNode.Aggregation(
                                (FunctionCall) translate(aggregate.getAggregations().get(i), inputChannelNames).get(),
                                ((CallExpression) aggregate.getAggregations().get(i)).getSignature(),
                                Optional.empty()));
            }

            return Optional.of(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            source.get(),
                            aggregations,
                            groupingSetDescriptor,
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            groupIdSymbol));
        }

        private Optional<PlanNode> visitTableScan(TableScanExpression tableScan, Context context)
        {
            List<Symbol> outputChannelNames = context.getOutput();
            List<ColumnHandle> columnHandles = tableScan.getOutput().stream()
                    .filter(ColumnReferenceExpression.class::isInstance)
                    .map(ColumnReferenceExpression.class::cast)
                    .map(ColumnReferenceExpression::getColumnHandle)
                    .collect(toImmutableList());
            checkArgument(columnHandles.size() == tableScan.getOutput().size(), "tableScan must contains all columnHandle");

            ImmutableMap.Builder<Symbol, ColumnHandle> columnHandleMap = new ImmutableMap.Builder();
            for (int i = 0; i < columnHandles.size(); i++) {
                columnHandleMap.put(outputChannelNames.get(i), columnHandles.get(i));
            }

            Optional<TableLayoutHandle> tableLayoutHandle = Optional.empty();
            if (tableScan.getConnectorTableLayoutHandle().isPresent()) {
                tableLayoutHandle = Optional.of(
                        new TableLayoutHandle(
                                connectorId,
                                metadata.getTransactionHandle(session, connectorId),
                                tableScan.getConnectorTableLayoutHandle().get()));
            }

            return Optional.of(
                    new TableScanNode(
                            idAllocator.getNextId(),
                            new TableHandle(connectorId, tableScan.getTableHandle()),
                            outputChannelNames,
                            columnHandleMap.build(),
                            tableLayoutHandle,
                            TupleDomain.all(),
                            TupleDomain.all()));
        }

        private String getNameHint(ColumnExpression rowExpression)
        {
            //TODO name hit for NamedColumnHandle (which provides getName)
            if (rowExpression instanceof CallExpression) {
                return ((CallExpression) rowExpression).getSignature().getName();
            }
            else if (rowExpression instanceof ColumnReferenceExpression) {
                return "col";
            }
            return "expr";
        }

        private List<Symbol> getInputChannels(UnaryTableExpression node)
        {
            return getInputChannels(node, ImmutableMap.of());
        }

        private List<Symbol> getInputChannels(UnaryTableExpression node, Map<Integer, Symbol> knownInput)
        {
            // If expression is InputReference just re-use output name
            return IntStream.range(0, node.getSource().getOutput().size())
                    .boxed()
                    .map(i ->
                            knownInput.containsKey(i) ?
                                    knownInput.get(i) :
                                    symbolAllocator.newSymbol(
                                            getNameHint(node.getSource().getOutput().get(i)),
                                            node.getSource().getOutput().get(i).getType()))
                    .collect(toImmutableList());
        }

        private Optional<Expression> translate(ColumnExpression rowExpression, List<Symbol> inputChannelNames)
        {
            return ColumnExpressionToSqlTranslator.translate(rowExpression, inputChannelNames, ImmutableMap.of(), literalEncoder, functionRegistry);
        }

        private void checkNoColumnInputs(UnaryTableExpression node)
        {
            int numColumnReferences = node.getOutput().stream()
                    .mapToInt(column -> listColumnHandles(column).size())
                    .sum();
            checkArgument(numColumnReferences == 0, "Unary node %s has uncleaned column reference", node);
        }
    }
}
