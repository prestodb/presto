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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.AggregationNode.GroupingSetDescriptor;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.usePerfectlyConsistentHistories;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.DEFAULT;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.IGNORE_SAFE_CONSTANTS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.IGNORE_SCAN_CONSTANTS;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.expressions.CanonicalRowExpressionRewriter.canonicalizeRowExpression;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.StandardErrorCode.PLAN_SERIALIZATION_ERROR;
import static com.facebook.presto.sql.planner.CanonicalPartitioningScheme.getCanonicalPartitioningScheme;
import static com.facebook.presto.sql.planner.CanonicalTableScanNode.CanonicalTableHandle.getCanonicalTableHandle;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class CanonicalPlanGenerator
        extends InternalPlanVisitor<Optional<PlanNode>, CanonicalPlanGenerator.Context>
{
    private static final String CANONICAL_STRING = "CANONICAL";

    // Not using a new override to objectMapper because PlanNodeId has a JsonValue annotation which cannot be directly overriden in a serializer
    private final PlanNodeIdAllocator planNodeidAllocator;
    private final VariableAllocator variableAllocator = new VariableAllocator();
    // TODO: DEFAULT strategy has a very different canonicalizaiton implementation, refactor it into a separate class.
    private final PlanCanonicalizationStrategy strategy;
    private final ObjectMapper objectMapper;
    private final Session session;

    public CanonicalPlanGenerator(PlanCanonicalizationStrategy strategy, ObjectMapper objectMapper, Session session)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.session = requireNonNull(session, "session is null");
        this.planNodeidAllocator = createPlanNodeIdAllocator(strategy);
    }

    private PlanNodeIdAllocator createPlanNodeIdAllocator(PlanCanonicalizationStrategy strategy)
    {
        //ToDO: For HBO we always want planNodeId to be canonicalized but currently fragment result caching is using the same class with default strategy
        // refactor the default strategy to a different class
        if (strategy.equals(DEFAULT)) {
            return new PlanNodeIdAllocator();
        }
        else {
            return new PlanNodeIdAllocator()
            {
                @Override
                public PlanNodeId getNextId()
                {
                    return new PlanNodeId(CANONICAL_STRING);
                }
            };
        }
    }

    public static Optional<CanonicalPlanFragment> generateCanonicalPlanFragment(PlanNode root, PartitioningScheme partitioningScheme, ObjectMapper objectMapper, Session session)
    {
        Context context = new Context();
        Optional<PlanNode> canonicalPlan = root.accept(new CanonicalPlanGenerator(PlanCanonicalizationStrategy.DEFAULT, objectMapper, session), context);
        if (!context.getExpressions().keySet().containsAll(partitioningScheme.getOutputLayout())) {
            return Optional.empty();
        }
        return canonicalPlan.map(planNode -> new CanonicalPlanFragment(new CanonicalPlan(planNode, DEFAULT), getCanonicalPartitioningScheme(partitioningScheme, context.getExpressions())));
    }

    // Returns `CanonicalPlan`. If we encounter a `PlanNode` with unimplemented canonicalization, we return `Optional.empty()`
    public static Optional<CanonicalPlan> generateCanonicalPlan(PlanNode root, PlanCanonicalizationStrategy strategy, ObjectMapper objectMapper, Session session)
    {
        Optional<PlanNode> canonicalPlanNode = root.accept(new CanonicalPlanGenerator(strategy, objectMapper, session), new CanonicalPlanGenerator.Context());
        return canonicalPlanNode.map(planNode -> new CanonicalPlan(planNode, strategy));
    }

    @Override
    public Optional<PlanNode> visitPlan(PlanNode node, Context context)
    {
        // TODO: Support canonicalization for more plan node types
        return Optional.empty();
    }

    @Override
    public Optional<PlanNode> visitStatsEquivalentPlanNodeWithLimit(StatsEquivalentPlanNodeWithLimit node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> limit = node.getLimit().accept(this, context);
        if (!limit.isPresent()) {
            return Optional.empty();
        }

        Optional<PlanNode> plan = node.getPlan().accept(this, context);
        if (!plan.isPresent()) {
            return Optional.empty();
        }

        PlanNode result = new StatsEquivalentPlanNodeWithLimit(plan.get().getId(), plan.get(), limit.get());
        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitTableWriter(TableWriterNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> columns = node.getColumns().stream()
                .map(variable -> rename(variable, "", context))
                .sorted()
                .collect(toImmutableList());
        List<String> columnNames = node.getColumnNames().stream().sorted().collect(toImmutableList());

        PlanNode result = new TableWriterNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getTarget().map(target -> CanonicalWriterTarget.from(target)),
                node.getRowCountVariable(),
                node.getFragmentVariable(),
                node.getTableCommitContextVariable(),
                columns,
                columnNames,
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitTableFinish(TableFinishNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode result = new TableFinishNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getTarget().map(target -> CanonicalWriterTarget.from(target)),
                node.getRowCountVariable(),
                Optional.empty(),
                Optional.empty(),
                node.getCteMaterializationInfo());
        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitLimit(LimitNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode result = new LimitNode(Optional.empty(), planNodeidAllocator.getNextId(), source.get(), node.getCount(), node.getStep());
        context.addLimitingNodePlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitTopN(TopNNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode result = new TopNNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getCount(),
                getCanonicalOrderingScheme(node.getOrderingScheme(), context.getExpressions()),
                node.getStep());
        context.addLimitingNodePlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitJoin(JoinNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }
        if (node.getType().equals(JoinType.RIGHT)) {
            return visitJoin(node.flipChildren(), context);
        }
        List<PlanNode> sources = new ArrayList<>();
        ImmutableList.Builder<RowExpression> allFilters = ImmutableList.builder();
        ImmutableList.Builder<EquiJoinClause> criterias = ImmutableList.builder();
        Stack<JoinNode> stack = new Stack<>();

        stack.push(node);
        while (!stack.empty()) {
            JoinNode top = stack.pop();
            top.getCriteria().forEach(criterias::add);
            // ReorderJoins can move predicates between `criteria` and `filters`, so we put all equalities
            // in `criteria` to make it consistent.
            if (top.getFilter().isPresent()) {
                List<RowExpression> filters = extractConjuncts(top.getFilter().get());
                filters.forEach(filter -> {
                    Optional<EquiJoinClause> criteria = toEquiJoinClause(filter);
                    criteria.ifPresent(criterias::add);
                    allFilters.add(filter);
                });
            }
            for (PlanNode source : top.getSources()) {
                if (source instanceof JoinNode
                        && ((JoinNode) source).getType().equals(node.getType())
                        && shouldMergeJoinNodes(node.getType())) {
                    stack.push((JoinNode) source);
                }
                else {
                    sources.add(source);
                }
            }
        }

        // Sort sources if all are INNER, or full outer join of 2 nodes
        if (shouldMergeJoinNodes(node.getType()) || (node.getType().equals(JoinType.FULL) && sources.size() == 2)) {
            Optional<List<Integer>> sourceIndexes = orderSources(sources);
            if (!sourceIndexes.isPresent()) {
                return Optional.empty();
            }
            sources = sourceIndexes.get().stream().map(sources::get).collect(toImmutableList());
        }

        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        for (PlanNode source : sources) {
            Optional<PlanNode> newSource = source.accept(this, context);
            if (!newSource.isPresent()) {
                return Optional.empty();
            }
            newSources.add(newSource.get());
        }
        Set<EquiJoinClause> newCriterias = criterias.build().stream()
                .map(criteria -> canonicalize(criteria, context))
                .sorted(comparing(EquiJoinClause::toString))
                .collect(toCollection(LinkedHashSet::new));
        Set<RowExpression> newFilters = allFilters.build().stream()
                .map(filter -> inlineAndCanonicalize(context.getExpressions(), filter))
                .sorted(comparing(this::writeValueAsString))
                .collect(toCollection(LinkedHashSet::new));
        List<VariableReferenceExpression> outputVariables = node.getOutputVariables().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted()
                .collect(toImmutableList());

        PlanNode result = new CanonicalJoinNode(
                planNodeidAllocator.getNextId(),
                newSources.build(),
                node.getType(),
                newCriterias,
                newFilters,
                outputVariables);
        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitSemiJoin(SemiJoinNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        Optional<PlanNode> filteringSource = node.getFilteringSource().accept(this, context);
        if (!filteringSource.isPresent()) {
            return Optional.empty();
        }

        VariableReferenceExpression sourceJoinVariable = inlineAndCanonicalize(context.getExpressions(), node.getSourceJoinVariable());
        VariableReferenceExpression filteringSourceJoinVariable = inlineAndCanonicalize(context.getExpressions(), node.getFilteringSourceJoinVariable());
        VariableReferenceExpression semiJoinOutput = rename(node.getSemiJoinOutput(), "semijoinoutput", context);

        PlanNode result = new SemiJoinNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                filteringSource.get(),
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitUnion(UnionNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<List<Integer>> sourceIndexes = orderSources(node.getSources());
        if (!sourceIndexes.isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<PlanNode> canonicalSources = ImmutableList.builder();
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> outputsToInputs = ImmutableMap.builder();

        for (Integer sourceIndex : sourceIndexes.get()) {
            Optional<PlanNode> canonicalSource = node.getSources().get(sourceIndex).accept(this, context);
            if (!canonicalSource.isPresent()) {
                return Optional.empty();
            }
            canonicalSources.add(canonicalSource.get());
        }

        node.getVariableMapping().forEach((outputVariable, sourceVariables) -> {
            ImmutableList.Builder<VariableReferenceExpression> newSourceVariablesBuilder = ImmutableList.builder();
            sourceIndexes.get().forEach(index -> {
                newSourceVariablesBuilder.add(inlineAndCanonicalize(context.getExpressions(), sourceVariables.get(index)));
            });
            ImmutableList<VariableReferenceExpression> newSourceVariables = newSourceVariablesBuilder.build();
            VariableReferenceExpression newVariable = variableAllocator.newVariable(newSourceVariables.get(0));
            outputVariables.add(newVariable);
            context.mapExpression(outputVariable, newVariable);
            outputsToInputs.put(newVariable, newSourceVariables);
        });

        PlanNode result = new UnionNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                canonicalSources.build(),
                outputVariables.build().stream().sorted().collect(toImmutableList()),
                ImmutableSortedMap.copyOf(outputsToInputs.build()));

        context.addPlan(node, new CanonicalPlan(result, strategy));
        return Optional.of(result);
    }

    @Override
    public Optional<PlanNode> visitWindow(WindowNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        Set<VariableReferenceExpression> prePartitionedInputs = node.getPrePartitionedInputs().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted(comparing(this::writeValueAsString))
                .collect(toImmutableSet());

        DataOrganizationSpecification specification = new DataOrganizationSpecification(
                node.getSpecification().getPartitionBy().stream()
                        .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                        .sorted(comparing(this::writeValueAsString))
                        .collect(toImmutableList()),
                node.getOrderingScheme().map(scheme -> getCanonicalOrderingScheme(scheme, context.getExpressions())));

        Map<VariableReferenceExpression, WindowNode.Function> windowFunctions = node.getWindowFunctions()
                .entrySet().stream()
                .map(entry -> {
                    WindowNode.Function function = entry.getValue();
                    CallExpression callExpression = new CallExpression(
                            Optional.empty(),
                            function.getFunctionCall().getDisplayName(),
                            function.getFunctionCall().getFunctionHandle(),
                            function.getFunctionCall().getType(),
                            function.getFunctionCall().getArguments().stream()
                                    .map(expression -> inlineAndCanonicalize(context.getExpressions(), expression))
                                    .collect(toImmutableList()));
                    Optional<VariableReferenceExpression> startValue = function.getFrame().getStartValue()
                            .map(expression -> inlineAndCanonicalize(context.getExpressions(), expression));
                    Optional<VariableReferenceExpression> endValue = function.getFrame().getEndValue()
                            .map(expression -> inlineAndCanonicalize(context.getExpressions(), expression));
                    Optional<VariableReferenceExpression> sortKeyCoercedForFrameStartComparison = function.getFrame().getSortKeyCoercedForFrameStartComparison()
                            .map(expression -> inlineAndCanonicalize(context.getExpressions(), expression));
                    Optional<VariableReferenceExpression> sortKeyCoercedForFrameEndComparison = function.getFrame().getSortKeyCoercedForFrameEndComparison()
                            .map(expression -> inlineAndCanonicalize(context.getExpressions(), expression));
                    WindowNode.Frame frame = new WindowNode.Frame(
                            function.getFrame().getType(),
                            function.getFrame().getStartType(),
                            startValue,
                            sortKeyCoercedForFrameStartComparison,
                            function.getFrame().getEndType(),
                            endValue,
                            sortKeyCoercedForFrameEndComparison,
                            startValue.map(ignored -> ""),
                            endValue.map(ignored -> ""));
                    WindowNode.Function newFunction = new WindowNode.Function(
                            callExpression,
                            frame,
                            function.isIgnoreNulls());
                    return Maps.immutableEntry(entry.getKey(), newFunction);
                })
                .sorted(comparing(entry -> writeValueAsString(entry.getValue())))
                .map(entry -> {
                    VariableReferenceExpression variable = rename(entry.getKey(), entry.getValue().getFunctionCall().getDisplayName(), context);
                    return Maps.immutableEntry(variable, entry.getValue());
                })
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        PlanNode canonicalPlan = new WindowNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                specification,
                windowFunctions,
                Optional.empty(),
                prePartitionedInputs,
                node.getPreSortedOrderPrefix());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));

        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitValues(ValuesNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        List<List<RowExpression>> rows = node.getRows().stream()
                .map(row -> row.stream().map(expression -> inlineAndCanonicalize(context.getExpressions(), expression)).collect(toImmutableList()))
                .collect(toImmutableList());

        List<VariableReferenceExpression> outputVariables = node.getOutputVariables().stream()
                .map(variable -> rename(variable, "", context))
                .collect(toImmutableList());

        PlanNode canonicalPlan = new ValuesNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                outputVariables,
                rows,
                Optional.empty());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitMarkDistinct(MarkDistinctNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> distinctVariables = node.getDistinctVariables().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted(comparing(this::writeValueAsString))
                .collect(toImmutableList());

        VariableReferenceExpression markerVariable = rename(node.getMarkerVariable(), "is_distinct", context);
        PlanNode canonicalPlan = new MarkDistinctNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                markerVariable,
                distinctVariables,
                Optional.empty());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitAssignUniqueId(AssignUniqueId node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        VariableReferenceExpression idVariable = rename(node.getIdVariable(), "unique", context);

        PlanNode canonicalPlan = new AssignUniqueId(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                idVariable);
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitEnforceSingleRow(EnforceSingleRowNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode canonicalPlan = new EnforceSingleRowNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitRowNumber(RowNumberNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> partitionBy = node.getPartitionBy().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted(comparing(this::writeValueAsString))
                .collect(toImmutableList());

        VariableReferenceExpression rowNumberVariable = rename(node.getRowNumberVariable(), "row_number", context);
        PlanNode canonicalPlan = new RowNumberNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                partitionBy,
                rowNumberVariable,
                node.getMaxRowCountPerPartition(),
                node.isPartial(),
                Optional.empty());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitTopNRowNumber(TopNRowNumberNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> partitionBy = node.getPartitionBy().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted(comparing(this::writeValueAsString))
                .collect(toImmutableList());

        VariableReferenceExpression rowNumberVariable = rename(node.getRowNumberVariable(), "row_number", context);
        PlanNode canonicalPlan = new TopNRowNumberNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                new DataOrganizationSpecification(
                        partitionBy,
                        node.getSpecification().getOrderingScheme().map(scheme -> getCanonicalOrderingScheme(scheme, context.getExpressions()))),
                TopNRowNumberNode.RankingFunction.ROW_NUMBER,
                rowNumberVariable,
                node.getMaxRowCountPerPartition(),
                node.isPartial(),
                Optional.empty());
        context.addLimitingNodePlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitDistinctLimit(DistinctLimitNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> distinctVariables = node.getDistinctVariables().stream()
                .map(variable -> inlineAndCanonicalize(context.getExpressions(), variable))
                .sorted(comparing(this::writeValueAsString))
                .collect(toImmutableList());

        PlanNode canonicalPlan = new DistinctLimitNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getLimit(),
                node.isPartial(),
                distinctVariables,
                Optional.empty(),
                0);
        context.addLimitingNodePlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitSort(SortNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode canonicalPlan = new SortNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                getCanonicalOrderingScheme(node.getOrderingScheme(), context.getExpressions()),
                node.isPartial(),
                node.getPartitionBy());
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitOutput(OutputNode node, Context context)
    {
        if (strategy == DEFAULT) {
            return Optional.empty();
        }

        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<RowExpressionReference> rowExpressionReferences = node.getOutputVariables().stream()
                .map(variable -> new RowExpressionReference(inlineAndCanonicalize(context.getExpressions(), variable, strategy == IGNORE_SAFE_CONSTANTS), variable))
                .sorted(comparing(rowExpressionReference -> writeValueAsString(rowExpressionReference.getRowExpression())))
                .collect(toImmutableList());

        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
        for (RowExpressionReference rowExpressionReference : rowExpressionReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(rowExpressionReference.getRowExpression());
            context.mapExpression(rowExpressionReference.getVariableReferenceExpression(), reference);
            assignments.put(reference, rowExpressionReference.getRowExpression());
        }
        // Rewrite OutputNode as ProjectNode
        PlanNode canonicalPlan = new ProjectNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                new Assignments(assignments.build()),
                ProjectNode.Locality.LOCAL);
        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));

        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitAggregation(AggregationNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        // Steps to get canonical aggregations:
        //   1. Transform aggregation into canonical form
        //   2. Sort based on canonical aggregation expression
        //   3. Get new variable reference for aggregation expression
        //   4. Record mapping from original variable reference to the new one
        List<AggregationReference> aggregationReferences = node.getAggregations().entrySet().stream()
                .map(entry -> new AggregationReference(getCanonicalAggregation(entry.getValue(), context.getExpressions()), entry.getKey()))
                .sorted(comparing(aggregationReference -> writeValueAsString(aggregationReference.getAggregation().getCall())))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (AggregationReference aggregationReference : aggregationReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(aggregationReference.getAggregation().getCall());
            context.mapExpression(aggregationReference.getVariableReferenceExpression(), reference);
            aggregations.put(reference, aggregationReference.getAggregation());
        }

        PlanNode canonicalPlan = new AggregationNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                aggregations.build(),
                getCanonicalGroupingSetDescriptor(node.getGroupingSets(), context.getExpressions()),
                node.getPreGroupedVariables().stream()
                        .map(variable -> context.getExpressions().get(variable))
                        .collect(toImmutableList()),
                node.getStep(),
                node.getHashVariable().map(ignored -> variableAllocator.newHashVariable()),
                node.getGroupIdVariable().map(variable -> context.getExpressions().get(variable)),
                // ignore aggregationId when creating the canonical plan
                Optional.empty());

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitSequence(SequenceNode node, Context context)
    {
        node.getCteProducers().forEach(x -> x.accept(this, context));
        return node.getPrimarySource().accept(this, context);
    }

    @Override
    public Optional<PlanNode> visitCteProducer(CteProducerNode node, Context context)
    {
        return node.getSource().accept(this, context);
    }

    @Override
    public Optional<PlanNode> visitCteConsumer(CteConsumerNode node, Context context)
    {
        return node.getOriginalSource().accept(this, context);
    }

    @Override
    public Optional<PlanNode> visitCteReference(CteReferenceNode node, Context context)
    {
        return node.getSource().accept(this, context);
    }

    private Aggregation getCanonicalAggregation(Aggregation aggregation, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new Aggregation(
                inlineAndCanonicalize(context, aggregation.getCall()),
                aggregation.getFilter().map(filter -> inlineAndCanonicalize(context, filter)),
                aggregation.getOrderBy().map(orderBy -> getCanonicalOrderingScheme(orderBy, context)),
                aggregation.isDistinct(),
                aggregation.getMask().map(mask -> inlineAndCanonicalize(context, mask)));
    }

    private static OrderingScheme getCanonicalOrderingScheme(OrderingScheme orderingScheme, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new OrderingScheme(
                orderingScheme.getOrderBy().stream()
                        .map(orderBy -> new Ordering(inlineAndCanonicalize(context, orderBy.getVariable()), orderBy.getSortOrder()))
                        .collect(toImmutableList()));
    }

    private static GroupingSetDescriptor getCanonicalGroupingSetDescriptor(GroupingSetDescriptor groupingSetDescriptor, Map<VariableReferenceExpression, VariableReferenceExpression> context)
    {
        return new GroupingSetDescriptor(
                groupingSetDescriptor.getGroupingKeys().stream()
                        .map(key -> inlineAndCanonicalize(context, key))
                        .collect(toImmutableList()),
                groupingSetDescriptor.getGroupingSetCount(),
                groupingSetDescriptor.getGlobalGroupingSets());
    }

    private static class AggregationReference
    {
        private final Aggregation aggregation;
        private final VariableReferenceExpression variableReferenceExpression;

        public AggregationReference(Aggregation aggregation, VariableReferenceExpression variableReferenceExpression)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public Aggregation getAggregation()
        {
            return aggregation;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    @Override
    public Optional<PlanNode> visitGroupId(GroupIdNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, VariableReferenceExpression> entry : node.getGroupingColumns().entrySet()) {
            VariableReferenceExpression column = context.getExpressions().get(entry.getValue());
            VariableReferenceExpression reference = variableAllocator.newVariable(column, "gid");
            context.mapExpression(entry.getKey(), reference);
            groupingColumns.put(reference, column);
        }

        ImmutableList.Builder<List<VariableReferenceExpression>> groupingSets = ImmutableList.builder();
        for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
            groupingSets.add(groupingSet.stream()
                    .map(variable -> context.getExpressions().get(variable))
                    .collect(toImmutableList()));
        }

        VariableReferenceExpression groupId = variableAllocator.newVariable("groupid", INTEGER);
        context.mapExpression(node.getGroupIdVariable(), groupId);

        PlanNode canonicalPlan = new GroupIdNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                groupingSets.build(),
                groupingColumns.build(),
                node.getAggregationArguments().stream()
                        .map(variable -> context.getExpressions().get(variable))
                        .collect(toImmutableList()),
                groupId);

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitUnnest(UnnestNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        // Generate canonical unnestVariables.
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> newUnnestVariables = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariable : node.getUnnestVariables().entrySet()) {
            VariableReferenceExpression input = (VariableReferenceExpression) inlineAndCanonicalize(context.getExpressions(), unnestVariable.getKey());
            ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : unnestVariable.getValue()) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable(Optional.empty(), "unnest_field", variable.getType());
                context.mapExpression(variable, newVariable);
                newVariables.add(newVariable);
            }
            newUnnestVariables.put(input, newVariables.build());
        }

        // Generate canonical ordinality variable
        Optional<VariableReferenceExpression> ordinalityVariable = node.getOrdinalityVariable()
                .map(variable -> {
                    VariableReferenceExpression newVariable = variableAllocator.newVariable(Optional.empty(), "unnest_ordinality", variable.getType());
                    context.mapExpression(variable, newVariable);
                    return newVariable;
                });

        PlanNode canonicalPlan = new UnnestNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                node.getReplicateVariables().stream()
                        .map(variable -> (VariableReferenceExpression) inlineAndCanonicalize(context.getExpressions(), variable))
                        .collect(toImmutableList()),
                newUnnestVariables.build(),
                ordinalityVariable);

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitProject(ProjectNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        List<RowExpressionReference> rowExpressionReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new RowExpressionReference(inlineAndCanonicalize(context.getExpressions(), entry.getValue(), strategy == IGNORE_SAFE_CONSTANTS || strategy == IGNORE_SCAN_CONSTANTS), entry.getKey()))
                .sorted(comparing(rowExpressionReference -> writeValueAsString(rowExpressionReference.getRowExpression())))
                .collect(toImmutableList());
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.builder();
        for (RowExpressionReference rowExpressionReference : rowExpressionReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(rowExpressionReference.getRowExpression());
            context.mapExpression(rowExpressionReference.getVariableReferenceExpression(), reference);
            assignments.put(reference, rowExpressionReference.getRowExpression());
        }

        PlanNode canonicalPlan = new ProjectNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                new Assignments(assignments.build()),
                node.getLocality());

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    // Variable names and plan node ids can change with what order we process nodes because of our
    // stateful canonicalization using `variableAllocator` and `planNodeIdAllocator`.
    // We want to order sources in a consistent manner, because the order matters when hashing plan.
    // Returns a list of indices in input sources array, with a canonical order.
    private Optional<List<Integer>> orderSources(List<PlanNode> sources)
    {
        // Try heuristic where we sort sources by the tables they scan.
        Optional<List<Integer>> sourcesByTables = orderSourcesByTables(sources);
        if (sourcesByTables.isPresent()) {
            return sourcesByTables;
        }

        if (!usePerfectlyConsistentHistories(session)) {
            return Optional.of(IntStream.range(0, sources.size()).boxed().collect(toImmutableList()));
        }

        // We canonicalize each source independently, and use its representation to order sources.
        Multimap<String, Integer> sourceToPosition = TreeMultimap.create();
        for (int i = 0; i < sources.size(); ++i) {
            Optional<CanonicalPlan> canonicalSource = generateCanonicalPlan(sources.get(i), strategy, objectMapper, session);
            if (!canonicalSource.isPresent()) {
                return Optional.empty();
            }
            sourceToPosition.put(canonicalSource.get().toString(objectMapper), i);
        }
        return Optional.of(sourceToPosition.values().stream().collect(toImmutableList()));
    }

    // Order sources by list of tables they use. If any 2 sources are using the same set of tables, we give up
    // and return Optional.empty().
    // Returns a list of indices in input sources array, with a canonical order
    private Optional<List<Integer>> orderSourcesByTables(List<PlanNode> sources)
    {
        Multimap<String, Integer> sourceToPosition = TreeMultimap.create();
        for (int i = 0; i < sources.size(); ++i) {
            List<String> tables = new ArrayList<>();

            PlanNodeSearcher.searchFrom(sources.get(i))
                    .where(node -> node instanceof TableScanNode)
                    .findAll()
                    .forEach(node -> tables.add(((TableScanNode) node).getTable().getConnectorHandle().toString()));
            sourceToPosition.put(tables.stream().sorted().collect(Collectors.joining(",")), i);
        }
        String lastIdentifier = ",";
        for (Map.Entry<String, Integer> entry : sourceToPosition.entries()) {
            String identifier = entry.getKey();
            if (lastIdentifier.equals(identifier)) {
                return Optional.empty();
            }
            lastIdentifier = identifier;
        }
        return Optional.of(sourceToPosition.values().stream().collect(toImmutableList()));
    }

    private static class CanonicalWriterTarget
            extends TableWriterNode.WriterTarget
    {
        private final ConnectorId connectorId;
        // Include classname of WriterTarget, as it signifies type of table operation.
        private final String writerTargetType;

        @JsonCreator
        public CanonicalWriterTarget(
                @JsonProperty("connectorId") ConnectorId connectorId,
                @JsonProperty("writerTargetType") String writerTargetType)
        {
            this.connectorId = connectorId;
            this.writerTargetType = writerTargetType;
        }

        @JsonProperty
        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        @JsonProperty
        public String getWriterTargetType()
        {
            return writerTargetType;
        }

        @Override
        public SchemaTableName getSchemaTableName()
        {
            // Just return a sample table name, which is always same
            return new SchemaTableName("schema", "table");
        }

        @Override
        public Optional<List<OutputColumnMetadata>> getOutputColumns()
        {
            return Optional.empty();
        }

        @Override
        public String toString()
        {
            return format("WriterTarget{connectorId: %s, type: %s}", connectorId, writerTargetType);
        }

        private static CanonicalWriterTarget from(TableWriterNode.WriterTarget target)
        {
            return new CanonicalWriterTarget(target.getConnectorId(), target.getClass().getSimpleName());
        }
    }

    private static class RowExpressionReference
    {
        private final RowExpression rowExpression;
        private final VariableReferenceExpression variableReferenceExpression;

        public RowExpressionReference(RowExpression rowExpression, VariableReferenceExpression variableReferenceExpression)
        {
            this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public RowExpression getRowExpression()
        {
            return rowExpression;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    @Override
    public Optional<PlanNode> visitFilter(FilterNode node, Context context)
    {
        Optional<PlanNode> source = node.getSource().accept(this, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        PlanNode canonicalPlan = new FilterNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                source.get(),
                inlineAndCanonicalize(context.getExpressions(), node.getPredicate()));

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    @Override
    public Optional<PlanNode> visitTableScan(TableScanNode node, Context context)
    {
        List<ColumnReference> columnReferences = node.getAssignments().entrySet().stream()
                .map(entry -> new ColumnReference(entry.getValue(), entry.getKey()))
                .sorted(comparing(columnReference -> columnReference.getColumnHandle().toString()))
                .collect(toImmutableList());
        ImmutableList.Builder<VariableReferenceExpression> outputVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (ColumnReference columnReference : columnReferences) {
            VariableReferenceExpression reference = variableAllocator.newVariable(Optional.empty(), columnReference.getColumnHandle().toString(), columnReference.getVariableReferenceExpression().getType());
            context.mapExpression(columnReference.getVariableReferenceExpression(), reference);
            outputVariables.add(reference);
            assignments.put(reference, columnReference.getColumnHandle());
        }

        PlanNode canonicalPlan = new CanonicalTableScanNode(
                Optional.empty(),
                planNodeidAllocator.getNextId(),
                getCanonicalTableHandle(node.getTable(), strategy),
                outputVariables.build(),
                assignments.build());

        context.addPlan(node, new CanonicalPlan(canonicalPlan, strategy));
        return Optional.of(canonicalPlan);
    }

    private boolean shouldMergeJoinNodes(JoinType type)
    {
        return type.equals(JoinType.INNER);
    }

    private VariableReferenceExpression rename(VariableReferenceExpression variable, String nameHint, Context context)
    {
        VariableReferenceExpression newVariable = variableAllocator.newVariable(Optional.empty(), nameHint, variable.getType());
        context.mapExpression(variable, newVariable);
        return newVariable;
    }

    private String writeValueAsString(Object object)
    {
        try {
            return objectMapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(PLAN_SERIALIZATION_ERROR, "Cannot serialize plan to JSON", e);
        }
    }

    private static EquiJoinClause canonicalize(EquiJoinClause criteria, Context context)
    {
        VariableReferenceExpression left = inlineAndCanonicalize(context.getExpressions(), criteria.getLeft());
        VariableReferenceExpression right = inlineAndCanonicalize(context.getExpressions(), criteria.getRight());
        return left.compareTo(right) > 0 ? new EquiJoinClause(left, right) : new EquiJoinClause(right, left);
    }

    private static Optional<EquiJoinClause> toEquiJoinClause(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression callExpression = (CallExpression) expression;
        boolean isValid = callExpression.getDisplayName().equals(EQUAL.getFunctionName().getObjectName())
                && callExpression.getArguments().size() == 2
                && callExpression.getArguments().get(0) instanceof VariableReferenceExpression
                && callExpression.getArguments().get(1) instanceof VariableReferenceExpression;

        if (!isValid) {
            return Optional.empty();
        }

        return Optional.of(new EquiJoinClause(
                (VariableReferenceExpression) callExpression.getArguments().get(0),
                (VariableReferenceExpression) callExpression.getArguments().get(1)));
    }

    private static <T extends RowExpression> T inlineAndCanonicalize(
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            T expression)
    {
        return inlineAndCanonicalize(context, expression, false);
    }

    private static <T extends RowExpression> T inlineAndCanonicalize(
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            T expression,
            boolean removeConstants)
    {
        return (T) canonicalizeRowExpression(inlineVariables(variable -> context.getOrDefault(variable, variable), expression), removeConstants);
    }

    private static class ColumnReference
    {
        private final ColumnHandle columnHandle;
        private final VariableReferenceExpression variableReferenceExpression;

        public ColumnReference(ColumnHandle columnHandle, VariableReferenceExpression variableReferenceExpression)
        {
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.variableReferenceExpression = requireNonNull(variableReferenceExpression, "variableReferenceExpression is null");
        }

        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        public VariableReferenceExpression getVariableReferenceExpression()
        {
            return variableReferenceExpression;
        }
    }

    public static class Context
    {
        private final Map<VariableReferenceExpression, VariableReferenceExpression> expressions = new HashMap<>();
        private final Map<PlanNode, CanonicalPlan> canonicalPlans = new IdentityHashMap<>();
        private final Map<PlanNode, PlanNode> canonicalPlanToPlan = new IdentityHashMap<>();
        private final Map<PlanNode, List<TableScanNode>> inputTables = new IdentityHashMap<>();

        public Map<VariableReferenceExpression, VariableReferenceExpression> getExpressions()
        {
            return expressions;
        }

        public Map<PlanNode, CanonicalPlan> getCanonicalPlans()
        {
            return canonicalPlans;
        }

        public Map<PlanNode, List<TableScanNode>> getInputTables()
        {
            return inputTables;
        }

        public void mapExpression(VariableReferenceExpression from, VariableReferenceExpression to)
        {
            expressions.put(from, to);
        }

        private void addLimitingNodePlan(PlanNode limit, CanonicalPlan canonicalPlan)
        {
            if (!limit.getStatsEquivalentPlanNode().isPresent()) {
                addPlanInternal(limit, canonicalPlan);
                return;
            }
            // When limits are involved, we can only know canonicalized plans after topmost limit has been canonicalized.
            // Once we are at topmost limit, we cache canonicalized plans for all sub-plans.
            PlanNode statsEquivalentPlanNode = limit.getStatsEquivalentPlanNode().get();
            StatsEquivalentPlanNodeWithLimit statsEquivalentPlanNodeWithLimit = (StatsEquivalentPlanNodeWithLimit) statsEquivalentPlanNode;
            if (childrenCount(statsEquivalentPlanNodeWithLimit.getLimit()) != childrenCount(statsEquivalentPlanNodeWithLimit.getPlan())) {
                addPlanInternal(limit, canonicalPlan);
                return;
            }
            forTree(PlanNode::getSources)
                    .depthFirstPreOrder(limit)
                    .forEach(child -> {
                        CanonicalPlan childCanonicalPlan = child == limit ? canonicalPlan : canonicalPlans.get(child);
                        if (childCanonicalPlan == null || !child.getStatsEquivalentPlanNode().isPresent()) {
                            return;
                        }
                        // Only save canonicalized plans for stats equivalent plan nodes.
                        canonicalPlans.remove(child);
                        inputTables.remove(child);
                        addPlanInternal(
                                child.getStatsEquivalentPlanNode().get(),
                                new CanonicalPlan(
                                        new StatsEquivalentPlanNodeWithLimit(childCanonicalPlan.getPlan().getId(), childCanonicalPlan.getPlan(), canonicalPlan.getPlan()),
                                        canonicalPlan.getStrategy()));
                    });
        }

        private void addPlan(PlanNode plan, CanonicalPlan canonicalPlan)
        {
            if (!plan.getStatsEquivalentPlanNode().isPresent()) {
                addPlanInternal(plan, canonicalPlan);
                return;
            }
            PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
            if (childrenCount(plan) == childrenCount(statsEquivalentPlanNode)) {
                addPlanInternal(statsEquivalentPlanNode, canonicalPlan);
            }
            else {
                addPlanInternal(plan, canonicalPlan);
            }
        }

        private int childrenCount(PlanNode root)
        {
            return Iterables.size(forTree(PlanNode::getSources).depthFirstPreOrder(root));
        }

        private void addPlanInternal(PlanNode plan, CanonicalPlan canonicalPlan)
        {
            ImmutableList.Builder<TableScanNode> inputs = ImmutableList.builder();
            canonicalPlans.put(plan, canonicalPlan);
            canonicalPlanToPlan.put(canonicalPlan.getPlan(), plan);
            for (PlanNode node : forTree(PlanNode::getSources).depthFirstPreOrder(canonicalPlan.getPlan())) {
                if (node instanceof CanonicalTableScanNode) {
                    if (canonicalPlanToPlan.containsKey(node)) {
                        inputs.add((TableScanNode) canonicalPlanToPlan.get(node));
                    }
                }
            }
            inputTables.put(plan, inputs.build());
        }
    }
}
