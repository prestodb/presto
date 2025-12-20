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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.plan.WindowNode.Frame.BoundType;
import com.facebook.presto.spi.plan.WindowNode.Frame.WindowType;
import com.facebook.presto.spi.statistics.SourceInfo;
import com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.StrictAssignedSymbolsMatcher.actualAssignments;
import static com.facebook.presto.sql.planner.assertions.StrictSymbolsMatcher.actualOutputs;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class PlanMatchPattern
{
    private final List<Matcher> matchers = new ArrayList<>();

    private final List<PlanMatchPattern> sourcePatterns;
    private boolean anyTree;

    public static PlanMatchPattern node(Class<? extends PlanNode> nodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new PlanNodeMatcher(nodeClass));
    }

    public static PlanMatchPattern any(PlanMatchPattern... sources)
    {
        return new PlanMatchPattern(ImmutableList.copyOf(sources));
    }

    /**
     * Matches to any tree of nodes with children matching to given source matchers.
     * anyNodeTree(tableScanNode("nation")) - will match to any plan which all leafs contain
     * any node containing table scan from nation table.
     *
     * @note anyTree does not match zero nodes. E.g. output(anyTree(tableScan)) will NOT match TableScan node followed by OutputNode.
     */
    public static PlanMatchPattern anyTree(PlanMatchPattern... sources)
    {
        return any(sources).matchToAnyNodeTree();
    }

    public static PlanMatchPattern anyNot(Class<? extends PlanNode> excludeNodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new NotPlanNodeMatcher(excludeNodeClass));
    }

    public static PlanMatchPattern tableScan(String expectedTableName)
    {
        return TableScanMatcher.create(expectedTableName);
    }

    public static PlanMatchPattern tableScan(String expectedTableName, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = tableScan(expectedTableName);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern strictTableScan(String expectedTableName, Map<String, String> columnReferences)
    {
        return tableScan(expectedTableName, columnReferences)
                .withExactAssignedOutputs(columnReferences.values().stream()
                        .map(columnName -> columnReference(expectedTableName, columnName))
                        .collect(toImmutableList()));
    }

    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint)
    {
        return TableScanMatcher.builder(expectedTableName)
                .expectedConstraint(constraint)
                .build();
    }

    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = constrainedTableScan(expectedTableName, constraint);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern constrainedTableScanWithTableLayout(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = TableScanMatcher.builder(expectedTableName)
                .expectedConstraint(constraint)
                .hasTableLayout()
                .build();
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern indexSource(String expectedTableName)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName));
    }

    public static PlanMatchPattern indexSource(String expectedTableName, Map<String, String> columnReferences)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName))
                .addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern strictIndexSource(String expectedTableName, Map<String, String> columnReferences)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName))
                .withExactAssignedOutputs(columnReferences.values().stream()
                        .map(columnName -> columnReference(expectedTableName, columnName))
                        .collect(toImmutableList()));
    }

    public static PlanMatchPattern constrainedIndexSource(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName, constraint))
                .addColumnReferences(expectedTableName, columnReferences);
    }

    private PlanMatchPattern addColumnReferences(String expectedTableName, Map<String, String> columnReferences)
    {
        columnReferences.forEach((key, value) -> withAlias(key, columnReference(expectedTableName, value)));
        return this;
    }

    public static PlanMatchPattern aggregation(
            Map<String, ExpectedValueProvider<FunctionCall>> aggregations,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source);
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern aggregation(
            Map<String, ExpectedValueProvider<FunctionCall>> aggregations,
            Step step,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source).with(new AggregationStepMatcher(step));
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern aggregation(
            GroupingSetDescriptor groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            Map<Symbol, Symbol> masks,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        return aggregation(groupingSets, aggregations, ImmutableList.of(), masks, groupId, step, source);
    }

    public static PlanMatchPattern aggregation(
            GroupingSetDescriptor groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            List<String> preGroupedSymbols,
            Map<Symbol, Symbol> masks,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source);
        aggregations.entrySet().forEach(
                aggregation ->
                {
                    if (aggregation.getKey().isPresent() && masks.containsKey(new Symbol(aggregation.getKey().get()))) {
                        result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue(), masks.get(new Symbol(aggregation.getKey().get()))));
                    }
                    else {
                        result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue()));
                    }
                });
        // Put the AggregationMatcher at the end as the mask mapping will use the output mapping from aggregation function calls above
        result.with(new AggregationMatcher(groupingSets, preGroupedSymbols, masks, groupId, step));
        return result;
    }

    public static PlanMatchPattern markDistinct(
            String markerSymbol,
            List<String> distinctSymbols,
            PlanMatchPattern source)
    {
        return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
                new SymbolAlias(markerSymbol),
                toSymbolAliases(distinctSymbols),
                Optional.empty()));
    }

    public static PlanMatchPattern markDistinct(
            String markerSymbol,
            List<String> distinctSymbols,
            String hashSymbol,
            PlanMatchPattern source)
    {
        return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
                new SymbolAlias(markerSymbol),
                toSymbolAliases(distinctSymbols),
                Optional.of(new SymbolAlias(hashSymbol))));
    }

    public static ExpectedValueProvider<WindowNode.Frame> windowFrame(
            WindowType type,
            BoundType startType,
            Optional<String> startValue,
            BoundType endType,
            Optional<String> endValue,
            Optional<String> sortKey)
    {
        return windowFrame(type, startType, startValue, Optional.empty(), sortKey, Optional.empty(), endType, endValue, Optional.empty(), sortKey, Optional.empty());
    }

    public static ExpectedValueProvider<WindowNode.Frame> windowFrame(
            WindowType type,
            BoundType startType,
            Optional<String> startValue,
            Optional<Type> startValueType,
            Optional<String> sortKeyForStartComparison,
            Optional<Type> sortKeyForStartComparisonType,
            BoundType endType,
            Optional<String> endValue,
            Optional<Type> endValueType,
            Optional<String> sortKeyForEndComparison,
            Optional<Type> sortKeyForEndComparisonType)
    {
        return new WindowFrameProvider(
                type,
                startType,
                startValue.map(SymbolAlias::new),
                startValueType,
                sortKeyForStartComparison.map(SymbolAlias::new),
                sortKeyForStartComparisonType,
                endType,
                endValue.map(SymbolAlias::new),
                endValueType,
                sortKeyForEndComparison.map(SymbolAlias::new),
                sortKeyForEndComparisonType);
    }

    public static PlanMatchPattern window(Consumer<WindowMatcher.Builder> windowMatcherBuilderConsumer, PlanMatchPattern source)
    {
        WindowMatcher.Builder windowMatcherBuilder = new WindowMatcher.Builder(source);
        windowMatcherBuilderConsumer.accept(windowMatcherBuilder);
        return windowMatcherBuilder.build();
    }

    public static PlanMatchPattern rowNumber(Consumer<RowNumberMatcher.Builder> rowNumberMatcherBuilderConsumer, PlanMatchPattern source)
    {
        RowNumberMatcher.Builder rowNumberMatcherBuilder = new RowNumberMatcher.Builder(source);
        rowNumberMatcherBuilderConsumer.accept(rowNumberMatcherBuilder);
        return rowNumberMatcherBuilder.build();
    }

    public static PlanMatchPattern topNRowNumber(Consumer<TopNRowNumberMatcher.Builder> topNRowNumberMatcherBuilderConsumer, PlanMatchPattern source)
    {
        TopNRowNumberMatcher.Builder topNRowNumberMatcherBuilder = new TopNRowNumberMatcher.Builder(source);
        topNRowNumberMatcherBuilderConsumer.accept(topNRowNumberMatcherBuilder);
        return topNRowNumberMatcherBuilder.build();
    }

    public static PlanMatchPattern sort(PlanMatchPattern source)
    {
        return node(SortNode.class, source);
    }

    public static PlanMatchPattern sort(List<Ordering> orderBy, PlanMatchPattern source)
    {
        return node(SortNode.class, source)
                .with(new SortMatcher(orderBy));
    }

    public static PlanMatchPattern topN(long count, List<Ordering> orderBy, PlanMatchPattern source)
    {
        return node(TopNNode.class, source).with(new TopNMatcher(count, orderBy));
    }

    public static PlanMatchPattern output(PlanMatchPattern source)
    {
        return node(OutputNode.class, source);
    }

    public static PlanMatchPattern output(List<String> outputs, PlanMatchPattern source)
    {
        PlanMatchPattern result = output(source);
        result.withOutputs(outputs);
        return result;
    }

    public static PlanMatchPattern strictOutput(List<String> outputs, PlanMatchPattern source)
    {
        return output(outputs, source).withExactOutputs(outputs);
    }

    public static PlanMatchPattern project(PlanMatchPattern source)
    {
        return node(ProjectNode.class, source);
    }

    public static PlanMatchPattern project(Map<String, ExpressionMatcher> assignments, PlanMatchPattern source)
    {
        PlanMatchPattern result = project(source);
        assignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
        return result;
    }

    public static PlanMatchPattern strictProject(Map<String, ExpressionMatcher> assignments, PlanMatchPattern source)
    {
        /*
         * Under the current implementation of project, all of the outputs are also in the assignment.
         * If the implementation changes, this will need to change too.
         */
        return project(assignments, source)
                .withExactAssignedOutputs(assignments.values())
                .withExactAssignments(assignments.values());
    }

    public static PlanMatchPattern semiJoin(PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return semiJoin(sourceSymbolAlias, filteringSymbolAlias, outputAlias, Optional.empty(), source, filtering);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, Optional<SemiJoinNode.DistributionType> distributionType, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias, distributionType));
    }

    public static PlanMatchPattern join(PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right);
    }

    public static PlanMatchPattern join(JoinType joinType, List<ExpectedValueProvider<EquiJoinClause>> expectedEquiCriteria, PlanMatchPattern left, PlanMatchPattern right)
    {
        return join(joinType, expectedEquiCriteria, Optional.empty(), left, right);
    }

    public static PlanMatchPattern join(JoinType joinType, List<ExpectedValueProvider<EquiJoinClause>> expectedEquiCriteria, Optional<String> expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return join(joinType, expectedEquiCriteria, expectedFilter, Optional.empty(), left, right);
    }

    public static PlanMatchPattern join(JoinType joinType, List<ExpectedValueProvider<EquiJoinClause>> expectedEquiCriteria, Optional<String> expectedFilter, Optional<JoinDistributionType> expectedDistributionType, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(
                new JoinMatcher(
                        joinType,
                        expectedEquiCriteria,
                        expectedFilter.map(predicate -> rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(predicate))),
                        expectedDistributionType,
                        Optional.empty()));
    }

    public static PlanMatchPattern join(
            JoinType joinType,
            List<ExpectedValueProvider<EquiJoinClause>> expectedEquiCriteria,
            Map<String, String> expectedDynamicFilter,
            Optional<String> expectedStaticFilter,
            PlanMatchPattern leftSource,
            PlanMatchPattern right)
    {
        Map<SymbolAlias, SymbolAlias> expectedDynamicFilterAliases = expectedDynamicFilter.entrySet().stream()
                .collect(toImmutableMap(entry -> new SymbolAlias(entry.getKey()), entry -> new SymbolAlias(entry.getValue())));
        DynamicFilterMatcher dynamicFilterMatcher = new DynamicFilterMatcher(
                expectedDynamicFilterAliases,
                expectedStaticFilter.map(predicate -> rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(predicate))));
        JoinMatcher joinMatcher = new JoinMatcher(
                joinType,
                expectedEquiCriteria,
                Optional.empty(),
                Optional.empty(),
                Optional.of(dynamicFilterMatcher));

        return node(JoinNode.class, anyTree(node(FilterNode.class, leftSource).with(dynamicFilterMatcher)), right)
                .with(joinMatcher);
    }

    public static PlanMatchPattern indexJoin(PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(IndexJoinNode.class, left, right);
    }

    public static PlanMatchPattern cteConsumer(String cteName)
    {
        CteConsumerMatcher cteConsumerMatcher = new CteConsumerMatcher(cteName);
        return node(CteConsumerNode.class).with(cteConsumerMatcher);
    }

    public static PlanMatchPattern cteProducer(String cteName, PlanMatchPattern source)
    {
        CteProducerMatcher cteProducerMatcher = new CteProducerMatcher(cteName);
        return node(CteProducerNode.class, source).with(cteProducerMatcher);
    }

    public static PlanMatchPattern sequence(PlanMatchPattern... sources)
    {
        return node(SequenceNode.class, sources);
    }

    public static PlanMatchPattern spatialJoin(String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return spatialJoin(expectedFilter, Optional.empty(), left, right);
    }

    public static PlanMatchPattern spatialJoin(String expectedFilter, Optional<String> kdbTree, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(SpatialJoinNode.class, left, right).with(
                new SpatialJoinMatcher(SpatialJoinNode.SpatialJoinType.INNER, rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedFilter, new ParsingOptions())), kdbTree));
    }

    public static PlanMatchPattern spatialLeftJoin(String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(SpatialJoinNode.class, left, right).with(
                new SpatialJoinMatcher(SpatialJoinNode.SpatialJoinType.LEFT, rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedFilter, new ParsingOptions())), Optional.empty()));
    }

    public static PlanMatchPattern mergeJoin(JoinType joinType, List<ExpectedValueProvider<EquiJoinClause>> expectedEquiCriteria, Optional<Expression> filter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(MergeJoinNode.class, left, right).with(
                new MergeJoinMatcher(
                        joinType,
                        expectedEquiCriteria,
                        filter));
    }

    public static PlanMatchPattern unnest(PlanMatchPattern source)
    {
        return node(UnnestNode.class, source);
    }

    public static PlanMatchPattern unnest(Map<String, List<String>> unnestVariables, PlanMatchPattern source)
    {
        return node(UnnestNode.class, source).with(new UnnestMatcher(unnestVariables));
    }

    public static PlanMatchPattern exchange(PlanMatchPattern... sources)
    {
        return node(ExchangeNode.class, sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, PlanMatchPattern... sources)
    {
        return exchange(scope, type, ImmutableList.of(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, List<Ordering> orderBy, PlanMatchPattern... sources)
    {
        return exchange(scope, type, orderBy, ImmutableSet.of(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, List<Ordering> orderBy, Set<String> partitionedBy, PlanMatchPattern... sources)
    {
        return node(ExchangeNode.class, sources)
                .with(new ExchangeMatcher(scope, type, orderBy, partitionedBy));
    }

    public static PlanMatchPattern union(PlanMatchPattern... sources)
    {
        return node(UnionNode.class, sources);
    }

    public static PlanMatchPattern assignUniqueId(String uniqueSymbolAlias, PlanMatchPattern source)
    {
        return node(AssignUniqueId.class, source)
                .withAlias(uniqueSymbolAlias, new AssignUniqueIdMatcher());
    }

    public static PlanMatchPattern intersect(PlanMatchPattern... sources)
    {
        return node(IntersectNode.class, sources);
    }

    public static PlanMatchPattern except(PlanMatchPattern... sources)
    {
        return node(ExceptNode.class, sources);
    }

    public static ExpectedValueProvider<EquiJoinClause> equiJoinClause(String left, String right)
    {
        return new EquiJoinClauseProvider(new SymbolAlias(left), new SymbolAlias(right));
    }

    public static SymbolAlias symbol(String alias)
    {
        return new SymbolAlias(alias);
    }

    public static PlanMatchPattern filter(String expectedPredicate, PlanMatchPattern source)
    {
        return filter(rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedPredicate)), source);
    }

    public static PlanMatchPattern filterWithDecimal(String expectedPredicate, PlanMatchPattern source)
    {
        return filter(rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedPredicate, new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL))), source);
    }

    public static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source)
    {
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate));
    }

    public static PlanMatchPattern filter(PlanMatchPattern source)
    {
        return node(FilterNode.class, source);
    }

    public static PlanMatchPattern apply(List<String> correlationSymbolAliases, Map<String, ExpressionMatcher> subqueryAssignments, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        PlanMatchPattern result = node(ApplyNode.class, inputPattern, subqueryPattern)
                .with(new CorrelationMatcher(correlationSymbolAliases));
        subqueryAssignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
        return result;
    }

    public static PlanMatchPattern lateral(List<String> correlationSymbolAliases, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        return node(LateralJoinNode.class, inputPattern, subqueryPattern)
                .with(new CorrelationMatcher(correlationSymbolAliases));
    }

    public static PlanMatchPattern groupingSet(List<List<String>> groups, Map<String, String> identityMappings, String groupIdAlias, PlanMatchPattern source)
    {
        return node(GroupIdNode.class, source).with(new GroupIdMatcher(groups, identityMappings, groupIdAlias));
    }

    public static PlanMatchPattern groupingSet(List<List<String>> groups, Map<String, String> identityMappings, String groupIdAlias, Map<String, ExpressionMatcher> groupingColumns, PlanMatchPattern source)
    {
        PlanMatchPattern result = node(GroupIdNode.class, source).with(new GroupIdMatcher(groups, identityMappings, groupIdAlias));
        groupingColumns.entrySet().forEach(
                groupingColumn -> result.withAlias(groupingColumn.getKey(), groupingColumn.getValue()));
        return result;
    }

    private static PlanMatchPattern values(
            Map<String, Integer> aliasToIndex,
            Optional<Integer> expectedOutputSymbolCount,
            Optional<List<List<Expression>>> expectedRows)
    {
        return node(ValuesNode.class).with(new ValuesMatcher(aliasToIndex, expectedOutputSymbolCount, expectedRows));
    }

    private static PlanMatchPattern values(List<String> aliases, Optional<List<List<Expression>>> expectedRows)
    {
        return values(
                Maps.uniqueIndex(IntStream.range(0, aliases.size()).boxed().iterator(), aliases::get),
                Optional.of(aliases.size()),
                expectedRows);
    }

    public static PlanMatchPattern values(Map<String, Integer> aliasToIndex)
    {
        return values(aliasToIndex, Optional.empty(), Optional.empty());
    }

    public static PlanMatchPattern values(int rowCount)
    {
        return values(ImmutableList.of(), nCopies(rowCount, ImmutableList.of()));
    }

    public static PlanMatchPattern values(String... aliases)
    {
        return values(ImmutableList.copyOf(aliases));
    }

    public static PlanMatchPattern values(List<String> aliases, List<List<Expression>> expectedRows)
    {
        return values(aliases, Optional.of(expectedRows));
    }

    public static PlanMatchPattern values(List<String> aliases)
    {
        return values(aliases, Optional.empty());
    }

    public static PlanMatchPattern offset(long rowCount, PlanMatchPattern source)
    {
        return node(OffsetNode.class, source).with(new OffsetMatcher(rowCount));
    }

    public static PlanMatchPattern limit(long limit, PlanMatchPattern source)
    {
        return limit(limit, false, source);
    }

    public static PlanMatchPattern limit(long limit, boolean partial, PlanMatchPattern source)
    {
        return node(LimitNode.class, source).with(new LimitMatcher(limit, partial));
    }

    public static PlanMatchPattern enforceSingleRow(PlanMatchPattern source)
    {
        return node(EnforceSingleRowNode.class, source);
    }

    public static PlanMatchPattern callDistributedProcedure(PlanMatchPattern source)
    {
        return node(CallDistributedProcedureNode.class, source);
    }

    public static PlanMatchPattern tableFinish(PlanMatchPattern source)
    {
        return node(TableFinishNode.class, source);
    }

    public static PlanMatchPattern tableWriter(List<String> columns, List<String> columnNames, PlanMatchPattern source)
    {
        return node(TableWriterNode.class, source).with(new TableWriterMatcher(columns, columnNames));
    }

    public static PlanMatchPattern remoteSource(List<PlanFragmentId> sourceFragmentIds, Map<String, Integer> outputSymbolAliases)
    {
        return node(RemoteSourceNode.class).with(new RemoteSourceMatcher(sourceFragmentIds, outputSymbolAliases));
    }

    public static PlanMatchPattern tableFunction(Consumer<TableFunctionMatcher.Builder> handler, PlanMatchPattern... sources)
    {
        TableFunctionMatcher.Builder builder = new TableFunctionMatcher.Builder(sources);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern tableFunctionProcessor(Consumer<TableFunctionProcessorMatcher.Builder> handler, PlanMatchPattern source)
    {
        TableFunctionProcessorMatcher.Builder builder = new TableFunctionProcessorMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern tableFunctionProcessor(Consumer<TableFunctionProcessorMatcher.Builder> handler)
    {
        TableFunctionProcessorMatcher.Builder builder = new TableFunctionProcessorMatcher.Builder();
        handler.accept(builder);
        return builder.build();
    }

    public PlanMatchPattern(List<PlanMatchPattern> sourcePatterns)
    {
        requireNonNull(sourcePatterns, "sourcePatterns are null");

        this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
    }

    List<PlanMatchingState> shapeMatches(PlanNode node)
    {
        ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
        if (anyTree) {
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchingState(nCopies(sourcesCount, this)));
            }
            else {
                states.add(new PlanMatchingState(ImmutableList.of(this)));
            }
        }
        if (node instanceof GroupReference) {
            if (sourcePatterns.isEmpty() && shapeMatchesMatchers(node)) {
                states.add(new PlanMatchingState(ImmutableList.of()));
            }
        }
        else if (node.getSources().size() == sourcePatterns.size() && shapeMatchesMatchers(node)) {
            states.add(new PlanMatchingState(sourcePatterns));
        }
        return states.build();
    }

    private boolean shapeMatchesMatchers(PlanNode node)
    {
        return matchers.stream().allMatch(it -> it.shapeMatches(node));
    }

    MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        SymbolAliases.Builder newAliases = SymbolAliases.builder();

        for (Matcher matcher : matchers) {
            MatchResult matchResult;
            if (matcher instanceof AggregationMatcher) {
                matchResult = matcher.detailMatches(node, stats, session, metadata, symbolAliases.withNewAliases(newAliases.build()));
            }
            else {
                matchResult = matcher.detailMatches(node, stats, session, metadata, symbolAliases);
            }
            if (!matchResult.isMatch()) {
                return NO_MATCH;
            }
            newAliases.putAll(matchResult.getAliases());
        }

        return match(newAliases.build());
    }

    public PlanMatchPattern with(Matcher matcher)
    {
        matchers.add(matcher);
        return this;
    }

    public PlanMatchPattern withAlias(String alias)
    {
        return withAlias(Optional.of(alias), new AliasPresent(alias));
    }

    public PlanMatchPattern withAlias(String alias, RvalueMatcher matcher)
    {
        return withAlias(Optional.of(alias), matcher);
    }

    public PlanMatchPattern withAlias(Optional<String> alias, RvalueMatcher matcher)
    {
        matchers.add(new AliasMatcher(alias, matcher));
        return this;
    }

    public PlanMatchPattern withNumberOfOutputColumns(int numberOfSymbols)
    {
        matchers.add(new SymbolCardinalityMatcher(numberOfSymbols));
        return this;
    }

    /*
     * This is useful if you already know the bindings for the aliases you expect to find
     * in the outputs. This is the case for symbols that are produced by a direct or indirect
     * source of the node you're applying this to.
     */
    public PlanMatchPattern withExactOutputs(String... expectedAliases)
    {
        return withExactOutputs(ImmutableList.copyOf(expectedAliases));
    }

    public PlanMatchPattern withExactOutputs(List<String> expectedAliases)
    {
        matchers.add(new StrictSymbolsMatcher(actualOutputs(), expectedAliases));
        return this;
    }

    /*
     * withExactAssignments and withExactAssignedOutputs are needed for matching symbols
     * that are produced in the node that you're matching. The name of the symbol bound to
     * the alias is *not* known when the Matcher is run, and so you need to match by what
     * is being assigned to it.
     */
    public PlanMatchPattern withExactAssignedOutputs(RvalueMatcher... expectedAliases)
    {
        return withExactAssignedOutputs(ImmutableList.copyOf(expectedAliases));
    }

    public PlanMatchPattern withExactAssignedOutputs(Collection<? extends RvalueMatcher> expectedAliases)
    {
        matchers.add(new StrictAssignedSymbolsMatcher(actualOutputs(), expectedAliases));
        return this;
    }

    public PlanMatchPattern withExactAssignments(Collection<? extends RvalueMatcher> expectedAliases)
    {
        matchers.add(new StrictAssignedSymbolsMatcher(actualAssignments(), expectedAliases));
        return this;
    }

    public PlanMatchPattern withOutputRowCount(double expectedOutputRowCount)
    {
        matchers.add(new StatsOutputRowCountMatcher(expectedOutputRowCount));
        return this;
    }

    public PlanMatchPattern withSourceInfo(SourceInfo sourceInfo)
    {
        matchers.add(new StatsSourceInfoMatcher(sourceInfo));
        return this;
    }

    public PlanMatchPattern withConfidenceLevel(ConfidenceLevel confidenceLevel)
    {
        matchers.add(new StatsConfidenceLevelMatcher(confidenceLevel));
        return this;
    }

    public PlanMatchPattern withOutputRowCount(double expectedOutputRowCount, String expectedSourceInfo)
    {
        matchers.add(new StatsOutputRowCountMatcher(expectedOutputRowCount, expectedSourceInfo));
        return this;
    }

    public PlanMatchPattern withOutputRowCount(boolean exactMatch, String expectedSourceInfo)
    {
        matchers.add(new StatsOutputRowCountMatcher(exactMatch, expectedSourceInfo));
        return this;
    }

    public PlanMatchPattern withApproximateOutputRowCount(double expectedOutputRowCount, double error)
    {
        matchers.add(new ApproximateStatsOutputRowCountMatcher(expectedOutputRowCount, error));
        return this;
    }

    public PlanMatchPattern withOutputSize(double expectedOutputSize)
    {
        matchers.add(new StatsOutputSizeMatcher(expectedOutputSize));
        return this;
    }

    public PlanMatchPattern withJoinStatistics(double expectedJoinBuildKeyCount, double expectedNullJoinBuildKeyCount, double expectedJoinProbeKeyCount, double expectedNullJoinProbeKeyCount)
    {
        matchers.add(new StatsJoinKeyCountMatcher(expectedJoinBuildKeyCount, expectedNullJoinBuildKeyCount, expectedJoinProbeKeyCount, expectedNullJoinProbeKeyCount));
        return this;
    }

    public static RvalueMatcher columnReference(String tableName, String columnName)
    {
        return new ColumnReference(tableName, columnName);
    }

    public static ExpressionMatcher expression(String expression)
    {
        return new ExpressionMatcher(expression);
    }

    public static ExpressionMatcher expression(String expression, ParsingOptions.DecimalLiteralTreatment decimalLiteralTreatment)
    {
        return new ExpressionMatcher(expression, decimalLiteralTreatment);
    }

    public static ExpressionMatcher expression(Expression expression)
    {
        return new ExpressionMatcher(expression);
    }

    public PlanMatchPattern withOutputs(String... aliases)
    {
        return withOutputs(ImmutableList.copyOf(aliases));
    }

    public PlanMatchPattern withOutputs(List<String> aliases)
    {
        matchers.add(new OutputMatcher(aliases));
        return this;
    }

    public PlanMatchPattern matchToAnyNodeTree()
    {
        anyTree = true;
        return this;
    }

    public boolean isTerminated()
    {
        return sourcePatterns.isEmpty();
    }

    public static PlanTestSymbol anySymbol()
    {
        return new AnySymbol();
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(String name, List<String> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), toSymbolAliases(args));
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(String name, List<String> args, List<Ordering> orderBy)
    {
        return new FunctionCallProvider(QualifiedName.of(name), toSymbolAliases(args), orderBy);
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(
            String name,
            Optional<WindowFrame> frame,
            List<String> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), frame, false, toSymbolAliases(args));
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(
            String name,
            boolean distinct,
            List<PlanTestSymbol> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), distinct, args);
    }

    public static List<Expression> toSymbolReferences(List<PlanTestSymbol> aliases, SymbolAliases symbolAliases)
    {
        return aliases
                .stream()
                .map(arg -> arg.toSymbol(symbolAliases).toSymbolReference())
                .collect(toImmutableList());
    }

    private static List<PlanTestSymbol> toSymbolAliases(List<String> aliases)
    {
        return aliases
                .stream()
                .map(PlanMatchPattern::symbol)
                .collect(toImmutableList());
    }

    public static ExpectedValueProvider<DataOrganizationSpecification> specification(
            List<String> partitionBy,
            List<String> orderBy,
            Map<String, SortOrder> orderings)
    {
        return new SpecificationProvider(
                partitionBy
                        .stream()
                        .map(SymbolAlias::new)
                        .collect(toImmutableList()),
                orderBy
                        .stream()
                        .map(SymbolAlias::new)
                        .collect(toImmutableList()),
                orderings
                        .entrySet()
                        .stream()
                        .collect(toImmutableMap(entry -> new SymbolAlias(entry.getKey()), Map.Entry::getValue)));
    }

    public static Ordering sort(String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering)
    {
        return new Ordering(field, ordering, nullOrdering);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        toString(builder, 0);
        return builder.toString();
    }

    private void toString(StringBuilder builder, int indent)
    {
        checkState(matchers.stream().filter(PlanNodeMatcher.class::isInstance).count() <= 1);

        builder.append(indentString(indent)).append("- ");
        if (anyTree) {
            builder.append("anyTree");
        }
        else {
            builder.append("node");
        }

        Optional<PlanNodeMatcher> planNodeMatcher = matchers.stream()
                .filter(PlanNodeMatcher.class::isInstance)
                .map(PlanNodeMatcher.class::cast)
                .findFirst();

        if (planNodeMatcher.isPresent()) {
            builder.append("(").append(planNodeMatcher.get().getNodeClass().getSimpleName()).append(")");
        }

        builder.append("\n");

        List<Matcher> matchersToPrint = matchers.stream()
                .filter(matcher -> !(matcher instanceof PlanNodeMatcher))
                .collect(toImmutableList());

        for (Matcher matcher : matchersToPrint) {
            builder.append(indentString(indent + 1)).append(matcher.toString()).append("\n");
        }

        for (PlanMatchPattern pattern : sourcePatterns) {
            pattern.toString(builder, indent + 1);
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet();
    }

    public static GroupingSetDescriptor singleGroupingSet(String... groupingKeys)
    {
        return singleGroupingSet(ImmutableList.copyOf(groupingKeys));
    }

    public static GroupingSetDescriptor singleGroupingSet(List<String> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static class GroupingSetDescriptor
    {
        private final List<String> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        public GroupingSetDescriptor(List<String> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
        {
            this.groupingKeys = groupingKeys;
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = globalGroupingSets;
        }

        public List<String> getGroupingKeys()
        {
            return groupingKeys;
        }

        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("keys", groupingKeys)
                    .add("count", groupingSetCount)
                    .add("globalSets", globalGroupingSets)
                    .toString();
        }
    }

    public static class Ordering
    {
        private final String field;
        private final SortItem.Ordering ordering;
        private final SortItem.NullOrdering nullOrdering;

        private Ordering(String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering)
        {
            this.field = field;
            this.ordering = ordering;
            this.nullOrdering = nullOrdering;
        }

        public String getField()
        {
            return field;
        }

        public SortItem.Ordering getOrdering()
        {
            return ordering;
        }

        public SortItem.NullOrdering getNullOrdering()
        {
            return nullOrdering;
        }

        public SortOrder getSortOrder()
        {
            checkState(nullOrdering != UNDEFINED, "nullOrdering is undefined");
            if (ordering == ASCENDING) {
                if (nullOrdering == FIRST) {
                    return ASC_NULLS_FIRST;
                }
                else {
                    return ASC_NULLS_LAST;
                }
            }
            else {
                checkState(ordering == DESCENDING);
                if (nullOrdering == FIRST) {
                    return DESC_NULLS_FIRST;
                }
                else {
                    return DESC_NULLS_LAST;
                }
            }
        }

        @Override
        public String toString()
        {
            String result = field + " " + ordering;
            if (nullOrdering != UNDEFINED) {
                result += " NULLS " + nullOrdering;
            }

            return result;
        }
    }
}
