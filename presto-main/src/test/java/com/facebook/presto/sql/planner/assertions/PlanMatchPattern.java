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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.StrictAssignedSymbolsMatcher.actualAssignments;
import static com.facebook.presto.sql.planner.assertions.StrictSymbolsMatcher.actualOutputs;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.FIRST;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
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

    public static PlanMatchPattern tableScan(String expectedTableName, String originalConstraint)
    {
        Expression expectedOriginalConstraint = rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(originalConstraint));
        return TableScanMatcher.builder(expectedTableName)
                .expectedOriginalConstraint(expectedOriginalConstraint)
                .build();
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

    public static PlanMatchPattern constrainedIndexSource(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName, constraint))
                .addColumnReferences(expectedTableName, columnReferences);
    }

    private PlanMatchPattern addColumnReferences(String expectedTableName, Map<String, String> columnReferences)
    {
        columnReferences.entrySet().forEach(
                reference -> withAlias(reference.getKey(), columnReference(expectedTableName, reference.getValue())));
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
            List<List<String>> groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            Map<Symbol, Symbol> masks,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        return aggregation(groupingSets, aggregations, ImmutableList.of(), masks, groupId, step, source);
    }

    public static PlanMatchPattern aggregation(
            List<List<String>> groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            List<String> preGroupedSymbols,
            Map<Symbol, Symbol> masks,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source).with(new AggregationMatcher(groupingSets, preGroupedSymbols, masks, groupId, step));
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
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
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<String> startValue,
            FrameBound.Type endType,
            Optional<String> endValue)
    {
        return new WindowFrameProvider(
                type,
                startType,
                startValue.map(SymbolAlias::new),
                endType,
                endValue.map(SymbolAlias::new));
    }

    public static PlanMatchPattern window(Consumer<WindowMatcher.Builder> windowMatcherBuilderConsumer, PlanMatchPattern source)
    {
        WindowMatcher.Builder windowMatcherBuilder = new WindowMatcher.Builder(source);
        windowMatcherBuilderConsumer.accept(windowMatcherBuilder);
        return windowMatcherBuilder.build();
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

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias));
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria, PlanMatchPattern left, PlanMatchPattern right)
    {
        return join(joinType, expectedEquiCriteria, Optional.empty(), left, right);
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria, Optional<String> expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return join(joinType, expectedEquiCriteria, expectedFilter, Optional.empty(), left, right);
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<ExpectedValueProvider<JoinNode.EquiJoinClause>> expectedEquiCriteria, Optional<String> expectedFilter, Optional<JoinNode.DistributionType> expectedDistributionType, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(
                new JoinMatcher(
                        joinType,
                        expectedEquiCriteria,
                        expectedFilter.map(predicate -> rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(predicate))),
                        expectedDistributionType));
    }

    public static PlanMatchPattern spatialJoin(String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(
                new SpatialJoinMatcher(INNER, rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedFilter, new ParsingOptions()))));
    }

    public static PlanMatchPattern spatialLeftJoin(String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(
                new SpatialJoinMatcher(LEFT, rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(expectedFilter, new ParsingOptions()))));
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
        return node(ExchangeNode.class, sources)
                .with(new ExchangeMatcher(scope, type, orderBy));
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

    public static ExpectedValueProvider<JoinNode.EquiJoinClause> equiJoinClause(String left, String right)
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

    public static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source)
    {
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate));
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

    public static PlanMatchPattern groupingSet(List<List<String>> groups, String groupIdAlias, PlanMatchPattern source)
    {
        return node(GroupIdNode.class, source).with(new GroupIdMatcher(groups, ImmutableMap.of(), groupIdAlias));
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

    public static PlanMatchPattern limit(long limit, PlanMatchPattern source)
    {
        return node(LimitNode.class, source).with(new LimitMatcher(limit));
    }

    public static PlanMatchPattern enforceSingleRow(PlanMatchPattern source)
    {
        return node(EnforceSingleRowNode.class, source);
    }

    public static PlanMatchPattern tableWriter(List<String> columns, List<String> columnNames, PlanMatchPattern source)
    {
        return node(TableWriterNode.class, source).with(new TableWriterMatcher(columns, columnNames));
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
            MatchResult matchResult = matcher.detailMatches(node, stats, session, metadata, symbolAliases);
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

    public PlanMatchPattern withExactAssignments(RvalueMatcher... expectedAliases)
    {
        return withExactAssignments(ImmutableList.copyOf(expectedAliases));
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

    public static RvalueMatcher columnReference(String tableName, String columnName)
    {
        return new ColumnReference(tableName, columnName);
    }

    public static ExpressionMatcher expression(String expression)
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

    public static ExpectedValueProvider<WindowNode.Specification> specification(
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
