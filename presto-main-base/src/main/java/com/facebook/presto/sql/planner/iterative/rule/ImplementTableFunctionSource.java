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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.plan.WindowNode.Frame;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.ROWS;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFunction;
import static com.facebook.presto.sql.relational.Expressions.coalesce;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * This rule prepares cartesian product of partitions
 * from all inputs of table function.
 * <p>
 * It rewrites TableFunctionNode with potentially many sources
 * into a TableFunctionProcessorNode. The new node has one
 * source being a combination of the original sources.
 * <p>
 * The original sources are combined with joins. The join
 * conditions depend on the prune when empty property, and on
 * the co-partitioning of sources.
 * <p>
 * The resulting source should be partitioned and ordered
 * according to combined schemas from the component sources.
 * <p>
 * Example transformation for two sources, both with set semantics
 * and KEEP WHEN EMPTY property:
 * <pre>
 * - TableFunction foo
 *      - source T1(a1, b1) PARTITION BY a1 ORDER BY b1
 *      - source T2(a2, b2) PARTITION BY a2
 * </pre>
 * Is transformed into:
 * <pre>
 * - TableFunctionDataProcessor foo
 *      PARTITION BY (a1, a2), ORDER BY combined_row_number
 *      - Project
 *          marker_1 <= IF(table1_row_number = combined_row_number, table1_row_number, CAST(null AS bigint))
 *          marker_2 <= IF(table2_row_number = combined_row_number, table2_row_number, CAST(null AS bigint))
 *          - Project
 *              combined_row_number <= IF(COALESCE(table1_row_number, BIGINT '-1') > COALESCE(table2_row_number, BIGINT '-1'), table1_row_number, table2_row_number)
 *              combined_partition_size <= IF(COALESCE(table1_partition_size, BIGINT '-1') > COALESCE(table2_partition_size, BIGINT '-1'), table1_partition_size, table2_partition_size)
 *              - FULL Join
 *                  [table1_row_number = table2_row_number OR
 *                   table1_row_number > table2_partition_size AND table2_row_number = BIGINT '1' OR
 *                   table2_row_number > table1_partition_size AND table1_row_number = BIGINT '1']
 *                  - Window [PARTITION BY a1 ORDER BY b1]
 *                      table1_row_number <= row_number()
 *                      table1_partition_size <= count()
 *                          - source T1(a1, b1)
 *                  - Window [PARTITION BY a2]
 *                      table2_row_number <= row_number()
 *                      table2_partition_size <= count()
 *                          - source T2(a2, b2)
 * </pre>
 */
public class ImplementTableFunctionSource
        implements Rule<TableFunctionNode>
{
    private static final Pattern<TableFunctionNode> PATTERN = tableFunction();
    private static final Frame FULL_FRAME = new Frame(
            ROWS,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private static final DataOrganizationSpecification UNORDERED_SINGLE_PARTITION = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

    private final Metadata metadata;

    public ImplementTableFunctionSource(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableFunctionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionNode node, Captures captures, Context context)
    {
        if (node.getSources().isEmpty()) {
            return Result.ofPlanNode(new TableFunctionProcessorNode(
                    node.getId(),
                    node.getName(),
                    node.getProperOutputs(),
                    Optional.empty(),
                    false,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableSet.of(),
                    0,
                    Optional.empty(),
                    node.getHandle()));
        }

        if (node.getSources().size() == 1) {
            // Single source does not require pre-processing.
            // If the source has row semantics, its specification is empty.
            // If the source has set semantics, its specification is present, even if there is no partitioning or ordering specified.
            // This property can be used later to choose optimal distribution.
            TableArgumentProperties sourceProperties = getOnlyElement(node.getTableArgumentProperties());
            return Result.ofPlanNode(new TableFunctionProcessorNode(
                    node.getId(),
                    node.getName(),
                    node.getProperOutputs(),
                    Optional.of(getOnlyElement(node.getSources())),
                    sourceProperties.isPruneWhenEmpty(),
                    ImmutableList.of(sourceProperties.getPassThroughSpecification()),
                    ImmutableList.of(sourceProperties.getRequiredColumns()),
                    Optional.empty(),
                    sourceProperties.getSpecification(),
                    ImmutableSet.of(),
                    0,
                    Optional.empty(),
                    node.getHandle()));
        }
        Map<String, SourceWithProperties> sources = mapSourcesByName(node.getSources(), node.getTableArgumentProperties());
        ImmutableList.Builder<NodeWithVariables> intermediateResultsBuilder = ImmutableList.builder();

        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();

        // Create call expression for row_number
        FunctionHandle rowNumberFunctionHandle = functionAndTypeManager.resolveFunction(Optional.of(context.getSession().getSessionFunctions()),
                context.getSession().getTransactionId(),
                functionAndTypeManager.getFunctionAndTypeResolver().qualifyObjectName(QualifiedName.of("row_number")),
                ImmutableList.of());

        FunctionMetadata rowNumberFunctionMetadata = functionAndTypeManager.getFunctionMetadata(rowNumberFunctionHandle);
        CallExpression rowNumberFunction = new CallExpression("row_number", rowNumberFunctionHandle, functionAndTypeManager.getType(rowNumberFunctionMetadata.getReturnType()), ImmutableList.of());

        // Create call expression for count
        FunctionHandle countFunctionHandle = functionAndTypeManager.resolveFunction(Optional.of(context.getSession().getSessionFunctions()),
                context.getSession().getTransactionId(),
                functionAndTypeManager.getFunctionAndTypeResolver().qualifyObjectName(QualifiedName.of("count")),
                ImmutableList.of());

        FunctionMetadata countFunctionMetadata = functionAndTypeManager.getFunctionMetadata(countFunctionHandle);
        CallExpression countFunction = new CallExpression("count", countFunctionHandle, functionAndTypeManager.getType(countFunctionMetadata.getReturnType()), ImmutableList.of());

        // handle co-partitioned sources
        for (List<String> copartitioningList : node.getCopartitioningLists()) {
            List<SourceWithProperties> sourceList = copartitioningList.stream()
                    .map(sources::get)
                    .collect(toImmutableList());
            intermediateResultsBuilder.add(copartition(sourceList, rowNumberFunction, countFunction, context, metadata));
        }

        // prepare non-co-partitioned sources
        Set<String> copartitionedSources = node.getCopartitioningLists().stream()
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
        sources.entrySet().stream()
                .filter(entry -> !copartitionedSources.contains(entry.getKey()))
                .map(entry -> planWindowFunctionsForSource(entry.getValue().source(), entry.getValue().properties(), rowNumberFunction, countFunction, context))
                .forEach(intermediateResultsBuilder::add);

        NodeWithVariables finalResultSource;

        List<NodeWithVariables> intermediateResultSources = intermediateResultsBuilder.build();
        if (intermediateResultSources.size() == 1) {
            finalResultSource = getOnlyElement(intermediateResultSources);
        }
        else {
            NodeWithVariables first = intermediateResultSources.get(0);
            NodeWithVariables second = intermediateResultSources.get(1);
            JoinedNodes joined = join(first, second, context, metadata);

            for (int i = 2; i < intermediateResultSources.size(); i++) {
                NodeWithVariables joinedWithSymbols = appendHelperSymbolsForJoinedNodes(joined, context, metadata);
                joined = join(joinedWithSymbols, intermediateResultSources.get(i), context, metadata);
            }

            finalResultSource = appendHelperSymbolsForJoinedNodes(joined, context, metadata);
        }

        // For each source, all source's output symbols are mapped to the source's row number symbol.
        // The row number symbol will be later converted to a marker of "real" input rows vs "filler" input rows of the source.
        // The "filler" input rows are the rows appended while joining partitions of different lengths,
        // to fill the smaller partition up to the bigger partition's size. They are a side effect of the algorithm,
        // and should not be processed by the table function.
        Map<VariableReferenceExpression, VariableReferenceExpression> rowNumberSymbols = finalResultSource.rowNumberSymbolsMapping();

        // The max row number symbol from all joined partitions.
        VariableReferenceExpression finalRowNumberSymbol = finalResultSource.rowNumber();
        // Combined partitioning lists from all sources.
        List<VariableReferenceExpression> finalPartitionBy = finalResultSource.partitionBy();

        NodeWithMarkers marked = appendMarkerSymbols(finalResultSource.node(), ImmutableSet.copyOf(rowNumberSymbols.values()), finalRowNumberSymbol, context, metadata);

        // Remap the symbol mapping: replace the row number symbol with the corresponding marker symbol.
        // In the new map, every source symbol is associated with the corresponding marker symbol.
        // Null value of the marker indicates that the source value should be ignored by the table function.
        ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> markerSymbols = rowNumberSymbols.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> marked.variableToMarker().get(entry.getValue())));

        // Use the final row number symbol for ordering the combined sources.
        // It runs along each partition in the cartesian product, numbering the partition's rows according to the expected ordering / orderings.
        // note: ordering is necessary even if all the source tables are not ordered. Thanks to the ordering, the original rows
        // of each input table come before the "filler" rows.
        ImmutableList.Builder<Ordering> newOrderings = ImmutableList.builder();
        newOrderings.add(new Ordering(finalRowNumberSymbol, ASC_NULLS_LAST));
        Optional<OrderingScheme> finalOrderBy = Optional.of(new OrderingScheme(newOrderings.build()));

        // derive the prune when empty property
        boolean pruneWhenEmpty = node.getTableArgumentProperties().stream().anyMatch(TableArgumentProperties::isPruneWhenEmpty);

        // Combine the pass through specifications from all sources
        List<PassThroughSpecification> passThroughSpecifications = node.getTableArgumentProperties().stream()
                .map(TableArgumentProperties::getPassThroughSpecification)
                .collect(toImmutableList());

        // Combine the required symbols from all sources
        List<List<VariableReferenceExpression>> requiredVariables = node.getTableArgumentProperties().stream()
                .map(TableArgumentProperties::getRequiredColumns)
                .collect(toImmutableList());

        return Result.ofPlanNode(new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getProperOutputs(),
                Optional.of(marked.node()),
                pruneWhenEmpty,
                passThroughSpecifications,
                requiredVariables,
                Optional.of(markerSymbols),
                Optional.of(new DataOrganizationSpecification(finalPartitionBy, finalOrderBy)),
                ImmutableSet.of(),
                0,
                Optional.empty(),
                node.getHandle()));
    }

    private static Map<String, SourceWithProperties> mapSourcesByName(List<PlanNode> sources, List<TableArgumentProperties> properties)
    {
        return Streams.zip(sources.stream(), properties.stream(), SourceWithProperties::new)
                .collect(toImmutableMap(entry -> entry.properties().getArgumentName(), identity()));
    }

    private static NodeWithVariables planWindowFunctionsForSource(
            PlanNode source,
            TableArgumentProperties argumentProperties,
            CallExpression rowNumberFunction,
            CallExpression countFunction,
            Context context)
    {
        String argumentName = argumentProperties.getArgumentName();

        VariableReferenceExpression rowNumber = context.getVariableAllocator().newVariable(argumentName + "_row_number", BIGINT);
        Map<VariableReferenceExpression, VariableReferenceExpression> rowNumberSymbolMapping = source.getOutputVariables().stream()
                .collect(toImmutableMap(identity(), symbol -> rowNumber));

        VariableReferenceExpression partitionSize = context.getVariableAllocator().newVariable(argumentName + "_partition_size", BIGINT);

        // If the source has set semantics, its specification is present, even if there is no partitioning or ordering specified.
        // If the source has row semantics, its specification is empty. Currently, such source is processed
        // as if it was a single partition. Alternatively, it could be split into smaller partitions of arbitrary size.
        DataOrganizationSpecification specification = argumentProperties.getSpecification().orElse(UNORDERED_SINGLE_PARTITION);

        PlanNode innerWindow = new WindowNode(
                source.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                source,
                specification,
                ImmutableMap.of(
                        rowNumber, new WindowNode.Function(rowNumberFunction, FULL_FRAME, false)),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        PlanNode window = new WindowNode(
                innerWindow.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerWindow,
                specification,
                ImmutableMap.of(
                        partitionSize, new WindowNode.Function(countFunction, FULL_FRAME, false)),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        return new NodeWithVariables(window, rowNumber, partitionSize, specification.getPartitionBy(), argumentProperties.isPruneWhenEmpty(), rowNumberSymbolMapping);
    }

    private static NodeWithVariables copartition(
            List<SourceWithProperties> sourceList,
            CallExpression rowNumberFunction,
            CallExpression countFunction,
            Context context,
            Metadata metadata)
    {
        checkArgument(sourceList.size() >= 2, "co-partitioning list should contain at least two tables");

        // Reorder the co-partitioned sources to process the sources with prune when empty property first.
        // It allows to use inner or side joins instead of outer joins.
        sourceList = sourceList.stream()
                .sorted(Comparator.comparingInt(source -> source.properties().isPruneWhenEmpty() ? -1 : 1))
                .collect(toImmutableList());

        NodeWithVariables first = planWindowFunctionsForSource(sourceList.get(0).source(), sourceList.get(0).properties(), rowNumberFunction, countFunction, context);
        NodeWithVariables second = planWindowFunctionsForSource(sourceList.get(1).source(), sourceList.get(1).properties(), rowNumberFunction, countFunction, context);
        JoinedNodes copartitioned = copartition(first, second, context, metadata);

        for (int i = 2; i < sourceList.size(); i++) {
            NodeWithVariables copartitionedWithSymbols = appendHelperSymbolsForCopartitionedNodes(copartitioned, context, metadata);
            NodeWithVariables next = planWindowFunctionsForSource(sourceList.get(i).source(), sourceList.get(i).properties(), rowNumberFunction, countFunction, context);
            copartitioned = copartition(copartitionedWithSymbols, next, context, metadata);
        }

        return appendHelperSymbolsForCopartitionedNodes(copartitioned, context, metadata);
    }

    private static JoinedNodes copartition(NodeWithVariables left, NodeWithVariables right, Context context, Metadata metadata)
    {
        checkArgument(left.partitionBy().size() == right.partitionBy().size(), "co-partitioning lists do not match");

        // In StatementAnalyzer we require that co-partitioned tables have non-empty partitioning column lists.
        // Co-partitioning tables with empty partition by would be ineffective.
        checkState(!left.partitionBy().isEmpty(), "co-partitioned tables must have partitioning columns");

        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());

        Optional<RowExpression> copartitionConjuncts = Streams.zip(
                        left.partitionBy.stream(),
                        right.partitionBy.stream(),
                (leftColumn, rightColumn) -> new CallExpression("NOT",
                        functionResolution.notFunction(),
                        BOOLEAN,
                        ImmutableList.of(
                                new CallExpression(IS_DISTINCT_FROM.name(),
                                        functionResolution.comparisonFunction(IS_DISTINCT_FROM, INTEGER, INTEGER),
                                        BOOLEAN,
                                        ImmutableList.of(leftColumn, rightColumn)))))
                .<RowExpression>map(expr -> expr)
                .reduce((expr, conjunct) -> new SpecialFormExpression(SpecialFormExpression.Form.AND,
                        BOOLEAN,
                        ImmutableList.of(expr, conjunct)));

        // Align matching partitions (co-partitions) from left and right source, according to row number.
        // Matching partitions are identified by their corresponding partitioning columns being NOT DISTINCT from each other.
        // If one or both sources are ordered, the row number reflects the ordering.
        // The second and third disjunct in the join condition account for the situation when partitions have different sizes.
        // It preserves the outstanding rows from the bigger partition, matching them to the first row from the smaller partition.
        //
        // (P1_1 IS NOT DISTINCT FROM P2_1) AND (P1_2 IS NOT DISTINCT FROM P2_2) AND ...
        // AND (
        //      R1 = R2
        //      OR
        //      (R1 > S2 AND R2 = 1)
        //      OR
        //      (R2 > S1 AND R1 = 1))

        SpecialFormExpression orExpression = new SpecialFormExpression(SpecialFormExpression.Form.OR,
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression(EQUAL.name(),
                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                BOOLEAN,
                                ImmutableList.of(left.rowNumber, right.rowNumber)),
                        new SpecialFormExpression(SpecialFormExpression.Form.OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(SpecialFormExpression.Form.AND,
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new CallExpression(GREATER_THAN.name(),
                                                                functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(left.rowNumber, right.partitionSize)),
                                                        new CallExpression(EQUAL.name(),
                                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(right.rowNumber, new ConstantExpression(1L, BIGINT))))),
                                        new SpecialFormExpression(SpecialFormExpression.Form.AND,
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new CallExpression(GREATER_THAN.name(),
                                                                functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(right.rowNumber, left.partitionSize)),
                                                        new CallExpression(EQUAL.name(),
                                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(left.rowNumber, new ConstantExpression(1L, BIGINT)))))))));
        RowExpression joinCondition = copartitionConjuncts.map(
                conjunct -> new SpecialFormExpression(SpecialFormExpression.Form.AND,
                        BOOLEAN,
                        ImmutableList.of(conjunct, orExpression)))
                .orElse(orExpression);

        // The join type depends on the prune when empty property of the sources.
        // If a source is prune when empty, we should not process any co-partition which is not present in this source,
        // so effectively the other source becomes inner side of the join.
        //
        // example:
        // table T1 partition by P1              table T2 partition by P2
        //   P1     C1                             P2     C2
        //  ----------                            ----------
        //   1     'a'                             2     'c'
        //   2     'b'                             3     'd'
        //
        // co-partitioning results:
        // 1) T1 is prune when empty: do LEFT JOIN to drop co-partition '3'
        //   P1     C1     P2     C2
        //  ------------------------
        //   1      'a'    null   null
        //   2      'b'    2      'c'
        //
        // 2) T2 is prune when empty: do RIGHT JOIN to drop co-partition '1'
        //   P1     C1     P2     C2
        //  ------------------------
        //   2      'b'     2     'c'
        //   null   null    3     'd'
        //
        // 3) T1 and T2 are both prune when empty: do INNER JOIN to drop co-partitions '1' and '3'
        //   P1     C1     P2     C2
        //  ------------------------
        //   2      'b'    2      'c'
        //
        // 4) neither table is prune when empty: do FULL JOIN to preserve all co-partitions
        //   P1     C1     P2     C2
        //  ------------------------
        //   1      'a'    null   null
        //   2      'b'    2      'c'
        //   null   null   3      'd'
        JoinType joinType;
        if (left.pruneWhenEmpty() && right.pruneWhenEmpty()) {
            joinType = INNER;
        }
        else if (left.pruneWhenEmpty()) {
            joinType = LEFT;
        }
        else if (right.pruneWhenEmpty()) {
            joinType = RIGHT;
        }
        else {
            joinType = FULL;
        }

        return new JoinedNodes(
                new JoinNode(
                        Optional.empty(),
                        context.getIdAllocator().getNextId(),
                        joinType,
                        left.node(),
                        right.node(),
                        ImmutableList.of(),
                        Stream.concat(left.node().getOutputVariables().stream(),
                                        right.node().getOutputVariables().stream())
                                .collect(Collectors.toList()),
                        Optional.of(joinCondition),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()),
                left.rowNumber(),
                left.partitionSize(),
                left.partitionBy(),
                left.pruneWhenEmpty(),
                left.rowNumberSymbolsMapping(),
                right.rowNumber(),
                right.partitionSize(),
                right.partitionBy(),
                right.pruneWhenEmpty(),
                right.rowNumberSymbolsMapping());
    }

    private static NodeWithVariables appendHelperSymbolsForCopartitionedNodes(
            JoinedNodes copartitionedNodes,
            Context context,
            Metadata metadata)
    {
        checkArgument(copartitionedNodes.leftPartitionBy().size() == copartitionedNodes.rightPartitionBy().size(), "co-partitioning lists do not match");

        // Derive row number for joined partitions: this is the bigger partition's row number. One of the combined values might be null as a result of outer join.
        VariableReferenceExpression joinedRowNumber = context.getVariableAllocator().newVariable("combined_row_number", BIGINT);
        RowExpression rowNumberExpression = new SpecialFormExpression(
                IF,
                BIGINT,
                ImmutableList.of(
                        new CallExpression(
                                GREATER_THAN.name(),
                                metadata.getFunctionAndTypeManager().resolveOperator(
                                        OperatorType.GREATER_THAN,
                                        fromTypes(BIGINT, BIGINT)),
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                copartitionedNodes.leftRowNumber(),
                                                new ConstantExpression(-1L, BIGINT)),
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                copartitionedNodes.rightRowNumber(),
                                                new ConstantExpression(-1L, BIGINT)))),
                        copartitionedNodes.leftRowNumber(),
                        copartitionedNodes.rightRowNumber()));

        // Derive partition size for joined partitions: this is the bigger partition's size. One of the combined values might be null as a result of outer join.
        VariableReferenceExpression joinedPartitionSize = context.getVariableAllocator().newVariable("combined_partition_size", BIGINT);
        RowExpression partitionSizeExpression = new SpecialFormExpression(
                IF,
                BIGINT,
                ImmutableList.of(
                        new CallExpression(
                                GREATER_THAN.name(),
                                metadata.getFunctionAndTypeManager().resolveOperator(
                                        OperatorType.GREATER_THAN,
                                        fromTypes(BIGINT, BIGINT)),
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                copartitionedNodes.leftPartitionSize(),
                                                new ConstantExpression(-1L, BIGINT)),
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                copartitionedNodes.rightPartitionSize(),
                                                new ConstantExpression(-1L, BIGINT)))),
                        copartitionedNodes.leftPartitionSize(),
                        copartitionedNodes.rightPartitionSize()));

        // Derive partitioning columns for joined partitions.
        // Either the combined partitioning columns are pairwise NOT DISTINCT (this is the co-partitioning rule),
        // or one of them is null as a result of outer join.
        ImmutableList.Builder<VariableReferenceExpression> joinedPartitionBy = ImmutableList.builder();
        Assignments.Builder joinedPartitionByAssignments = Assignments.builder();
        for (int i = 0; i < copartitionedNodes.leftPartitionBy().size(); i++) {
            VariableReferenceExpression leftColumn = copartitionedNodes.leftPartitionBy().get(i);
            VariableReferenceExpression rightColumn = copartitionedNodes.rightPartitionBy().get(i);
            Type type = context.getVariableAllocator().getVariables().get(leftColumn.getName());

            VariableReferenceExpression joinedColumn = context.getVariableAllocator().newVariable("combined_partition_column", type);
            joinedPartitionByAssignments.put(joinedColumn, coalesce(leftColumn, rightColumn));
            joinedPartitionBy.add(joinedColumn);
        }

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                copartitionedNodes.joinedNode(),
                Assignments.builder()
                        .putAll(
                                copartitionedNodes.joinedNode().getOutputVariables().stream()
                                        .collect(toImmutableMap(v -> v, v -> v)))
                        .put(joinedRowNumber, rowNumberExpression)
                        .put(joinedPartitionSize, partitionSizeExpression)
                        .putAll(joinedPartitionByAssignments.build())
                        .build());
        boolean joinedPruneWhenEmpty = copartitionedNodes.leftPruneWhenEmpty() || copartitionedNodes.rightPruneWhenEmpty();

        Map<VariableReferenceExpression, VariableReferenceExpression> joinedRowNumberSymbolsMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                .putAll(copartitionedNodes.leftRowNumberSymbolsMapping())
                .putAll(copartitionedNodes.rightRowNumberSymbolsMapping())
                .buildOrThrow();

        return new NodeWithVariables(project, joinedRowNumber, joinedPartitionSize, joinedPartitionBy.build(), joinedPruneWhenEmpty, joinedRowNumberSymbolsMapping);
    }

    private static JoinedNodes join(NodeWithVariables left, NodeWithVariables right, Context context, Metadata metadata)
    {
        // Align rows from left and right source according to row number. Because every partition is row-numbered, this produces cartesian product of partitions.
        // If one or both sources are ordered, the row number reflects the ordering.
        // The second and third disjunct in the join condition account for the situation when partitions have different sizes. It preserves the outstanding rows
        // from the bigger partition, matching them to the first row from the smaller partition.
        //
        // R1 = R2
        // OR
        // (R1 > S2 AND R2 = 1)
        // OR
        // (R2 > S1 AND R1 = 1)

        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        RowExpression joinCondition = new SpecialFormExpression(SpecialFormExpression.Form.OR,
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression(EQUAL.name(),
                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                BOOLEAN,
                                ImmutableList.of(left.rowNumber, right.rowNumber)),
                        new SpecialFormExpression(SpecialFormExpression.Form.OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(SpecialFormExpression.Form.AND,
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new CallExpression(GREATER_THAN.name(),
                                                                functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(left.rowNumber, right.partitionSize)),
                                                        new CallExpression(EQUAL.name(),
                                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(right.rowNumber, new ConstantExpression(1L, BIGINT))))),
                                        new SpecialFormExpression(SpecialFormExpression.Form.AND,
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new CallExpression(GREATER_THAN.name(),
                                                                functionResolution.comparisonFunction(GREATER_THAN, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(right.rowNumber, left.partitionSize)),
                                                        new CallExpression(EQUAL.name(),
                                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                                BOOLEAN,
                                                                ImmutableList.of(left.rowNumber, new ConstantExpression(1L, BIGINT)))))))));
        JoinType joinType;
        if (left.pruneWhenEmpty() && right.pruneWhenEmpty()) {
            joinType = INNER;
        }
        else if (left.pruneWhenEmpty()) {
            joinType = LEFT;
        }
        else if (right.pruneWhenEmpty()) {
            joinType = RIGHT;
        }
        else {
            joinType = FULL;
        }

        return new JoinedNodes(
                new JoinNode(
                        Optional.empty(),
                        context.getIdAllocator().getNextId(),
                        joinType,
                        left.node(),
                        right.node(),
                        ImmutableList.of(),
                        Stream.concat(left.node().getOutputVariables().stream(),
                                        right.node().getOutputVariables().stream())
                                .collect(Collectors.toList()),
                        Optional.of(joinCondition),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()),
                left.rowNumber(),
                left.partitionSize(),
                left.partitionBy(),
                left.pruneWhenEmpty(),
                left.rowNumberSymbolsMapping(),
                right.rowNumber(),
                right.partitionSize(),
                right.partitionBy(),
                right.pruneWhenEmpty(),
                right.rowNumberSymbolsMapping());
    }

    private static NodeWithVariables appendHelperSymbolsForJoinedNodes(JoinedNodes joinedNodes, Context context, Metadata metadata)
    {
        // Derive row number for joined partitions: this is the bigger partition's row number. One of the combined values might be null as a result of outer join.
        VariableReferenceExpression joinedRowNumber = context.getVariableAllocator().newVariable("combined_row_number", BIGINT);
        RowExpression rowNumberExpression = new SpecialFormExpression(
                IF,
                BIGINT,
                ImmutableList.of(
                        new CallExpression(
                                GREATER_THAN.name(),
                                metadata.getFunctionAndTypeManager().resolveOperator(
                                        OperatorType.GREATER_THAN,
                                        fromTypes(BIGINT, BIGINT)),
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                joinedNodes.leftRowNumber(),
                                                new ConstantExpression(-1L, BIGINT)),
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                joinedNodes.rightRowNumber(),
                                                new ConstantExpression(-1L, BIGINT)))),
                        joinedNodes.leftRowNumber(),
                        joinedNodes.rightRowNumber()));

        // Derive partition size for joined partitions: this is the bigger partition's size. One of the combined values might be null as a result of outer join.
        VariableReferenceExpression joinedPartitionSize = context.getVariableAllocator().newVariable("combined_partition_size", BIGINT);
        RowExpression partitionSizeExpression = new SpecialFormExpression(
                IF,
                BIGINT,
                ImmutableList.of(
                        new CallExpression(
                                GREATER_THAN.name(),
                                metadata.getFunctionAndTypeManager().resolveOperator(
                                        OperatorType.GREATER_THAN,
                                        fromTypes(BIGINT, BIGINT)),
                                BOOLEAN,
                                ImmutableList.of(
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                joinedNodes.leftPartitionSize(),
                                                new ConstantExpression(-1L, BIGINT)),
                                        new SpecialFormExpression(
                                                COALESCE,
                                                BIGINT,
                                                joinedNodes.rightPartitionSize(),
                                                new ConstantExpression(-1L, BIGINT)))),
                        joinedNodes.leftPartitionSize(),
                        joinedNodes.rightPartitionSize()));

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                joinedNodes.joinedNode(),
                Assignments.builder()
                        .putAll(
                                joinedNodes.joinedNode().getOutputVariables().stream()
                                        .collect(toImmutableMap(v -> v, v -> v)))
                        .put(joinedRowNumber, rowNumberExpression)
                        .put(joinedPartitionSize, partitionSizeExpression)
                        .build());

        List<VariableReferenceExpression> joinedPartitionBy = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(joinedNodes.leftPartitionBy())
                .addAll(joinedNodes.rightPartitionBy())
                .build();

        boolean joinedPruneWhenEmpty = joinedNodes.leftPruneWhenEmpty() || joinedNodes.rightPruneWhenEmpty();

        Map<VariableReferenceExpression, VariableReferenceExpression> joinedRowNumberSymbolsMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                .putAll(joinedNodes.leftRowNumberSymbolsMapping())
                .putAll(joinedNodes.rightRowNumberSymbolsMapping())
                .buildOrThrow();

        return new NodeWithVariables(project, joinedRowNumber, joinedPartitionSize, joinedPartitionBy, joinedPruneWhenEmpty, joinedRowNumberSymbolsMapping);
    }

    private static NodeWithMarkers appendMarkerSymbols(PlanNode node, Set<VariableReferenceExpression> variables, VariableReferenceExpression referenceSymbol, Context context, Metadata metadata)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(
                node.getOutputVariables().stream()
                    .collect(toImmutableMap(v -> v, v -> v)));

        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> variablesToMarkers = ImmutableMap.builder();

        for (VariableReferenceExpression variable : variables) {
            VariableReferenceExpression marker = context.getVariableAllocator().newVariable("marker", BIGINT);
            variablesToMarkers.put(variable, marker);
            RowExpression ifExpression = new SpecialFormExpression(
                    IF,
                    BIGINT,
                    ImmutableList.of(
                            new CallExpression(
                                    EQUAL.name(),
                                    metadata.getFunctionAndTypeManager().resolveOperator(
                                            OperatorType.EQUAL,
                                            fromTypes(BIGINT, BIGINT)),
                                    BOOLEAN,
                                    ImmutableList.of(variable, referenceSymbol)),
                            variable,
                            new ConstantExpression(null, BIGINT)));
            assignments.put(marker, ifExpression);
        }

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                node,
                assignments.build());

        return new NodeWithMarkers(project, variablesToMarkers.buildOrThrow());
    }

    private static class SourceWithProperties
    {
        private final PlanNode source;
        private final TableArgumentProperties properties;

        public SourceWithProperties(PlanNode source, TableArgumentProperties properties)
        {
            this.source = requireNonNull(source, "source is null");
            this.properties = requireNonNull(properties, "properties is null");
        }

        public PlanNode source()
        {
            return source;
        }

        public TableArgumentProperties properties()
        {
            return properties;
        }
    }

    public static final class NodeWithVariables
    {
        private final PlanNode node;
        private final VariableReferenceExpression rowNumber;
        private final VariableReferenceExpression partitionSize;
        private final List<VariableReferenceExpression> partitionBy;
        private final boolean pruneWhenEmpty;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> rowNumberSymbolsMapping;

        public NodeWithVariables(PlanNode node, VariableReferenceExpression rowNumber, VariableReferenceExpression partitionSize,
                                 List<VariableReferenceExpression> partitionBy, boolean pruneWhenEmpty,
                                 Map<VariableReferenceExpression, VariableReferenceExpression> rowNumberSymbolsMapping)
        {
            this.node = requireNonNull(node, "node is null");
            this.rowNumber = requireNonNull(rowNumber, "rowNumber is null");
            this.partitionSize = requireNonNull(partitionSize, "partitionSize is null");
            this.partitionBy = ImmutableList.copyOf(partitionBy);
            this.pruneWhenEmpty = pruneWhenEmpty;
            this.rowNumberSymbolsMapping = ImmutableMap.copyOf(rowNumberSymbolsMapping);
        }

        public PlanNode node()
        {
            return node;
        }

        public VariableReferenceExpression rowNumber()
        {
            return rowNumber;
        }

        public VariableReferenceExpression partitionSize()
        {
            return partitionSize;
        }

        public List<VariableReferenceExpression> partitionBy()
        {
            return partitionBy;
        }

        public boolean pruneWhenEmpty()
        {
            return pruneWhenEmpty;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> rowNumberSymbolsMapping()
        {
            return rowNumberSymbolsMapping;
        }
    }

    private static class JoinedNodes
    {
        private final PlanNode joinedNode;
        private final VariableReferenceExpression leftRowNumber;
        private final VariableReferenceExpression leftPartitionSize;
        private final List<VariableReferenceExpression> leftPartitionBy;
        private final boolean leftPruneWhenEmpty;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> leftRowNumberSymbolsMapping;
        private final VariableReferenceExpression rightRowNumber;
        private final VariableReferenceExpression rightPartitionSize;
        private final List<VariableReferenceExpression> rightPartitionBy;
        private final boolean rightPruneWhenEmpty;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> rightRowNumberSymbolsMapping;

        public JoinedNodes(
                PlanNode joinedNode,
                VariableReferenceExpression leftRowNumber,
                VariableReferenceExpression leftPartitionSize,
                List<VariableReferenceExpression> leftPartitionBy,
                boolean leftPruneWhenEmpty,
                Map<VariableReferenceExpression, VariableReferenceExpression> leftRowNumberSymbolsMapping,
                VariableReferenceExpression rightRowNumber,
                VariableReferenceExpression rightPartitionSize,
                List<VariableReferenceExpression> rightPartitionBy,
                boolean rightPruneWhenEmpty,
                Map<VariableReferenceExpression, VariableReferenceExpression> rightRowNumberSymbolsMapping)
        {
            this.joinedNode = requireNonNull(joinedNode, "joinedNode is null");
            this.leftRowNumber = requireNonNull(leftRowNumber, "leftRowNumber is null");
            this.leftPartitionSize = requireNonNull(leftPartitionSize, "leftPartitionSize is null");
            this.leftPartitionBy = ImmutableList.copyOf(requireNonNull(leftPartitionBy, "leftPartitionBy is null"));
            this.leftPruneWhenEmpty = leftPruneWhenEmpty;
            this.leftRowNumberSymbolsMapping = ImmutableMap.copyOf(requireNonNull(leftRowNumberSymbolsMapping, "leftRowNumberSymbolsMapping is null"));
            this.rightRowNumber = requireNonNull(rightRowNumber, "rightRowNumber is null");
            this.rightPartitionSize = requireNonNull(rightPartitionSize, "rightPartitionSize is null");
            this.rightPartitionBy = ImmutableList.copyOf(requireNonNull(rightPartitionBy, "rightPartitionBy is null"));
            this.rightPruneWhenEmpty = rightPruneWhenEmpty;
            this.rightRowNumberSymbolsMapping = ImmutableMap.copyOf(requireNonNull(rightRowNumberSymbolsMapping, "rightRowNumberSymbolsMapping is null"));
        }

        public PlanNode joinedNode()
        {
            return joinedNode;
        }
        public VariableReferenceExpression leftRowNumber()
        {
            return leftRowNumber;
        }
        public VariableReferenceExpression leftPartitionSize()
        {
            return leftPartitionSize;
        }
        public List<VariableReferenceExpression> leftPartitionBy()
        {
            return leftPartitionBy;
        }
        public boolean leftPruneWhenEmpty()
        {
            return leftPruneWhenEmpty;
        }
        public Map<VariableReferenceExpression, VariableReferenceExpression> leftRowNumberSymbolsMapping()
        {
            return leftRowNumberSymbolsMapping;
        }
        public VariableReferenceExpression rightRowNumber()
        {
            return rightRowNumber;
        }
        public VariableReferenceExpression rightPartitionSize()
        {
            return rightPartitionSize;
        }
        public List<VariableReferenceExpression> rightPartitionBy()
        {
            return rightPartitionBy;
        }
        public boolean rightPruneWhenEmpty()
        {
            return rightPruneWhenEmpty;
        }
        public Map<VariableReferenceExpression, VariableReferenceExpression> rightRowNumberSymbolsMapping()
        {
            return rightRowNumberSymbolsMapping;
        }
    }

    private static class NodeWithMarkers
    {
        private final PlanNode node;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> variableToMarker;

        public NodeWithMarkers(PlanNode node, Map<VariableReferenceExpression, VariableReferenceExpression> variableToMarker)
        {
            this.node = requireNonNull(node, "node is null");
            this.variableToMarker = ImmutableMap.copyOf(requireNonNull(variableToMarker, "symbolToMarker is null"));
        }

        public PlanNode node()
        {
            return node;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> variableToMarker()
        {
            return variableToMarker;
        }
    }
}
