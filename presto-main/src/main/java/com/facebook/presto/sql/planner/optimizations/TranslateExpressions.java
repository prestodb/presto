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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Function;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.execution.warnings.WarningCollector.NOOP;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.spatialJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFinish;
import static com.facebook.presto.sql.planner.plan.Patterns.tableWriterMergeNode;
import static com.facebook.presto.sql.planner.plan.Patterns.tableWriterNode;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.facebook.presto.sql.planner.plan.Patterns.window;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TranslateExpressions
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public TranslateExpressions(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParseris null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ValuesExpressionTranslation(),
                new FilterExpressionTranslation(),
                new ProjectExpressionTranslation(),
                new ApplyExpressionTranslation(),
                new WindowExpressionTranslation(),
                new JoinExpressionTranslation(),
                new SpatialJoinExpressionTranslation(),
                new AggregationExpressionTranslation(),
                new TableFinishExpressionTranslation(),
                new TableWriterExpressionTranslation(),
                new TableWriterMergeExpressionTranslation());
    }

    private final class SpatialJoinExpressionTranslation
            implements Rule<SpatialJoinNode>
    {
        @Override
        public Pattern<SpatialJoinNode> getPattern()
        {
            return spatialJoin();
        }

        @Override
        public Result apply(SpatialJoinNode spatialJoinNode, Captures captures, Context context)
        {
            RowExpression filter = spatialJoinNode.getFilter();
            RowExpression rewritten = removeOriginalExpression(filter, context);

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new SpatialJoinNode(
                    spatialJoinNode.getId(),
                    spatialJoinNode.getType(),
                    spatialJoinNode.getLeft(),
                    spatialJoinNode.getRight(),
                    spatialJoinNode.getOutputVariables(),
                    rewritten,
                    spatialJoinNode.getLeftPartitionVariable(),
                    spatialJoinNode.getRightPartitionVariable(),
                    spatialJoinNode.getKdbTree()));
        }
    }

    private final class JoinExpressionTranslation
            implements Rule<JoinNode>
    {
        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            if (!joinNode.getFilter().isPresent()) {
                return Result.empty();
            }

            RowExpression filter = joinNode.getFilter().get();
            RowExpression rewritten = removeOriginalExpression(filter, context);

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    joinNode.getLeft(),
                    joinNode.getRight(),
                    joinNode.getCriteria(),
                    joinNode.getOutputVariables(),
                    Optional.of(rewritten),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType()));
        }
    }

    private final class WindowExpressionTranslation
            implements Rule<WindowNode>
    {
        @Override
        public Pattern<WindowNode> getPattern()
        {
            return window();
        }

        @Override
        public Result apply(WindowNode windowNode, Captures captures, Context context)
        {
            checkState(windowNode.getSource() != null);
            boolean anyRewritten = false;
            ImmutableMap.Builder<VariableReferenceExpression, Function> functions = ImmutableMap.builder();
            for (Entry<VariableReferenceExpression, Function> entry : windowNode.getWindowFunctions().entrySet()) {
                ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
                CallExpression callExpression = entry.getValue().getFunctionCall();
                for (RowExpression argument : callExpression.getArguments()) {
                    RowExpression rewritten = removeOriginalExpression(argument, context);
                    if (rewritten != argument) {
                        anyRewritten = true;
                    }
                    newArguments.add(rewritten);
                }
                functions.put(
                        entry.getKey(),
                        new Function(
                                call(
                                        callExpression.getDisplayName(),
                                        callExpression.getFunctionHandle(),
                                        callExpression.getType(),
                                        newArguments.build()),
                                entry.getValue().getFrame()));
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new WindowNode(
                        windowNode.getId(),
                        windowNode.getSource(),
                        windowNode.getSpecification(),
                        functions.build(),
                        windowNode.getHashVariable(),
                        windowNode.getPrePartitionedInputs(),
                        windowNode.getPreSortedOrderPrefix()));
            }
            return Result.empty();
        }
    }

    private final class ProjectExpressionTranslation
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments assignments = projectNode.getAssignments();
            Optional<Assignments> rewrittenAssignments = translateAssignments(assignments, context);

            if (!rewrittenAssignments.isPresent()) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), rewrittenAssignments.get()));
        }
    }

    private final class ApplyExpressionTranslation
            implements Rule<ApplyNode>
    {
        @Override
        public Pattern<ApplyNode> getPattern()
        {
            return applyNode();
        }

        @Override
        public Result apply(ApplyNode applyNode, Captures captures, Context context)
        {
            Assignments assignments = applyNode.getSubqueryAssignments();
            Optional<Assignments> rewrittenAssignments = translateAssignments(assignments, context);

            if (!rewrittenAssignments.isPresent()) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ApplyNode(
                    applyNode.getId(),
                    applyNode.getInput(),
                    applyNode.getSubquery(),
                    rewrittenAssignments.get(),
                    applyNode.getCorrelation(),
                    applyNode.getOriginSubqueryError()));
        }
    }

    private final class FilterExpressionTranslation
            implements Rule<FilterNode>
    {
        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            checkState(filterNode.getSource() != null);
            RowExpression rewritten = removeOriginalExpression(filterNode.getPredicate(), context);

            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class ValuesExpressionTranslation
            implements Rule<ValuesNode>
    {
        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableList.Builder<List<RowExpression>> rows = ImmutableList.builder();
            for (List<RowExpression> row : valuesNode.getRows()) {
                ImmutableList.Builder<RowExpression> newRow = ImmutableList.builder();
                for (RowExpression rowExpression : row) {
                    RowExpression rewritten = removeOriginalExpression(rowExpression, context);
                    if (rowExpression != rewritten) {
                        anyRewritten = true;
                    }
                    newRow.add(rewritten);
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputVariables(), rows.build()));
            }
            return Result.empty();
        }
    }

    private final class AggregationExpressionTranslation
            implements Rule<AggregationNode>
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            boolean changed = false;
            ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> rewrittenAggregation = builder();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation rewritten = translateAggregation(entry.getValue(), context.getSession(), context.getVariableAllocator().getTypes());
                rewrittenAggregation.put(entry.getKey(), rewritten);
                if (!rewritten.equals(entry.getValue())) {
                    changed = true;
                }
            }

            if (changed) {
                AggregationNode aggregationNode = new AggregationNode(
                        node.getId(),
                        node.getSource(),
                        rewrittenAggregation.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedVariables(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable());
                return Result.ofPlanNode(aggregationNode);
            }
            return Result.empty();
        }
    }

    private final class TableFinishExpressionTranslation
            implements Rule<TableFinishNode>
    {
        @Override
        public Pattern<TableFinishNode> getPattern()
        {
            return tableFinish();
        }

        @Override
        public Result apply(TableFinishNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new TableFinishNode(
                        node.getId(),
                        node.getSource(),
                        node.getTarget(),
                        node.getRowCountVariable(),
                        rewrittenStatisticsAggregation,
                        node.getStatisticsAggregationDescriptor()));
            }
            return Result.empty();
        }
    }

    private final class TableWriterExpressionTranslation
            implements Rule<TableWriterNode>
    {
        @Override
        public Pattern<TableWriterNode> getPattern()
        {
            return tableWriterNode();
        }

        @Override
        public Result apply(TableWriterNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new TableWriterNode(
                        node.getId(),
                        node.getSource(),
                        node.getTarget(),
                        node.getRowCountVariable(),
                        node.getFragmentVariable(),
                        node.getTableCommitContextVariable(),
                        node.getColumns(),
                        node.getColumnNames(),
                        node.getPartitioningScheme(),
                        rewrittenStatisticsAggregation));
            }
            return Result.empty();
        }
    }

    private final class TableWriterMergeExpressionTranslation
            implements Rule<TableWriterMergeNode>
    {
        @Override
        public Pattern<TableWriterMergeNode> getPattern()
        {
            return tableWriterMergeNode();
        }

        @Override
        public Result apply(TableWriterMergeNode node, Captures captures, Context context)
        {
            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new TableWriterMergeNode(
                        node.getId(),
                        node.getSource(),
                        node.getRowCountVariable(),
                        node.getFragmentVariable(),
                        node.getTableCommitContextVariable(),
                        rewrittenStatisticsAggregation));
            }
            return Result.empty();
        }
    }

    private Optional<StatisticAggregations> translateStatisticAggregation(StatisticAggregations statisticAggregations, Rule.Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> rewrittenAggregation = builder();
        boolean changed = false;
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : statisticAggregations.getAggregations().entrySet()) {
            AggregationNode.Aggregation rewritten = translateAggregation(entry.getValue(), context.getSession(), context.getVariableAllocator().getTypes());
            rewrittenAggregation.put(entry.getKey(), rewritten);
            if (!rewritten.equals(entry.getValue())) {
                changed = true;
            }
        }
        if (changed) {
            return Optional.of(new StatisticAggregations(rewrittenAggregation.build(), statisticAggregations.getGroupingVariables()));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    public AggregationNode.Aggregation translateAggregation(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
    {
        Map<NodeRef<Expression>, Type> types = analyzeAggregationExpressionTypes(aggregation, session, typeProvider);

        return new AggregationNode.Aggregation(
                new CallExpression(
                        aggregation.getCall().getDisplayName(),
                        aggregation.getCall().getFunctionHandle(),
                        aggregation.getCall().getType(),
                        aggregation.getArguments().stream().map(argument -> removeOriginalExpression(argument, session, types)).collect(toImmutableList())),
                aggregation.getFilter().map(filter -> removeOriginalExpression(filter, session, types)),
                aggregation.getOrderBy(),
                aggregation.isDistinct(),
                aggregation.getMask());
    }

    private Map<NodeRef<Expression>, Type> analyzeAggregationExpressionTypes(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
    {
        List<LambdaExpression> lambdaExpressions = aggregation.getArguments().stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .filter(LambdaExpression.class::isInstance)
                .map(LambdaExpression.class::cast)
                .collect(toImmutableList());
        ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
        if (!lambdaExpressions.isEmpty()) {
            List<FunctionType> functionTypes = metadata.getFunctionManager().getFunctionMetadata(aggregation.getFunctionHandle()).getArgumentTypes().stream()
                    .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                    .map(typeSignature -> (FunctionType) (metadata.getTypeManager().getType(typeSignature)))
                    .collect(toImmutableList());
            InternalAggregationFunction internalAggregationFunction = metadata.getFunctionManager().getAggregateFunctionImplementation(aggregation.getFunctionHandle());
            List<Class> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
            verify(lambdaExpressions.size() == functionTypes.size());
            verify(lambdaExpressions.size() == lambdaInterfaces.size());

            for (int i = 0; i < lambdaExpressions.size(); i++) {
                LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                FunctionType functionType = functionTypes.get(i);

                // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                // which requires the types of all sub-expressions.
                //
                // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                //
                // This does not work here since the function call representation in final aggregation node
                // is currently a hack: it takes intermediate type as input, and may not be a valid
                // function call in Presto.
                //
                // TODO: Once the final aggregation function call representation is fixed,
                // the same mechanism in project and filter expression should be used here.
                verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                Map<String, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                    LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                    Type type = functionType.getArgumentTypes().get(j);
                    lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                    lambdaArgumentSymbolTypes.put(argument.getName().getValue(), type);
                }
                // the lambda expression itself
                builder.put(NodeRef.of(lambdaExpression), functionType)
                        // expressions from lambda arguments
                        .putAll(lambdaArgumentExpressionTypes)
                        // expressions from lambda body
                        .putAll(getExpressionTypes(
                                session,
                                metadata,
                                sqlParser,
                                TypeProvider.copyOf(lambdaArgumentSymbolTypes),
                                lambdaExpression.getBody(),
                                emptyList(),
                                NOOP));
            }
        }
        for (RowExpression argument : aggregation.getArguments()) {
            if (!isExpression(argument) || castToExpression(argument) instanceof LambdaExpression) {
                continue;
            }
            builder.putAll(analyze(castToExpression(argument), session, typeProvider));
        }
        if (aggregation.getFilter().isPresent() && isExpression(aggregation.getFilter().get())) {
            builder.putAll(analyze(castToExpression(aggregation.getFilter().get()), session, typeProvider));
        }
        return builder.build();
    }

    private Map<NodeRef<Expression>, Type> analyze(Expression expression, Session session, TypeProvider typeProvider)
    {
        return getExpressionTypes(
                session,
                metadata,
                sqlParser,
                typeProvider,
                expression,
                emptyList(),
                NOOP);
    }

    private RowExpression toRowExpression(Expression expression, Session session, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), session, false);
    }

    private RowExpression removeOriginalExpression(RowExpression expression, Rule.Context context)
    {
        if (isExpression(expression)) {
            return toRowExpression(
                    castToExpression(expression),
                    context.getSession(),
                    analyze(castToExpression(expression), context.getSession(), context.getVariableAllocator().getTypes()));
        }
        return expression;
    }

    private RowExpression removeOriginalExpression(RowExpression rowExpression, Session session, Map<NodeRef<Expression>, Type> types)
    {
        if (isExpression(rowExpression)) {
            Expression expression = castToExpression(rowExpression);
            return toRowExpression(expression, session, types);
        }
        return rowExpression;
    }

    /**
     * Return Optional.empty() to denote unchanged assignments
     */
    private Optional<Assignments> translateAssignments(Assignments assignments, Rule.Context context)
    {
        Assignments.Builder builder = Assignments.builder();
        boolean anyRewritten = false;
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
            RowExpression expression = entry.getValue();
            RowExpression rewritten;
            if (isExpression(expression)) {
                rewritten = toRowExpression(
                        castToExpression(expression),
                        context.getSession(),
                        analyze(castToExpression(expression), context.getSession(), context.getVariableAllocator().getTypes()));
                anyRewritten = true;
            }
            else {
                rewritten = expression;
            }
            builder.put(entry.getKey(), rewritten);
        }
        if (!anyRewritten) {
            return Optional.empty();
        }
        return Optional.of(builder.build());
    }
}
