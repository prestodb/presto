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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.spatialJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFinish;
import static com.facebook.presto.sql.planner.plan.Patterns.tableWriterNode;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.facebook.presto.sql.planner.plan.Patterns.window;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Objects.requireNonNull;

public class RowExpressionRewriteRuleSet
{
    public interface PlanRowExpressionRewriter
    {
        RowExpression rewrite(RowExpression expression, Rule.Context context);
    }

    protected final PlanRowExpressionRewriter rewriter;

    public RowExpressionRewriteRuleSet(PlanRowExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                valueRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule(),
                projectRowExpressionRewriteRule(),
                applyNodeRowExpressionRewriteRule(),
                windowRowExpressionRewriteRule(),
                joinRowExpressionRewriteRule(),
                spatialJoinRowExpressionRewriteRule(),
                aggregationRowExpressionRewriteRule(),
                tableFinishRowExpressionRewriteRule(),
                tableWriterRowExpressionRewriteRule());
    }

    public Rule<ValuesNode> valueRowExpressionRewriteRule()
    {
        return new ValuesRowExpressionRewrite();
    }

    public Rule<FilterNode> filterRowExpressionRewriteRule()
    {
        return new FilterRowExpressionRewrite();
    }

    public Rule<ProjectNode> projectRowExpressionRewriteRule()
    {
        return new ProjectRowExpressionRewrite();
    }

    public Rule<ApplyNode> applyNodeRowExpressionRewriteRule()
    {
        return new ApplyRowExpressionRewrite();
    }

    public Rule<WindowNode> windowRowExpressionRewriteRule()
    {
        return new WindowRowExpressionRewrite();
    }

    public Rule<JoinNode> joinRowExpressionRewriteRule()
    {
        return new JoinRowExpressionRewrite();
    }

    public Rule<SpatialJoinNode> spatialJoinRowExpressionRewriteRule()
    {
        return new SpatialJoinRowExpressionRewrite();
    }

    public Rule<TableFinishNode> tableFinishRowExpressionRewriteRule()
    {
        return new TableFinishRowExpressionRewrite();
    }

    public Rule<TableWriterNode> tableWriterRowExpressionRewriteRule()
    {
        return new TableWriterRowExpressionRewrite();
    }

    public Rule<AggregationNode> aggregationRowExpressionRewriteRule()
    {
        return new AggregationRowExpressionRewrite();
    }

    private final class ProjectRowExpressionRewrite
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
            Assignments.Builder builder = Assignments.builder();
            boolean anyRewritten = false;
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().getMap().entrySet()) {
                RowExpression rewritten = rewriter.rewrite(entry.getValue(), context);
                if (!rewritten.equals(entry.getValue())) {
                    anyRewritten = true;
                }
                builder.put(entry.getKey(), rewritten);
            }
            Assignments assignments = builder.build();
            if (anyRewritten) {
                return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments, projectNode.getLocality()));
            }
            return Result.empty();
        }
    }

    private final class SpatialJoinRowExpressionRewrite
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
            RowExpression rewritten = rewriter.rewrite(filter, context);

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

    private final class JoinRowExpressionRewrite
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
            RowExpression rewritten = rewriter.rewrite(filter, context);

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
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters()));
        }
    }

    private final class WindowRowExpressionRewrite
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
            ImmutableMap.Builder<VariableReferenceExpression, WindowNode.Function> functions = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : windowNode.getWindowFunctions().entrySet()) {
                ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
                CallExpression callExpression = entry.getValue().getFunctionCall();
                for (RowExpression argument : callExpression.getArguments()) {
                    RowExpression rewritten = rewriter.rewrite(argument, context);
                    if (rewritten != argument) {
                        anyRewritten = true;
                    }
                    newArguments.add(rewritten);
                }
                functions.put(
                        entry.getKey(),
                        new WindowNode.Function(
                                call(
                                        callExpression.getDisplayName(),
                                        callExpression.getFunctionHandle(),
                                        callExpression.getType(),
                                        newArguments.build()),
                                entry.getValue().getFrame(),
                                entry.getValue().isIgnoreNulls()));
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

    private final class ApplyRowExpressionRewrite
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

    private Optional<Assignments> translateAssignments(Assignments assignments, Rule.Context context)
    {
        Assignments.Builder builder = Assignments.builder();
        assignments.getMap()
                .entrySet()
                .stream()
                .forEach(entry -> builder.put(entry.getKey(), rewriter.rewrite(entry.getValue(), context)));
        Assignments rewritten = builder.build();
        if (rewritten.equals(assignments)) {
            return Optional.empty();
        }
        return Optional.of(rewritten);
    }

    private final class FilterRowExpressionRewrite
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
            RowExpression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);

            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class ValuesRowExpressionRewrite
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
                    RowExpression rewritten = rewriter.rewrite(rowExpression, context);
                    if (!rewritten.equals(rowExpression)) {
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

    private final class AggregationRowExpressionRewrite
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
                AggregationNode.Aggregation rewritten = rewriteAggregation(entry.getValue(), context);
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

    private final class TableFinishRowExpressionRewrite
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

    private Optional<StatisticAggregations> translateStatisticAggregation(StatisticAggregations statisticAggregations, Rule.Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> rewrittenAggregation = builder();
        boolean changed = false;
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : statisticAggregations.getAggregations().entrySet()) {
            AggregationNode.Aggregation rewritten = rewriteAggregation(entry.getValue(), context);
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

    private final class TableWriterRowExpressionRewrite
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
                        node.getNotNullColumnVariables(),
                        node.getTablePartitioningScheme(),
                        node.getPreferredShufflePartitioningScheme(),
                        rewrittenStatisticsAggregation));
            }
            return Result.empty();
        }
    }

    private AggregationNode.Aggregation rewriteAggregation(AggregationNode.Aggregation aggregation, Rule.Context context)
    {
        RowExpression rewrittenCall = rewriter.rewrite(aggregation.getCall(), context);
        checkArgument(rewrittenCall instanceof CallExpression, "Aggregation CallExpression must be rewritten to CallExpression");
        return new AggregationNode.Aggregation(
                (CallExpression) rewrittenCall,
                aggregation.getFilter().map(filter -> rewriter.rewrite(filter, context)),
                aggregation.getOrderBy(),
                aggregation.isDistinct(),
                aggregation.getMask());
    }
}
