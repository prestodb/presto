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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Function;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.execution.warnings.WarningCollector.NOOP;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.spatialJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.facebook.presto.sql.planner.plan.Patterns.window;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkState;
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
        // TODO: finish all other PlanNodes that have Expression
        return ImmutableSet.of(
                new ValuesExpressionTranslation(),
                new FilterExpressionTranslation(),
                new WindowExpressionTranslation(),
                new JoinExpressionTranslation(),
                new SpatialJoinExpressionTranslation());
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
            RowExpression rewritten;
            if (isExpression(filter)) {
                rewritten = toRowExpression(castToExpression(filter), context);
            }
            else {
                rewritten = filter;
            }

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new SpatialJoinNode(
                    spatialJoinNode.getId(),
                    spatialJoinNode.getType(),
                    spatialJoinNode.getLeft(),
                    spatialJoinNode.getRight(),
                    spatialJoinNode.getOutputSymbols(),
                    rewritten,
                    spatialJoinNode.getLeftPartitionSymbol(),
                    spatialJoinNode.getRightPartitionSymbol(),
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
            RowExpression rewritten;
            if (!joinNode.getFilter().isPresent()) {
                return Result.empty();
            }

            RowExpression filter = joinNode.getFilter().get();
            if (isExpression(filter)) {
                rewritten = toRowExpression(castToExpression(filter), context);
            }
            else {
                rewritten = filter;
            }

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    joinNode.getLeft(),
                    joinNode.getRight(),
                    joinNode.getCriteria(),
                    joinNode.getOutputSymbols(),
                    Optional.of(rewritten),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
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
            ImmutableMap.Builder<Symbol, Function> functions = ImmutableMap.builder();
            for (Entry<Symbol, Function> entry : windowNode.getWindowFunctions().entrySet()) {
                ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
                CallExpression callExpression = entry.getValue().getFunctionCall();
                for (RowExpression argument : callExpression.getArguments()) {
                    if (isExpression(argument)) {
                        RowExpression rewritten = toRowExpression(castToExpression(argument), context);
                        anyRewritten = true;
                        newArguments.add(rewritten);
                    }
                    else {
                        newArguments.add(argument);
                    }
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
                        windowNode.getHashSymbol(),
                        windowNode.getPrePartitionedInputs(),
                        windowNode.getPreSortedOrderPrefix()));
            }
            return Result.empty();
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
            RowExpression rewritten;
            if (isExpression(filterNode.getPredicate())) {
                rewritten = toRowExpression(castToExpression(filterNode.getPredicate()), context);
            }
            else {
                rewritten = filterNode.getPredicate();
            }

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
                    if (isExpression(rowExpression)) {
                        RowExpression rewritten = toRowExpression(castToExpression(rowExpression), context);
                        anyRewritten = true;
                        newRow.add(rewritten);
                    }
                    else {
                        newRow.add(rowExpression);
                    }
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Result.empty();
        }
    }

    private RowExpression toRowExpression(Expression expression, Rule.Context context)
    {
        Map<NodeRef<Expression>, Type> types = getExpressionTypes(
                context.getSession(),
                metadata,
                sqlParser,
                context.getSymbolAllocator().getTypes(),
                ImmutableList.of(expression),
                ImmutableList.of(),
                NOOP,
                false);

        return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), context.getSession(), false);
    }
}
