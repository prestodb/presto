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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ValidateNoLeftOverOriginalExpression
        implements PlanSanityChecker.Checker
{
    private final RowExpressionChecker checker = new RowExpressionChecker();

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        planNode.accept(checker, null);
    }

    private class RowExpressionChecker
            extends InternalPlanVisitor<Void, Void>
    {
        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(source -> source.accept(this, context));
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            validateAggregations(node.getAggregations().values(), node);
            return super.visitAggregation(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            node.getAssignments().getExpressions()
                    .forEach(expression -> validateExpression(expression, node));
            return super.visitProject(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            node.getRows().stream()
                    .flatMap(List::stream)
                    .forEach(expression -> validateExpression(expression, node));
            return super.visitValues(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getFilter().ifPresent(expression -> validateExpression(expression, node));
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            node.getWindowFunctions().values()
                    .stream()
                    .map(WindowNode.Function::getFunctionCall)
                    .map(CallExpression::getArguments)
                    .flatMap(List::stream)
                    .forEach(expression -> validateExpression(expression, node));
            return super.visitWindow(node, context);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            node.getStatisticsAggregation().ifPresent(
                    statisticAggregations -> validateAggregations(statisticAggregations.getAggregations().values(), node));
            return super.visitTableWriter(node, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            node.getStatisticsAggregation().ifPresent(
                    statisticAggregations -> validateAggregations(statisticAggregations.getAggregations().values(), node));
            return super.visitTableFinish(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            validateExpression(node.getPredicate(), node);
            return super.visitFilter(node, context);
        }

        private void validateAggregations(Collection<AggregationNode.Aggregation> aggregations, PlanNode node)
        {
            for (AggregationNode.Aggregation aggregation : aggregations) {
                aggregation.getArguments().forEach(argument -> validateExpression(argument, node));
                aggregation.getFilter().ifPresent(filter -> validateExpression(filter, node));
            }
        }

        private void validateExpression(RowExpression expression, PlanNode node)
        {
            checkArgument(!isExpression(expression), format("Has expression %s in node %s", expression, node));
        }
    }
}
