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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.WindowNode;

import java.util.Optional;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkArgument;

public class VerifyNoOriginalExpression
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        plan.accept(new Visitor(), null);
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            visitPlan(node, context);

            node.getAssignments().getExpressions().forEach(Visitor::checkNotOriginalExpression);
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            visitPlan(node, context);

            node.getAggregations().values().forEach(Visitor::checkAggregation);
            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            visitPlan(node, context);

            node.getWindowFunctions().values().forEach(
                    windowNodeFunction -> windowNodeFunction.getFunctionCall().getArguments().forEach(Visitor::checkNotOriginalExpression));
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            visitPlan(node, context);

            checkNotOriginalExpression(node.getPredicate());
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            visitPlan(node, context);

            node.getRows().forEach(rows -> rows.forEach(Visitor::checkNotOriginalExpression));
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            visitPlan(node, context);

            node.getFilter().ifPresent(Visitor::checkNotOriginalExpression);
            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            visitPlan(node, context);

            checkNotOriginalExpression(node.getFilter());
            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            visitPlan(node, context);

            node.getSubqueryAssignments().getExpressions().forEach(Visitor::checkNotOriginalExpression);
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            visitPlan(node, context);

            checkStatisticsAggregation(node.getStatisticsAggregation());
            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            visitPlan(node, context);

            checkStatisticsAggregation(node.getStatisticsAggregation());
            return null;
        }

        @Override
        public Void visitTableWriteMerge(TableWriterMergeNode node, Void context)
        {
            visitPlan(node, context);

            checkStatisticsAggregation(node.getStatisticsAggregation());
            return null;
        }

        private static void checkStatisticsAggregation(Optional<StatisticAggregations> statisticsAggregation)
        {
            statisticsAggregation.ifPresent(
                    statisticAggregations -> statisticAggregations.getAggregations().values().forEach(Visitor::checkAggregation));
        }

        private static void checkAggregation(AggregationNode.Aggregation aggregation)
        {
            aggregation.getFilter().ifPresent(Visitor::checkNotOriginalExpression);
            aggregation.getArguments().forEach(Visitor::checkNotOriginalExpression);
        }

        private static void checkNotOriginalExpression(RowExpression expression)
        {
            checkArgument(!isExpression(expression), "Unexpected original expression %s", expression);
        }
    }
}
