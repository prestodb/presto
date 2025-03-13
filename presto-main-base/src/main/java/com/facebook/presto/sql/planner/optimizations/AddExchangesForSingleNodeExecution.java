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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isSingleNodeExecutionEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.PlannerUtils.containsSystemTableScan;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.iterative.rule.PickTableLayout.pushPredicateIntoTableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static java.util.Objects.requireNonNull;

public class AddExchangesForSingleNodeExecution
        implements PlanOptimizer
{
    private final Metadata metadata;

    public AddExchangesForSingleNodeExecution(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isSingleNodeExecutionEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            AddExchangesForSingleNodeExecution.Rewriter rewriter = new AddExchangesForSingleNodeExecution.Rewriter(idAllocator, metadata, session);
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private boolean planChanged;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            PlanNode plan = pushPredicateIntoTableScan(node, new ConstantExpression(true, BOOLEAN), true, session, idAllocator, metadata);
            // Presto Java and Presto Native use different hash functions for partitioning
            // An additional exchange makes sure the data flows through a native worker in case it need to be partitioned for downstream processing
            if (containsSystemTableScan(plan)) {
                plan = gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, plan);
            }
            return plan;
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Void> context)
        {
            return addGatherExchange(node, context);
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Void> context)
        {
            return addGatherExchange(node, context);
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Void> context)
        {
            return addGatherExchange(node, context);
        }

        private PlanNode addGatherExchange(PlanNode node, RewriteContext<Void> context)
        {
            PlanNode child = node.getSources().get(0).accept(this, context);

            ExchangeNode gather;
            // In case the child is an exchange, don't add another exchange. Instead, convert it to gather exchange.
            if (child instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) child;
                gather = new ExchangeNode(
                        exchangeNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        GATHER,
                        REMOTE_STREAMING,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), exchangeNode.getOutputVariables()),
                        exchangeNode.getSources(),
                        exchangeNode.getInputs(),
                        true,
                        Optional.empty());
            }
            else {
                gather = gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child);
            }
            planChanged = true;
            return ChildReplacer.replaceChildren(node, ImmutableList.of(gather));
        }
    }
}
