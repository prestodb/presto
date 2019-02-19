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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.ConnectorOptimizationRuleManager;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorOptimizationRule;
import com.facebook.presto.spi.relation.TableExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.connector.PlanNodeToTableExpressionTranslator;
import com.facebook.presto.sql.planner.iterative.connector.TableExpressionToPlanNodeTranslator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConnectorOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser parser;
    private final ConnectorOptimizationRuleManager ruleManager;
    private final LiteralEncoder literalEncoder;

    public ConnectorOptimizer(Metadata metadata, SqlParser parser, ConnectorOptimizationRuleManager ruleManager, LiteralEncoder literalEncoder)
    {
        this.metadata = metadata;
        this.parser = parser;
        this.ruleManager = ruleManager;
        this.literalEncoder = literalEncoder;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Memo memo = new Memo(idAllocator, plan, ImmutableSet.of(new ConnectorTraitCollector()));

        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        PlanNodeToTableExpressionTranslator planNodeToTableExpressionTranslator = new PlanNodeToTableExpressionTranslator(metadata, symbolAllocator.getTypes(), session, parser, lookup);
        TableExpressionToPlanNodeTranslator tableExpressionToPlanNodeTranslator = new TableExpressionToPlanNodeTranslator(idAllocator, symbolAllocator, literalEncoder, metadata);
        Context context = new Context(memo, System.nanoTime(), timeout.toMillis(), session, planNodeToTableExpressionTranslator, tableExpressionToPlanNodeTranslator);
        exploreGroup(memo.getRootGroup(), context);

        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context)
    {
        boolean progress = false;
        boolean done = false;

        while (!done) {
            done = true;
            context.checkTimeoutNotExhausted();
            // apply root to bottom of tree first
            while (exploreChildren(group, context)) {
                done = false;
                progress = true;
            }
            if (exploreNode(group, context)) {
                done = false;
                progress = true;
            }
        }

        return progress;
    }

    private boolean exploreChildren(int group, Context context)
    {
        boolean progress = false;

        PlanNode expression = context.memo.getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context)) {
                progress = true;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context)
    {
        PlanNode node = context.memo.getNode(group);
        Set<ConnectorId> connectors = context.memo.getTraitGroup(group).getTraitSet(ConnectorId.class);
        if (connectors.size() != 1) {
            return false;
        }
        ConnectorId connectorId = getOnlyElement(connectors);
        Optional<TableExpression> tableExpression = context.planNodeToTableExpressionTranslator.translate(node);
        if (!tableExpression.isPresent()) {
            return false;
        }
        ConnectorSession connectorSession = context.session.toConnectorSession(connectorId);
        boolean progress = false;
        boolean done = false;

        while (!done) {
            done = true;
            for (ConnectorOptimizationRule rule : ruleManager.getRules(connectorId)) {
                context.checkTimeoutNotExhausted();
                if (rule.enabled(connectorSession) && rule.match(connectorSession, tableExpression.get())) {
                    Optional<TableExpression> rewritten = rule.optimize(connectorSession, tableExpression.get());
                    if (rewritten.isPresent()) {
                        Optional<PlanNode> rewrittenPlanNode = context.tableExpressionToPlanNodeTranslator.translate(context.session, connectorId, rewritten.get(), node.getOutputSymbols());
                        checkArgument(rewrittenPlanNode.isPresent(), "Rewrite rule should return something that can be converted back to plan");
                        node = context.memo.replace(group, rewrittenPlanNode.get(), format("%s - %s", connectorId, rule.getClass().getName()));
                        tableExpression = rewritten;
                        done = false;
                        progress = true;
                    }
                }
            }
        }

        return progress;
    }

    private static class Context
    {
        private final Memo memo;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;
        private final PlanNodeToTableExpressionTranslator planNodeToTableExpressionTranslator;
        private final TableExpressionToPlanNodeTranslator tableExpressionToPlanNodeTranslator;

        public Context(
                Memo memo,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                PlanNodeToTableExpressionTranslator planNodeToTableExpressionTranslator,
                TableExpressionToPlanNodeTranslator tableExpressionToPlanNodeTranslator)
        {
            this.planNodeToTableExpressionTranslator = planNodeToTableExpressionTranslator;
            this.tableExpressionToPlanNodeTranslator = tableExpressionToPlanNodeTranslator;
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.session = session;
        }

        public void checkTimeoutNotExhausted()
        {
            if ((NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                throw new PrestoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds));
            }
        }
    }

    private static class ConnectorTraitCollector
            implements Memo.TraitCollector
    {
        @Override
        public boolean canApplyTo(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public Memo.TraitGroup exploreTraits(PlanNode node)
        {
            return Memo.TraitGroup.emptyTraitGroup()
                    .addTrait(((TableScanNode) node).getTable().getConnectorId());
        }
    }
}
