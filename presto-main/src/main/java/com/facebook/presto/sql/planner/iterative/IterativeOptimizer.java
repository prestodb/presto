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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private final List<PlanOptimizer> legacyRules;
    private final Set<Rule> rules;
    private final StatsRecorder stats;

    public IterativeOptimizer(StatsRecorder stats, Set<Rule> rules)
    {
        this(stats, ImmutableList.of(), rules);
    }

    public IterativeOptimizer(StatsRecorder stats, List<PlanOptimizer> legacyRules, Set<Rule> newRules)
    {
        this.legacyRules = ImmutableList.copyOf(legacyRules);
        this.rules = ImmutableSet.copyOf(newRules);
        this.stats = stats;

        stats.registerAll(rules);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        // only disable new rules if we have legacy rules to fall back to
        if (!SystemSessionProperties.isNewOptimizerEnabled(session) && !legacyRules.isEmpty()) {
            for (PlanOptimizer optimizer : legacyRules) {
                plan = optimizer.optimize(plan, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator);
            }

            return plan;
        }

        Memo memo = new Memo(idAllocator, plan);

        Lookup lookup = node -> {
            if (node instanceof GroupReference) {
                return memo.getNode(((GroupReference) node).getGroupId());
            }

            return node;
        };

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        exploreGroup(memo.getRootGroup(), new Context(memo, lookup, idAllocator, symbolAllocator, System.nanoTime(), timeout.toMillis(), session));

        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context);

        while (exploreChildren(group, context)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context)
    {
        PlanNode node = context.getMemo().getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            if (isTimeLimitExhausted(context)) {
                throw new PrestoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", context.getTimeoutInMilliseconds()));
            }

            done = true;
            for (Rule rule : rules) {
                Optional<PlanNode> transformed;
                long duration;
                try {
                    long start = System.nanoTime();
                    transformed = rule.apply(node, context.getLookup(), context.getIdAllocator(), context.getSymbolAllocator(), context.getSession());
                    duration = System.nanoTime() - start;
                }
                catch (RuntimeException e) {
                    stats.recordFailure(rule);
                    throw e;
                }
                stats.record(rule, duration, transformed.isPresent());

                if (transformed.isPresent()) {
                    node = context.getMemo().replace(group, transformed.get(), rule.getClass().getName());

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private boolean isTimeLimitExhausted(Context context)
    {
        return ((System.nanoTime() - context.getStartTimeInNanos()) / 1_000_000) >= context.getTimeoutInMilliseconds();
    }

    private boolean exploreChildren(int group, Context context)
    {
        boolean progress = false;

        PlanNode expression = context.getMemo().getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context)) {
                progress = true;
            }
        }

        return progress;
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session)
        {
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.lookup = lookup;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.session = session;
        }

        public Memo getMemo()
        {
            return memo;
        }

        public Lookup getLookup()
        {
            return lookup;
        }

        public PlanNodeIdAllocator getIdAllocator()
        {
            return idAllocator;
        }

        public SymbolAllocator getSymbolAllocator()
        {
            return symbolAllocator;
        }

        public long getStartTimeInNanos()
        {
            return startTimeInNanos;
        }

        public long getTimeoutInMilliseconds()
        {
            return timeoutInMilliseconds;
        }

        public Session getSession()
        {
            return session;
        }
    }
}
