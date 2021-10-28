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
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Matcher;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final List<PlanOptimizer> legacyRules;
    private final RuleIndex ruleIndex;

    public IterativeOptimizer(RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> rules)
    {
        this(stats, statsCalculator, costCalculator, ImmutableList.of(), rules);
    }

    public IterativeOptimizer(RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.legacyRules = ImmutableList.copyOf(legacyRules);
        this.ruleIndex = RuleIndex.builder()
                .register(newRules)
                .build();

        stats.registerAll(newRules);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        // only disable new rules if we have legacy rules to fall back to
        if (!SystemSessionProperties.isNewOptimizerEnabled(session) && !legacyRules.isEmpty()) {
            for (PlanOptimizer optimizer : legacyRules) {
                plan = optimizer.optimize(plan, session, variableAllocator.getTypes(), variableAllocator, idAllocator, warningCollector);
            }

            return plan;
        }

        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Matcher matcher = new PlanNodeMatcher(lookup);

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        Context context = new Context(memo, lookup, idAllocator, variableAllocator, System.nanoTime(), timeout.toMillis(), session, warningCollector);
        boolean planChanged = exploreGroup(memo.getRootGroup(), context, matcher);
        if (!planChanged) {
            return plan;
        }
        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context, Matcher matcher)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context, matcher);

        while (exploreChildren(group, context, matcher)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context, matcher)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context, Matcher matcher)
    {
        PlanNode node = context.memo.getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            context.checkTimeoutNotExhausted();

            done = true;
            Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
            while (possiblyMatchingRules.hasNext()) {
                Rule<?> rule = possiblyMatchingRules.next();

                if (!rule.isEnabled(context.session)) {
                    continue;
                }

                Rule.Result result = transform(node, rule, matcher, context);

                if (result.getTransformedPlan().isPresent()) {
                    node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private <T> Rule.Result transform(PlanNode node, Rule<T> rule, Matcher matcher, Context context)
    {
        Rule.Result result;

        Match<T> match = matcher.match(rule.getPattern(), node);

        if (match.isEmpty()) {
            return Rule.Result.empty();
        }

        long duration;
        try {
            long start = System.nanoTime();
            result = rule.apply(match.value(), match.captures(), ruleContext(context));
            duration = System.nanoTime() - start;
        }
        catch (RuntimeException e) {
            stats.recordFailure(rule);
            throw e;
        }
        stats.record(rule, duration, !result.isEmpty());
        if (SystemSessionProperties.isVerboseRuntimeStatsEnabled(context.session)) {
            context.session.getRuntimeStats().addMetricValue(String.format("rule%sTimeNanos", rule.getClass().getSimpleName()), duration);
        }

        return result;
    }

    private boolean exploreChildren(int group, Context context, Matcher matcher)
    {
        boolean progress = false;

        PlanNode expression = context.memo.getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context, matcher)) {
                progress = true;
            }
        }

        return progress;
    }

    private Rule.Context ruleContext(Context context)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(context.memo), context.lookup, context.session, context.variableAllocator.getTypes());
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(context.memo), context.session);

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return context.lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return context.idAllocator;
            }

            @Override
            public PlanVariableAllocator getVariableAllocator()
            {
                return context.variableAllocator;
            }

            @Override
            public Session getSession()
            {
                return context.session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                context.checkTimeoutNotExhausted();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                return context.warningCollector;
            }
        };
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;
        private final WarningCollector warningCollector;

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                PlanVariableAllocator variableAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                WarningCollector warningCollector)
        {
            checkArgument(timeoutInMilliseconds >= 0, "Timeout has to be a non-negative number [milliseconds]");

            this.memo = memo;
            this.lookup = lookup;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.startTimeInNanos = startTimeInNanos;
            this.timeoutInMilliseconds = timeoutInMilliseconds;
            this.session = session;
            this.warningCollector = warningCollector;
        }

        public void checkTimeoutNotExhausted()
        {
            if ((NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                throw new PrestoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds));
            }
        }
    }
}
