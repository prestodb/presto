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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.rule.RowExpressionRewriteRuleSet;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getOptimizersToEnableVerboseRuntimeStats;
import static com.facebook.presto.SystemSessionProperties.isVerboseOptimizerInfoEnabled;
import static com.facebook.presto.SystemSessionProperties.isVerboseRuntimeStatsEnabled;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final List<PlanOptimizer> legacyRules;
    private final RuleIndex ruleIndex;
    private final Optional<LogicalPropertiesProvider> logicalPropertiesProvider;

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> rules)
    {
        this(metadata, stats, statsCalculator, costCalculator, ImmutableList.of(), Optional.empty(), rules);
    }

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, Optional<LogicalPropertiesProvider> logicalPropertiesProvider, Set<Rule<?>> rules)
    {
        this(metadata, stats, statsCalculator, costCalculator, ImmutableList.of(), logicalPropertiesProvider, rules);
    }

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, List<PlanOptimizer> legacyRules, Set<Rule<?>> newRules)
    {
        this(metadata, stats, statsCalculator, costCalculator, legacyRules, Optional.empty(), newRules);
    }

    public IterativeOptimizer(Metadata metadata, RuleStatsRecorder stats, StatsCalculator statsCalculator, CostCalculator costCalculator, List<PlanOptimizer> legacyRules, Optional<LogicalPropertiesProvider> logicalPropertiesProvider, Set<Rule<?>> newRules)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.legacyRules = ImmutableList.copyOf(legacyRules);
        this.ruleIndex = RuleIndex.builder()
                .register(newRules)
                .build();
        this.logicalPropertiesProvider = requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider is null");

        stats.registerAll(newRules);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        // only disable new rules if we have legacy rules to fall back to
        if (!SystemSessionProperties.isNewOptimizerEnabled(session) && !legacyRules.isEmpty()) {
            boolean planChanged = false;
            for (PlanOptimizer optimizer : legacyRules) {
                PlanOptimizerResult planOptimizerResult = optimizer.optimize(plan, session, TypeProvider.viewOf(variableAllocator.getVariables()), variableAllocator, idAllocator, warningCollector);
                plan = planOptimizerResult.getPlanNode();
                planChanged = planChanged || planOptimizerResult.isOptimizerTriggered();
            }

            return PlanOptimizerResult.optimizerResult(plan, planChanged);
        }

        Memo memo;
        if (SystemSessionProperties.isExploitConstraints(session)) {
            memo = new Memo(idAllocator, plan, logicalPropertiesProvider);
        }
        else {
            memo = new Memo(idAllocator, plan, Optional.empty());
        }

        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Matcher matcher = new PlanNodeMatcher(lookup);

        Duration timeout = SystemSessionProperties.getOptimizerTimeout(session);
        StatsProvider statsProvider = new CachingStatsProvider(
                statsCalculator,
                Optional.of(memo),
                lookup,
                session,
                TypeProvider.viewOf(variableAllocator.getVariables()));
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(memo), session);
        Context context = new Context(memo, lookup, idAllocator, variableAllocator, System.nanoTime(), timeout.toMillis(), session, warningCollector, costProvider, statsProvider, metadata, types);
        boolean planChanged = exploreGroup(memo.getRootGroup(), context, matcher);
        context.collectOptimizerInformation();
        if (!planChanged) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        return PlanOptimizerResult.optimizerResult(memo.extract(), true);
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
                    if (isVerboseOptimizerInfoEnabled(context.session) && isApplicable(node, rule, matcher, context)) {
                        context.addRulesApplicable(getNameOfOptimizerRule(rule));
                    }
                    continue;
                }

                Rule.Result result = transform(node, rule, matcher, context);

                if (result.getTransformedPlan().isPresent()) {
                    // If we rewrite a plan node, topmost node should remain statistically equivalent.
                    PlanNode transformedNode = result.getTransformedPlan().get();
                    PlanNode resolvedtransformedNode = context.lookup.resolve(result.getTransformedPlan().get());
                    if (node.getStatsEquivalentPlanNode().isPresent() && !resolvedtransformedNode.getStatsEquivalentPlanNode().isPresent()) {
                        if (transformedNode instanceof GroupReference) {
                            context.memo.assignStatsEquivalentPlanNode((GroupReference) transformedNode, node.getStatsEquivalentPlanNode());
                        }
                        else {
                            transformedNode = transformedNode.assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode());
                        }
                    }
                    context.addRulesTriggered(getNameOfOptimizerRule(rule), node, transformedNode, rule.isCostBased(context.session), rule.getStatsSource());
                    node = context.memo.replace(group, transformedNode, rule.getClass().getName());

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private String getNameOfOptimizerRule(Rule<?> rule)
    {
        String ruleName = rule.getClass().getSimpleName();
        if (rule instanceof RowExpressionRewriteRuleSet.RowExpressionRewriteRule) {
            ruleName = ((RowExpressionRewriteRuleSet.RowExpressionRewriteRule) rule).getOptimizerNameForLog();
        }
        return ruleName;
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
        if (isVerboseRuntimeStatsEnabled(context.session) || trackOptimizerRuntime(context.session, rule)) {
            context.session.getRuntimeStats().addMetricValue(String.format("rule%sTimeNanos", getNameOfOptimizerRule(rule)), NANO, duration);
        }

        return result;
    }

    private boolean trackOptimizerRuntime(Session session, Rule rule)
    {
        String optimizerString = getOptimizersToEnableVerboseRuntimeStats(session);
        if (optimizerString.isEmpty()) {
            return false;
        }
        List<String> optimizers = Splitter.on(",").trimResults().splitToList(optimizerString);
        return optimizers.contains(getNameOfOptimizerRule(rule));
    }

    private <T> boolean isApplicable(PlanNode node, Rule<T> rule, Matcher matcher, Context context)
    {
        Match<T> match = matcher.match(rule.getPattern(), node);
        if (match.isEmpty()) {
            return false;
        }

        Rule.Result result = rule.apply(match.value(), match.captures(), ruleContext(context));
        return !result.isEmpty();
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
            public VariableAllocator getVariableAllocator()
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
                return context.statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return context.costProvider;
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

            @Override
            public Optional<LogicalPropertiesProvider> getLogicalPropertiesProvider()
            {
                return logicalPropertiesProvider;
            }
        };
    }

    private static class RuleTriggered
    {
        private final String rule;
        private final Optional<String> oldNode;
        private final Optional<String> newNode;
        private boolean isCostBased;
        private final Optional<String> statsSource;

        public RuleTriggered(String rule, Optional<String> oldNode, Optional<String> newNode, boolean isCostBased, String statsSource)
        {
            this.rule = requireNonNull(rule, "rule is null");
            this.oldNode = requireNonNull(oldNode, "oldNode is null");
            this.newNode = requireNonNull(newNode, "newNode is null");
            this.isCostBased = isCostBased;
            this.statsSource = statsSource == null ? Optional.empty() : Optional.of(statsSource);
        }

        public String getRule()
        {
            return rule;
        }

        public Optional<String> getOldNode()
        {
            return oldNode;
        }

        public Optional<String> getNewNode()
        {
            return newNode;
        }

        public boolean isCostBased()
        {
            return isCostBased;
        }

        public Optional<String> getStatsSource()
        {
            return statsSource;
        }
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final long startTimeInNanos;
        private final long timeoutInMilliseconds;
        private final Session session;
        private final WarningCollector warningCollector;
        private final CostProvider costProvider;
        private final StatsProvider statsProvider;
        private final Set<RuleTriggered> rulesTriggered;
        private final Set<String> rulesApplicable;
        private final Metadata metadata;
        private final TypeProvider types;

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator,
                long startTimeInNanos,
                long timeoutInMilliseconds,
                Session session,
                WarningCollector warningCollector,
                CostProvider costProvider,
                StatsProvider statsProvider,
                Metadata metadata,
                TypeProvider types)
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
            this.costProvider = costProvider;
            this.statsProvider = statsProvider;
            this.metadata = metadata;
            this.types = types;
            this.rulesTriggered = new HashSet<>();
            this.rulesApplicable = new HashSet<>();
        }

        public void checkTimeoutNotExhausted()
        {
            if ((NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos)) >= timeoutInMilliseconds) {
                throw new PrestoException(OPTIMIZER_TIMEOUT, format("The optimizer exhausted the time limit of %d ms", timeoutInMilliseconds));
            }
        }

        public void addRulesTriggered(String rule, PlanNode oldNode, PlanNode newNode, boolean isCostBased, String statsSource)
        {
            Optional<String> before = Optional.empty();
            Optional<String> after = Optional.empty();

            if (SystemSessionProperties.isVerboseOptimizerResults(session, rule)) {
                before = Optional.of(PlannerUtils.getPlanString(oldNode, session, types, metadata, false));
                after = Optional.of(PlannerUtils.getPlanString(newNode, session, types, metadata, false));
            }

            rulesTriggered.add(new RuleTriggered(rule, before, after, isCostBased, statsSource));
        }

        public void addRulesApplicable(String rule)
        {
            rulesApplicable.add(rule);
        }

        public void collectOptimizerInformation()
        {
            rulesTriggered.stream().map(
                    x -> new PlanOptimizerInformation(x.getRule(), true, Optional.empty(), Optional.empty(), Optional.of(x.isCostBased()), x.getStatsSource()))
                    .distinct().forEach(rule -> session.getOptimizerInformationCollector().addInformation(rule));

            if (SystemSessionProperties.isVerboseOptimizerResults(session)) {
                rulesTriggered.stream().filter(x -> x.getNewNode().isPresent()).forEach(x -> session.getOptimizerResultCollector().addOptimizerResult(x.getRule(), x.getOldNode().get(), x.getNewNode().get()));
            }
            rulesApplicable.forEach(x -> session.getOptimizerInformationCollector().addInformation(
                    new PlanOptimizerInformation(x, false, Optional.of(true), Optional.empty(), Optional.empty(), Optional.empty())));
        }
    }
}
