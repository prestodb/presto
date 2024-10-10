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
import com.facebook.presto.spi.plan.PlanCheckerProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.checkerframework.checker.lock.qual.GuardedBy;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Perform checks on the plan that may generate warnings or errors.
 */
public final class PlanChecker
{
    @GuardedBy("this")
    private volatile Multimap<Stage, Checker> checkers;

    @Inject
    public PlanChecker(FeaturesConfig featuresConfig)
    {
        this(featuresConfig, false);
    }

    public PlanChecker(FeaturesConfig featuresConfig, boolean forceSingleNode)
    {
        this.checkers = HashMultimap.create();

        checkers.putAll(
                Stage.INTERMEDIATE,
                Arrays.asList(
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoUnresolvedSymbolExpression()));
        checkers.putAll(
                Stage.FRAGMENT,
                Arrays.asList(
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyNoFilteredAggregations(),
                        new VerifyNoIntermediateFormExpression(),
                        new ValidateStreamingJoins(featuresConfig)));
        checkers.putAll(
                Stage.FINAL,
                Arrays.asList(
                        new CheckUnsupportedExternalFunctions(),
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoFilteredAggregations(),
                        new ValidateAggregationsWithDefaultValues(forceSingleNode),
                        new ValidateStreamingAggregations(),
                        new VerifyNoIntermediateFormExpression(),
                        new VerifyProjectionLocality(),
                        new DynamicFiltersChecker(),
                        new WarnOnScanWithoutPartitionPredicate(featuresConfig)));
        if (featuresConfig.isNativeExecutionEnabled() && (featuresConfig.isDisableTimeStampWithTimeZoneForNative() ||
                featuresConfig.isDisableIPAddressForNative())) {
            checkers.put(Stage.INTERMEDIATE, new CheckUnsupportedPrestissimoTypes(featuresConfig));
        }
    }

    public synchronized void update(PlanCheckerProvider provider)
    {
        checkers.putAll(Stage.INTERMEDIATE, fromSpi(provider.getPlanCheckersIntermediate()));
        checkers.putAll(Stage.FRAGMENT, fromSpi(provider.getPlanCheckersFragment()));
        checkers.putAll(Stage.FINAL, fromSpi(provider.getPlanCheckersFinal()));
    }

    public synchronized void validateFinalPlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.FINAL).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public synchronized void validateIntermediatePlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public synchronized void validatePlanFragment(PlanFragment planFragment, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.FRAGMENT).forEach(checker -> checker.validateFragment(planFragment, session, metadata, warningCollector));
    }

    private static List<Checker> fromSpi(List<com.facebook.presto.spi.plan.PlanChecker> checkers)
    {
        return checkers.stream().map(SpiCheckerAdapter::new).collect(Collectors.toList());
    }

    public interface Checker
    {
        void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector);

        default void validateFragment(PlanFragment planFragment, Session session, Metadata metadata, WarningCollector warningCollector)
        {
            validate(planFragment.getRoot(), session, metadata, warningCollector);
        }
    }

    private enum Stage
    {
        INTERMEDIATE, FINAL, FRAGMENT
    }

    private static class SpiCheckerAdapter
            implements Checker
    {
        private final com.facebook.presto.spi.plan.PlanChecker checker;

        public SpiCheckerAdapter(com.facebook.presto.spi.plan.PlanChecker checker)
        {
            this.checker = checker;
        }

        @Override
        public void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
        {
            checker.validate(planNode, warningCollector);
        }

        @Override
        public void validateFragment(PlanFragment planFragment, Session session, Metadata metadata, WarningCollector warningCollector)
        {
            checker.validateFragment(toSimplePlanFragment(planFragment), warningCollector);
        }

        private SimplePlanFragment toSimplePlanFragment(PlanFragment planFragment)
        {
            return new SimplePlanFragment(
                    planFragment.getId(),
                    planFragment.getRoot(),
                    planFragment.getVariables(),
                    planFragment.getPartitioning(),
                    planFragment.getTableScanSchedulingOrder(),
                    planFragment.getPartitioningScheme(),
                    planFragment.getStageExecutionDescriptor(),
                    planFragment.isOutputTableWriterFragment());
        }
    }
}
