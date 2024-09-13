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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.sanity.plancheckerprovidermanagers.PlanCheckerProviderManager;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Perform checks on the plan that may generate warnings or errors.
 */
public final class PlanChecker
{
    private final PlanCheckerProviderManager providerMgr;
    private Multimap<Stage, Checker> checkers;
    private boolean providedInitialized;

    @Inject
    public PlanChecker(FeaturesConfig featuresConfig, PlanCheckerProviderManager providerManager)
    {
        this(featuresConfig, false, providerManager);
    }

    public PlanChecker(FeaturesConfig featuresConfig, boolean forceSingleNode)
    {
        this(featuresConfig, forceSingleNode, null);
    }

    public PlanChecker(FeaturesConfig featuresConfig, boolean forceSingleNode, PlanCheckerProviderManager providerManager)
    {
        this.providerMgr = providerManager;
        ImmutableListMultimap.Builder<Stage, Checker> builder = ImmutableListMultimap.builder();
        builder.putAll(
                        Stage.INTERMEDIATE,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoUnresolvedSymbolExpression())
                .putAll(
                        Stage.FRAGMENT,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyNoFilteredAggregations(),
                        new VerifyNoIntermediateFormExpression(),
                        new ValidateStreamingJoins(featuresConfig))
                .putAll(
                        Stage.FINAL,
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
                        new WarnOnScanWithoutPartitionPredicate(featuresConfig));
        if (featuresConfig.isNativeExecutionEnabled() && (featuresConfig.isDisableTimeStampWithTimeZoneForNative() ||
                featuresConfig.isDisableIPAddressForNative())) {
            builder.put(Stage.INTERMEDIATE, new CheckUnsupportedPrestissimoTypes(featuresConfig));
        }
        checkers = builder.build();
    }

    public void validateFinalPlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        ensureProvidedInitialized();
        checkers.get(Stage.FINAL).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public void validateIntermediatePlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        ensureProvidedInitialized();
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public void validatePlanFragment(PlanFragment planFragment, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        ensureProvidedInitialized();
        checkers.get(Stage.FRAGMENT).forEach(checker -> checker.validateFragment(planFragment, session, metadata, warningCollector));
    }

    private static List<Checker> fromSpi(List<com.facebook.presto.spi.plan.PlanChecker> checkers)
    {
        return checkers.stream().map(SpiCheckerAdapter::new).collect(Collectors.toList());
    }

    private void ensureProvidedInitialized()
    {
        if (!providedInitialized) {
            if (providerMgr != null) {
                checkers = ImmutableListMultimap.<Stage, Checker>builder().putAll(checkers)
                        .putAll(
                                Stage.INTERMEDIATE,
                                fromSpi(providerMgr.getPlanCheckerProvider().getPlanCheckersIntermediate()))
                        .putAll(
                                Stage.FRAGMENT,
                                fromSpi(providerMgr.getPlanCheckerProvider().getPlanCheckersFragment()))
                        .putAll(
                                Stage.FINAL,
                                fromSpi(providerMgr.getPlanCheckerProvider().getPlanCheckersFinal()))
                        .build();
            }
            providedInitialized = true;
        }
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
