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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Perform checks on the plan that may generate warnings or errors.
 */
public final class PlanChecker
{
    private final Multimap<Stage, Checker> checkers;
    private final PlanCheckerProviderManager planCheckerProviderManager;

    @Inject
    public PlanChecker(FeaturesConfig featuresConfig, PlanCheckerProviderManager planCheckerProviderManager)
    {
        this(featuresConfig, false, planCheckerProviderManager);
    }

    public PlanChecker(FeaturesConfig featuresConfig, boolean forceSingleNode, PlanCheckerProviderManager planCheckerProviderManager)
    {
        this.planCheckerProviderManager = requireNonNull(planCheckerProviderManager, "planCheckerProviderManager is null");
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
        checkers.get(Stage.FINAL).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
        for (PlanCheckerProvider provider : planCheckerProviderManager.getPlanCheckerProviders()) {
            for (com.facebook.presto.spi.plan.PlanChecker checker : provider.getFinalPlanCheckers()) {
                checker.validate(planNode, warningCollector);
            }
        }
    }

    public void validateIntermediatePlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
        for (PlanCheckerProvider provider : planCheckerProviderManager.getPlanCheckerProviders()) {
            for (com.facebook.presto.spi.plan.PlanChecker checker : provider.getIntermediatePlanCheckers()) {
                checker.validate(planNode, warningCollector);
            }
        }
    }

    public void validatePlanFragment(PlanFragment planFragment, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.FRAGMENT).forEach(checker -> checker.validateFragment(planFragment, session, metadata, warningCollector));
        for (PlanCheckerProvider provider : planCheckerProviderManager.getPlanCheckerProviders()) {
            for (com.facebook.presto.spi.plan.PlanChecker checker : provider.getFragmentPlanCheckers()) {
                checker.validateFragment(toSimplePlanFragment(planFragment), warningCollector);
            }
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

    private static SimplePlanFragment toSimplePlanFragment(PlanFragment planFragment)
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
