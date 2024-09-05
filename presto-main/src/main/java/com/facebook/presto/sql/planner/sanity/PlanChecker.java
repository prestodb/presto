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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import javax.inject.Inject;

/**
 * Perform checks on the plan that may generate warnings or errors.
 */
public final class PlanChecker
{
    private final Multimap<Stage, Checker> checkers;

    @Inject
    public PlanChecker(FeaturesConfig featuresConfig)
    {
        this(featuresConfig, false);
    }

    public PlanChecker(FeaturesConfig featuresConfig, boolean forceSingleNode)
    {
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
    }

    public void validateIntermediatePlan(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public void validatePlanFragment(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector)
    {
        checkers.get(Stage.FRAGMENT).forEach(checker -> checker.validate(planNode, session, metadata, warningCollector));
    }

    public interface Checker
    {
        void validate(PlanNode planNode, Session session, Metadata metadata, WarningCollector warningCollector);
    }

    private enum Stage
    {
        INTERMEDIATE, FINAL, FRAGMENT
    }
}
