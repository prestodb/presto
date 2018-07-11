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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

/**
 * It is going to be executed to verify logical planner correctness
 */
public final class PlanSanityChecker
{
    public static final PlanSanityChecker DISTRIBUTED_PLAN_SANITY_CHECKER = new PlanSanityChecker(false);

    private final Multimap<Stage, Checker> checkers;

    public PlanSanityChecker(boolean forceSingleNode)
    {
        checkers = ImmutableListMultimap.<Stage, Checker>builder()
                .putAll(
                        Stage.INTERMEDIATE,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new NoSubqueryExpressionLeftChecker(),
                        new NoIdentifierLeftChecker(),
                        new VerifyOnlyOneOutputNode())
                .putAll(
                        Stage.FINAL,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new NoSubqueryExpressionLeftChecker(),
                        new NoIdentifierLeftChecker(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoFilteredAggregations(),
                        new ValidateAggregationsWithDefaultValues(forceSingleNode),
                        new ValidateStreamingAggregations())
                .build();
    }

    public void validateFinalPlan(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        checkers.get(Stage.FINAL).forEach(checker -> checker.validate(planNode, session, metadata, sqlParser, types));
    }

    public void validateIntermediatePlan(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, sqlParser, types));
    }

    public interface Checker
    {
        void validate(PlanNode planNode, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types);
    }

    private enum Stage
    {
        INTERMEDIATE, FINAL
    }
}
