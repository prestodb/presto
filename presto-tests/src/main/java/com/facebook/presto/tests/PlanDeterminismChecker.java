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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.testing.LocalQueryRunner;

import java.util.function.Function;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

public class PlanDeterminismChecker
{
    private static final int MINIMUM_SUBSEQUENT_SAME_PLANS = 10;

    private final LocalQueryRunner localQueryRunner;
    private final Function<String, String> planEquivalenceFunction;

    public PlanDeterminismChecker(LocalQueryRunner localQueryRunner)
    {
        this(localQueryRunner, Function.identity());
    }

    public PlanDeterminismChecker(LocalQueryRunner localQueryRunner, Function<String, String> planEquivalenceFunction)
    {
        this.localQueryRunner = localQueryRunner;
        this.planEquivalenceFunction = planEquivalenceFunction;
    }

    public void checkPlanIsDeterministic(String sql)
    {
        checkPlanIsDeterministic(localQueryRunner.getDefaultSession(), sql);
    }

    public void checkPlanIsDeterministic(Session session, String sql)
    {
        IntStream.range(1, MINIMUM_SUBSEQUENT_SAME_PLANS)
                .mapToObj(attempt -> getPlanText(session, sql))
                .map(planEquivalenceFunction)
                .reduce((previous, current) -> {
                    assertEquals(previous, current);
                    return current;
                });
    }

    private String getPlanText(Session session, String sql)
    {
        return localQueryRunner.inTransaction(session, transactionSession -> {
            Plan plan = localQueryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
            return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), localQueryRunner.getMetadata(), transactionSession);
        });
    }
}
