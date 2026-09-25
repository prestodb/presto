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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.PARALLELIZE_CHAINED_AGGREGATION;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;

/**
 * Distributed-plan tests for {@link com.facebook.presto.sql.planner.iterative.rule.ParallelizeChainedAggregation}.
 * <p>
 * Builds the full distributed plan (multi-stage with REMOTE_STREAMING exchanges and PARTIAL/FINAL
 * splits) and inspects the result to confirm the rule fires by inserting a LOCAL ROUND_ROBIN
 * exchange between the outer PARTIAL and the chain leading to the inner FINAL.
 */
public class TestParallelizeChainedAggregationPlan
        extends BasePlanTest
{
    private static final String CHAINED_AGG_SQL =
            "SELECT sum(s) FROM (" +
                    "  SELECT sum(extendedprice) AS s, linestatus, returnflag, shipmode " +
                    "  FROM lineitem GROUP BY linestatus, returnflag, shipmode" +
                    ") GROUP BY linestatus, returnflag";

    @Test
    public void testRuleInsertsLocalRoundRobinExchange()
    {
        // The rule must add exactly one extra LOCAL REPARTITION exchange (the round-robin) above
        // the outer PARTIAL to parallelize it across local drivers.
        long enabledLocalRepartitions = countLocalRepartitionExchanges(distributedPlan(CHAINED_AGG_SQL, sessionWith(true)));
        long disabledLocalRepartitions = countLocalRepartitionExchanges(distributedPlan(CHAINED_AGG_SQL, sessionWith(false)));
        assertEquals(enabledLocalRepartitions, disabledLocalRepartitions + 1,
                "Enabling the rule should insert exactly one local round-robin (LOCAL REPARTITION) exchange. " +
                        "enabled=" + enabledLocalRepartitions + ", disabled=" + disabledLocalRepartitions);
    }

    @Test
    public void testAggregationStepsUnchanged()
    {
        // The rule must not change any aggregation steps — inner stays FINAL, outer stays PARTIAL.
        List<AggregationNode.Step> enabled = aggregationSteps(distributedPlan(CHAINED_AGG_SQL, sessionWith(true)));
        List<AggregationNode.Step> disabled = aggregationSteps(distributedPlan(CHAINED_AGG_SQL, sessionWith(false)));
        assertEquals(enabled.stream().filter(s -> s == FINAL).count(),
                disabled.stream().filter(s -> s == FINAL).count(),
                "FINAL step count must be unchanged. enabled=" + enabled + ", disabled=" + disabled);
        assertEquals(enabled.stream().filter(s -> s == PARTIAL).count(),
                disabled.stream().filter(s -> s == PARTIAL).count(),
                "PARTIAL step count must be unchanged. enabled=" + enabled + ", disabled=" + disabled);
    }

    private Session sessionWith(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PARALLELIZE_CHAINED_AGGREGATION, Boolean.toString(enabled))
                .build();
    }

    private static long countLocalRepartitionExchanges(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode
                        && ((ExchangeNode) node).getScope() == LOCAL
                        && ((ExchangeNode) node).getType() == REPARTITION)
                .findAll()
                .size();
    }

    private Plan distributedPlan(String sql, Session session)
    {
        return plan(session, sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
    }

    private static List<AggregationNode.Step> aggregationSteps(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof AggregationNode)
                .findAll()
                .stream()
                .map(node -> ((AggregationNode) node).getStep())
                .collect(Collectors.toList());
    }
}
