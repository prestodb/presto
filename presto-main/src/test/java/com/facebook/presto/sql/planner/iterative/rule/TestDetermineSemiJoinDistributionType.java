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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;

@Test(singleThreaded = true)
public class TestDetermineSemiJoinDistributionType
        extends BaseRuleTest
{
    @Test
    public void testDetermineDistributionType()
    {
        testDetermineDistributionType(true, PARTITIONED);
        testDetermineDistributionType(false, REPLICATED);
    }

    private void testDetermineDistributionType(boolean sessionDistributedJoin, DistributionType expectedDistribution)
    {
        tester().assertThat(new DetermineSemiJoinDistributionType())
                .on(p ->
                        p.semiJoin(
                                p.values(p.symbol("A1", BIGINT)),
                                p.values(p.symbol("B1", BIGINT)),
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("SJO", BIGINT),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .setSystemProperty(DISTRIBUTED_JOIN, Boolean.toString(sessionDistributedJoin))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "SJO",
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0)),
                        Optional.of(expectedDistribution)));
    }

    @Test
    public void testRetainDistributionType()
    {
        tester().assertThat(new DetermineSemiJoinDistributionType())
                .on(p ->
                        p.semiJoin(
                                p.values(p.symbol("A1", BIGINT)),
                                p.values(p.symbol("B1", BIGINT)),
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("SJO", BIGINT),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED)))
                .doesNotFire();
    }
}
