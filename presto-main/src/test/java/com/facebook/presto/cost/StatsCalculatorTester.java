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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.USE_NEW_STATS_CALCULATOR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class StatsCalculatorTester
{
    private final StatsCalculator statsCalculator;
    private final Metadata metadata;
    private final Session session;

    public StatsCalculatorTester()
    {
        this(createQueryRunner());
    }

    private StatsCalculatorTester(LocalQueryRunner queryRunner)
    {
        this.statsCalculator = queryRunner.getStatsCalculator();
        this.session = queryRunner.getDefaultSession();
        this.metadata = queryRunner.getMetadata();
    }

    private static LocalQueryRunner createQueryRunner()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(USE_NEW_STATS_CALCULATOR, "true")
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog(session.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
        return queryRunner;
    }

    public StatsCalculatorAssertion assertStatsFor(Function<PlanBuilder, PlanNode> planProvider)
    {
        return new StatsCalculatorAssertion(statsCalculator, metadata, session).on(planProvider);
    }
}
