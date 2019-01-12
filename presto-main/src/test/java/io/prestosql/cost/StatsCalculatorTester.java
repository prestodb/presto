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
package io.prestosql.cost;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.testing.LocalQueryRunner;

import java.util.function.Function;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class StatsCalculatorTester
        implements AutoCloseable
{
    private final StatsCalculator statsCalculator;
    private final Metadata metadata;
    private final Session session;
    private final LocalQueryRunner queryRunner;

    public StatsCalculatorTester()
    {
        this(testSessionBuilder().build());
    }

    public StatsCalculatorTester(Session session)
    {
        this(createQueryRunner(session));
    }

    private StatsCalculatorTester(LocalQueryRunner queryRunner)
    {
        this.statsCalculator = queryRunner.getStatsCalculator();
        this.session = queryRunner.getDefaultSession();
        this.metadata = queryRunner.getMetadata();
        this.queryRunner = queryRunner;
    }

    private static LocalQueryRunner createQueryRunner(Session session)
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(session);
        queryRunner.createCatalog(session.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    public StatsCalculatorAssertion assertStatsFor(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        PlanNode planNode = planProvider.apply(planBuilder);
        return new StatsCalculatorAssertion(statsCalculator, session, planNode, planBuilder.getTypes());
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
