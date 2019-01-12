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

package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpcds.TpcdsConnectorFactory;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.testing.LocalQueryRunner;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCDS queries.
 * This class is using TPCDS connector configured in way to mock Hive connector with unpartitioned TPCDS tables.
 */
public class TestTpcdsCostBasedPlan
        extends AbstractCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpcdsCostBasedPlan()
    {
        super(() -> {
            String catalog = "local";
            Session.SessionBuilder sessionBuilder = testSessionBuilder()
                    .setCatalog(catalog)
                    .setSchema("sf3000.0")
                    .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name());

            LocalQueryRunner queryRunner = LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(sessionBuilder.build(), 8);
            queryRunner.createCatalog(
                    catalog,
                    new TpcdsConnectorFactory(1),
                    ImmutableMap.of());
            return queryRunner;
        });
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return IntStream.range(1, 100)
                .boxed()
                .flatMap(i -> {
                    String queryId = format("q%02d", i);
                    if (i == 14 || i == 23 || i == 24 || i == 39) {
                        return Stream.of(queryId + "_1", queryId + "_2");
                    }
                    return Stream.of(queryId);
                })
                .map(queryId -> format("/sql/presto/tpcds/%s.sql", queryId));
    }

    @SuppressWarnings("unused")
    public static final class UpdateTestFiles
    {
        // Intellij doesn't handle well situation when test class has main(), hence inner class.

        private UpdateTestFiles() {}

        public static void main(String[] args)
                throws Exception
        {
            new TestTpcdsCostBasedPlan().generate();
        }
    }
}
