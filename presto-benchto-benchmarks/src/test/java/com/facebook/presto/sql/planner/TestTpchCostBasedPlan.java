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

package com.facebook.presto.sql.planner;

import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.HANDLE_COMPLEX_EQUI_JOINS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
import static java.lang.String.format;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPCH queries.
 * This class is using TPCH connector configured in way to mock Hive connector with unpartitioned TPCH tables.
 */
public class TestTpchCostBasedPlan
        extends AbstractCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public TestTpchCostBasedPlan()
    {
        super(() -> {
            String catalog = "local";
            SessionBuilder sessionBuilder = testSessionBuilder()
                    .setCatalog(catalog)
                    .setSchema("sf3000.0")
                    .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                    .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                    .setSystemProperty(HANDLE_COMPLEX_EQUI_JOINS, "true");

            LocalQueryRunner queryRunner = LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(sessionBuilder.build(), 8);
            queryRunner.createCatalog(
                    catalog,
                    new TpchConnectorFactory(1, false, false),
                    ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.SIMPLIFIED.name()));
            return queryRunner;
        });
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return IntStream.rangeClosed(1, 22)
                .mapToObj(i -> format("q%02d", i))
                .map(queryId -> format("/sql/presto/tpch/%s.sql", queryId));
    }

    @SuppressWarnings("unused")
    public static final class UpdateTestFiles
    {
        // Intellij doesn't handle well situation when test class has main(), hence inner class.

        private UpdateTestFiles() {}

        public static void main(String[] args)
                throws Exception
        {
            new TestTpchCostBasedPlan().generate();
        }
    }
}
