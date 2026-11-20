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

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.DISJOINT_STRING_OR_ESTIMATION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.HANDLE_COMPLEX_EQUI_JOINS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

/**
 * Join plans over TPC-H sf3000 statistics for filters that OR together equalities on varchar columns.
 * Each query has a golden plan with disjoint string OR estimation on and, where the plan differs, one
 * under {@code disjoint_off/} with it off.
 */
public class TestOrOfVarcharEqualitiesCostBasedPlan
        extends AbstractCostBasedPlanTest
{
    public TestOrOfVarcharEqualitiesCostBasedPlan()
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
        return Stream.of(
                        "lineitem_orders_01_pair",
                        "lineitem_orders_12_pairs",
                        "lineitem_orders_shipmode_in",
                        "lineitem_orders_cross_column_or",
                        "part_lineitem_01_pair",
                        "part_lineitem_40_pairs",
                        "lineitem_orders_part_40_pairs")
                .map(queryId -> format("/sql/presto/or_of_varchar/%s.sql", queryId));
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void disjointEstimationOffPlansMatch(String queryResourcePath)
    {
        String sql = read(queryResourcePath);
        String offPlan = generateQueryPlan(sql, disjointEstimationOffSession());
        if (!offPlan.equals(generateQueryPlan(sql, getQueryRunner().getDefaultSession()))) {
            assertEquals(offPlan, read(getDisjointOffPlanResourcePath(queryResourcePath)));
        }
    }

    @Override
    public void generate()
            throws Exception
    {
        super.generate();
        initPlanTest();
        try {
            getQueryResourcePaths().forEach(queryResourcePath -> {
                String sql = read(queryResourcePath);
                String offPlan = generateQueryPlan(sql, disjointEstimationOffSession());
                if (!offPlan.equals(generateQueryPlan(sql, getQueryRunner().getDefaultSession()))) {
                    Path path = Paths.get(getSourcePath().toString(), "src/test/resources", getDisjointOffPlanResourcePath(queryResourcePath));
                    try {
                        createParentDirs(path.toFile());
                        write(offPlan.getBytes(UTF_8), path.toFile());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }
        finally {
            destroyPlanTest();
        }
    }

    private Session disjointEstimationOffSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(DISJOINT_STRING_OR_ESTIMATION_ENABLED, "false")
                .build();
    }

    private static String getDisjointOffPlanResourcePath(String queryResourcePath)
    {
        Path planPath = Paths.get(getQueryPlanResourcePath(queryResourcePath));
        return planPath.getParent().resolve("disjoint_off/" + planPath.getFileName()).toString();
    }

    @SuppressWarnings("unused")
    public static final class UpdateTestFiles
    {
        // Intellij doesn't handle well situation when test class has main(), hence inner class.

        private UpdateTestFiles() {}

        public static void main(String[] args)
                throws Exception
        {
            new TestOrOfVarcharEqualitiesCostBasedPlan().generate();
        }
    }
}
