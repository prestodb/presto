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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.nativetests.NativeTestsUtils.createNativeQueryRunner;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPlanCheckerRouterPlugin
        extends BasePlanCheckerTest
{
    private QueryRunner nativeQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setStorageFormat(storageFormat)
                .build();
    }

    @BeforeClass
    public void initNativeQueryRunner()
    {
        nativeQueryRunner = getQueryRunner();
    }

    @Test
    public void testNativeCompatibleQueries()
            throws Exception
    {
        if (sidecarEnabled) {
            List<String> queries = getNativeCompatibleQueries();
            for (String query : queries) {
                runQuery(query, httpServerUri);
            }
            // testFailingNativeQueries() test case will run before this.
            assertEquals(planCheckerRouterPluginPrestoClient.getNativeClusterRedirectRequests().getTotalCount(), queries.size() + getFailingNativeQueries().length);
        }
    }

    @Test
    public void testNativeIncompatibleQueries()
            throws Exception
    {
        if (sidecarEnabled) {
            List<String> queries = getNativeIncompatibleQueries();
            for (String query : queries) {
                runQuery(query, httpServerUri);
            }
            // testFailingQueriesOnBothClusters() test case will run before this.
            // Since all the queries are failing on a native plan checker cluster, we redirect them to a java cluster and will count as a Java cluster redirect.
            assertEquals(planCheckerRouterPluginPrestoClient.getJavaClusterRedirectRequests().getTotalCount(), queries.size() + getFailingQueriesOnBothClustersProvider().length);
        }
    }

    // These are the queries that are redirected to a native cluster as the failures won't be caught during the EXPLAIN (TYPE VALIDATE) call.
    @Test(dataProvider = "failingNativeQueriesProvider")
    public void testFailingNativeQueries(String query, String exceptionMessage)
            throws Exception
    {
        if (sidecarEnabled) {
            runQuery(query, httpServerUri, Optional.of(exceptionMessage));
        }
    }

    @Test(dataProvider = "failingQueriesOnBothClustersProvider")
    public void testFailingQueriesOnBothClusters(String query, String exceptionMessage)
            throws SQLException
    {
        if (sidecarEnabled) {
            runQuery(query, httpServerUri, Optional.of(exceptionMessage));
        }
    }

    @Test(dependsOnMethods =
            {"testFailingQueriesOnBothClusters",
                    "testFailingNativeQueries",
                    "testNativeCompatibleQueries",
                    "testNativeIncompatibleQueries"})
    public void testPlanCheckerClusterNotAvailable()
            throws SQLException
    {
        if (sidecarEnabled) {
            closeAllRuntimeException(nativeQueryRunner);
            nativeQueryRunner = null;
            List<String> queries = getNativeIncompatibleQueries();
            for (String query : queries) {
                runQuery(query, httpServerUri);
            }
            assertEquals(planCheckerRouterPluginPrestoClient.getFallBackToJavaClusterRedirectRequests().getTotalCount(), queries.size());
        }
    }

    @DataProvider(name = "failingQueriesOnBothClustersProvider")
    public Object[][] getFailingQueriesOnBothClustersProvider()
    {
        return new Object[][] {
                {"select * from nation", "line 1:15: Schema must be specified when session schema is not set"}};
    }

    @DataProvider(name = "failingNativeQueriesProvider")
    public Object[][] getFailingNativeQueries()
    {
        return new Object[][] {
                {"SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)",
                        "Error from native plan checker:  Unsupported window type: 2"}};
    }

    private static List<String> getNativeCompatibleQueries()
    {
        String catalog = "hive";
        String schema = "tpch";
        return ImmutableList.of(
                format("SELECT lower(comment) from %s.%s.region", catalog, schema),
                "SHOW FUNCTIONS",
                format("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0))) FROM %s.%s.orders", catalog, schema));
    }

    private static List<String> getNativeIncompatibleQueries()
    {
        return ImmutableList.of(
                "SELECT array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)]," +
                        " (x, y) -> if (x < y, 1, if (x > y, -1, 0)))",
                "SELECT x AS y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x");
    }
}
