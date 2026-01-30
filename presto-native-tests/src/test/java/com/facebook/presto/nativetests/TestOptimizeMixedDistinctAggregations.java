
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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregationsNative
{
    private String storageFormat;
    private boolean charNToVarcharImplicitCast;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        charNToVarcharImplicitCast = getCharNToVarcharImplicitCastForTest(
                sidecarEnabled, parseBoolean(System.getProperty("charNToVarcharImplicitCast", "false")));
        super.init(storageFormat, charNToVarcharImplicitCast, sidecarEnabled);
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setImplicitCastCharNToVarchar(charNToVarcharImplicitCast)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setExtraCoordinatorProperties(
                        ImmutableMap.of("optimizer.optimize-mixed-distinct-aggregations", "true"))
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    // data mismatch
    @Override
    @Test(enabled = false)
    public void testGroupByWithNulls() {}

    // crashes the worker
    @Override
    @Test(enabled = false)
    public void testGroupByRow() {}

    // crashes the worker
    @Override
    @Test(enabled = false)
    public void testGroupedRow() {}

    // aggregated test cases start here

    @Test
    public void testVarcharMismatches()
    {
        // testExtractDistinctAggregationOptimizer
        assertQuery("SELECT max(orderstatus), COUNT(DISTINCT orderkey), sum(DISTINCT orderkey) FROM orders");
        assertQuery("SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");
        assertQuery("SELECT shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");
        assertQuery("SELECT clerk, shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");

        // testGroupByCaseNoElse
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // testGroupByCase
        // operand in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in GROUP BY clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in GROUP BY clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus ELSE 'x' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'else' in GROUP BY clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN 'x' ELSE orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test(enabled = false)
    public void testDataMismatch()
    {
        // testExtractDistinctAggregationOptimizer
        assertQuery("SELECT count(DISTINCT a), max(b) FROM (VALUES (row(1, 2), 3)) t(a, b)", "VALUES (1, 3)");
    }

    // only after Pramod's cherry pick for CASE during expression conversion
    @Test
    public void testExpressionWithoutDependenciesPresent()
    {
        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");

        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");

        // testGroupByCaseNoElse
        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' END");

        // testGroupByCase
        // Invalid node. Expression dependencies ([orderstatus]) not in source plan output ([])
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");
    }
}
