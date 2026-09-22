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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;

/**
 * Regression tests for the stack overflow bug triggered by Iceberg tables with large numbers of
 * partitions when pushdown_filter_enabled=true (the required default on Prestissimo clusters).
 *
 * ExpressionConverter.toIcebergExpression() built a left-nested Or chain of depth N for N
 * single-point equality values, overflowing ExpressionVisitors' recursive visit() call.
 * Fixed by emitting Expressions.in() (depth 1) instead.
 *
 * Workaround: SET SESSION pushdown_filter_enabled = false (confirmed by customer).
 */
public class TestLargePartitionStackOverflow
        extends AbstractTestQueryFramework
{
    private static final int PARTITION_COUNT = 3_000;
    private static final String TABLE = "large_partitioned_overflow";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"))
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    @BeforeClass
    public void createTable()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS tpch");
        // Drop first to handle aborted-run leftovers; without this, an unguarded CREATE TABLE
        // throws TableAlreadyExistsException and TestNG marks all tests as configuration-failed.
        queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
        queryRunner.execute(
                "CREATE TABLE " + TABLE + " (val INTEGER, created_date DATE) " +
                "WITH (partitioning = ARRAY['created_date'])");

        // Insert PARTITION_COUNT rows in batches of 90 to stay under the 100-open-writers limit.
        // Epoch-day 18000 = DATE '2019-04-14'; date_add avoids from_unixtime's session-timezone
        // conversion which shifts the date on non-UTC JVMs.
        int batchSize = 90;
        for (int start = 18000; start < 18000 + PARTITION_COUNT; start += batchSize) {
            int end = Math.min(start + batchSize - 1, 18000 + PARTITION_COUNT - 1);
            queryRunner.execute(
                    "INSERT INTO " + TABLE + " SELECT 1, date_add('day', d, DATE '1970-01-01') " +
                    "FROM UNNEST(SEQUENCE(" + start + ", " + end + ")) AS t(d)");
        }
    }

    @AfterClass(alwaysRun = true)
    public void dropTable()
    {
        QueryRunner queryRunner = getQueryRunner();
        if (queryRunner != null) {
            queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    @Test
    public void testSelectStarPlanWithPushdownEnabled()
    {
        // Regression test: planning SELECT * on a 3000-partition table with pushdown=true
        // previously overflowed with "statement is too large (stack overflow during analysis)".
        plan("SELECT * FROM " + TABLE + " LIMIT 1", pushdownSession(true));
    }

    @Test
    public void testCountStarPlanWithPushdownEnabled()
    {
        // Regression test: same overflow triggered by COUNT(*) on a large partitioned table.
        plan("SELECT COUNT(*) FROM " + TABLE, pushdownSession(true));
    }

    @Test
    public void testSelectWithPartitionFilterExecutesCorrectly()
    {
        // Verify execution returns the correct row for a single-partition filter.
        // Epoch day 18000 = DATE '2019-04-14'; this partition contains exactly 1 row (val=1).
        // Runs without pushdown_filter_enabled because the Java connector does not support
        // filter pushdown at execution time (native/Prestissimo workers only).
        assertQuery(
                "SELECT val FROM " + TABLE + " WHERE created_date = DATE '2019-04-14'",
                "VALUES 1");
    }

    private Session pushdownSession(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(enabled))
                .build();
    }
}
