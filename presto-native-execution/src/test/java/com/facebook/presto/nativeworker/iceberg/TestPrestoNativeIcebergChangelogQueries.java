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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;

/**
 * End-to-end tests for Iceberg changelog metadata table functionality in Presto Native.
 * Tests the $changelog system table which provides change data capture (CDC) capabilities.
 *
 * These tests mirror the functionality tested in presto-iceberg's TestIcebergTableChangelog.java
 * to ensure feature parity between Presto Java and Presto Native.
 */
public class TestPrestoNativeIcebergChangelogQueries
        extends AbstractTestQueryFramework
{
    private long[] snapshots = new long[0];

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    @BeforeClass
    public void init()
            throws Exception
    {
        super.init();
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());

        // Clean up any existing test table from previous runs
        javaQueryRunner.execute("DROP TABLE IF EXISTS ctas_orders");

        // Create test table with multiple snapshots
        javaQueryRunner.execute("CREATE TABLE ctas_orders as SELECT * FROM tpch.tiny.orders LIMIT 10");
        javaQueryRunner.execute("TRUNCATE TABLE ctas_orders");
        javaQueryRunner.execute("INSERT INTO ctas_orders SELECT * FROM tpch.tiny.orders LIMIT 20");
        javaQueryRunner.execute("INSERT INTO ctas_orders SELECT * FROM tpch.tiny.orders LIMIT 30");

        javaQueryRunner.execute("DROP TABLE IF EXISTS orders_tiny");
        javaQueryRunner.execute("CREATE TABLE orders_tiny as SELECT * FROM tpch.tiny.orders");

        snapshots = Lists.reverse(
                        javaQueryRunner.execute("SELECT snapshot_id FROM \"ctas_orders$snapshots\" ORDER BY committed_at").getOnlyColumn()
                                .collect(Collectors.toList()))
                // reverse and skip the latest snapshot ID since it's invalid
                // to get the changelog for the current snapshot
                .stream().skip(1)
                .mapToLong(Long.class::cast)
                .toArray();
    }

    @Test
    public void testSchema()
    {
        assertQuery(String.format("SHOW COLUMNS FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testBasicSelect()
    {
        for (long id : snapshots) {
            assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\"", id));
        }
    }

    @Test
    public void testNoSnapSpecified()
    {
        assertQuery("SELECT * FROM \"ctas_orders$changelog\"");
    }

    @Test
    public void testSelectSingleColumn()
    {
        assertQuery(String.format("SELECT operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectMultiColumn()
    {
        assertQuery(String.format("SELECT operation, ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectMultiColumnReorder()
    {
        assertQuery(String.format("SELECT rowdata, rowdata.orderkey, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectPredicatePrimaryKey()
    {
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE rowdata.orderkey > 9000", snapshots[0]));
    }

    @Test
    public void testSelectPredicateStaticColumns()
    {
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE ordinal != 0", snapshots[0]));
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE ordinal = 0", snapshots[0]));
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE snapshotid = 0", snapshots[0]));
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE snapshotid != 0", snapshots[0]));
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE operation != 'INSERT'", snapshots[0]));
        assertQuery(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE operation = 'INSERT'", snapshots[0]));
    }

    @Test
    public void testVerifyProjectAndFilterOutput()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());
        javaQueryRunner.execute("DROP TABLE IF EXISTS test_changelog");
        javaQueryRunner.execute("CREATE TABLE test_changelog (a int, b int)  WITH (partitioning = ARRAY['a'], delete_mode = 'copy-on-write')");
        javaQueryRunner.execute("INSERT INTO test_changelog VALUES (1, 2)");
        javaQueryRunner.execute("INSERT INTO test_changelog VALUES (2, 2)");
        javaQueryRunner.execute("DELETE FROM test_changelog WHERE a = 2");

        long[] testSnapshots = Lists.reverse(
                        javaQueryRunner.execute("SELECT snapshot_id FROM \"test_changelog$snapshots\" ORDER BY committed_at desc").getOnlyColumn()
                                .collect(Collectors.toList()))
                // skip the earliest snapshot since the changelog starts from there.
                .stream().skip(1)
                .mapToLong(Long.class::cast)
                .toArray();

        assertQuery("SELECT snapshotid FROM \"test_changelog$changelog\" order by ordinal asc");
        // Verify correct projections for single columns
        assertQuery("SELECT ordinal FROM \"test_changelog$changelog\" order by ordinal asc");
        assertQuery("SELECT operation FROM \"test_changelog$changelog\" order by ordinal asc");
        assertQuery("SELECT rowdata.a, rowdata.b FROM \"test_changelog$changelog\" order by ordinal asc");
        // Verify correct filters results on filters
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE ordinal = 0 order by ordinal asc");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE ordinal = 1 order by ordinal asc");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE operation = 'INSERT' order by ordinal asc");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE operation = 'DELETE' order by ordinal asc");
        assertQueryReturnsEmptyResult("SELECT * FROM \"test_changelog$changelog\" WHERE operation = 'AAABBBCCC'");
        assertQuery(String.format("SELECT ordinal FROM \"test_changelog$changelog\" WHERE snapshotid = %d order by ordinal asc", testSnapshots[0]));
        assertQuery(String.format("SELECT ordinal FROM \"test_changelog$changelog\" WHERE snapshotid = %d order by ordinal asc", testSnapshots[1]));
        assertQuery("SELECT * FROM \"test_changelog$changelog\" WHERE rowdata.a = 2 order by ordinal asc");
    }

    @Test
    public void testSelectCount()
    {
        assertQuery(String.format("SELECT count(*) FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testPrimaryKeyProjection()
    {
        assertQuery(String.format("SELECT rowdata.orderkey FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testAggregation()
    {
        assertQuery(String.format("SELECT approx_distinct(rowdata.orderkey) FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT count(*) FROM \"ctas_orders@%d$changelog\" GROUP BY ordinal", snapshots[0]));

        // GROUP BY CUBE
        assertQuery(String.format("SELECT operation, ordinal, count(*), sum(rowdata.totalprice)" +
                " FROM \"ctas_orders@%d$changelog\"" +
                " GROUP BY CUBE (operation, ordinal)", snapshots[0]));
        // CUBE, ROLLUP and GROUPING SETS require column references, so nested row fields are projected in a subquery
        assertQuery("SELECT operation, orderstatus, count(*), max(orderkey), grouping(operation, orderstatus)" +
                " FROM (SELECT operation, rowdata.orderstatus AS orderstatus, rowdata.orderkey AS orderkey FROM \"ctas_orders$changelog\")" +
                " GROUP BY CUBE (operation, orderstatus)");

        // GROUP BY ROLLUP
        assertQuery(String.format("SELECT snapshotid, operation, ordinal, count(*), min(rowdata.orderkey)" +
                " FROM \"ctas_orders@%d$changelog\"" +
                " GROUP BY ROLLUP (snapshotid, operation, ordinal)", snapshots[0]));
        assertQuery("SELECT ordinal, operation, orderpriority, count(*), sum(totalprice), grouping(ordinal, operation, orderpriority)" +
                " FROM (SELECT ordinal, operation, rowdata.orderpriority AS orderpriority, rowdata.totalprice AS totalprice FROM \"ctas_orders$changelog\")" +
                " GROUP BY ROLLUP (ordinal, operation, orderpriority)");

        // GROUPING SETS and mixed grouping
        assertQuery("SELECT operation, ordinal, orderstatus, count(*)" +
                " FROM (SELECT operation, ordinal, rowdata.orderstatus AS orderstatus FROM \"ctas_orders$changelog\")" +
                " GROUP BY GROUPING SETS ((operation), (ordinal, orderstatus), ())");
        assertQuery("SELECT operation, ordinal, orderstatus, count(*)" +
                " FROM (SELECT operation, ordinal, rowdata.orderstatus AS orderstatus FROM \"ctas_orders$changelog\")" +
                " GROUP BY operation, CUBE (ordinal), ROLLUP (orderstatus)");
    }

    @Test
    public void testWindowFunctions()
    {
        // Ranking functions. Order keys are unique within an ordinal, so row_number results are deterministic.
        assertQuery(String.format("SELECT operation, ordinal, rowdata.orderkey," +
                " row_number() OVER (PARTITION BY operation ORDER BY ordinal, rowdata.orderkey)" +
                " FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery("SELECT operation, ordinal, rowdata.orderkey," +
                " row_number() OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey DESC)," +
                " rank() OVER (PARTITION BY operation ORDER BY ordinal)," +
                " dense_rank() OVER (ORDER BY rowdata.orderstatus)" +
                " FROM \"ctas_orders$changelog\"");
        assertQuery("SELECT ordinal, rowdata.orderkey," +
                " percent_rank() OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)," +
                " cume_dist() OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)," +
                " ntile(4) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)" +
                " FROM \"ctas_orders$changelog\"");

        // Value functions
        assertQuery("SELECT ordinal, operation, rowdata.orderkey," +
                " lag(rowdata.orderkey) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)," +
                " lead(rowdata.totalprice, 2) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)," +
                " first_value(rowdata.orderkey) OVER (PARTITION BY operation ORDER BY ordinal, rowdata.orderkey)," +
                " last_value(rowdata.orderkey) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey" +
                " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)," +
                " nth_value(rowdata.orderkey, 3) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey" +
                " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)" +
                " FROM \"ctas_orders$changelog\"");

        // Aggregate window functions with frames
        assertQuery("SELECT ordinal, operation, rowdata.orderkey," +
                " count(*) OVER (PARTITION BY ordinal)," +
                " sum(rowdata.totalprice) OVER (PARTITION BY ordinal ORDER BY rowdata.orderkey)," +
                " avg(rowdata.totalprice) OVER (PARTITION BY operation ORDER BY ordinal, rowdata.orderkey" +
                " ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)," +
                " min(rowdata.orderkey) OVER (PARTITION BY snapshotid ORDER BY rowdata.orderkey" +
                " ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)," +
                " max(rowdata.orderkey) OVER (PARTITION BY operation ORDER BY ordinal" +
                " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)" +
                " FROM \"ctas_orders$changelog\"");

        // Window function over an aggregation
        assertQuery("SELECT ordinal, operation, count(*) AS cnt," +
                " sum(count(*)) OVER (PARTITION BY operation ORDER BY ordinal)," +
                " rank() OVER (ORDER BY count(*) DESC, ordinal)" +
                " FROM \"ctas_orders$changelog\"" +
                " GROUP BY ordinal, operation");
    }

    @Test
    public void testStaticColumnProjections()
    {
        assertQuery(String.format("SELECT operation, ordinal, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT snapshotid, ordinal, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT ordinal, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT operation, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testCombinedColumnProjections()
    {
        assertQuery(String.format("SELECT rowdata.orderkey, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT rowdata.orderkey, ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuery(String.format("SELECT rowdata.orderkey, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testJoinOnSnapshotTimestamp()
    {
        assertQuery(String.format("SELECT snap.committed_at, change.operation, rowdata.orderkey, ordinal" +
                " FROM \"ctas_orders$snapshots\" as snap" +
                " JOIN \"ctas_orders@%d$changelog\" as change" +
                " ON change.snapshotid = snap.snapshot_id" +
                " ORDER BY snap.committed_at asc", snapshots[0]));
    }

    @Test
    public void testRightOuterJoin()
    {
        assertQuery(String.format("SELECT orderkey, operation, ordinal, snapshotid" +
                "   FROM ctas_orders as sample" +
                "   RIGHT OUTER JOIN \"ctas_orders@%d$changelog\" as cl" +
                "   ON cl.rowdata.orderkey = sample.orderkey", snapshots[0]));

        assertQuery(String.format("SELECT orderkey, operation, ordinal, snapshotid" +
                "   FROM orders_tiny as sample" +
                "   RIGHT OUTER JOIN \"ctas_orders@%d$changelog\" as cl" +
                "   ON cl.rowdata.orderkey = sample.orderkey", snapshots[0]));
    }

    @Test
    public void testDisallowedDropColumn()
    {
        assertQueryFails(String.format("ALTER TABLE \"ctas_orders@%d$changelog\" DROP COLUMN ordinal", snapshots[0]), "only the data table can have columns dropped");
    }

    @Test
    public void testDisallowedAddColumn()
    {
        assertQueryFails(String.format("ALTER TABLE \"ctas_orders@%d$changelog\" ADD COLUMN orderkey_added int", snapshots[0]), "only the data table can have columns added");
    }

    @Test
    public void testDisallowedRenameColumn()
    {
        assertQueryFails(String.format("ALTER TABLE \"ctas_orders@%d$changelog\" RENAME COLUMN ordinal TO ordinal_renamed", snapshots[0]), "only the data table can have columns renamed");
    }

    @Test
    public void testDisallowedDropTable()
    {
        assertQueryFails(String.format("DROP TABLE \"ctas_orders@%d$changelog\"", snapshots[0]), "only the data table can be dropped");
    }

    @Test
    public void testChangelogWithSchemaChange()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());
        javaQueryRunner.execute("DROP TABLE IF EXISTS changelog_alter");
        javaQueryRunner.execute("CREATE TABLE changelog_alter (a int, b int, c int)");
        javaQueryRunner.execute("INSERT INTO changelog_alter VALUES (0,1,2)");
        javaQueryRunner.execute("INSERT INTO changelog_alter VALUES (1,2,3), (2,3,4), (3,4,5), (4,5,6), (5,6,7)");
        javaQueryRunner.execute("ALTER TABLE changelog_alter ADD COLUMN d int");
        javaQueryRunner.execute("TRUNCATE TABLE changelog_alter");
        javaQueryRunner.execute("ALTER TABLE changelog_alter DROP COLUMN a");
        javaQueryRunner.execute("INSERT INTO changelog_alter VALUES (1,2,3), (2,3,4), (3,4,5), (4,5,6), (5,6,7)");
        assertQuery("SELECT * FROM \"changelog_alter$changelog\"");

        javaQueryRunner.execute("ALTER TABLE changelog_alter ALTER COLUMN d FIRST");
        assertQuery("SELECT * FROM \"changelog_alter$changelog\"");
        javaQueryRunner.execute("ALTER TABLE changelog_alter ALTER COLUMN b AFTER c");
        assertQuery("SELECT * FROM \"changelog_alter$changelog\"");
    }

    @Test
    public void testChangelogQueryResults()
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());
        javaQueryRunner.execute("DROP TABLE IF EXISTS changelog_results");
        javaQueryRunner.execute("CREATE TABLE changelog_results (c int)");
        javaQueryRunner.execute("INSERT INTO changelog_results VALUES 0");
        javaQueryRunner.execute("INSERT INTO changelog_results VALUES 1, 2, 3, 4, 5");
        javaQueryRunner.execute("TRUNCATE TABLE changelog_results");
        javaQueryRunner.execute("INSERT INTO changelog_results VALUES 1, 2, 3, 4, 5");

        long insert0Snapshot = getSnapshot(0, "changelog_results");
        long insert5ValuesSnapshot = getSnapshot(1, "changelog_results");
        long truncateSnapshot = getSnapshot(2, "changelog_results");
        long insert5AgainSnapshot = getSnapshot(3, "changelog_results");

        // test initial insert
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", insert0Snapshot, insert5ValuesSnapshot));
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert0Snapshot, insert5ValuesSnapshot));
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT' ORDER BY rowdata.c", insert0Snapshot, insert5ValuesSnapshot));

        // test after truncate
        assertQueryReturnsEmptyResult(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", insert5ValuesSnapshot, truncateSnapshot));
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE' ORDER BY rowdata.c", insert5ValuesSnapshot, truncateSnapshot));
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert5ValuesSnapshot, truncateSnapshot));

        // test changelog across the insertion and truncate snapshots
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", insert0Snapshot, truncateSnapshot));
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE'", insert0Snapshot, truncateSnapshot));
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert0Snapshot, truncateSnapshot));
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", insert0Snapshot, truncateSnapshot));

        // test changelog across delete and 2nd insert
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", truncateSnapshot, insert5AgainSnapshot));
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE'", truncateSnapshot, insert5AgainSnapshot));
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal ORDER BY ordinal", truncateSnapshot, insert5AgainSnapshot));
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", truncateSnapshot, insert5AgainSnapshot));

        javaQueryRunner.execute("DROP TABLE changelog_results");
    }

    private long getSnapshot(int idx, String tableName)
    {
        QueryRunner javaQueryRunner = ((QueryRunner) getExpectedQueryRunner());
        return javaQueryRunner.execute(String.format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName)).getOnlyColumn()
                .mapToLong(Long.class::cast)
                .skip(idx).findFirst().getAsLong();
    }

    @Test
    public void testApplyChangelogFunctionInSystemNamespace()
    {
        assertQueryFails("SELECT iceberg.system.apply_changelog(1, 'INSERT', 'test_value') IS NOT NULL",
                " Aggregate function not registered: iceberg.system.apply_changelog");
    }

    @Test
    public void testApplyChangelogFunctionNotInGlobalNamespace()
    {
        assertQueryFails(
                "SELECT apply_changelog(1, 'INSERT', 'test_value')",
                "line 1:8: Function apply_changelog not registered");
    }

    @Test
    public void testApplyChangelogFunctionNotInPrestoDefaultNamespace()
    {
        assertQueryFails(
                "SELECT presto.default.apply_changelog(1, 'INSERT', 'test_value')",
                "line 1:8: Function apply_changelog not registered");
    }
}
