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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;

public class TestIcebergTableChangelog
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .build()
                .getQueryRunner();
    }

    private long[] snapshots = new long[0];

    @Override
    @BeforeClass
    public void init()
            throws Exception
    {
        super.init();
        assertQuerySucceeds("CREATE TABLE ctas_orders as SELECT * FROM orders LIMIT 10");
        assertQuerySucceeds("TRUNCATE TABLE ctas_orders");
        assertQuerySucceeds("INSERT INTO ctas_orders SELECT * FROM orders LIMIT 20");
        assertQuerySucceeds("INSERT INTO ctas_orders SELECT * FROM orders LIMIT 30");
        snapshots = Lists.reverse(
                        getQueryRunner().execute("SELECT snapshot_id FROM \"ctas_orders$snapshots\" ORDER BY committed_at").getOnlyColumn()
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
        assertQuery(String.format("SHOW COLUMNS FROM \"ctas_orders@%d$changelog\"", snapshots[0]),
                "VALUES" +
                        "('operation', 'varchar', '', '', null ,null, 2147483647)," +
                        "('ordinal', 'bigint', '', '', 19, null, null)," +
                        "('snapshotid', 'bigint', '', '', 19, null, null)," +
                        "('rowdata', 'row(\"orderkey\" bigint, \"custkey\" bigint, \"orderstatus\" varchar, \"totalprice\" double, \"orderdate\" date, \"orderpriority\" varchar, \"clerk\" varchar, \"shippriority\" integer, \"comment\" varchar)', '', '', null, null, null)");
    }

    @Test
    public void testBasicSelect()
    {
        for (long id : snapshots) {
            assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\"", id));
        }
    }

    @Test
    public void testNoSnapSpecified()
    {
        assertQuerySucceeds("SELECT * FROM \"ctas_orders$changelog\"");
    }

    @Test
    public void testSelectSingleColumn()
    {
        assertQuerySucceeds(String.format("SELECT operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectMultiColumn()
    {
        assertQuerySucceeds(String.format("SELECT operation, ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectMultiColumnReorder()
    {
        assertQuerySucceeds(String.format("SELECT rowdata, rowdata.orderkey, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testSelectPredicatePrimaryKey()
    {
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE rowdata.orderkey > 9000", snapshots[0]));
    }

    @Test
    public void testSelectPredicateStaticColumns()
    {
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE ordinal != 0", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE ordinal = 0", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE snapshotid = 0", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE snapshotid != 0", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE operation != 'INSERT'", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT * FROM \"ctas_orders@%d$changelog\" WHERE operation = 'INSERT'", snapshots[0]));
    }

    @Test
    public void testVerifyProjectAndFilterOutput()
    {
        assertQuerySucceeds("CREATE TABLE test_changelog (a int, b int)  WITH (partitioning = ARRAY['a'], delete_mode = 'copy-on-write')");
        assertQuerySucceeds("INSERT INTO test_changelog VALUES (1, 2)");
        assertQuerySucceeds("INSERT INTO test_changelog VALUES (2, 2)");
        assertQuerySucceeds("DELETE FROM test_changelog WHERE a = 2");
        long[] testSnapshots = Lists.reverse(
                        getQueryRunner().execute("SELECT snapshot_id FROM \"test_changelog$snapshots\" ORDER BY committed_at desc").getOnlyColumn()
                                .collect(Collectors.toList()))
                // skip the earliest snapshot since the changelog starts from there.
                .stream().skip(1)
                .mapToLong(Long.class::cast)
                .toArray();
        assertQuery("SELECT snapshotid FROM \"test_changelog$changelog\" order by ordinal asc", "VALUES " + Joiner.on(", ").join(Arrays.stream(testSnapshots).iterator()));
        // Verify correct projections for single columns
        assertQuery("SELECT ordinal FROM \"test_changelog$changelog\" order by ordinal asc", "VALUES 0, 1");
        assertQuery("SELECT operation FROM \"test_changelog$changelog\" order by ordinal asc", "VALUES 'INSERT', 'DELETE'");
        assertQuery("SELECT rowdata.a, rowdata.b FROM \"test_changelog$changelog\" order by ordinal asc", "VALUES (2, 2), (2, 2)"); // inserted then deleted
        // Verify correct filters results on filters
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE ordinal = 0 order by ordinal asc", "VALUES (0, 'INSERT')");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE ordinal = 1 order by ordinal asc", "VALUES (1, 'DELETE')");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE operation = 'INSERT' order by ordinal asc", "VALUES (0, 'INSERT')");
        assertQuery("SELECT ordinal, operation FROM \"test_changelog$changelog\" WHERE operation = 'DELETE' order by ordinal asc", "VALUES (1, 'DELETE')");
        assertQueryReturnsEmptyResult("SELECT * FROM \"test_changelog$changelog\" WHERE operation = 'AAABBBCCC'");
        assertQuery(String.format("SELECT ordinal FROM \"test_changelog$changelog\" WHERE snapshotid = %d order by ordinal asc", testSnapshots[0]), "VALUES 0");
        assertQuery(String.format("SELECT ordinal FROM \"test_changelog$changelog\" WHERE snapshotid = %d order by ordinal asc", testSnapshots[1]), "VALUES 1");
        assertQuery("SELECT * FROM \"test_changelog$changelog\" WHERE rowdata.a = 2 order by ordinal asc", String.format("VALUES ('INSERT', 0, %d, (2, 2)), ('DELETE', 1, %d, (2, 2))", testSnapshots[0], testSnapshots[1]));
    }

    @Test
    public void testSelectCount()
    {
        assertQuerySucceeds(String.format("SELECT count(*) FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testCountGroupByAggregation()
    {
        assertQuerySucceeds(String.format("SELECT count(*) FROM \"ctas_orders@%d$changelog\" GROUP BY ordinal", snapshots[0]));
    }

    @Test
    public void testPrimaryKeyProjection()
    {
        assertQuerySucceeds(String.format("SELECT rowdata.orderkey FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testBasicAggregation()
    {
        assertQuerySucceeds(String.format("SELECT approx_distinct(rowdata.orderkey) FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testStaticColumnProjections()
    {
        assertQuerySucceeds(String.format("SELECT operation, ordinal, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT snapshotid, ordinal, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT ordinal, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT operation, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testCombinedColumnProjections()
    {
        assertQuerySucceeds(String.format("SELECT rowdata.orderkey, operation FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT rowdata.orderkey, ordinal FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
        assertQuerySucceeds(String.format("SELECT rowdata.orderkey, snapshotid FROM \"ctas_orders@%d$changelog\"", snapshots[0]));
    }

    @Test
    public void testJoinOnSnapshotTimestamp()
    {
        assertQuerySucceeds(String.format("SELECT snap.committed_at, change.operation, rowdata.orderkey, ordinal" +
                " FROM \"ctas_orders$snapshots\" as snap" +
                " JOIN \"ctas_orders@%d$changelog\" as change" +
                " ON change.snapshotid = snap.snapshot_id" +
                " ORDER BY snap.committed_at asc", snapshots[0]));
    }

    @Test
    public void testRightOuterJoin()
    {
        assertQuerySucceeds(String.format("SELECT orderkey, operation, ordinal, snapshotid" +
                "   FROM orders as sample" +
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
    public void testChangelogWithScemaChange()
    {
        assertQuerySucceeds("CREATE TABLE changelog_alter (c int)");
        assertQuerySucceeds("INSERT INTO changelog_alter VALUES 0");
        assertQuerySucceeds("INSERT INTO changelog_alter VALUES 1, 2, 3, 4, 5");
        assertQuerySucceeds("ALTER TABLE changelog_alter ADD COLUMN d int");
        assertQuerySucceeds("TRUNCATE TABLE changelog_alter");
        assertQuerySucceeds("ALTER TABLE changelog_alter DROP COLUMN c");
        assertQuerySucceeds("INSERT INTO changelog_alter VALUES 1, 2, 3, 4, 5");
        assertQuerySucceeds("SELECT * FROM \"changelog_alter$changelog\"");
    }

    @Test
    public void testChangelogQueryResults()
    {
        assertQuerySucceeds("CREATE TABLE changelog_results (c int)");
        assertQuerySucceeds("INSERT INTO changelog_results VALUES 0");
        assertQuerySucceeds("INSERT INTO changelog_results VALUES 1, 2, 3, 4, 5");
        assertQuerySucceeds("TRUNCATE TABLE changelog_results");
        assertQuerySucceeds("INSERT INTO changelog_results VALUES 1, 2, 3, 4, 5");

        long insert0Snapshot = getSnapshot(0, "changelog_results");
        long insert5ValuesSnapshot = getSnapshot(1, "changelog_results");
        long truncateSnapshot = getSnapshot(2, "changelog_results");
        long insert5AgainSnapshot = getSnapshot(3, "changelog_results");

        // test initial insert
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", insert0Snapshot, insert5ValuesSnapshot),
                "VALUES 1, 2, 3, 4, 5");
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert0Snapshot, insert5ValuesSnapshot),
                "VALUES (0, 5)");
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT' ORDER BY rowdata.c", insert0Snapshot, insert5ValuesSnapshot),
                "VALUES 1, 2, 3, 4, 5");

        // test after truncate
        assertQueryReturnsEmptyResult(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", insert5ValuesSnapshot, truncateSnapshot));
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE' ORDER BY rowdata.c", insert5ValuesSnapshot, truncateSnapshot),
                "VALUES 0, 1, 2, 3, 4, 5");
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert5ValuesSnapshot, truncateSnapshot),
                "VALUES (0, 6)");

        // test changelog across the insertion and truncate snapshots
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", insert0Snapshot, truncateSnapshot),
                "VALUES 5");
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE'", insert0Snapshot, truncateSnapshot),
                "VALUES 6");
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal", insert0Snapshot, truncateSnapshot),
                "VALUES (0, 5), (1, 6)");
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", insert0Snapshot, truncateSnapshot),
                "VALUES 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5");

        // test changelog across delete and 2nd insert
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'INSERT'", truncateSnapshot, insert5AgainSnapshot),
                "VALUES 5");
        assertQuery(String.format("SELECT count(*) FROM \"changelog_results@%d$changelog@%d\" WHERE operation = 'DELETE'", truncateSnapshot, insert5AgainSnapshot),
                "VALUES 0");
        assertQuery(String.format("SELECT ordinal, count(*) FROM \"changelog_results@%d$changelog@%d\" GROUP BY ordinal ORDER BY ordinal", truncateSnapshot, insert5AgainSnapshot),
                "VALUES (0, 5)");
        assertQuery(String.format("SELECT rowdata.c FROM \"changelog_results@%d$changelog@%d\" ORDER BY rowdata.c", truncateSnapshot, insert5AgainSnapshot),
                "VALUES 1, 2, 3, 4, 5");

        assertQuerySucceeds("DROP TABLE changelog_results");
    }

    private long getSnapshot(int idx, String tableName)
    {
        return getQueryRunner().execute(String.format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at", tableName)).getOnlyColumn()
                .mapToLong(Long.class::cast)
                .skip(idx).findFirst().getAsLong();
    }

    @Test
    public void testApplyChangelogFunctionInSystemNamespace()
    {
        assertQuery(
                "SELECT iceberg.system.apply_changelog(1, 'INSERT', 'test_value') IS NOT NULL",
                "SELECT true");
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
