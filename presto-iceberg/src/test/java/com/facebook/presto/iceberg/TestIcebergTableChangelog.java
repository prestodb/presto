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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergTableChangelog
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        return createIcebergQueryRunner(properties, ImmutableMap.of());
    }

    @Override
    @BeforeClass
    public void init()
            throws Exception
    {
        super.init();
        assertQuerySucceeds("CALL iceberg.system.create_sample_table('tpch', 'orders', 'orderkey')");
        assertQuerySucceeds("INSERT INTO \"orders\" SELECT * FROM tpch.tiny.orders TABLESAMPLE BERNOULLI(1)");
        assertQuerySucceeds("INSERT INTO \"orders\" SELECT * FROM tpch.tiny.orders TABLESAMPLE BERNOULLI(1)");
    }

    @Test
    public void testSchema()
    {
        assertQuery("SHOW COLUMNS FROM \"orders$changelog\"",
                "VALUES" +
                        "('operation', 'varchar', '', '')," +
                        "('ordinal', 'bigint', '', '')," +
                        "('snapshotid', 'bigint', '', '')," +
                        "('primarykey', 'bigint', '', '')," +
                        "('rowdata', 'row(\"orderkey\" bigint, \"custkey\" bigint, \"orderstatus\" varchar, \"totalprice\" double, \"orderdate\" date, \"orderpriority\" varchar, \"clerk\" varchar, \"shippriority\" integer, \"comment\" varchar)', '', '')");
    }

    @Test
    public void testBasicSelect()
    {
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\"");
    }

    @Test
    public void testSelectSingleColumn()
    {
        assertQuerySucceeds("SELECT operation FROM \"orders$changelog\"");
    }

    @Test
    public void testSelectMultiColumn()
    {
        assertQuerySucceeds("SELECT operation, ordinal FROM \"orders$changelog\"");
    }

    @Test
    public void testSelectMultiColumnReorder()
    {
        assertQuerySucceeds("SELECT rowdata, primarykey, operation FROM \"orders$changelog\"");
    }

    @Test
    public void testSelectPredicatePrimaryKey()
    {
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE primarykey > 9000");
    }

    @Test
    public void testSelectPredicateStaticColumns()
    {
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE ordinal != 0");
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE ordinal = 0");
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE snapshotid = 0");
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE snapshotid != 0");
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE operation != 'INSERT'");
        assertQuerySucceeds("SELECT * FROM \"orders$changelog\" WHERE operation = 'INSERT'");
    }

    @Test
    public void testSelectCount()
    {
        assertQuerySucceeds("SELECT count(*) FROM \"orders$changelog\"");
    }

    @Test
    public void testCountGroupByAggregation()
    {
        assertQuerySucceeds("SELECT count(*) FROM \"orders$changelog\" GROUP BY ordinal");
    }

    @Test
    public void testPrimaryKeyProjection()
    {
        assertQuerySucceeds("SELECT primarykey FROM \"orders$changelog\"");
    }

    @Test
    public void testBasicAggregation()
    {
        assertQuerySucceeds("SELECT count(primarykey) FROM \"orders$changelog\"");
    }

    @Test
    public void testStaticColumnProjections()
    {
        assertQuerySucceeds("SELECT operation, ordinal, snapshotid FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT snapshotid, ordinal, operation FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT ordinal, snapshotid FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT operation, snapshotid FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT snapshotid FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT ordinal FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT operation FROM \"orders$changelog\"");
    }

    @Test
    public void testCombinedColumnProjections()
    {
        assertQuerySucceeds("SELECT primarykey, operation FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT primarykey, ordinal FROM \"orders$changelog\"");
        assertQuerySucceeds("SELECT primarykey, snapshotid FROM \"orders$changelog\"");
    }

    @Test
    public void testJoinOnSnapshotTimestamp()
    {
        assertQuerySucceeds("SELECT * FROM \"orders$snapshots\"");
        assertQuerySucceeds("SELECT snap.committed_at, change.operation, primarykey, ordinal" +
                " FROM \"orders$snapshots\" as snap" +
                " JOIN \"orders$changelog\" as change" +
                " ON change.snapshotid = snap.snapshot_id" +
                " ORDER BY snap.committed_at asc");
    }

    @Test
    public void testRightOuterJoinOnSamples()
    {
        assertQuerySucceeds("INSERT INTO \"orders$samples\" SELECT * FROM orders TABLESAMPLE BERNOULLI(1)");
        assertQuerySucceeds("SELECT orderkey, operation, ordinal, snapshotid" +
                "   FROM \"orders$samples\" as sample" +
                "   RIGHT OUTER JOIN \"orders$changelog\" as cl" +
                "   ON cl.primarykey = sample.orderkey");
    }
}
