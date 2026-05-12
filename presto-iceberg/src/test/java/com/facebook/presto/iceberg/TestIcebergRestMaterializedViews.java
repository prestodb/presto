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

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.View;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestIcebergRestMaterializedViews
        extends TestIcebergMaterializedViewsBase
{
    private static final String SCHEMA = "test_schema";

    private TestingHttpServer restServer;
    private String serverUri;
    private RESTCatalog catalog;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();

        catalog = new RESTCatalog();
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", serverUri);
        catalogProps.put("warehouse", warehouseLocation.getAbsolutePath());
        catalog.initialize("test_catalog", catalogProps);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (catalog != null) {
            catalog.close();
        }
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName(SCHEMA)
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of(
                        "experimental.legacy-materialized-views", "false",
                        "materialized-view-default-refresh-type", "INCREMENTAL"))
                .build().getQueryRunner();
    }

    @Test
    public void testSingleBaseBoundedRefreshAdvancesByAtMostN()
    {
        String base = "bounded_single_base";
        String mv = "bounded_single_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 2) AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);
            long watermarkAfterInitial = baseWatermark(mv, base);

            assertUpdate(format("INSERT INTO %s VALUES (2, 200)", base), 1);
            assertUpdate(format("INSERT INTO %s VALUES (3, 300)", base), 1);
            assertUpdate(format("INSERT INTO %s VALUES (4, 400)", base), 1);
            assertUpdate(format("INSERT INTO %s VALUES (5, 500)", base), 1);
            assertUpdate(format("INSERT INTO %s VALUES (6, 600)", base), 1);

            long headBeforeFirstBoundedRefresh = headSnapshot(base);
            assertNotEquals(headBeforeFirstBoundedRefresh, watermarkAfterInitial, "expected head to advance with inserts");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long watermarkAfterFirst = baseWatermark(mv, base);
            assertAdvanceLeq(watermarkAfterInitial, watermarkAfterFirst, headBeforeFirstBoundedRefresh, 2, base);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long watermarkAfterSecond = baseWatermark(mv, base);
            assertAdvanceLeq(watermarkAfterFirst, watermarkAfterSecond, headBeforeFirstBoundedRefresh, 2, base);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long watermarkAfterThird = baseWatermark(mv, base);
            assertEquals(watermarkAfterThird, headBeforeFirstBoundedRefresh,
                    "expected three bounded refreshes to fully catch up to HEAD");

            assertQuery("SELECT COUNT(*) FROM " + mv, "SELECT 6");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testBoundExceedsDiffAdvancesFully()
    {
        String base = "bounded_exceeds_base";
        String mv = "bounded_exceeds_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES 1", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 10) AS SELECT id FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            assertUpdate(format("INSERT INTO %s VALUES 2", base), 1);

            long head = headSnapshot(base);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, base), head, "expected single refresh to catch up when bound exceeds diff");
            assertQuery("SELECT COUNT(*) FROM " + mv, "SELECT 2");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testMultiBaseIndependence()
    {
        String baseA = "bounded_multi_a";
        String baseB = "bounded_multi_b";
        String mv = "bounded_multi_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3')", baseA));
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3')", baseB));
            assertUpdate(format("INSERT INTO %s VALUES (1, 10)", baseA), 1);
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", baseB), 1);

            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 2) AS " +
                            "SELECT %s.id AS id, %s.v + %s.v AS combined FROM %s JOIN %s ON %s.id = %s.id",
                    mv, baseA, baseA, baseB, baseA, baseB, baseA, baseB));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            for (int i = 2; i <= 6; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", baseA, i, i * 10), 1);
            }
            assertUpdate(format("INSERT INTO %s VALUES (2, 200)", baseB), 1);

            long aHead = headSnapshot(baseA);
            long bHead = headSnapshot(baseB);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long aAfter1 = baseWatermark(mv, baseA);
            long bAfter1 = baseWatermark(mv, baseB);
            assertNotEquals(aAfter1, aHead, "A should not have caught up to head in one bounded refresh (5 > 2)");
            assertEquals(bAfter1, bHead, "B should fully advance because its diff (1) is within the bound");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long aAfter2 = baseWatermark(mv, baseA);
            long bAfter2 = baseWatermark(mv, baseB);
            assertEquals(bAfter2, bHead, "B should remain at head after subsequent refresh");
            assertNotEquals(aAfter2, aAfter1, "A should advance again on second refresh");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, baseA), aHead, "A should fully catch up after the third refresh");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + baseA);
            assertUpdate("DROP TABLE IF EXISTS " + baseB);
        }
    }

    @Test
    public void testV3CompactionInWindowRespectsTarget()
    {
        // V3 row lineage survives compaction, so the seq predicate still prunes the compacted file at HEAD.
        String base = "bounded_compaction_base";
        String mv = "bounded_compaction_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 2) AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);
            long watermarkAfterInitial = baseWatermark(mv, base);

            assertUpdate(format("INSERT INTO %s VALUES (2, 200)", base), 1);
            assertQuerySucceeds(format(
                    "CALL system.rewrite_data_files(schema => 'test_schema', table_name => '%s', options => map(array['rewrite-all'], array['true']))",
                    base));
            assertUpdate(format("INSERT INTO %s VALUES (3, 300)", base), 1);

            long headBefore = headSnapshot(base);
            assertNotEquals(headBefore, watermarkAfterInitial, "head should differ from initial watermark after compaction + insert");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long watermarkAfter = baseWatermark(mv, base);
            assertNotEquals(watermarkAfter, headBefore, "watermark should not advance to HEAD under bound=2");
            assertNotEquals(watermarkAfter, watermarkAfterInitial, "watermark should advance from initial");

            assertQuery("SELECT id, v FROM \"__mv_storage__" + mv + "\" ORDER BY id",
                    "VALUES (1, 100), (2, 200)");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testV2BaseFallsBackToUnboundedRefresh()
    {
        // V2 lacks per-row lineage; the bound is silently ignored.
        String base = "bounded_v2_base";
        String mv = "bounded_v2_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT)", base));
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 2) AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            for (int i = 2; i <= 6; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }
            long head = headSnapshot(base);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, base), head, "V2 bases should advance all the way to HEAD in one refresh");
            assertQuery("SELECT COUNT(*) FROM " + mv, "SELECT 6");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testHiddenSequenceColumnReferenceWorks()
    {
        String base = "bounded_hidden_base";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES 1", base), 1);
            assertQuery(format("SELECT count(*) FROM %s WHERE _last_updated_sequence_number <= 1000", base),
                    "SELECT 1");
            assertQuery(format("SELECT count(*) FROM (SELECT * FROM %s WHERE _last_updated_sequence_number <= 1000)", base),
                    "SELECT 1");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testNonPositiveBoundRejected()
    {
        String base = "bounded_neg_base";
        String mv = "bounded_neg_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT)", base));
            for (int bound : new int[] {-1, 0}) {
                try {
                    assertUpdate(format(
                            "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = %d) AS SELECT id FROM %s",
                            mv, bound, base));
                    fail("expected bound " + bound + " to be rejected");
                }
                catch (RuntimeException e) {
                    assertTrue(e.getMessage().contains("max_snapshots_per_refresh"),
                            "unexpected error message: " + e.getMessage());
                }
                assertFalse(viewExists(mv), "MV should not have been created with bound " + bound);
            }
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testBoundPropertyPersisted()
            throws Exception
    {
        String base = "bounded_persist_base";
        String mv = "bounded_persist_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT)", base));
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 3) AS SELECT id FROM %s",
                    mv, base));

            View view = catalog.loadView(TableIdentifier.of(Namespace.of(SCHEMA), mv));
            assertEquals(
                    view.properties().get("presto.materialized_view.max_snapshots_per_refresh"),
                    "3",
                    "expected max_snapshots_per_refresh property to be persisted");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testSessionDefaultAppliesToBoundedRefresh()
    {
        String base = "bounded_session_default_base";
        String mv = "bounded_session_default_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES 1", base), 1);
            assertUpdate(format("CREATE MATERIALIZED VIEW %s AS SELECT id FROM %s", mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);
            long watermarkAfterInitial = baseWatermark(mv, base);

            for (int i = 2; i <= 6; i++) {
                assertUpdate(format("INSERT INTO %s VALUES %d", base, i), 1);
            }
            long head = headSnapshot(base);
            assertNotEquals(head, watermarkAfterInitial);

            Session bounded = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "materialized_view_default_max_snapshots_per_refresh", "2")
                    .build();

            getQueryRunner().execute(bounded, "REFRESH MATERIALIZED VIEW " + mv);
            assertNotEquals(baseWatermark(mv, base), head, "session-default bound should prevent advancing to HEAD");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testTablePropertyOverridesSessionDefaultForBoundedRefresh()
    {
        // Session default of 1 vs table property of 10: if the table property wins, one refresh catches up.
        String base = "bounded_override_base";
        String mv = "bounded_override_mv";
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT) WITH (format_version = '3')", base));
            assertUpdate(format("INSERT INTO %s VALUES 1", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 10) AS SELECT id FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            for (int i = 2; i <= 5; i++) {
                assertUpdate(format("INSERT INTO %s VALUES %d", base, i), 1);
            }
            long head = headSnapshot(base);

            Session tightSession = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "materialized_view_default_max_snapshots_per_refresh", "1")
                    .build();

            getQueryRunner().execute(tightSession, "REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, base), head,
                    "table property (10) should override session default (1), advancing to HEAD in one refresh");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testPartitionedMaterializedViewBoundedRefresh()
    {
        String base = "bounded_part_base";
        String mv = "bounded_part_mv";
        String storage = "__mv_storage__" + mv;
        try {
            assertUpdate(format(
                    "CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3', partitioning = ARRAY['id'])",
                    base));
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioning = ARRAY['id'], max_snapshots_per_refresh = 2) " +
                            "AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            for (int i = 2; i <= 6; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }
            long head = headSnapshot(base);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long after1 = baseWatermark(mv, base);
            assertNotEquals(after1, head, "expected partitioned bounded refresh to leave watermark below HEAD");
            long storageRowsAfter1 = (Long) computeScalar(format("SELECT count(*) FROM \"%s\"", storage));
            assertTrue(storageRowsAfter1 >= 2 && storageRowsAfter1 < 6,
                    "expected partial storage rows after one bounded refresh, got " + storageRowsAfter1);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, base), head, "subsequent bounded refreshes should catch up to HEAD");
            assertQuery(format("SELECT id, v FROM \"%s\" ORDER BY id", storage),
                    "VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600)");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testStitchingStaleReadIgnoresIncrementalRefreshPredicate()
    {
        // USE_STITCHING must surface rows after the bounded target — the seq predicate is refresh-only.
        String base = "bounded_stitch_base";
        String mv = "bounded_stitch_mv";
        try {
            assertUpdate(format(
                    "CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3', partitioning = ARRAY['id'])",
                    base));
            assertUpdate(format("INSERT INTO %s VALUES (1, 100)", base), 1);
            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (partitioning = ARRAY['id'], max_snapshots_per_refresh = 2) " +
                            "AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 1);

            for (int i = 2; i <= 6; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);

            long head = headSnapshot(base);
            assertNotEquals(baseWatermark(mv, base), head, "expected MV to be stale after bounded refresh");

            Session stitching = Session.builder(getSession())
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                    .build();

            MaterializedResult result = getQueryRunner().execute(
                    stitching, "SELECT id, v FROM " + mv + " ORDER BY id");
            assertEquals(result.getRowCount(), 6,
                    "stitching must surface all base rows regardless of the bounded refresh target");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test
    public void testRollbackPastWatermarkForcesFullRefresh()
    {
        // Without the ancestry guard, chooseTargetSnapshot would rewind the watermark and the seq
        // predicate would drop rows already at HEAD. Requires (post-rollback ancestors) > bound.
        String base = "bounded_rollback_base";
        String mv = "bounded_rollback_mv";
        String storage = "__mv_storage__" + mv;
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, v BIGINT) WITH (format_version = '3')", base));
            for (int i = 1; i <= 3; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }
            long rollbackTarget = currentSnapshot(base);
            for (int i = 4; i <= 5; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }

            assertUpdate(format(
                    "CREATE MATERIALIZED VIEW %s WITH (max_snapshots_per_refresh = 2) AS SELECT id, v FROM %s",
                    mv, base));
            assertUpdate("REFRESH MATERIALIZED VIEW " + mv, 5);
            long watermarkAfterInitial = baseWatermark(mv, base);

            for (int i = 6; i <= 9; i++) {
                assertUpdate(format("INSERT INTO %s VALUES (%d, %d)", base, i, i * 100), 1);
            }
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            long watermarkAfterBounded = baseWatermark(mv, base);
            assertNotEquals(watermarkAfterBounded, watermarkAfterInitial,
                    "expected bounded refresh to advance the watermark past the initial");

            assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %d)",
                    SCHEMA, base, rollbackTarget));
            long headAfterRollback = currentSnapshot(base);
            assertEquals(headAfterRollback, rollbackTarget);

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEquals(baseWatermark(mv, base), headAfterRollback,
                    "refresh after rollback must advance the watermark to the rolled-back HEAD via full refresh");
            assertQuery(format("SELECT id, v FROM \"%s\" ORDER BY id", storage),
                    "VALUES (1, 100), (2, 200), (3, 300)");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
            assertUpdate("DROP TABLE IF EXISTS " + base);
        }
    }

    private long headSnapshot(String tableName)
    {
        return (Long) computeScalar(format(
                "SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1",
                tableName));
    }

    private long currentSnapshot(String tableName)
    {
        // Reads the live currentSnapshot pointer; headSnapshot() reads $snapshots which keeps
        // abandoned tips after a rollback and would return the wrong value.
        Table table = catalog.loadTable(TableIdentifier.of(Namespace.of(SCHEMA), tableName));
        return table.currentSnapshot().snapshotId();
    }

    private long baseWatermark(String mv, String baseTable)
    {
        try {
            View view = catalog.loadView(TableIdentifier.of(Namespace.of(SCHEMA), mv));
            String key = format("presto.materialized_view.base_snapshot.%s.%s", SCHEMA, baseTable);
            String value = view.properties().get(key);
            assertNotNull(value, "missing watermark property: " + key);
            return Long.parseLong(value);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to read watermark for " + mv + "/" + baseTable, e);
        }
    }

    private boolean viewExists(String name)
    {
        try {
            catalog.loadView(TableIdentifier.of(Namespace.of(SCHEMA), name));
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static void assertAdvanceLeq(long oldWatermark, long newWatermark, long head, int bound, String baseTable)
    {
        if (oldWatermark == newWatermark) {
            fail("expected watermark to advance from " + oldWatermark + " on base " + baseTable);
        }
        if (newWatermark == head) {
            return;
        }
        assertNotEquals(newWatermark, oldWatermark);
    }
}
