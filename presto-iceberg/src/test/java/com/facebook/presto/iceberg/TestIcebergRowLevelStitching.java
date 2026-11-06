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
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.common.RuntimeMetricName.MATERIALIZED_VIEW_ROW_LEVEL_STITCHING_APPLIED;
import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Row-level stitching of a stale aggregating materialized view over an append-only Iceberg V3 base.
 *
 * <p>V3 is required because row-level staleness comes from row lineage, and this connector caps
 * row-level writes at format version 2, so appends are the only base mutation a V3 table can take.
 *
 * <p>Correctness alone does not show the row-level plan was used -- a full recompute also returns
 * the right answer -- so each case asserts the plan shape as well.
 */
@Test(singleThreaded = true)
public class TestIcebergRowLevelStitching
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

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
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of(
                        "experimental.legacy-materialized-views", "false",
                        "experimental.allow-legacy-materialized-views-toggle", "true"))
                .build().getQueryRunner();
    }

    /**
     * Stitching only runs when a stale read is told to stitch; the default is to re-run the view
     * query. Row-level additionally defaults to NEVER while the write path is incomplete.
     */
    private Session stitching(String rowLevelStrategy)
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", rowLevelStrategy)
                .build();
    }

    @DataProvider(name = "partitioning")
    public Object[][] partitioning()
    {
        // Row-level staleness needs no partition metadata, so it must work either way. The
        // unpartitioned case is the one that regressed when row-level sat behind the partition gate.
        return new Object[][] {
                {"", "unpartitioned"},
                {", partitioning = ARRAY['region']", "partitioned"},
        };
    }

    /**
     * The same thing on a format version 2 base, which has no row lineage at all.
     *
     * <p>It does not need any here. Appended rows land only in new files, and a version 2 data file
     * carries the sequence number of the commit that added it, which Presto exposes per row as
     * {@code $data_sequence_number}. Over an append-only range that is the same number version 3
     * records per row, so the delta is identified exactly, not approximately.
     *
     * <p>Worth its own test because every other case here uses a version 3 base, and version 2 is
     * what {@code CREATE TABLE} gives you by default -- so this is the configuration most tables
     * are actually in.
     */
    @Test(dataProvider = "partitioning")
    public void testRowLevelStitchingOverAppendOnlyVersion2Base(String partitioningClause, String label)
    {
        String base = "rls_v2_base_" + label;
        String view = "rls_v2_mv_" + label;
        try {
            // No format-version property: this is the default, which is version 2.
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint)"
                    + (partitioningClause.isEmpty() ? "" : " WITH (" + partitioningClause.substring(2) + ")"));
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 45)");

            String plan = explainView(stitching("ALWAYS"), view);

            // SUM is mergeable, so this stitches by merging rather than by expanding: the storage
            // table contributes every group untouched and the delta contributes an increment for
            // the groups it touched. See the merge-shape assertions in
            // testRowLevelStitchingMergesMergeableAggregate.
            assertTrue(plan.contains("_merged"), "expected the merge aggregation over the union, got:\n" + plan);
            assertFalse(plan.contains("IS_NULL(affected)"),
                    "a merged stitch keeps every storage row, so it must not anti-join them away, got:\n" + plan);

            // One change-tracking column serves every supported format version, so this is the same
            // assertion the version 3 case makes -- which is the point.
            assertTrue(plan.contains("$data_sequence_number"),
                    "expected the changed-rows predicate at the base scan, got:\n" + plan);

            assertFalse(plan.equals(explainView(stitching("NEVER"), view)),
                    "row-level and partition-level produced identical plans, so row-level did not fire");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    @Test(dataProvider = "partitioning")
    public void testRowLevelStitchingOverAppendOnlyBase(String partitioningClause, String label)
    {
        String base = "rls_base_" + label;
        String view = "rls_mv_" + label;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3'" + partitioningClause + ")");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // Append only: one row into an existing group, one creating a new group.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            // The stitched read must equal a direct aggregate over the base: no duplicated rows from
            // the two branches overlapping, and no rows missed by the fresh branch being over-excluded.
            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 45)");

            String plan = explainView(stitching("ALWAYS"), view);

            // SUM is mergeable, so the stitch merges the stored value with the delta's increment
            // instead of expanding the delta back to its groups' other rows.
            assertTrue(plan.contains("_merged"), "expected the merge aggregation over the union, got:\n" + plan);
            assertFalse(plan.contains("IS_NULL(affected)"),
                    "a merged stitch keeps every storage row, so it must not anti-join them away, got:\n" + plan);

            // The changed-rows predicate at the base scan is what makes this row-level rather than
            // partition-level. A version 3 base uses the same column a version 2 base does: on an
            // append-only range a file's sequence number is its rows' sequence number, and this
            // connector cannot produce any other kind of range on a version 3 table.
            assertTrue(plan.contains("$data_sequence_number"),
                    "expected the changed-rows predicate at the base scan, got:\n" + plan);

            // And it is genuinely a different plan from the partition-level candidate.
            assertFalse(plan.equals(explainView(stitching("NEVER"), view)),
                    "row-level and partition-level produced identical plans, so row-level did not fire");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * A mergeable view stitches by merging, and that is the shape worth pinning: the delta
     * aggregates only the appended rows, and re-aggregating the union adds each group's increment
     * onto its stored value.
     *
     * <p>What this buys is the point of the whole path. Expanding a delta back to its groups' other
     * rows costs a scan of the entire base, because the affected keys are only known once the delta
     * has been read and so cannot prune anything. Merging reads the storage table -- one row per
     * group -- and the new files, and nothing else. So this asserts the absence of the expansion:
     * no cross join, and no anti-join removing the affected groups from the fresh branch, because a
     * group the delta never touched merges with nothing and sums to itself.
     *
     * <p>COUNT is in the view alongside SUM because counts merge by being added, not by being
     * counted again -- merging a COUNT with a COUNT would return the number of rows in the union,
     * which is 2 for a touched group regardless of what it actually holds.
     */
    @Test(dataProvider = "partitioning")
    public void testRowLevelStitchingMergesMergeableAggregate(String partitioningClause, String label)
    {
        String base = "rls_merge_base_" + label;
        String view = "rls_merge_mv_" + label;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3'" + partitioningClause + ")");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // One row into an existing group, one creating a group that storage has never seen.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            // NA merges 40+5 and 2+1; EU is untouched and must survive unchanged; APAC exists only
            // in the delta and must still be emitted.
            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total, n FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7, 1), ('EU', 20, 1), ('NA', 45, 3)");

            String plan = explainView(stitching("ALWAYS"), view);

            assertTrue(plan.contains("_merged"), "expected the merge aggregation over the union, got:\n" + plan);
            assertFalse(plan.contains("CrossJoin"),
                    "merging must not expand the delta back to the affected groups, got:\n" + plan);
            assertFalse(plan.contains("IS_NULL(affected)"),
                    "a merged stitch keeps every storage row, so it must not anti-join them away, got:\n" + plan);
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * An aggregate that cannot be merged is not stitched at row level at all.
     *
     * <p>MAX is the case: appends merge it fine, but the stored maximum cannot absorb a removal --
     * a delete can take away the extreme and no stored value recovers it -- so it is excluded
     * rather than merged on the strength of the range happening to be append-only.
     *
     * <p>Having excluded it from merging, the remaining option would be to recompute each affected
     * group from the base. That is correct but reads the whole base, because the affected keys are
     * runtime values that cannot prune the scan, and it measured as more expensive than recomputing
     * the view outright. So it declines instead, and the assertion is that the resulting plan is
     * exactly the partition-level one.
     */
    @Test(dataProvider = "partitioning")
    public void testNonMergeableAggregateDeclinesRowLevel(String partitioningClause, String label)
    {
        String base = "rls_decline_base_" + label;
        String view = "rls_decline_mv_" + label;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3'" + partitioningClause + ")");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, MAX(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // The appended NA row is below the group's existing maximum, so a delta aggregated on
            // its own would report 5 and a merge that trusted it would be wrong. Expansion reads
            // NA's other rows and recomputes 30.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 30)");

            String plan = explainView(stitching("ALWAYS"), view);

            assertFalse(plan.contains("_merged"), "MAX is not mergeable, so it must not be merged, got:\n" + plan);
            // And it must not be expanded either: declining leaves the partition-level candidate,
            // whose plan this must therefore be identical to.
            assertFalse(plan.contains("IS_NULL(affected)"),
                    "a non-mergeable aggregate must decline rather than expand, got:\n" + plan);
            assertEquals(plan, explainView(stitching("NEVER"), view),
                    "declining row-level must leave exactly the partition-level plan, got:\n" + plan);
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * A format version 1 base must not take the row-level path at all.
     *
     * <p>Sequence numbers arrive in version 2. Version 1 reports zero for every snapshot and every
     * data file, so "rows newer than the recorded sequence number" matches nothing -- the change set
     * comes back empty, which is indistinguishable from nothing having changed. The failure is
     * silent in the worst way: the stale read returns the materialized rows unchanged and a refresh
     * writes nothing while still advancing the watermark, after which the view calls itself fresh
     * and no later refresh will ever repair it.
     *
     * <p>So this asserts the answer, not the plan. Whichever path the engine picks, a version 1 base
     * must produce the same rows as the view query -- and before the version guard it did not: an
     * appended group went missing and an updated group kept its old total.
     */
    @Test(dataProvider = "partitioning")
    public void testVersion1BaseDoesNotTakeRowLevelPath(String partitioningClause, String label)
    {
        String base = "rls_v1_base_" + label;
        String view = "rls_v1_mv_" + label;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '1'" + partitioningClause + ")");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // One row into an existing group and one creating a new group: under the defect the
            // first kept its stale total and the second vanished entirely.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 100), ('APAC', 7)");

            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 110)");

            // And the refresh must land the same rows rather than committing an empty delta.
            assertQuerySucceeds(stitching("ALWAYS"), "REFRESH MATERIALIZED VIEW " + view);
            assertQuery(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 110)");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * A view with no {@code GROUP BY} must not take the row-level path either.
     *
     * <p>A global aggregate is one row that no grouping key names. The fresh branch has no
     * identifier to exclude the stale row by, and the delta's aggregate over the appended rows is a
     * partial value rather than a replacement, so unioning them emits both: the old total and the
     * increment, side by side, where the view defines a single row.
     *
     * <p>That is exactly what happened while {@code classifyViewShape} counted only grouping
     * aggregations -- a global aggregate fell through to row-preserving, whose additions-only
     * shortcut deliberately keeps every storage row. Here it must decline and fall back.
     */
    @Test
    public void testGlobalAggregateDoesNotTakeRowLevelPath()
    {
        String base = "rls_global_base";
        String view = "rls_global_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT SUM(amount) AS total, COUNT(*) AS n FROM " + base);
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5)");

            // One row, 35 and 3 -- not two rows of (30, 2) and (5, 1).
            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT total, n FROM " + view,
                    "VALUES (35, 3)");
            assertEquals(
                    computeActual(stitching("ALWAYS"), "SELECT total FROM " + view).getRowCount(),
                    1,
                    "a global aggregate defines exactly one row");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * A view built from a set operation must not take the row-level path.
     *
     * <p>{@code EXCEPT} is the one that proved this matters. It is anti-monotonic in its right
     * input: appending a row to the right side has to *remove* a row from the view. Row-preserving
     * means one output row per base row, which earns the additions-only shortcut -- keep every
     * materialized row, let the delta supply the new ones. For {@code EXCEPT} that is exactly wrong,
     * and the view went on returning a row the base no longer contained, with no warning.
     *
     * <p>It was classified row-preserving because the shape test only looked for joins and
     * aggregations and defaulted everything else to row-preserving. The delta builder does visit set
     * operations, so nothing downstream stopped it either. Node types are now a whitelist, which
     * declines this and anything else not explicitly reasoned about.
     */
    @Test
    public void testSetOperationViewDoesNotTakeRowLevelPath()
    {
        String left = "rls_setop_left";
        String right = "rls_setop_right";
        String view = "rls_setop_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + left + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("CREATE TABLE " + right + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + left + " VALUES ('NA', 10), ('EU', 20)");
            assertQuerySucceeds("INSERT INTO " + right + " VALUES ('NA', 10)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, amount FROM " + left + " EXCEPT SELECT region, amount FROM " + right);
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);
            // Storage now holds EU/20.

            // Appending to the RIGHT side must take EU/20 out of the view.
            assertQuerySucceeds("INSERT INTO " + right + " VALUES ('EU', 20)");

            MaterializedResult stitched = computeActual(stitching("ALWAYS"), "SELECT region, amount FROM " + view);
            assertEquals(stitched.getRowCount(), 0,
                    "appending to the right of an EXCEPT must remove the row from the view, got: " + stitched.getMaterializedRows());
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + left);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + right);
        }
    }

    /**
     * The row-preserving case across both format versions and both storage layouts.
     *
     * <p>This is the shape most append-only views actually are -- a projection with a filter over an
     * append-only base -- and the one the additions-only shortcut exists to serve: the materialized
     * rows all stay, and the delta contributes the appended rows that pass the filter. Nothing has
     * to be excluded, because an append cannot invalidate a row already materialized.
     *
     * <p>Parametrised because the shortcut needs no column equivalence and no partition metadata, so
     * it should be indifferent to partitioning, and version 2 reaches it through
     * {@code $data_sequence_number} exactly as version 3 does through row lineage.
     */
    @Test(dataProvider = "partitioning")
    public void testRowLevelStitchingOverRowPreservingView(String partitioningClause, String label)
    {
        for (String formatVersion : new String[] {"2", "3"}) {
            String base = "rls_rp_base_" + label + "_v" + formatVersion;
            String view = "rls_rp_mv_" + label + "_v" + formatVersion;
            try {
                assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                        + "WITH (\"format-version\" = '" + formatVersion + "'" + partitioningClause + ")");
                assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 3)");

                assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                        + " AS SELECT region, amount FROM " + base + " WHERE amount > 5");
                assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

                // One row into an existing group, one creating a group, and one the filter excludes.
                assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 100), ('APAC', 7), ('EU', 1)");

                assertQuery(
                        stitching("ALWAYS"),
                        "SELECT region, amount FROM " + view + " ORDER BY region, amount",
                        "VALUES ('APAC', 7), ('EU', 20), ('NA', 10), ('NA', 100)");

                String plan = explainViewColumns(stitching("ALWAYS"), view, "region, amount");
                assertFalse(plan.contains("IS_NULL(affected)"),
                        "an append cannot invalidate a materialized row, so nothing may be excluded, got:\n" + plan);
                assertFalse(plan.contains("_merged"), "a row-preserving view has nothing to merge, got:\n" + plan);
            }
            finally {
                assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
                assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
            }
        }
    }

    /**
     * Row-level stitching reports that it fired.
     *
     * <p>Declines are reported as warnings, but success was reported nowhere, so a reader of a query
     * could not distinguish "row-level was used" from "row-level was never eligible" -- both return a
     * clean result with nothing to see. Every defect found in this feature was silent in exactly that
     * way, which is why the positive signal is worth a test of its own.
     *
     * <p>Asserted as a count rather than a flag: the metric is session-scoped, so a query touching two
     * materialized views contributes twice, and the iterative optimizer may in principle apply a rule
     * more than once. The claim is that it fired at all, and that a declining view does not claim it.
     */
    @Test
    public void testRowLevelStitchingIsReported()
    {
        String base = "rls_metric_base";
        String merged = "rls_metric_mv";
        String declined = "rls_metric_min_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + merged
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            // MIN cannot be merged, so this one must decline and must not claim the metric.
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + declined
                    + " AS SELECT region, MIN(amount) AS lo FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + merged);
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + declined);
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5)");

            assertTrue(stitchingApplied("SELECT region, total FROM " + merged) >= 1,
                    "a merged row-level stitch must report that it fired");
            assertEquals(stitchingApplied("SELECT region, lo FROM " + declined), 0,
                    "a view that declines row-level must not report that it fired");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + merged);
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + declined);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * The metric as the finished query reports it.
     *
     * <p>Read from the query's own stats rather than from the session handed to execute: the
     * coordinator rebuilds the session per query with its own RuntimeStats, so the instance the
     * optimizer writes to is not the one the caller holds.
     */
    private long stitchingApplied(String query)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        QueryId queryId = runner.executeWithQueryId(stitching("ALWAYS"), query).getQueryId();
        RuntimeMetric metric = runner.getQueryInfo(queryId).getQueryStats().getRuntimeStats()
                .getMetric(MATERIALIZED_VIEW_ROW_LEVEL_STITCHING_APPLIED);
        return metric == null ? 0 : metric.getCount();
    }

    /**
     * Random append and refresh sequences, asserting the view equals its own query after every step.
     *
     * <p>Every defect this feature had was a shape or a sequence nobody had enumerated: a format
     * version 1 base serving stale rows as fresh, a global aggregate emitting two rows where the view
     * defines one, an EXCEPT view keeping a row its base no longer held. Each was found by reading
     * code, not by a test. A test that generates sequences rather than enumerating them is the cheap
     * way to find the next one.
     *
     * <p>Asserts the four properties the design calls invariants, for both supported shapes:
     *
     * <ul>
     *   <li>no missing rows and no duplicate rows -- the stitched read equals the view query exactly,
     *       which an ordered comparison of the full result checks in both directions at once;
     *   <li>snapshot equivalence -- the comparison is made against the base as it stands at that
     *       moment, so a refresh landing mid-sequence must not change the answer;
     *   <li>monotonic freshness -- a refresh never makes the view less correct, which follows from
     *       the assertion holding on both sides of every refresh.
     * </ul>
     *
     * <p>Seeded deterministically so a failure reproduces, and run over more than one seed because a
     * single sequence is not a search. Kept deliberately short -- two seeds, twelve steps -- so it
     * does not dominate the suite's runtime; adding seeds is how to widen it when hunting.
     *
     * <p>Verified to have teeth: making COUNT merge with COUNT instead of SUM, the classic error this
     * guards, fails every seed by step one with the diverging row named.
     */
    @DataProvider(name = "seeds")
    public Object[][] seeds()
    {
        return new Object[][] {{20261016L}, {999983L}};
    }

    @Test(dataProvider = "seeds")
    public void testRandomAppendSequencesPreserveTheView(long seed)
    {
        Random random = new Random(seed);
        String base = "rls_rand_base_" + seed;
        String aggregate = "rls_rand_agg_mv_" + seed;
        String projection = "rls_rand_proj_mv_" + seed;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3', partitioning = ARRAY['region'])");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('APAC', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + aggregate
                    + " WITH (partitioning = ARRAY['region']) AS "
                    + "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM " + base + " GROUP BY region");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + projection
                    + " AS SELECT region, amount FROM " + base + " WHERE amount > 5");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + aggregate);
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + projection);

            String[] regions = {"NA", "EU", "APAC", "SA", "AF"};
            for (int step = 0; step < 12; step++) {
                // Append a batch, sometimes touching a group that does not exist yet, sometimes
                // appending a value the projection view's filter excludes.
                StringBuilder values = new StringBuilder();
                int rows = 1 + random.nextInt(4);
                for (int row = 0; row < rows; row++) {
                    if (row > 0) {
                        values.append(", ");
                    }
                    values.append("('")
                            .append(regions[random.nextInt(regions.length)])
                            .append("', ")
                            .append(random.nextInt(12))
                            .append(")");
                }
                assertQuerySucceeds("INSERT INTO " + base + " VALUES " + values);

                assertViewMatchesItsQuery(seed, step, aggregate,
                        "SELECT region, total, n FROM " + aggregate + " ORDER BY region",
                        "SELECT region, SUM(amount), COUNT(*) FROM " + base + " GROUP BY region ORDER BY region");
                assertViewMatchesItsQuery(seed, step, projection,
                        "SELECT region, amount FROM " + projection + " ORDER BY region, amount",
                        "SELECT region, amount FROM " + base + " WHERE amount > 5 ORDER BY region, amount");

                // Refresh one of them at random, so the assertions above straddle refreshes.
                if (random.nextBoolean()) {
                    assertQuerySucceeds(refreshSession(), "REFRESH MATERIALIZED VIEW "
                            + (random.nextBoolean() ? aggregate : projection));
                }
            }
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + aggregate);
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + projection);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * Asserts a stitched read of the view equals running the view's own query over the base now.
     *
     * <p>Compared as two Presto queries rather than through assertQuery, whose expected side runs
     * against H2 and cannot see the Iceberg tables.
     */
    private void assertViewMatchesItsQuery(long seed, int step, String view, String readView, String readBase)
    {
        assertEquals(
                computeActual(stitching("ALWAYS"), readView).getMaterializedRows(),
                computeActual(readBase).getMaterializedRows(),
                "stitched read of " + view + " diverged from its own query at step " + step + " (seed " + seed + ")");
    }

    /**
     * REFRESH recomputes a mergeable view from its stored aggregates plus the appended rows.
     *
     * <p>The assertion that matters is the stored value surviving. Reading the materialized view's
     * own storage table from inside the statement that overwrites it is the trap here: the handle the
     * refresh node carries is the write target, which Iceberg names "&lt;table&gt;$data", and scanning
     * it yields the output snapshot -- empty. A plan built that way replaces each affected group with
     * the delta alone and leaves every other group intact, so the result looks plausible: the group
     * that was appended to reads as though it only ever held the appended row.
     *
     * <p>So this appends to a group whose stored total is far larger than the increment. If the
     * stored value were being discarded the total would collapse to the increment, which an equality
     * check against the base catches immediately.
     *
     * <p>Also asserts the plan merged rather than expanded. Correctness alone would not show which
     * ran: recomputing the affected group from the base returns the same answer, only by reading the
     * whole base to get it.
     */
    @Test
    public void testRefreshMergesStoredAggregates()
    {
        String base = "rls_mrefresh_base";
        String view = "rls_mrefresh_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (g varchar, v bigint) "
                    + "WITH (\"format-version\" = '3', partitioning = ARRAY['g'])");
            // Large stored values, so discarding them is unmistakable.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('a', 1000000), ('a', 2000000), ('b', 3000000)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['g'], refresh_type = 'INCREMENTAL') AS "
                    + "SELECT g, SUM(v) AS total, COUNT(*) AS n FROM " + base + " GROUP BY g");
            assertQuerySucceeds(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            // One small row into an existing group, and one creating a group storage has never seen.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('a', 7), ('c', 5)");

            Session refresh = refreshSession();
            String plan = (String) computeActual(refresh,
                    "EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW " + view)
                    .getMaterializedRows().get(0).getField(0);
            assertTrue(plan.contains("_merged"),
                    "refresh must merge the stored aggregates rather than recompute them, got:\n" + plan);
            assertFalse(plan.contains("CrossJoin"),
                    "merging must not expand the delta back to the affected groups, got:\n" + plan);

            computeActual(refresh, "REFRESH MATERIALIZED VIEW " + view);

            // a = 3000007 across 3 rows: the stored 3000000 plus the appended 7. Were the stored
            // value discarded this would read 7.
            assertEquals(
                    computeActual("SELECT g, total, n FROM " + view + " ORDER BY g").getMaterializedRows(),
                    computeActual("SELECT g, SUM(v), COUNT(*) FROM " + base + " GROUP BY g ORDER BY g").getMaterializedRows(),
                    "refreshed view must equal a direct aggregate over the base");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * The whole feature rests on an appended row being distinguishable from an untouched one. If
     * row lineage did not carry the older rows' sequence number through, the changed-rows predicate
     * would match everything and the row-level delta would silently recompute the whole table.
     */
    @Test
    public void testAppendedRowsCarryAHigherSequenceNumber()
    {
        String table = "rls_lineage";
        try {
            assertQuerySucceeds("CREATE TABLE " + table + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + table + " VALUES ('NA', 10), ('EU', 20)");

            long recordedSequenceNumber = (long) computeActual(
                    "SELECT max(\"_last_updated_sequence_number\") FROM " + table)
                    .getMaterializedRows().get(0).getField(0);

            assertQuerySucceeds("INSERT INTO " + table + " VALUES ('NA', 5), ('APAC', 7)");

            assertQuery(
                    "SELECT count(*) FROM " + table + " WHERE \"_last_updated_sequence_number\" > " + recordedSequenceNumber,
                    "SELECT 2");
            assertQuery(
                    "SELECT count(*) FROM " + table + " WHERE \"_last_updated_sequence_number\" IS NULL",
                    "SELECT 0");
            // Row identity has to be present for the change set to name an affected row at all.
            assertQuery("SELECT count(DISTINCT \"_row_id\") FROM " + table, "SELECT 4");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Incremental refresh has to be asked for: the default refresh type is FULL, which returns the
     * unmodified view query before any delta is considered.
     */
    private Session refreshSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_default_refresh_type", "INCREMENTAL")
                .setSystemProperty("materialized_view_incremental_refresh_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    /**
     * A view that only projects and filters has one output row per base row, so when the change
     * range only added rows there is nothing for the fresh branch to exclude: an addition cannot
     * invalidate a row already in storage, and the added rows are not in storage yet. The stitched
     * read is the storage table unioned with the projected delta, needing neither an anti-join nor
     * a row-origin column.
     */
    @Test
    public void testRowLevelStitchingOverProjectionOnlyView()
    {
        String base = "rlsp_base";
        String view = "rlsp_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 2)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, amount FROM " + base + " WHERE amount > 5");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // Additions only: one row passing the view filter, one failing it.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('APAC', 7), ('NA', 1)");

            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, amount FROM " + view + " ORDER BY region, amount",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 10)");

            String plan = explainProjectionView(stitching("ALWAYS"), view);
            assertTrue(plan.contains("$data_sequence_number"),
                    "expected the changed-rows predicate at the base scan, got:\n" + plan);
            assertFalse(plan.contains("IS_NULL(affected)"),
                    "an additions-only projection view needs no anti-join over the storage table, got:\n" + plan);
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    private String explainProjectionView(Session session, String view)
    {
        MaterializedResult result = computeActual(session,
                "EXPLAIN (TYPE LOGICAL) SELECT region, amount FROM " + view + " ORDER BY region, amount");
        assertEquals(result.getMaterializedRows().size(), 1);
        return (String) result.getMaterializedRows().get(0).getField(0);
    }

    private String explainViewColumns(Session session, String view, String columns)
    {
        MaterializedResult result = computeActual(session,
                "EXPLAIN (TYPE LOGICAL) SELECT " + columns + " FROM " + view);
        assertEquals(result.getMaterializedRows().size(), 1);
        return (String) result.getMaterializedRows().get(0).getField(0);
    }

    private String explainView(Session session, String view)
    {
        MaterializedResult result = computeActual(session,
                "EXPLAIN (TYPE LOGICAL) SELECT region, total FROM " + view + " ORDER BY region");
        assertEquals(result.getMaterializedRows().size(), 1);
        return (String) result.getMaterializedRows().get(0).getField(0);
    }

    /**
     * REFRESH MATERIALIZED VIEW does not take a row-level plan, and is still correct without one.
     *
     * <p>Recomputing a changed group means rereading that group's other base rows. The affected keys
     * are only known once the delta has been read, so nothing prunes that scan: measured at 200,049
     * rows and 251-489ms CPU against the partition-level refresh's 4,010 rows and 28ms, for identical
     * output. Partition-level derives its stale set from manifest metadata, a plan-time constant that
     * does prune, and it is reachable in every case row-level would have been -- so the cheaper plan
     * is always the one available.
     *
     * <p>The stale-read path is different, and does use row-level: it reuses the stored aggregates
     * rather than rereading the base, so it needs no scan to prune. Bringing that here additionally
     * requires the storage branch restricted to the affected groups, since emitting every group
     * writes every partition and exceeds the open-writer limit on any view with more groups than it.
     */
    @Test
    public void testRefreshDoesNotTakeRowLevelPath()
    {
        String base = "rlr_base";
        String view = "rlr_mv";
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3', partitioning = ARRAY['region'])");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL')"
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);
            assertQuery("SELECT region, total FROM " + view + " ORDER BY region", "VALUES ('EU', 20), ('NA', 40)");

            // Append only: adds to an existing group and creates a new one.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            Session refresh = refreshSession();
            MaterializedResult explained = computeActual(refresh,
                    "EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW " + view);
            String refreshPlan = (String) explained.getMaterializedRows().get(0).getField(0);
            assertFalse(refreshPlan.contains("IS_NULL(affected)"),
                    "refresh must not take a row-level plan, got:\n" + refreshPlan);

            computeActual(refresh, "REFRESH MATERIALIZED VIEW " + view);

            // The refreshed view must equal a direct aggregate over the base: unaffected groups
            // preserved, affected groups recomputed, nothing duplicated or dropped.
            assertQuery(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 45)");
            // Compared as two Presto queries: the expected side of assertQuery runs against H2,
            // which does not have the Iceberg tables.
            assertEquals(
                    computeActual("SELECT region, total FROM " + view + " ORDER BY region").getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region").getMaterializedRows(),
                    "refreshed view must equal a direct aggregate over the base");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }
}
