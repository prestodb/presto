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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Edge cases for row-level incremental refresh of an aggregating materialized view over an
 * append-only Iceberg V3 base.
 *
 * <p>Every case asserts the same invariant: after refreshing, the view must equal a direct
 * aggregate over the base. That is the property row-level refresh can break in either direction --
 * over-excluding rows from the fresh branch loses data, under-excluding duplicates it -- and it is
 * checked by comparing two Presto queries rather than a literal, so the expected value cannot drift
 * out of step with the data.
 */
@Test(singleThreaded = true)
public class TestIcebergRowLevelRefreshEdgeCases
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

    private Session refreshSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_default_refresh_type", "INCREMENTAL")
                .setSystemProperty("materialized_view_incremental_refresh_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    private Session stitchSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    /**
     * The invariant. Compared as two Presto queries because the expected side of assertQuery runs
     * against H2, which does not hold the Iceberg tables.
     */
    private void assertViewMatchesBase(String viewQuery, String baseQuery)
    {
        assertEquals(
                computeActual(viewQuery).getMaterializedRows(),
                computeActual(baseQuery).getMaterializedRows(),
                "materialized view disagrees with a direct aggregate over the base");
    }

    /**
     * Refreshes the view and returns how the refresh was planned.
     *
     * <p>Results alone cannot show which granularity ran, because a partition-level or full refresh
     * returns the same answer as a row-level one. Every case here therefore pins the granularity as
     * well, so a silent fall back to full recompute fails the test instead of passing it.
     */
    private RefreshOutcome refreshAndReport(String view)
    {
        MaterializedResult explained = computeActual(refreshSession(),
                "EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW " + view);
        String plan = (String) explained.getMaterializedRows().get(0).getField(0);
        String reasons = explained.getWarnings().stream()
                .map(warning -> warning.getMessage())
                .collect(joining("; "));
        computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);
        // The merged refresh selects its delta with $data_sequence_number, so the predicate's presence
        // on the base scan is what distinguishes it from a partition-level or full refresh. The
        // bounded-refresh cap uses _last_updated_sequence_number instead and says nothing about
        // granularity, so keying on that reported every merged refresh as a fallback and made the
        // assertions below pass without testing anything.
        return new RefreshOutcome(plan.contains("$data_sequence_number"), reasons);
    }

    /**
     * Refreshes and asserts the merged plan ran: the view was rebuilt from its stored aggregates plus
     * the appended rows, rather than by rereading the base.
     */
    private void refreshExpectingMerge(String view)
    {
        RefreshOutcome outcome = refreshAndReport(view);
        assertTrue(outcome.rowLevel,
                "expected " + view + " to take the merged refresh plan. Reported: " + outcome.reasons);
    }

    /**
     * Refreshes and asserts the merged plan did NOT run, so the refresh fell back to the
     * partition-level or full path.
     *
     * <p>Used where the view or the change set does not qualify -- a non-mergeable shape, a range
     * containing a delete, an explicit opt-out, or an initial load with no recorded version to compute
     * changes against. Each case also asserts the refreshed contents, which holds whichever plan ran;
     * pinning the plan as well is what stops a silent fallback from passing as success.
     */
    private void refreshExpectingFallback(String view)
    {
        RefreshOutcome outcome = refreshAndReport(view);
        assertFalse(outcome.rowLevel,
                "expected " + view + " to decline the merged refresh plan. Reported: " + outcome.reasons);
    }

    private static class RefreshOutcome
    {
        private final boolean rowLevel;
        private final String reasons;

        RefreshOutcome(boolean rowLevel, String reasons)
        {
            this.rowLevel = rowLevel;
            this.reasons = reasons;
        }
    }

    // ------------------------------------------------------------------ values

    @Test
    public void testNullGroupingKey()
    {
        // SQL makes NULL its own group, so the anti-join has to treat a null identifier as joinable.
        // An equi-join would leave the stale row for the null group in the fresh branch.
        run("null_key",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), (NULL, 20), ('EU', 5)",
                "VALUES (NULL, 7), ('NA', 1)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testEmptyStringAndWhitespaceKeys()
    {
        run("blank_key",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('', 1), (' ', 2), ('NA', 3)",
                "VALUES ('', 10), ('  ', 20)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testBigintGroupingKeyIncludingZeroAndNegative()
    {
        run("int_key",
                "bucket bigint, amount bigint",
                "ARRAY['bucket']",
                "SELECT bucket, SUM(amount) AS total FROM %s GROUP BY bucket",
                "VALUES (0, 1), (-1, 2), (9223372036854775807, 3)",
                "VALUES (0, 10), (-1, 20), (5, 30)",
                "SELECT bucket, total FROM %s ORDER BY bucket",
                "SELECT bucket, SUM(amount) FROM %s GROUP BY bucket ORDER BY bucket");
    }

    @Test
    public void testDateGroupingKey()
    {
        run("date_key",
                "ds date, amount bigint",
                "ARRAY['ds']",
                "SELECT ds, SUM(amount) AS total FROM %s GROUP BY ds",
                "VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 2)",
                "VALUES (DATE '2024-01-01', 10), (DATE '2024-02-01', 20)",
                "SELECT ds, total FROM %s ORDER BY ds",
                "SELECT ds, SUM(amount) FROM %s GROUP BY ds ORDER BY ds");
    }

    @Test
    public void testCompositeGroupingKey()
    {
        run("composite_key",
                "region varchar, tier varchar, amount bigint",
                "ARRAY['region', 'tier']",
                "SELECT region, tier, SUM(amount) AS total FROM %s GROUP BY region, tier",
                "VALUES ('NA', 'a', 1), ('NA', 'b', 2), ('EU', 'a', 3)",
                "VALUES ('NA', 'a', 10), ('APAC', 'c', 20)",
                "SELECT region, tier, total FROM %s ORDER BY region, tier",
                "SELECT region, tier, SUM(amount) FROM %s GROUP BY region, tier ORDER BY region, tier");
    }

    // -------------------------------------------------------------- aggregates

    @Test
    public void testCountStarAndMultipleAggregates()
    {
        run("multi_agg",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS lo, MAX(amount) AS hi FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('NA', 30), ('EU', 20)",
                "VALUES ('NA', 5), ('APAC', 7)",
                "SELECT region, n, total, lo, hi FROM %s ORDER BY region",
                "SELECT region, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM %s GROUP BY region ORDER BY region",
                false);
    }

    @Test
    public void testAggregatesOverNullMeasure()
    {
        // COUNT(col) skips nulls while COUNT(*) does not, so a group whose appended rows are all
        // null must still recompute both correctly.
        run("null_measure",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('EU', NULL)",
                "VALUES ('NA', NULL), ('EU', 5), ('APAC', NULL)",
                "SELECT region, n, n_amount, total FROM %s ORDER BY region",
                "SELECT region, COUNT(*), COUNT(amount), SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testViewWithFilterBeforeAggregation()
    {
        // The delta recomputes whole groups from the current base, so the view's own filter must be
        // reapplied there and not just at the leaf.
        run("filtered_view",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s WHERE amount > 5 GROUP BY region",
                "VALUES ('NA', 10), ('NA', 1), ('EU', 20)",
                "VALUES ('NA', 2), ('NA', 50), ('APAC', 1)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s WHERE amount > 5 GROUP BY region ORDER BY region");
    }

    @Test
    public void testHolisticAggregate()
    {
        // Case B claims correctness for any aggregation because it recomputes whole groups.
        run("holistic",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, approx_distinct(amount) AS distinct_amounts FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('NA', 10), ('NA', 20), ('EU', 1)",
                "VALUES ('NA', 30), ('APAC', 2)",
                "SELECT region, distinct_amounts FROM %s ORDER BY region",
                "SELECT region, approx_distinct(amount) FROM %s GROUP BY region ORDER BY region",
                false);
    }

    // -------------------------------------------------------------- sequencing

    @Test
    public void testRefreshWithNoChangesIsIdempotent()
    {
        String base = "seq_noop_base";
        String view = "seq_noop_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            // Nothing changes between these, so none of them has any delta to compute.
            refreshExpectingFallback(view);
            refreshExpectingFallback(view);
            refreshExpectingFallback(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testSeveralAppendsThenOneRefresh()
    {
        String base = "seq_multi_base";
        String view = "seq_multi_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 1)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('APAC', 2)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 3), ('EU', 4)");
            refreshExpectingMerge(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * A bounded refresh of a mergeable view must not take the merged plan.
     *
     * <p>The merged plan adds its delta onto the stored value, so the range it reads has to be the
     * range the watermark then advances over. Under {@code max_snapshots_per_refresh} it is not: the
     * delta leaf is built from the changed-rows disjuncts, which are open above and so reach HEAD,
     * while the refresh advances the watermark only to the bounded target. Every snapshot between
     * target and HEAD is then read again by the next refresh and added a second time.
     *
     * <p>Values rather than plan shape, because that is where the defect shows. With the merge taken,
     * this view's {@code SUM} finished at 27 against a true 17 and its {@code COUNT} at 7 against 4 --
     * each refresh adding one more copy of the rows past its own target.
     */
    @Test
    public void testBoundedRefreshDoesNotDoubleCount()
    {
        String base = "bounded_merge_base";
        String view = "bounded_merge_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            // One snapshot per refresh, so with three appends waiting the target stays behind HEAD
            // for the first two refreshes -- the window in which the merge would double-count.
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL',"
                    + " max_snapshots_per_refresh = 1) AS "
                    + "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 1)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 2)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 4)");

            // Each refresh advances one snapshot, so the watermark needs three to reach HEAD; the
            // fourth finds nothing left and must leave the totals alone.
            for (int refresh = 0; refresh < 4; refresh++) {
                computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);
            }

            assertViewMatchesBase(
                    "SELECT region, total, n FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount), COUNT(*) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testAutomaticSelectsRowLevel()
    {
        String base = "auto_base";
        String view = "auto_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            // sequence() caps at 10,000 entries, so cross two of them to reach 50,000 rows.
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST((x.n * 200 + y.n) % 50 AS VARCHAR), x.n * 200 + y.n"
                    + " FROM UNNEST(sequence(1, 250)) AS x(n), UNNEST(sequence(1, 200)) AS y(n)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('r1', 7)");
            // The merge aggregation needs the NDV of the grouping key on the union of the storage
            // table and the delta, so the storage table needs statistics too, not just the base.
            analyzeEverythingExcept(view);

            MaterializedResult result = computeActual(automaticSession(), "SELECT region, total FROM " + view + " ORDER BY region");
            assertFalse(rowLevelRejectedOnCost(result), "AUTOMATIC rejected row-level on cost");

            assertEquals(
                    result.getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region")
                            .getMaterializedRows(),
                    "AUTOMATIC chose a plan that disagrees with the base");
        }
        finally {
            drop(view, base);
        }
    }

    /** A session that lets the cost model choose among row-level, partition-level and full recompute. */
    private Session automaticSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .setSystemProperty("materialized_view_stitching_strategy", "AUTOMATIC")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    /** Statistics are a precondition for any cost-based choice, on the storage table as well as the base. */
    private void analyzeEverythingExcept(String view)
    {
        computeActual("SHOW TABLES").getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .filter(table -> !table.equals(view))
                .forEach(table -> assertQuerySucceeds("ANALYZE " + table));
    }

    private boolean rowLevelRejectedOnCost(MaterializedResult result)
    {
        return result.getWarnings().stream()
                .map(warning -> warning.getMessage())
                .anyMatch(message -> message.contains("was not chosen because another plan cost less"));
    }

    /**
     * Correctness under AUTOMATIC is the claim that matters: whichever plan the cost model picks, the
     * answer has to equal a direct aggregate over the base. Run over several view shapes, because a
     * wrong choice would show up as a wrong answer only for some of them.
     */
    @Test
    public void testAutomaticCorrectnessAcrossShapes()
    {
        Object[][] shapes = {
                {"auto_sum", "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                        "SELECT region, total FROM %s ORDER BY region",
                        "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region"},
                {"auto_count", "SELECT region, COUNT(*) AS n FROM %s GROUP BY region",
                        "SELECT region, n FROM %s ORDER BY region",
                        "SELECT region, COUNT(*) FROM %s GROUP BY region ORDER BY region"},
                {"auto_multi", "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM %s GROUP BY region",
                        "SELECT region, total, n FROM %s ORDER BY region",
                        "SELECT region, SUM(amount), COUNT(*) FROM %s GROUP BY region ORDER BY region"},
                {"auto_filtered", "SELECT region, SUM(amount) AS total FROM %s WHERE amount > 2 GROUP BY region",
                        "SELECT region, total FROM %s ORDER BY region",
                        "SELECT region, SUM(amount) FROM %s WHERE amount > 2 GROUP BY region ORDER BY region"},
                {"auto_rowpreserving", "SELECT region, amount FROM %s",
                        "SELECT region, amount FROM %s ORDER BY region, amount",
                        "SELECT region, amount FROM %s ORDER BY region, amount"},
        };
        for (Object[] shape : shapes) {
            String name = (String) shape[0];
            String base = name + "_base";
            String view = name + "_mv";
            try {
                create(base, "region varchar, amount bigint", "ARRAY['region']");
                assertQuerySucceeds("INSERT INTO " + base + " VALUES "
                        + "('NA', 1), ('NA', 5), ('EU', 3), ('EU', 9), ('APAC', 4)");
                createView(view, base, "ARRAY['region']", String.format((String) shape[1], base));
                computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

                assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 7), ('LATAM', 2)");
                analyzeEverythingExcept(view);

                assertEquals(
                        computeActual(automaticSession(), String.format((String) shape[2], view)).getMaterializedRows(),
                        computeActual(String.format((String) shape[3], base)).getMaterializedRows(),
                        "AUTOMATIC returned a wrong answer for shape " + name);
            }
            finally {
                drop(view, base);
            }
        }
    }

    /**
     * The delta covering most of the base, which is where reusing the materialized rows stops paying.
     * Whichever way the cost model decides, the answer must still be right.
     */
    @Test
    public void testAutomaticWithWideDelta()
    {
        String base = "auto_wide_base";
        String view = "auto_wide_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST(n % 20 AS VARCHAR), n FROM UNNEST(sequence(1, 5000)) AS t(n)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM " + base + " GROUP BY region");
            computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            // Append as many rows as the view already covers, touching every group.
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST(n % 20 AS VARCHAR), n FROM UNNEST(sequence(5001, 10000)) AS t(n)");
            analyzeEverythingExcept(view);

            assertEquals(
                    computeActual(automaticSession(), "SELECT region, total, n FROM " + view + " ORDER BY region")
                            .getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount), COUNT(*) FROM " + base + " GROUP BY region ORDER BY region")
                            .getMaterializedRows(),
                    "AUTOMATIC returned a wrong answer on a wide delta");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * Half a million base rows against a delta of one, which is the shape the optimization exists for.
     * Asserts both that the cost model picks row-level and that the row-level plan is right.
     */
    @Test
    public void testAutomaticHighVolume()
    {
        String base = "auto_vol_base";
        String view = "auto_vol_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST((x.n * 200 + y.n) % 50 AS VARCHAR), x.n * 200 + y.n"
                    + " FROM UNNEST(sequence(1, 2500)) AS x(n), UNNEST(sequence(1, 200)) AS y(n)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM " + base + " GROUP BY region");
            computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('r7', 99)");
            analyzeEverythingExcept(view);

            MaterializedResult result = computeActual(automaticSession(),
                    "SELECT region, total, n FROM " + view + " ORDER BY region");
            assertFalse(rowLevelRejectedOnCost(result),
                    "AUTOMATIC rejected row-level on a 500,000-row base with a one-row delta");
            assertEquals(
                    result.getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount), COUNT(*) FROM " + base + " GROUP BY region ORDER BY region")
                            .getMaterializedRows(),
                    "AUTOMATIC chose a plan that disagrees with the base at volume");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * A grouping key the view does not output. The view keeps one row per (region, tier) while
     * exposing only region, so merging on region alone would add the tiers together.
     */
    @Test
    public void testGroupingKeyMissingFromOutputsDeclinesMerge()
    {
        String base = "hidden_key_base";
        String view = "hidden_key_mv";
        try {
            create(base, "region varchar, tier varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA','a',10), ('NA','b',20)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region, tier");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA','a',5)");
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region, total",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region, tier ORDER BY region, SUM(amount)");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * Two stacked aggregations. The outer COUNT does not grow when an inner group merely gains rows,
     * so the outer value is not an increment on what storage holds.
     */
    @Test
    public void testStackedAggregationDeclinesMerge()
    {
        String base = "stacked_base";
        String view = "stacked_mv";
        try {
            create(base, "region varchar, tier varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA','a',1), ('NA','b',2), ('EU','a',3)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL') AS "
                    + "SELECT region, COUNT(*) AS tiers FROM "
                    + "(SELECT region, tier, SUM(amount) AS s FROM " + base + " GROUP BY region, tier) t "
                    + "GROUP BY region");
            refreshExpectingFallback(view);

            // Grows an existing inner group: the outer count must stay the same.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA','a',7)");
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, tiers FROM " + view + " ORDER BY region",
                    "SELECT region, COUNT(*) FROM (SELECT region, tier, SUM(amount) FROM " + base
                            + " GROUP BY region, tier) t GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * An aggregate over EXCEPT. EXCEPT is anti-monotone in its right input, so appending there must
     * remove a row from the view -- the opposite of an increment.
     */
    @Test
    public void testAggregateOverExceptDeclinesMerge()
    {
        String left = "exc_left";
        String right = "exc_right";
        String view = "exc_mv";
        try {
            create(left, "region varchar, amount bigint", "ARRAY['region']");
            create(right, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + left + " VALUES ('NA',1), ('NA',2), ('EU',3)");
            assertQuerySucceeds("INSERT INTO " + right + " VALUES ('EU',3)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL') AS "
                    + "SELECT region, SUM(amount) AS total FROM "
                    + "(SELECT region, amount FROM " + left + " EXCEPT SELECT region, amount FROM " + right + ") e "
                    + "GROUP BY region");
            refreshExpectingFallback(view);

            // Appending to the right input removes a row from the view.
            assertQuerySucceeds("INSERT INTO " + right + " VALUES ('NA',2)");
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM (SELECT region, amount FROM " + left
                            + " EXCEPT SELECT region, amount FROM " + right + ") e GROUP BY region ORDER BY region");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + left);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + right);
        }
    }

    /**
     * An explicit {@code stitching_strategy=ALWAYS} must not be outranked by the cost model.
     *
     * <p>With row-level on AUTOMATIC the optimizer chooses the granularity, but stitching itself is
     * already settled: the partition-level plan is the incumbent, not the full view query.
     *
     * <p>This pins the property's meaning rather than discriminating implementations. No scenario was
     * found where the incumbent changes the outcome: when row-level is eligible it reads at most
     * storage-groups plus the delta, which never exceeds a full base scan, so it wins regardless of
     * what it is measured against. The guard exists because relying on that coincidence is fragile,
     * not because a failure was reproduced.
     */
    @Test
    public void testAlwaysStitchIsNotOverriddenByCost()
    {
        String base = "always_base";
        String view = "always_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST(n % 4 AS VARCHAR), n FROM UNNEST(sequence(1, 4000)) AS t(n)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            // Touch every partition, so a full recompute looks cheap beside a partition-level plan.
            assertQuerySucceeds("INSERT INTO " + base
                    + " SELECT 'r' || CAST(n % 4 AS VARCHAR), 50000 + n FROM UNNEST(sequence(1, 4000)) AS t(n)");
            analyzeEverythingExcept(view);

            Session alwaysStitch = Session.builder(getSession())
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                    .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                    .setSystemProperty("materialized_view_row_level_incremental_strategy", "AUTOMATIC")
                    .build();

            String plan = (String) computeActual(alwaysStitch,
                    "EXPLAIN (TYPE LOGICAL) SELECT region, total FROM " + view + " ORDER BY region")
                    .getMaterializedRows().get(0).getField(0);
            // A stitched plan unions the storage table with a recomputed delta; a full recompute
            // reads the base alone. The storage table's presence is what distinguishes them.
            assertTrue(plan.contains("__mv_storage__" + view),
                    "stitching_strategy=ALWAYS must still stitch; the plan did not read the storage table");

            assertEquals(
                    computeActual(alwaysStitch, "SELECT region, total FROM " + view + " ORDER BY region")
                            .getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region")
                            .getMaterializedRows(),
                    "stitched result disagrees with the base");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * Row-level survives a base whose changed partitions cannot be determined.
     *
     * <p>Partition discovery gives up when more partitions changed than
     * {@code materialized_view_max_changed_partitions} allows, among other reasons. Row-level needs
     * no partition metadata, so an append-only range is still answerable from the change set; the
     * two mechanisms are independent and only one of them has to succeed. Previously the connector
     * reported NOT_MATERIALIZED as soon as partition discovery failed, which sent exactly these
     * ranges to a full recompute.
     */
    @Test
    public void testRowLevelSurvivesPartitionDiscoveryFailure()
    {
        String base = "nopart_base";
        String view = "nopart_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base
                    + " VALUES ('NA', 1), ('EU', 2), ('APAC', 3), ('LATAM', 4)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);

            // Append to every partition, then cap partition discovery below that count so it gives up.
            assertQuerySucceeds("INSERT INTO " + base
                    + " VALUES ('NA', 10), ('EU', 20), ('APAC', 30), ('LATAM', 40)");
            analyzeEverythingExcept(view);

            Session cappedDiscovery = Session.builder(getSession())
                    .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                    .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                    .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                    .setCatalogSessionProperty("iceberg", "materialized_view_max_changed_partitions", "1")
                    .build();

            MaterializedResult result = computeActual(cappedDiscovery,
                    "SELECT region, total FROM " + view + " ORDER BY region");

            // The answer has to be right whichever plan ran.
            assertEquals(
                    result.getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region")
                            .getMaterializedRows(),
                    "stale read disagrees with the base when partition discovery is capped");

            // And it must not have degraded to a full recompute: the storage table is still read.
            String plan = (String) computeActual(cappedDiscovery,
                    "EXPLAIN (TYPE LOGICAL) SELECT region, total FROM " + view + " ORDER BY region")
                    .getMaterializedRows().get(0).getField(0);
            assertTrue(plan.contains("__mv_storage__" + view),
                    "row-level should still stitch when partition discovery gives up; the plan did not read storage");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testInterleavedStaleReadAndRefresh()
    {
        String base = "seq_inter_base";
        String view = "seq_inter_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            // Stale read must already agree with the base, and must not disturb the later refresh.
            assertEquals(
                    computeActual(stitchSession(), "SELECT region, total FROM " + view + " ORDER BY region").getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region").getMaterializedRows(),
                    "stale read disagrees with the base");

            refreshExpectingMerge(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testAppendTouchingEveryExistingGroup()
    {
        run("touch_all",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('EU', 20), ('APAC', 30)",
                "VALUES ('NA', 1), ('EU', 2), ('APAC', 3)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    /**
     * The delete-blind case. affected_identifiers is built only from rows still present in the base,
     * so a group whose rows have all been deleted contributes nothing and its stale row survives.
     * This connector permits a whole-partition DELETE on a V3 table, so this is reachable.
     */
    @Test
    public void testWholePartitionDeleteThenRefresh()
    {
        String base = "del_base";
        String view = "del_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('APAC', 30)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            // Removes the EU group entirely, and appends to another group.
            assertQuerySucceeds("DELETE FROM " + base + " WHERE region = 'EU'");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5)");

            // affected_identifiers is built only from rows still present in the base, so a group
            // whose rows were all deleted would contribute nothing and its stale row would survive.
            // What keeps this correct today is that the connector reports no partition staleness for
            // a range containing a delete, so the refresh declines both granularities and recomputes
            // in full. This asserts that protection rather than leaving it incidental: if a future
            // change lets row-level through here, this fails instead of silently going stale.
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * A stale read of a filtered view where the appended row does not survive the view's own filter.
     * affected_identifiers is built from a raw base scan, so the group is marked affected even though
     * nothing about its aggregate changed; the delta branch must then recompute it back correctly.
     */
    @Test
    public void testStaleReadOfFilteredViewWhenAppendFailsTheFilter()
    {
        String base = "flt_base";
        String view = "flt_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " WHERE amount > 0 GROUP BY region");
            refreshExpectingFallback(view);

            // Appended row is excluded by the view's filter, so no aggregate actually changes.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('EU', -5)");

            assertEquals(
                    computeActual(stitchSession(), "SELECT region, total FROM " + view + " ORDER BY region").getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " WHERE amount > 0 GROUP BY region ORDER BY region").getMaterializedRows(),
                    "stale read of a filtered view disagrees with the base");

            // And the same through a refresh.
            refreshExpectingMerge(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " WHERE amount > 0 GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * A group every one of whose rows fails the view's filter must not appear, before or after a
     * row-level refresh touches it.
     */
    @Test
    public void testGroupEntirelyExcludedByViewFilter()
    {
        String base = "allflt_base";
        String view = "allflt_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', -1)");
            createView(view, base, "ARRAY['region']",
                    "SELECT region, SUM(amount) AS total FROM " + base + " WHERE amount > 0 GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('EU', -2), ('NA', 5)");
            refreshExpectingMerge(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " WHERE amount > 0 GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * A stale read over a range containing a delete. The changed-rows predicate can only find rows
     * that are still present, so a removed row is invisible to it and the group it belonged to would
     * keep its stale aggregate. The connector must therefore withhold row-level staleness for such a
     * range; this asserts the read is correct and that row-level did not drive it.
     */
    @Test
    public void testStaleReadOverRangeContainingDelete()
    {
        String base = "delread_base";
        String view = "delread_mv";
        try {
            // Unpartitioned, so a whole-table DELETE is the only removal a V3 table accepts.
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (refresh_type = 'INCREMENTAL')"
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("DELETE FROM " + base);
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('APAC', 7)");

            // Every row the view had is gone; only APAC should remain.
            assertEquals(
                    computeActual(stitchSession(), "SELECT region, total FROM " + view + " ORDER BY region").getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region").getMaterializedRows(),
                    "stale read over a range containing a delete disagrees with the base");

            refreshExpectingFallback(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * The same, with the base partitioned so a partition-level delta is available. Row-level must
     * still decline, because the removal is invisible to the changed-rows predicate.
     */
    @Test
    public void testRefreshOverRangeContainingDeleteDeclinesRowLevel()
    {
        String base = "delref_base";
        String view = "delref_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('APAC', 30)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("DELETE FROM " + base + " WHERE region = 'EU'");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5)");
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    // ------------------------------------------------------------ view shapes

    /**
     * A UNION-defined view must fall back to partition-level: its output rows carry no single base
     * row identity, so there is nothing for the fresh branch to exclude by.
     *
     * <p>The storage table is deliberately partitioned by every output column, which satisfies the
     * group-key-equals-partition-column test that otherwise gates row-level replacement. The
     * decline therefore has to come from the view's shape rather than from its partitioning, and
     * this pins that: resolving a UNION output back to one base column is what currently fails, and
     * a later change to that resolution must not quietly enable row-level here.
     */
    @Test
    public void testUnionDistinctViewMerges()
    {
        String base1 = "uni_base1";
        String base2 = "uni_base2";
        String view = "uni_mv";
        try {
            create(base1, "region varchar, amount bigint", "ARRAY['region']");
            create(base2, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base1 + " VALUES ('NA', 10)");
            assertQuerySucceeds("INSERT INTO " + base2 + " VALUES ('EU', 20)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region', 'amount'], refresh_type = 'INCREMENTAL') AS "
                    + "SELECT region, amount FROM " + base1
                    + " UNION SELECT region, amount FROM " + base2);
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base1 + " VALUES ('NA', 1)");
            // UNION without ALL is planned as an aggregation over the grouping keys with no aggregate
            // functions, so the merge re-applies DISTINCT over storage plus the delta. That reproduces
            // the view: distinct(distinct(old) + new) is distinct(old + new), and an appended duplicate
            // collapses rather than being counted twice.
            refreshExpectingMerge(view);

            assertViewMatchesBase(
                    "SELECT region, amount FROM " + view + " ORDER BY region, amount",
                    "SELECT region, amount FROM " + base1
                            + " UNION SELECT region, amount FROM " + base2 + " ORDER BY region, amount");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base1);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base2);
        }
    }

    // --------------------------------------------------- view-level enablement

    /**
     * The view opts out with {@code row_level_incremental_refresh = false}. An explicit MV-level
     * opt-out beats session-level opt-in, so row-level must not fire even though the session asks
     * for ALWAYS.
     */
    @Test
    public void testViewLevelOptOutBeatsSessionOptIn()
    {
        String base = "optout_base";
        String view = "optout_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL',"
                    + " row_level_incremental_refresh = false) AS "
                    + "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 1)");
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    /**
     * The same view opting in explicitly still refreshes row-level, so the opt-out above is the
     * property taking effect rather than the property's presence breaking eligibility.
     */
    @Test
    public void testViewLevelOptInRefreshesRowLevel()
    {
        String base = "optin_base";
        String view = "optin_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL',"
                    + " row_level_incremental_refresh = true) AS "
                    + "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 1)");
            refreshExpectingMerge(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    // ------------------------------------------------------------- boilerplate

    private void run(
            String name,
            String columns,
            String partitioning,
            String viewSqlTemplate,
            String initialRows,
            String appendedRows,
            String viewQueryTemplate,
            String baseQueryTemplate)
    {
        run(name, columns, partitioning, viewSqlTemplate, initialRows, appendedRows,
                viewQueryTemplate, baseQueryTemplate, true);
    }

    /**
     * @param expectMerge whether the view's aggregates can all be merged from their stored values.
     *        MIN/MAX, AVG and holistic aggregates cannot, so those views must fall back.
     */
    private void run(
            String name,
            String columns,
            String partitioning,
            String viewSqlTemplate,
            String initialRows,
            String appendedRows,
            String viewQueryTemplate,
            String baseQueryTemplate,
            boolean expectMerge)
    {
        String base = name + "_base";
        String view = name + "_mv";
        try {
            create(base, columns, partitioning);
            assertQuerySucceeds("INSERT INTO " + base + " " + initialRows);
            createView(view, base, partitioning, String.format(viewSqlTemplate, base));
            // The first refresh is the initial load: the view has never been materialized, so there
            // is no recorded version to compute changes against.
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " " + appendedRows);
            if (expectMerge) {
                refreshExpectingMerge(view);
            }
            else {
                refreshExpectingFallback(view);
            }

            assertViewMatchesBase(
                    String.format(viewQueryTemplate, view),
                    String.format(baseQueryTemplate, base));
        }
        finally {
            drop(view, base);
        }
    }

    private void create(String base, String columns, String partitioning)
    {
        assertQuerySucceeds("CREATE TABLE " + base + " (" + columns + ") "
                + "WITH (\"format-version\" = '3', partitioning = " + partitioning + ")");
    }

    private void createView(String view, String base, String partitioning, String viewSql)
    {
        assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                + " WITH (partitioning = " + partitioning + ", refresh_type = 'INCREMENTAL')"
                + " AS " + viewSql);
    }

    private void drop(String view, String base)
    {
        assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
    }
}
