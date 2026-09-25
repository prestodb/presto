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
package com.facebook.presto.ducklake;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end {@code SHOW STATS} tests against the {@link TestingDuckLakeCatalog} fixture, proving
 * {@link com.facebook.presto.ducklake.statistics.TableStatisticsMaker} feeds the optimizer real row
 * counts and column ranges. {@code SHOW STATS} renders columns in a fixed order (see {@code
 * ShowStatsRewrite}): {@code column_name, data_size, distinct_values_count, nulls_fraction,
 * row_count, low_value, high_value}; the constants below read the result by that name rather than
 * relying on fragile whole-row equality.
 */
public class TestDuckLakeStatistics
        extends AbstractTestQueryFramework
{
    private static final int COLUMN_NAME = 0;
    private static final int DATA_SIZE = 1;
    private static final int DISTINCT_VALUES_COUNT = 2;
    private static final int NULLS_FRACTION = 3;
    private static final int ROW_COUNT = 4;
    private static final int LOW_VALUE = 5;
    private static final int HIGH_VALUE = 6;

    private DuckLakeQueryRunner duckLakeQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        duckLakeQueryRunner = DuckLakeQueryRunner.createQueryRunner();
        return duckLakeQueryRunner.getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void close()
            throws Exception
    {
        super.close();
        duckLakeQueryRunner.getTestingCatalog().close();
    }

    @Test
    public void testOrdersTableStatistics()
    {
        MaterializedResult stats = computeActual("SHOW STATS FOR tpch.orders");

        assertEquals(summaryRow(stats).getField(ROW_COUNT), 15000.0);

        MaterializedRow orderKey = columnRow(stats, "o_orderkey");
        assertEquals(orderKey.getField(LOW_VALUE), "1");
        assertEquals(orderKey.getField(HIGH_VALUE), "60000");
        assertEquals(orderKey.getField(NULLS_FRACTION), 0.0);
        assertTrue((Double) orderKey.getField(DATA_SIZE) > 0);

        MaterializedRow orderDate = columnRow(stats, "o_orderdate");
        assertEquals(orderDate.getField(LOW_VALUE), "1992-01-01");
        assertEquals(orderDate.getField(HIGH_VALUE), "1998-08-02");

        MaterializedRow totalPrice = columnRow(stats, "o_totalprice");
        assertEquals(totalPrice.getField(LOW_VALUE), "874.89");
        assertEquals(totalPrice.getField(HIGH_VALUE), "466001.28");

        MaterializedRow comment = columnRow(stats, "o_comment");
        assertNull(comment.getField(LOW_VALUE));
        assertNull(comment.getField(HIGH_VALUE));
        assertTrue((Double) comment.getField(DATA_SIZE) > 0);

        for (MaterializedRow row : stats.getMaterializedRows()) {
            assertNull(row.getField(DISTINCT_VALUES_COUNT));
        }
    }

    @Test
    public void testFilteredOrdersRowCountEstimateIsLowerThanUnfiltered()
    {
        // DuckLake's split source only prunes whole files, so it never marks a predicate as
        // enforced (DuckLakeMetadata.getTableLayouts always returns the full predicate as the
        // unenforced remainder) -- a genuine FilterNode always stays in the plan, which is exactly
        // why "SHOW STATS FOR (SELECT ... WHERE ...)" (which requires a residual-filter-free plan)
        // cannot be used here. The FilterNode's own row-count estimate is still computed by the
        // engine's FilterStatsCalculator from the TableScan's statistics using the predicate, so a
        // strictly lower estimate for the filtered query proves the o_orderkey range
        // TableStatisticsMaker reports is actually consumed by the optimizer.
        long unfilteredEstimate = topRowCountEstimate("SELECT * FROM tpch.orders");
        long filteredEstimate = topRowCountEstimate("SELECT * FROM tpch.orders WHERE o_orderkey < 10");
        assertTrue(
                filteredEstimate < unfilteredEstimate,
                "Expected filtered estimate (" + filteredEstimate + ") below unfiltered estimate (" + unfilteredEstimate + ")");
    }

    private long topRowCountEstimate(String query)
    {
        MaterializedResult explain = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        String plan = (String) explain.getOnlyValue();
        Matcher matcher = Pattern.compile("rows: (\\d+)").matcher(plan);
        assertTrue(matcher.find(), "No row estimate found in plan:\n" + plan);
        return Long.parseLong(matcher.group(1));
    }

    @Test
    public void testDeleteTableRowCountExcludesDeletedRows()
    {
        // del.simple starts at 1000 inserted rows and has 100 deleted by a positional delete file;
        // ducklake_table_stats.record_count would report the pre-delete 1000, so this proves row
        // count comes from surviving data-file/delete-file record counts instead.
        MaterializedResult stats = computeActual("SHOW STATS FOR del.simple");
        assertEquals(summaryRow(stats).getField(ROW_COUNT), 900.0);
    }

    @Test
    public void testInlinedTableRowCountIsParquetRowsOnly()
    {
        // inl.small has one 20-row Parquet data file plus rows inlined directly into
        // ducklake_inlined_data_* catalog tables. TableStatisticsMaker (see its class javadoc)
        // only visits data files, so it undercounts by the inlined rows -- a deliberate, bounded
        // gap, not a bug: DuckLake only ever inlines a small number of rows before flushing them
        // to a data file.
        MaterializedResult stats = computeActual("SHOW STATS FOR inl.small");
        assertEquals(summaryRow(stats).getField(ROW_COUNT), 20.0);
    }

    private static MaterializedRow summaryRow(MaterializedResult stats)
    {
        return stats.getMaterializedRows().stream()
                .filter(row -> row.getField(COLUMN_NAME) == null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("SHOW STATS produced no summary row"));
    }

    private static MaterializedRow columnRow(MaterializedResult stats, String columnName)
    {
        return stats.getMaterializedRows().stream()
                .filter(row -> columnName.equals(row.getField(COLUMN_NAME)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("SHOW STATS produced no row for column " + columnName));
    }
}
