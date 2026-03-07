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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_WIDEN_CAST_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end tests verifying that {@code PushDownWidenCast} correctly pushes widening type casts
 * (e.g., INTEGER→BIGINT) into Hive/Parquet {@code TableScanNode}s so that the Velox Parquet
 * reader can apply the coercion inline during column reading.
 *
 * <p>Plan verification checks two properties:
 * <ol>
 *   <li>The {@code TableScanNode} that reads an integer-typed column directly outputs a variable
 *       of the <em>wider</em> type after the optimization fires.</li>
 *   <li>No {@code ProjectNode} between the scan and the output contains a CAST expression from
 *       the narrow to the wide type.</li>
 * </ol>
 */
@Test(singleThreaded = true)
public class TestPushDownWidenCastHiveLogicalPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, LINE_ITEM),
                ImmutableMap.of(),
                Optional.empty());
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return getQueryRunner();
    }

    // -----------------------------------------------------------------------
    // Table set-up / tear-down helpers
    // -----------------------------------------------------------------------

    /**
     * Creates a PARQUET-format table containing representative narrow-type columns:
     * <ul>
     *   <li>{@code tiny_col}  — TINYINT</li>
     *   <li>{@code small_col} — SMALLINT</li>
     *   <li>{@code int_col}   — INTEGER  (shippriority from orders)</li>
     *   <li>{@code real_col}  — REAL</li>
     *   <li>{@code bigint_col}— BIGINT</li>
     * </ul>
     */
    private void createWidenCastTable(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS widen_cast_test");
        queryRunner.execute(
                "CREATE TABLE widen_cast_test WITH (format = 'PARQUET') AS " +
                "SELECT " +
                "    orderkey, " +
                "    cast(shippriority AS tinyint)  AS tiny_col, " +
                "    cast(shippriority AS smallint) AS small_col, " +
                "    shippriority                   AS int_col, " +
                "    cast(totalprice AS real)        AS real_col, " +
                "    cast(orderkey AS bigint)        AS bigint_col " +
                "FROM orders LIMIT 1000");
    }

    private void dropWidenCastTable(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS widen_cast_test");
    }

    // -----------------------------------------------------------------------
    // Session helpers
    // -----------------------------------------------------------------------

    /**
     * Session for plan-structure checks only. Includes {@code native_execution_enabled=true}
     * so the optimizer fires, but must NOT be used to actually execute queries on the Java path
     * (the Java connector does not perform type widening during scan).
     */
    private Session widenCastEnabledForPlan()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_DOWN_WIDEN_CAST_ENABLED, "true")
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .build();
    }

    // -----------------------------------------------------------------------
    // Plan inspection helpers
    // -----------------------------------------------------------------------

    /**
     * Walks every fragment in {@code subPlan} (root + children, recursively) and returns the
     * single {@link TableScanNode} that scans {@code tableName}. The rule runs
     * post-fragmentation in {@link com.facebook.presto.sql.planner.PlanFragmenterUtils}, so
     * inspecting the {@link com.facebook.presto.sql.planner.Plan} returned by {@code plan()}
     * would miss the rewrite — we have to walk the fragmented SubPlan.
     */
    private static TableScanNode findTableScan(SubPlan subPlan, String tableName)
    {
        ImmutableList.Builder<TableScanNode> found = ImmutableList.builder();
        collectTableScans(subPlan, tableName, found);
        ImmutableList<TableScanNode> scans = found.build();
        if (scans.size() != 1) {
            throw new IllegalStateException("expected exactly one TableScan for " + tableName + ", got " + scans.size());
        }
        return scans.get(0);
    }

    private static void collectTableScans(SubPlan subPlan, String tableName, ImmutableList.Builder<TableScanNode> out)
    {
        searchFrom(subPlan.getFragment().getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findAll()
                .forEach(node -> out.add((TableScanNode) node));
        for (SubPlan child : subPlan.getChildren()) {
            collectTableScans(child, tableName, out);
        }
    }

    private static boolean isTableScanNode(PlanNode node, String tableName)
    {
        return node instanceof TableScanNode &&
                ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle())
                        .getTableName().equals(tableName);
    }

    /**
     * Walks every fragment in {@code subPlan} and returns true if any {@code ProjectNode}
     * contains a widening-cast {@code CallExpression}. Used to assert that the rule eliminated
     * such Project nodes.
     */
    private static boolean subPlanHasWideningCastProject(SubPlan subPlan)
    {
        boolean here = searchFrom(subPlan.getFragment().getRoot())
                .where(node -> node instanceof ProjectNode)
                .findAll()
                .stream()
                .anyMatch(node -> {
                    ProjectNode project = (ProjectNode) node;
                    return project.getAssignments().getExpressions().stream()
                            .anyMatch(TestPushDownWidenCastHiveLogicalPlanner::isWideningCastExpr);
                });
        if (here) {
            return true;
        }
        for (SubPlan child : subPlan.getChildren()) {
            if (subPlanHasWideningCastProject(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isWideningCastExpr(RowExpression expr)
    {
        if (!(expr instanceof CallExpression)) {
            return false;
        }
        CallExpression call = (CallExpression) expr;
        // A CAST expression has display name "CAST" and exactly one argument.
        if (call.getArguments().size() != 1) {
            return false;
        }
        // Check the argument type is a known narrow numeric type and the output is wider.
        com.facebook.presto.common.type.Type from = call.getArguments().get(0).getType();
        com.facebook.presto.common.type.Type to = call.getType();
        return (from.equals(TINYINT) && (to.equals(SMALLINT) || to.equals(INTEGER) || to.equals(BIGINT)))
                || (from.equals(SMALLINT) && (to.equals(INTEGER) || to.equals(BIGINT)))
                || (from.equals(INTEGER) && to.equals(BIGINT))
                || (from.equals(REAL) && to.equals(DOUBLE));
    }

    private static boolean scanOutputsType(TableScanNode scan, com.facebook.presto.common.type.Type type)
    {
        return scan.getOutputVariables().stream().anyMatch(v -> v.getType().equals(type));
    }

    // -----------------------------------------------------------------------
    // Tests — plan structure (INTEGER → BIGINT)
    // -----------------------------------------------------------------------

    @Test
    public void testIntegerToBigintCastIsPushedDownToScan()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT CAST(int_col AS BIGINT) FROM widen_cast_test";

            // With optimization: scan must directly output BIGINT; no widening CAST in projects.
            // PushDownWidenCast runs post-fragmentation so we need the SubPlan tree, not the logical Plan.
            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");
            assertTrue(scanOutputsType(scanWith, BIGINT),
                    "Expected scan to output BIGINT after optimization");
            assertFalse(scanOutputsType(scanWith, INTEGER),
                    "Expected INTEGER column to be replaced by BIGINT in scan output");
            assertFalse(subPlanHasWideningCastProject(subPlanWith),
                    "Expected no widening CAST ProjectNode above scan when optimization is enabled");

            // Without optimization: scan outputs INTEGER; a CAST project must exist.
            SubPlan subPlanWithout = subplan(sql, getQueryRunner().getDefaultSession());
            TableScanNode scanWithout = findTableScan(subPlanWithout, "widen_cast_test");
            assertTrue(scanOutputsType(scanWithout, INTEGER),
                    "Expected scan to output INTEGER when optimization is disabled");
            assertTrue(subPlanHasWideningCastProject(subPlanWithout),
                    "Expected a widening CAST ProjectNode above scan when optimization is disabled");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    @Test
    public void testSmallintToIntegerCastIsPushedDownToScan()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT CAST(small_col AS INTEGER) FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");
            assertTrue(scanOutputsType(scanWith, INTEGER),
                    "Expected scan to output INTEGER after SMALLINT->INTEGER push-down");
            assertFalse(subPlanHasWideningCastProject(subPlanWith),
                    "Expected no widening CAST in ProjectNode above scan");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    @Test
    public void testTinyintToBigintCastIsPushedDownToScan()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT CAST(tiny_col AS BIGINT) FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");
            assertTrue(scanOutputsType(scanWith, BIGINT),
                    "Expected scan to output BIGINT after TINYINT->BIGINT push-down");
            assertFalse(subPlanHasWideningCastProject(subPlanWith),
                    "Expected no widening CAST in ProjectNode above scan");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    @Test
    public void testRealToDoubleCastIsPushedDownToScan()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT CAST(real_col AS DOUBLE) FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");
            assertTrue(scanOutputsType(scanWith, DOUBLE),
                    "Expected scan to output DOUBLE after REAL->DOUBLE push-down");
            assertFalse(subPlanHasWideningCastProject(subPlanWith),
                    "Expected no widening CAST in ProjectNode above scan");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    // -----------------------------------------------------------------------
    // Tests — negative: non-widening casts must NOT be pushed
    // -----------------------------------------------------------------------

    @Test
    public void testNarrowingCastIsNotPushedDown()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            // BIGINT -> INTEGER is a narrowing cast; must not be pushed.
            String sql = "SELECT CAST(bigint_col AS INTEGER) FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");
            // Scan must still output BIGINT for bigint_col (not replaced).
            assertTrue(scanOutputsType(scanWith, BIGINT),
                    "Expected scan to still output BIGINT — narrowing cast must not be pushed");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    @Test
    public void testOptimizationDisabledByDefault()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT CAST(int_col AS BIGINT) FROM widen_cast_test";

            // Default session has push_down_widen_cast_enabled = false.
            SubPlan subPlanDefault = subplan(sql, queryRunner.getDefaultSession());
            TableScanNode scanDefault = findTableScan(subPlanDefault, "widen_cast_test");
            assertTrue(scanOutputsType(scanDefault, INTEGER),
                    "Expected scan to output INTEGER with optimization disabled (default)");
            assertTrue(subPlanHasWideningCastProject(subPlanDefault),
                    "Expected CAST ProjectNode above scan with optimization disabled (default)");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    // -----------------------------------------------------------------------
    // Tests — conflict handling (narrow variable used elsewhere in the plan)
    // -----------------------------------------------------------------------

    /**
     * When the narrow variable is also passed through the ProjectNode (i.e. used in a second
     * output column), the optimization is skipped — the plan is unchanged and the scan outputs
     * only the narrow variable (the CAST remains in the ProjectNode).
     */
    @Test
    public void testConflictIsSkipped()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            // int_col is used in two places: the CAST and the raw column reference.
            String sql = "SELECT int_col, CAST(int_col AS BIGINT) FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");

            // The scan must NOT have BIGINT added (conflict → skip).
            assertFalse(scanOutputsType(scanWith, BIGINT),
                    "Expected no BIGINT in scan output when narrow var is used elsewhere (conflict case)");
            assertTrue(scanOutputsType(scanWith, INTEGER),
                    "Expected INTEGER column to remain in scan output in the conflict case");

            // The CAST must remain in a ProjectNode since the optimization was skipped.
            assertTrue(subPlanHasWideningCastProject(subPlanWith),
                    "Expected CAST ProjectNode to remain when optimization is skipped due to conflict");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    // -----------------------------------------------------------------------
    // Tests — multi-column widening in one ProjectNode
    // -----------------------------------------------------------------------

    @Test
    public void testMultipleWideningCastsInSingleProject()
    {
        QueryRunner queryRunner = getQueryRunner();
        try {
            createWidenCastTable(queryRunner);

            String sql = "SELECT " +
                    "CAST(tiny_col AS SMALLINT), " +
                    "CAST(small_col AS BIGINT), " +
                    "CAST(int_col AS BIGINT), " +
                    "CAST(real_col AS DOUBLE) " +
                    "FROM widen_cast_test";

            SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
            TableScanNode scanWith = findTableScan(subPlanWith, "widen_cast_test");

            // All four widened types should appear directly in scan output.
            assertTrue(scanOutputsType(scanWith, SMALLINT),
                    "Expected SMALLINT from TINYINT->SMALLINT push-down");
            assertTrue(scanOutputsType(scanWith, BIGINT),
                    "Expected BIGINT from INTEGER->BIGINT push-down");
            assertTrue(scanOutputsType(scanWith, DOUBLE),
                    "Expected DOUBLE from REAL->DOUBLE push-down");
            assertFalse(subPlanHasWideningCastProject(subPlanWith),
                    "Expected no widening CAST ProjectNodes above scan");
        }
        finally {
            dropWidenCastTable(queryRunner);
        }
    }

    // -----------------------------------------------------------------------
    // Tests — TPCH orders table (uses existing HiveQueryRunner tables)
    // -----------------------------------------------------------------------

    /**
     * Verify push-down works against the TPCH {@code orders} table (ORC/DWRF format) to confirm
     * the optimizer rule fires independently of file format — format-specific pushdown in the
     * reader is orthogonal to the planner-level cast elimination.
     */
    @Test
    public void testWideningCastOnTpchOrdersTable()
    {
        // orders.shippriority is INTEGER in the TPCH schema.
        String sql = "SELECT CAST(shippriority AS BIGINT), sum(CAST(shippriority AS BIGINT)) " +
                "FROM orders GROUP BY 1 ORDER BY 1";

        SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
        TableScanNode scanWith = findTableScan(subPlanWith, "orders");
        assertTrue(scanOutputsType(scanWith, BIGINT),
                "Expected BIGINT in scan output after CAST push-down on orders.shippriority");
        assertFalse(subPlanHasWideningCastProject(subPlanWith),
                "Expected no CAST ProjectNode above orders scan");
    }

    @Test
    public void testWideningCastWithFilterOnOrders()
    {
        // Verify the optimization still fires when there is a filter on the table.
        String sql = "SELECT CAST(shippriority AS BIGINT) FROM orders WHERE orderkey < 1000";

        SubPlan subPlanWith = subplan(sql, widenCastEnabledForPlan());
        TableScanNode scanWith = findTableScan(subPlanWith, "orders");
        assertTrue(scanOutputsType(scanWith, BIGINT),
                "Expected BIGINT scan output even with a filter present");
    }

    // -----------------------------------------------------------------------
    // NOTE on the producer-outputLayout invariant:
    //
    // A separate invariant the per-fragment design is meant to give us is that when a widening
    // CAST sits above an Exchange, the cast lands on the consumer fragment's RemoteSourceNode
    // (declaring wideVar:wider) while the producing fragment's PartitioningScheme.outputLayout
    // stays narrow on the wire. A previous test asserted that with
    // `SELECT CAST(MIN(int_col) AS BIGINT) FROM ...`. That test does NOT hold today because the
    // pre-fragmentation type analyzer folds the widening CAST into the aggregation's declared
    // output type, so by the time the per-fragment rule runs the CAST is gone and the producer
    // outputLayout is already BIGINT — there's nothing left for the rule to fix. Restoring a
    // meaningful test of this invariant needs a query shape (and possibly a planner tweak) where
    // the CAST survives pre-fragmentation; leaving as TODO.
    // -----------------------------------------------------------------------
}
