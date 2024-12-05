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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveNativeLogicalPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS),
                ImmutableMap.of("native-execution-enabled", "true"),
                Optional.empty());
    }

    @Test
    public void testPartialAggregatePushdownDisabled()
    {
        assertPlan(partialAggregatePushdownEnabled(),
                "select count(orderkey), max(orderpriority) from orders",
                anyTree(PlanMatchPattern.tableScan("orders")),
                plan -> assertNoAggregatedColumns(plan, "orders"));
    }

    private Session partialAggregatePushdownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private void assertNoAggregatedColumns(Plan plan, String tableName)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        for (ColumnHandle columnHandle : tableScan.getAssignments().values()) {
            assertTrue(columnHandle instanceof HiveColumnHandle);
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            assertNotSame(hiveColumnHandle.getColumnType(), HiveColumnHandle.ColumnType.AGGREGATED);
            assertFalse(hiveColumnHandle.getPartialAggregation().isPresent());
        }
    }

    private static boolean isTableScanNode(PlanNode node, String tableName)
    {
        return node instanceof TableScanNode && ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getTableName().equals(tableName);
    }
}
