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
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

public class TestHivePushdownDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestHivePushdownDistributedQueries()
    {
        super(() -> createQueryRunner(
                getTables(),
                ImmutableMap.of(
                        "experimental.pushdown-subfields-enabled", "true",
                        "experimental.enable-dynamic-filtering", "true",
                        "join-distribution-type", "BROADCAST"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true"),
                Optional.empty()));
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testJoinWithEmptyBuildSide()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .build();
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice = 123.4567");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.getQueryId(), "tpch:lineitem");
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertEquals(probeStats.getInputPositions(), 0L);
    }

    private OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        Plan plan = runner.getQueryPlan(queryId);
        PlanNodeId nodeId = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    if (!(node instanceof ProjectNode)) {
                        return false;
                    }
                    ProjectNode projectNode = (ProjectNode) node;
                    FilterNode filterNode = (FilterNode) projectNode.getSource();
                    TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                    return tableName.equals(tableScanNode.getTable().getConnectorHandle().toString());
                })
                .findOnlyElement()
                .getId();
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()))
                .collect(MoreCollectors.onlyElement());
    }

    private Long countRows(String tableName)
    {
        MaterializedResult result = getQueryRunner().execute("SELECT COUNT() FROM " + tableName);
        return (Long) result.getOnlyValue();
    }
}
