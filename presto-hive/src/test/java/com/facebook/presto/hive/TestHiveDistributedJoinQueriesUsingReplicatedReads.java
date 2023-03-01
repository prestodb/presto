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
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestJoinQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.ENABLE_REPLICATED_READS_FOR_BROADCAST_JOIN;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveDistributedJoinQueriesUsingReplicatedReads
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, ENABLE_REPLICATED_READS_FOR_BROADCAST_JOIN, Boolean.toString(true))
                .build();
    }

    @Test
    public void testShuffledStatsWithInnerJoinForReplicatedReads()
    {
        // NOTE: only test shuffled stats with distributed query runner and disk spilling is disabled.
        if (!(getQueryRunner() instanceof DistributedQueryRunner) || getQueryRunner().getDefaultSession().getSystemProperty("spill_enabled", Boolean.class)) {
            return;
        }
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();

        // Get the number of rows in orders table for query stats verification below.
        long ordersRows = getTableRowCount("orders");
        // Get the number of rows in lineitem table for query stats verification below.
        long lineitemRows = getTableRowCount("lineitem");

        String query = "SELECT a.orderkey, a.orderstatus, b.linenumber FROM orders a JOIN lineitem b ON a.orderkey = b.orderkey";
        // Set session property to enforce a hash partitioned join.
        Session partitionedJoin = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
        QueryId partitionQueryId = queryRunner.executeWithQueryId(partitionedJoin, query).getQueryId();
        QueryInfo partitionJoinQueryInfo = queryRunner.getQueryInfo(partitionQueryId);
        long expectedRawInputRows = ordersRows + lineitemRows;
        // Verify the number shuffled rows, raw input rows and output rows in hash partitioned join.
        // NOTE: the latter two shall be the same for both hash partitioned join and broadcast join.
        assertEquals(partitionJoinQueryInfo.getQueryStats().getRawInputPositions(), expectedRawInputRows);
        long expectedOutputRows = lineitemRows;
        assertEquals(partitionJoinQueryInfo.getQueryStats().getOutputPositions(), expectedOutputRows);
        long expectedPartitionJoinShuffledRows = lineitemRows + ordersRows + expectedOutputRows;
        assertEquals(partitionJoinQueryInfo.getQueryStats().getShuffledPositions(), expectedPartitionJoinShuffledRows);

        // Set session property to enforce a broadcast join.
        Session broadcastJoin = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();

        QueryId broadcastQueryId = queryRunner.executeWithQueryId(broadcastJoin, query).getQueryId();
        assertNotEquals(partitionQueryId, broadcastQueryId);
        QueryInfo broadcastJoinQueryInfo = queryRunner.getQueryInfo(broadcastQueryId);
        long expectedReplicatedInputRows = lineitemRows * queryRunner.getNodeCount() + ordersRows;
        assertNotEquals(partitionQueryId, broadcastQueryId);
        broadcastJoinQueryInfo = queryRunner.getQueryInfo(broadcastQueryId);
        assertEquals(broadcastJoinQueryInfo.getQueryStats().getRawInputPositions(), expectedReplicatedInputRows);
        assertEquals(broadcastJoinQueryInfo.getQueryStats().getOutputPositions(), expectedOutputRows);
        // NOTE: the number of shuffled bytes except the final output should be a multiple of the number of rows in lineitem table in broadcast join case.
        assertEquals(((broadcastJoinQueryInfo.getQueryStats().getShuffledPositions() - expectedOutputRows) % lineitemRows), 0);
        assertTrue((broadcastJoinQueryInfo.getQueryStats().getShuffledPositions() - expectedOutputRows) >= 0);
    }
}
