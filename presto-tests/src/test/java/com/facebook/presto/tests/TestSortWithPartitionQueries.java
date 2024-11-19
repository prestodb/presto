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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.optimizations.TestAddPartitionToSortRule;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkState;

public class TestSortWithPartitionQueries
        extends AbstractTestQueryFramework
{
    @Test
    public void testSortByPartition()
    {
        LocalQueryRunner localQueryRunner = (LocalQueryRunner) getQueryRunner();
        localQueryRunner.setAdditionalOptimizer(ImmutableList.of(new IterativeOptimizer(
                localQueryRunner.getMetadata(),
                new RuleStatsRecorder(),
                localQueryRunner.getStatsCalculator(),
                localQueryRunner.getEstimatedExchangesCostCalculator(),
                ImmutableSet.of(new TestAddPartitionToSortRule()))));
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(TASK_CONCURRENCY, "8")
                .build();

        MaterializedResult result = computeActual(session, "SELECT partkey, extendedprice from lineitem order by extendedprice");
        List<MaterializedRow> rowList = result.getMaterializedRows();
        Map<Long, Double> maxPrice = new HashMap<>();
        for (MaterializedRow row : rowList) {
            long partkey = (long) row.getField(0);
            double price = (double) row.getField(1);
            if (maxPrice.containsKey(partkey)) {
                checkState(price >= maxPrice.get(partkey));
            }
            maxPrice.put(partkey, price);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        return localQueryRunner;
    }
}
