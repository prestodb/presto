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
package com.facebook.presto;

import com.facebook.presto.CustomFunctions.CustomRank;
import com.facebook.presto.CustomFunctions.CustomSum;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry.FunctionListBuilder;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.metadata.FunctionRegistry.supplier;
import static com.facebook.presto.sql.analyzer.Type.BIGINT;
import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private LocalQueryRunner tpchLocalQueryRunner;
    private ExecutorService executor;

    public ExecutorService getExecutor()
    {
        if (executor == null) {
            executor = newCachedThreadPool(daemonThreadsNamed("test"));
        }
        return executor;
    }

    @AfterClass
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    protected int getNodeCount()
    {
        return 1;
    }

    @Override
    protected void setUpQueryFramework(String catalog, String schema)
    {
        tpchLocalQueryRunner = createTpchLocalQueryRunner(new Session("user", "test", catalog, schema, null, null), getExecutor());

        // dump query plan to console (for debugging)
        // tpchLocalQueryRunner.printPlan();
        tpchLocalQueryRunner.getMetadata().addFunctions(
                new FunctionFactory()
                {
                    @Override
                    public List<FunctionInfo> listFunctions()
                    {
                        return new FunctionListBuilder()
                                .aggregate("custom_sum", BIGINT, ImmutableList.of(BIGINT), BIGINT, new CustomSum())
                                .window("custom_rank", BIGINT, ImmutableList.<Type>of(), supplier(CustomRank.class))
                                .build();
                    }
                }.listFunctions()
        );
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return tpchLocalQueryRunner.execute(sql);
    }

    @Test
    public void testCustomSum()
            throws Exception
    {
        String query = "SELECT orderstatus, custom_sum(CAST(NULL AS BIGINT)) FROM orders GROUP BY orderstatus";
        assertQuery(query, query.replace("custom_sum", "sum"));
    }

    @Test
    public void testCustomRank()
            throws Exception
    {
        String query = " SELECT orderstatus, clerk, sales\n" +
                       "  , custom_rank() OVER (PARTITION BY x.orderstatus ORDER BY sales DESC) rnk\n" +
                       "  FROM (\n" +
                       "    SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                       "    FROM orders\n" +
                       "    GROUP BY orderstatus, clerk\n" +
                       "   ) x";

        MaterializedResult actual = computeActual(query);
        MaterializedResult expected = computeActual(query.replace("custom_rank", "rank"));

        assertEquals(actual, expected);
    }
}
