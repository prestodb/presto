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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.testing.SampledTpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestLocalQueriesSampled
        extends AbstractTestSampledQueries
{
    private QueryRunner queryRunner;
    private QueryRunner sampledQueryRunner;
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
    protected ConnectorSession setUpQueryFramework()
    {
        ConnectorSession session = new ConnectorSession("user", "test", "local", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, null, null);

        LocalQueryRunner queryRunner = new LocalQueryRunner(session, getExecutor());
        queryRunner.createCatalog(session.getCatalog(), new SampledTpchConnectorFactory(queryRunner.getNodeManager(), 1, 1), ImmutableMap.<String, String>of());
        queryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);
        this.queryRunner = queryRunner;

        LocalQueryRunner sampledQueryRunner = new LocalQueryRunner(session, getExecutor());
        sampledQueryRunner.createCatalog(session.getCatalog(), new SampledTpchConnectorFactory(sampledQueryRunner.getNodeManager(), 1, 2), ImmutableMap.<String, String>of());
        sampledQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);
        this.sampledQueryRunner = sampledQueryRunner;

        // dump query plan to console (for debugging)
        // tpchLocalQueryRunner.printPlan();

        return session;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return queryRunner.execute(sql).toJdbcTypes();
    }

    @Override
    protected MaterializedResult computeActualSampled(@Language("SQL") String sql)
    {
        return sampledQueryRunner.execute(sql).toJdbcTypes();
    }
}
