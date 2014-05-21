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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.tpch.testing.SampledTpchConnectorFactory;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestLocalQueries
        extends AbstractTestSampledQueries
{
    private LocalQueryRunner localQueryRunner;
    private LocalQueryRunner localSampledQueryRunner;
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
        localQueryRunner = new LocalQueryRunner(session, getExecutor());
        localSampledQueryRunner = new LocalQueryRunner(session, getExecutor());

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(session.getCatalog(), new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1), ImmutableMap.<String, String>of());
        localSampledQueryRunner.createCatalog(session.getCatalog(), new SampledTpchConnectorFactory(localSampledQueryRunner.getNodeManager(), 1, 2), ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        // dump query plan to console (for debugging)
        // tpchLocalQueryRunner.printPlan();

        return session;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return localQueryRunner.execute(sql).toJdbcTypes();
    }

    @Override
    protected MaterializedResult computeActualSampled(@Language("SQL") String sql)
    {
        return localSampledQueryRunner.execute(sql).toJdbcTypes();
    }
}
