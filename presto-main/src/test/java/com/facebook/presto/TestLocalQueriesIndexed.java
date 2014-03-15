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

import com.facebook.presto.spi.Session;
import com.facebook.presto.tpch.IndexedTpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestLocalQueriesIndexed
        extends AbstractTestIndexedQueries
{
    private LocalQueryRunner localIndexedQueryRunner;
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
    protected Session setUpQueryFramework()
    {
        Session session = new Session("user", "test", "local", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, null, null);
        localIndexedQueryRunner = new LocalQueryRunner(session, getExecutor());

        // add the tpch catalog
        // local queries run directly against the generator
        localIndexedQueryRunner.createCatalog(session.getCatalog(), new IndexedTpchConnectorFactory(localIndexedQueryRunner.getNodeManager(), getTpchIndexSpec(), 1), ImmutableMap.<String, String>of());

        // dump query plan to console (for debugging)
        // tpchLocalQueryRunner.printPlan();

        return session;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return localIndexedQueryRunner.execute(sql);
    }
}
