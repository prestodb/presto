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

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

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
        // tpchLocalQueryRunner.textLogicalPlan();
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return tpchLocalQueryRunner.execute(sql);
    }
}
