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
