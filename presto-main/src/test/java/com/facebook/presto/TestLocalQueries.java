package com.facebook.presto;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.util.LocalQueryRunner.createTpchLocalQueryRunner;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private LocalQueryRunner tpchLocalQueryRunner;

    @Override
    protected int getNodeCount()
    {
        return 1;
    }

    @Override
    protected void setUpQueryFramework(String catalog, String schema)
    {
        tpchLocalQueryRunner = createTpchLocalQueryRunner(new Session("user", "test", catalog, schema, null, null));

        // dump query plan to console (for debugging)
        // tpchLocalQueryRunner.textLogicalPlan();
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return tpchLocalQueryRunner.execute(sql);
    }
}
