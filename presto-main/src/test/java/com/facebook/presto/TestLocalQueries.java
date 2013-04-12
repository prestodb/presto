package com.facebook.presto;

import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import org.intellij.lang.annotations.Language;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private String catalog;
    private String schema;
    private DataStreamProvider dataStreamProvider;

    @Override
    protected void setUpQueryFramework(String catalog, String schema, TpchDataStreamProvider dataStreamProvider)
            throws Exception
    {
        this.catalog = catalog;
        this.schema = schema;
        this.dataStreamProvider = dataStreamProvider;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        Session session = new Session(null, catalog, schema);

        LocalQueryRunner runner = new LocalQueryRunner(dataStreamProvider, TpchSchema.createMetadata(), MockLocalStorageManager.createMockLocalStorageManager(), session);
        return runner.execute(sql);
    }
}
