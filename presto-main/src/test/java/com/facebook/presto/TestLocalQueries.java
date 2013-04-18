package com.facebook.presto;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MockStorageManager;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Throwables;
import org.intellij.lang.annotations.Language;

import java.io.IOException;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private String catalog;
    private String schema;
    private DataStreamProvider dataStreamProvider;
    private Metadata metadata;

    @Override
    protected void setUpQueryFramework(String catalog, String schema, TpchDataStreamProvider dataStreamProvider, TestingMetadata metadata)
            throws Exception
    {
        this.catalog = catalog;
        this.schema = schema;
        this.dataStreamProvider = dataStreamProvider;
        this.metadata = metadata;
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        Session session = new Session(null, catalog, schema);

        try {
            LocalQueryRunner runner = new LocalQueryRunner(dataStreamProvider, metadata, new MockStorageManager(), session);
            return runner.execute(sql);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
