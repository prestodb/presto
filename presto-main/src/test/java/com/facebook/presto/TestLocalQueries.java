package com.facebook.presto;

import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.TestingTpchBlocksProvider;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import static com.facebook.presto.util.TestingTpchBlocksProvider.readTpchRecords;

public class TestLocalQueries
        extends AbstractTestQueries
{
    private String catalog;
    private String schema;
    private DataStreamProvider dataStreamProvider;

    @Override
    protected void setUpQueryFramework(String catalog, String schema)
            throws Exception
    {
        this.catalog = catalog;
        this.schema = schema;

        TestingTpchBlocksProvider tpchBlocksProvider = new TestingTpchBlocksProvider(ImmutableMap.of(
                "orders", readTpchRecords("orders"),
                "lineitem", readTpchRecords("lineitem")));
        dataStreamProvider = new TpchDataStreamProvider(tpchBlocksProvider);
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        Session session = new Session(null, catalog, schema);

        LocalQueryRunner runner = new LocalQueryRunner(dataStreamProvider, TpchMetadata.createTpchMetadata(), MockLocalStorageManager.createMockLocalStorageManager(), session);
        return runner.execute(sql);
    }
}
