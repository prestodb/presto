package com.facebook.presto.tests;

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;

public class TestFractionalTableSample extends AbstractTestQueryFramework
{

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(TEST_SESSION);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @org.testng.annotations.Test
    public void testTableSampleWIthF()
    {
        assertQuerySucceeds("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.000000000000000000001)");
        assertQuerySucceeds("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.02)");
    }


}