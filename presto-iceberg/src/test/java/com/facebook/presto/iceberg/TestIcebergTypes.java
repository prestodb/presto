package com.facebook.presto.iceberg;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestIcebergTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testTimestampWithTimezone()
    {
        getQueryRunner().execute("CREATE TABLE test_timestamptz(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP WITH TIME ZONE, c TIMESTAMP WITH TIME ZONE)");
        String genericTimestamptz = "TIMESTAMP '1984-12-08 00:10:00 America/Los_Angeles'";
        String genericRow = "(" + genericTimestamptz + ", " + genericTimestamptz + ", " + genericTimestamptz + ")";
        for (int i = 0; i < 10; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz values " + genericRow);
        }
        MaterializedResult res = getQueryRunner().execute("DESCRIBE test_timestamptz");
        System.out.println(res.toString());
    }
}
