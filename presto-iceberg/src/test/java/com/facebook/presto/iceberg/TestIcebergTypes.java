package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertTrue;

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
        getQueryRunner().execute("CREATE TABLE test_timestamptz(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE)");

        String timestamptz = "TIMESTAMP '1984-12-08 00:10:00 America/Los_Angeles'";
        String timestamp = "TIMESTAMP '1984-12-08 00:10:00'";
        String row = "(" + timestamptz + ", " + timestamp + ", " + timestamptz + ")";
        for (int i = 0; i < 10; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz values " + row);
        }

        MaterializedResult initialRows = getQueryRunner().execute("SELECT * FROM test_timestamptz");
        List<Type> types = initialRows.getTypes();

        assertTrue(types.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(types.get(1) instanceof TimestampType);

        getQueryRunner().execute("CREATE TABLE test_timestamptz_partition(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE) " +
                "WITH (PARTITIONING = ARRAY['b'])");
        getQueryRunner().execute("INSERT INTO test_timestamptz_partition (a, b, c) SELECT a, b, c FROM test_timestamptz");

        MaterializedResult partitionRows = getQueryRunner().execute("SELECT * FROM test_timestamptz");
        List<Type> partitionTypes = partitionRows.getTypes();

        assertTrue(partitionTypes.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(partitionTypes.get(1) instanceof TimestampType);
    }
}
