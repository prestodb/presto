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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testTimestampWithTimezone()
    {
        String timestamptz = "TIMESTAMP '1984-12-08 00:10:00 UTC'";
        String timestamp = "TIMESTAMP '1984-12-08 00:10:00'";

        getQueryRunner().execute("CREATE TABLE test_timestamptz(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE)");
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

        String early_timestamptz = "TIMESTAMP '1980-12-08 00:10:00 America/Los_Angeles'";
        getQueryRunner().execute("CREATE TABLE test_timestamptz_filter(a TIMESTAMP WITH TIME ZONE)");

        for (int i = 0; i < 5; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz_filter VALUES (" + early_timestamptz + ")");
        }
        for (int i = 0; i < 5; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz_filter VALUES (" + timestamptz + ")");
        }

        MaterializedResult earlyRows = getQueryRunner().execute("SELECT a FROM test_timestamptz_filter WHERE a =" + timestamptz);
        assertEquals(earlyRows.getMaterializedRows().size(), 5);
    }
}
