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

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of());
    }

    @Test
    public void testTimestampWithTimezone()
    {
        String timestamptz = "TIMESTAMP '1984-12-08 00:10:00 America/Los_Angeles'";
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

        String earlyTimestamptz = "TIMESTAMP '1980-12-08 00:10:00 America/Los_Angeles'";
        getQueryRunner().execute("CREATE TABLE test_timestamptz_filter(a TIMESTAMP WITH TIME ZONE)");

        for (int i = 0; i < 5; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz_filter VALUES (" + earlyTimestamptz + ")");
        }
        for (int i = 0; i < 5; i++) {
            getQueryRunner().execute("INSERT INTO test_timestamptz_filter VALUES (" + timestamptz + ")");
        }

        MaterializedResult lateRows = getQueryRunner().execute("SELECT a FROM test_timestamptz_filter WHERE a > " + earlyTimestamptz);
        assertEquals(lateRows.getMaterializedRows().size(), 5);

        MaterializedResult rows = getQueryRunner().execute("SELECT * FROM test_timestamptz_filter");
        System.out.println(rows.toString());

        MaterializedResult earlyRows = getQueryRunner().execute("SELECT a FROM test_timestamptz_filter WHERE a <= " + earlyTimestamptz);
        assertEquals(earlyRows.getMaterializedRows().size(), 5);
    }
}
