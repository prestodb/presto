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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergTypes
        extends AbstractTestQueryFramework
{
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    @DataProvider(name = "testTimestampWithTimezone")
    public Object[][] createTestTimestampWithTimezoneData()
    {
        return new Object[][] {
                {Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "true")
                        .build()},
                {Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "false")
                        .build()}
        };
    }

    @Test(dataProvider = "testTimestampWithTimezone")
    public void testTimestampWithTimezone(Session session)
    {
        QueryRunner runner = getQueryRunner();
        String timestamptz = "TIMESTAMP '1984-12-08 00:10:00 America/Los_Angeles'";
        String timestamp = "TIMESTAMP '1984-12-08 00:10:00'";

        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE)");

        String row = "(" + timestamptz + ", " + timestamp + ", " + timestamptz + ")";
        for (int i = 0; i < 10; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz values " + row, 1);
        }

        MaterializedResult initialRows = runner.execute(session, "SELECT * FROM test_timestamptz");

        List<Type> types = initialRows.getTypes();
        assertTrue(types.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(types.get(1) instanceof TimestampType);

        List<MaterializedRow> rows = initialRows.getMaterializedRows();
        for (int i = 0; i < 10; i++) {
            assertEquals("[1984-12-08T08:10Z[UTC], 1984-12-08T00:10, 1984-12-08T08:10Z[UTC]]", rows.get(i).toString());
        }

        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz_partition");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz_partition(a TIMESTAMP WITH TIME ZONE, b TIMESTAMP, c TIMESTAMP WITH TIME ZONE) " +
                "WITH (PARTITIONING = ARRAY['b'])");
        assertUpdate(session, "INSERT INTO test_timestamptz_partition (a, b, c) SELECT a, b, c FROM test_timestamptz", 10);

        MaterializedResult partitionRows = runner.execute(session, "SELECT * FROM test_timestamptz");

        List<Type> partitionTypes = partitionRows.getTypes();
        assertTrue(partitionTypes.get(0) instanceof TimestampWithTimeZoneType);
        assertTrue(partitionTypes.get(1) instanceof TimestampType);

        rows = partitionRows.getMaterializedRows();
        for (int i = 0; i < 10; i++) {
            assertEquals("[1984-12-08T08:10Z[UTC], 1984-12-08T00:10, 1984-12-08T08:10Z[UTC]]", rows.get(i).toString());
        }

        String earlyTimestamptz = "TIMESTAMP '1980-12-08 00:10:00 America/Los_Angeles'";
        dropTableIfExists(runner, session.getCatalog().get(), session.getSchema().get(), "test_timestamptz_filter");
        assertQuerySucceeds(session, "CREATE TABLE test_timestamptz_filter(a TIMESTAMP WITH TIME ZONE)");

        for (int i = 0; i < 5; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz_filter VALUES (" + earlyTimestamptz + ")", 1);
        }
        for (int i = 0; i < 5; i++) {
            assertUpdate(session, "INSERT INTO test_timestamptz_filter VALUES (" + timestamptz + ")", 1);
        }

        MaterializedResult lateRows = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a > " + earlyTimestamptz);
        assertEquals(lateRows.getMaterializedRows().size(), 5);

        MaterializedResult lateRowsFromEquals = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a = " + timestamptz);
        com.facebook.presto.testing.assertions.Assert.assertEquals(lateRows, lateRowsFromEquals);

        MaterializedResult earlyRows = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a < " + timestamptz);
        assertEquals(earlyRows.getMaterializedRows().size(), 5);

        MaterializedResult earlyRowsFromEquals = runner.execute(session, "SELECT a FROM test_timestamptz_filter WHERE a = " + earlyTimestamptz);
        com.facebook.presto.testing.assertions.Assert.assertEquals(earlyRows, earlyRowsFromEquals);
    }
}
