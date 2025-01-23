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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Integration tests for reading Delta tables.
 */
@Test
public class TestDeltaIntegration
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test(dataProvider = "deltaReaderVersions")
    public void readPrimitiveTypeData(String version)
    {
        // Test reading following primitive types from a Delta table
        // (all integers, float, double, decimal, boolean, varchar, varbinary)
        String testQuery =
                format("SELECT * FROM \"%s\".\"%s\"", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        "data-reader-primitives"));
        String expResultsQuery = getPrimitiveTypeTableData();
        assertQuery(testQuery, expResultsQuery);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readArrayTypeData(String version)
    {
        // Test reading following array elements with type
        // (all integers, float, double, decimal, boolean, varchar, varbinary)
        String testQuery =
                format("SELECT * FROM \"%s\".\"%s\"", PATH_SCHEMA, goldenTablePathWithPrefix(version,
                        "data-reader-array-primitives"));

        // Create query for the expected results.
        List<String> expRows = new ArrayList<>();
        for (byte i = 0; i < 10; i++) {
            expRows.add(format("SELECT " +
                    "   array[cast(%s as integer)]," +
                    "   array[cast(%s as bigint)]," +
                    "   array[cast(%s as tinyint)]," +
                    "   array[cast(%s as smallint)]," +
                    "   array[%s]," +
                    "   array[cast(%s as real)]," +
                    "   array[cast(%s as double)], " +
                    "   array['%s'], " +
                    "   array[cast(X'0%s0%s' as varbinary)], " +
                    "   array[cast(%s as decimal)]", i, i, i, i, (i % 2 == 0 ? "true" : "false"), i, i, i, i, i, i));
        }
        String expResultsQuery = Joiner.on(" UNION ").join(expRows);

        assertQuery(testQuery, expResultsQuery);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readMapTypeData(String version)
    {
        // Test reading MAP data type columns from Delta table
        String testQuery =
                format("SELECT map_keys(a), map_values(e) FROM \"%s\".\"%s\"", PATH_SCHEMA,
                        goldenTablePathWithPrefix(version, "data-reader-map"));

        List<String> expRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expRows.add("SELECT " +
                    "   ARRAY[cast(" + i + " as integer)]," +
                    "   ARRAY[cast(" + i + " as decimal)]");
        }
        String expResultsQuery = Joiner.on(" UNION ").join(expRows);

        assertQuery(testQuery, expResultsQuery);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readTableRegisteredInHMS(String version)
    {
        String expResultsQuery = getPrimitiveTypeTableData();
        assertQuery("SELECT * FROM \"" + getVersionPrefix(version) +
                "data-reader-primitives\"", expResultsQuery);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readSpecificSnapshotVersion(String version)
    {
        String testQueryTemplate = "SELECT * FROM \"" + getVersionPrefix(version) +
                "snapshot-data3@%s\" WHERE col1 = 0";

        // read snapshot version 2
        String testQueryV2 = format(testQueryTemplate, "v2");
        String expResultsQueryV2 = "SELECT * FROM VALUES(0, 'data-2-0')";
        assertQuery(testQueryV2, expResultsQueryV2);

        // read snapshot version 3
        String testQueryV3 = format(testQueryTemplate, "v3");
        String expResultsQueryV3 = "SELECT * FROM VALUES(0, 'data-2-0'), (0, 'data-3-0')";
        assertQuery(testQueryV3, expResultsQueryV3);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readSpecificSnapshotAtGivenTimestamp(String version)
            throws Exception
    {
        String deltaTable = "snapshot-data3";
        String testQueryTemplate = "SELECT * FROM \"" + getVersionPrefix(version) +
                "snapshot-data3@%s\" WHERE col1 = 0";

        // Delta library looks at the last modification time of the checkpoint and commit files
        // to figure out when the snapshot is created. In the tests, as the test files are copied
        // to target location, the modification time will be the time when they are copied and not when
        // the snapshot is created. In order to test reading the snapshot version for a given timestamp,
        // manually update the modification time of the commit and checkpoint files.
        // 1637274601000L millis = 2021-11-18 10:30:01
        String deltaTableLocation = goldenTablePathWithPrefix(version, deltaTable);
        setCommitFileModificationTime(deltaTableLocation, 0, 1637231401000L);
        setCommitFileModificationTime(deltaTableLocation, 1, 1637231402000L);
        setCommitFileModificationTime(deltaTableLocation, 2, 1637231405000L);
        setCommitFileModificationTime(deltaTableLocation, 3, 1637231407000L);

        // read snapshot as of 2020-10-26 02:50:00 - this should fail as there are no snapshots before this timestamp
        String testQueryTs1 = format(testQueryTemplate, "t2020-10-27 02:50:00");
        assertQueryFails(
                testQueryTs1,
                ".*The provided timestamp 1603767000000 ms \\(2020-10-27T02:50:00Z\\) is before the earliest available version 0\\. Please use a timestamp greater than or equal to 1637231401000 ms \\(2021-11-18T10:30:01Z\\)\\.");

        // read snapshot as of 2021-11-18 10:30:02 - this should read the data from commit id 1.
        String testQueryTs2 = format(testQueryTemplate, "t2021-11-18 10:30:02");
        String expResultsQueryTs2 = "SELECT * FROM VALUES(0, 'data-0-0'), (0, 'data-1-0')";
        assertQuery(testQueryTs2, expResultsQueryTs2);

        // read snapshot as of 2021-11-18 10:30:07 - this should read the data from the latest commit
        String testQueryTs3 = format(testQueryTemplate, "t2021-11-18 10:30:07");
        String expResultsQueryTs3 = "SELECT * FROM VALUES(0, 'data-2-0'), (0, 'data-3-0')";
        assertQuery(testQueryTs3, expResultsQueryTs3);
    }

    @Test(enabled = false, dataProvider = "deltaReaderVersions") // Enable once the bug in Delta library is fixed
    public void readCheckpointedDeltaTable(String version)
    {
        // Delta table commits are periodically checkpointed into a parquet file.
        // Test Delta connector is able to read the checkpointed commits in a parquet file.
        // Test table has commit files (0-10) deleted. So it has to rely on reading the Parquet file
        // to fetch the files latest commit (i.e > 10).
        String testQueryTemplate = "SELECT * FROM \"" + getVersionPrefix(version) +
                "checkpointed-delta-table%s\" WHERE col1 in (0, 10, 15)";

        // read snapshot version 3 - expect can't time travel error
        String testQueryV3 = format(testQueryTemplate, "@v3");
        assertQueryFails(
                testQueryV3,
                "Can not find snapshot \\(3\\) in Delta table 'deltatables." +
                        getVersionPrefix(version) +
                        "checkpointed-delta-table\\@v3': No reproducible commits found at .*");

        // read latest data
        String testQueryLatest = format(testQueryTemplate, "");
        String expResultsQueryLatest = "SELECT * FROM VALUES(0, 'data-0-0'), (10, 'data-0-10'), (15, 'data-0-15')";
        assertQuery(testQueryLatest, expResultsQueryLatest);

        // read snapshot version 13
        String testQueryV13 = format(testQueryTemplate, "@v13");
        String expResultsQueryV13 = "SELECT * FROM VALUES(0, 'data-0-0'), (10, 'data-0-10')";
        assertQuery(testQueryV13, expResultsQueryV13);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readPartitionedTable(String version)
    {
        String testQuery1 = "SELECT * FROM \"" + getVersionPrefix(version) +
                "time-travel-partition-changes-b\" WHERE id in (10, 15, 12, 13)";
        String expResultsQuery1 = "SELECT * FROM VALUES(10, 0),(15, 1),(12, 0),(13, 1)";
        assertQuery(testQuery1, expResultsQuery1);

        // reorder the columns in output and query the partitioned table
        String testQuery2 = "SELECT part2, id FROM \"" + getVersionPrefix(version) +
                "time-travel-partition-changes-b\" WHERE id in (16, 14, 19)";
        String expResultsQuery2 = "SELECT * FROM VALUES(0, 16),(0, 14),(1, 19)";
        assertQuery(testQuery2, expResultsQuery2);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void readPartitionedTableAllDataTypes(String version)
    {
        String testQuery = "SELECT * FROM \"" + getVersionPrefix(version) +
                "data-reader-partition-values\"";
        String expResultsQuery = "SELECT * FROM VALUES" +
                "( 0," +
                "  cast(0 as bigint)," +
                "  cast(0 as smallint), " +
                "  cast(0 as tinyint), " +
                "  true, " +
                "  0.0, " +
                "  cast(0.0 as double), " +
                "  '0', " +
                "  DATE '2021-09-08', " +
                "  TIMESTAMP WITH TIME ZONE '2021-09-08 11:11:11 UTC', " +
                "  cast(0 as decimal)," +
                "  '0'" + // regular column
                "), " +
                "( 1," +
                "  cast(1 as bigint)," +
                "  cast(1 as smallint), " +
                "  cast(1 as tinyint), " +
                "  false, " +
                "  1.0, " +
                "  cast(1.0 as double), " +
                "  '1', " +
                "  DATE '2021-09-08', " +
                "  TIMESTAMP WITH TIME ZONE '2021-09-08 11:11:11 UTC', " +
                "  cast(1 as decimal), " +
                "  '1'" + // regular column
                "), " +
                "( null," +
                "  null," +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  null, " +
                "  '2'" + // regular column
                ")";
        assertQuery(testQuery, expResultsQuery);
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void testDeltaTimezoneTypeSupportINT96(String version)
    {
        /*
        https://docs.delta.io/3.2.1/api/java/kernel/index.html?io/delta/kernel/types/TimestampNTZType.html
        According to delta's type specifications, the expected behaviour for TimestampNTZ
        The timestamp without time zone type represents a local time in microsecond precision, which is independent of time zone.
        So TimestampNTZ is independent of local timezones and should return the same value regardless of the timezone.
        If legacy_timestamp is true, Presto TimestampNTZ (Timestamp) is adjusted to the timezone.
        If legacy_timestamp is false, TimestampNTZ is not adjusted.
        This test sets the timezone to UTC+3, and the original data file the timestamp is 12 AM.
        The proper delta implementation would return 12 AM regardless of the timezone, but with
        legacy_timestamp true we get 3 AM. legacy_timestamp set to false matches the specifications.
         */
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("UTC+3"))
                .setSystemProperty("legacy_timestamp", "false")
                .build();
        String testQuery = format("SELECT tpep_dropoff_datetime, tpep_pickup_datetime FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "test-typing"));

        MaterializedResult actual = computeActual(session, testQuery);

        String timestamptzField = actual.getMaterializedRows().get(0).getField(0).toString();

        assertEquals(timestamptzField, "2021-12-31T16:53:29Z[UTC]", "Delta Timestamp type not being read correctly.");
        if (version.equals("delta_v3")) {
            String timestamptzntz = actual.getMaterializedRows().get(0).getField(1).toString();
            assertEquals(timestamptzntz, "2022-01-01T00:35:40", "Delta TimestampNTZ type not being read correctly.");
        }
    }

    @Test(dataProvider = "deltaReaderVersions")
    public void testDeltaTimezoneTypeSupportINT64(String version)
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("UTC+3"))
                .setSystemProperty("legacy_timestamp", "false")
                .build();
        String testQuery = format("SELECT created_at_tz FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(version, "timestamp_64"));

        MaterializedResult actual = computeActual(session, testQuery);

        String timestamptzField = actual.getMaterializedRows().get(0).getField(0).toString();

        assertEquals(timestamptzField, "2025-05-22T09:24:11.321Z[UTC]", "Delta Timestamp type not being read correctly.");
        if (version.equals("delta_v3")) {
            String ntzTestQuery = format("SELECT created_at_ntz, created_at_ntz FROM \"%s\".\"%s\"",
                    PATH_SCHEMA, goldenTablePathWithPrefix(version, "timestamp_64"));

            actual = computeActual(session, ntzTestQuery);
            String timestamptzntz = actual.getMaterializedRows().get(0).getField(0).toString();
            assertEquals(timestamptzntz, "2025-05-22T12:25:16.544", "Delta TimestampNTZ type not being read correctly.");
        }
    }
    /**
     * Expected results for table "data-reader-primitives"
     */
    private static String getPrimitiveTypeTableData()
    {
        // Create query for the expected results.
        List<String> expRows = new ArrayList<>();
        for (byte i = 0; i < 10; i++) {
            expRows.add(format("SELECT " +
                    "   cast(%s as integer)," +
                    "   cast(%s as bigint)," +
                    "   cast(%s as tinyint)," +
                    "   cast(%s as smallint)," +
                    "   %s," +
                    "   cast(%s as real)," +
                    "   cast(%s as double), " +
                    "   '%s', " +
                    "   cast(X'0%s0%s' as varbinary), " +
                    "   cast(%s as decimal)", i, i, i, i, (i % 2 == 0 ? "true" : "false"), i, i, i, i, i, i));
        }
        expRows.add("SELECT null, null, null, null, null, null, null, null, null, null");
        return Joiner.on(" UNION ").join(expRows);
    }

    private static void setCommitFileModificationTime(String tableLocation, long commitId, long commitTimeMillis)
            throws Exception
    {
        Files.setLastModifiedTime(
                Paths.get(URI.create(tableLocation)).resolve("_delta_log/").resolve(format("%020d.json", commitId)),
                FileTime.from(commitTimeMillis, TimeUnit.MILLISECONDS));
    }
}
