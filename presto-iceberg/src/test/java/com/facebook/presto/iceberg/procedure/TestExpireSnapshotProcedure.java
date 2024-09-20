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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestExpireSnapshotProcedure
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "test_hive";
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), HADOOP, ImmutableMap.of());
    }

    public void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg." + TEST_SCHEMA + "." + tableName);
    }

    @DataProvider(name = "timezones")
    public Object[][] timezones()
    {
        return new Object[][] {
                {"UTC", true},
                {"America/Los_Angeles", true},
                {"Asia/Shanghai", true},
                {"UTC", false}};
    }

    @Test
    public void testExpireSnapshotsInEmptyTable()
    {
        String tableName = "default_empty_table";
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id integer, value integer)");
        Table table = loadTable(tableName);
        assertHasSize(table.snapshots(), 0);

        assertUpdate(format("CALL system.expire_snapshots('%s', '%s')", TEST_SCHEMA, tableName));
        assertUpdate(format("CALL system.expire_snapshots('%s', '%s', %s)", TEST_SCHEMA, tableName, "timestamp '1984-12-08 00:10:00.000'"));
        assertUpdate(format("CALL system.expire_snapshots('%s', '%s', %s, %s)", TEST_SCHEMA, tableName, "timestamp '1984-12-08 00:10:00.000'", 5));
        assertUpdate(format("CALL system.expire_snapshots('%s', '%s', %s, %s, %s)", TEST_SCHEMA, tableName, "timestamp '1984-12-08 00:10:00.000'", 5, "ARRAY[12345]"));
        table.refresh();
        assertHasSize(table.snapshots(), 0);

        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s')", TEST_SCHEMA, tableName));
        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', older_than => %s)", TEST_SCHEMA, tableName, "timestamp '1984-12-08 00:10:00.000'"));
        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', retain_last => %s)", TEST_SCHEMA, tableName, 5));
        assertUpdate(format("CALL system.expire_snapshots(schema => '%s', table_name => '%s', snapshot_ids => %s)", TEST_SCHEMA, tableName, "ARRAY[12345]"));
        table.refresh();
        assertHasSize(table.snapshots(), 0);

        dropTable(tableName);
    }

    @Test(dataProvider = "timezones")
    public void testExpireSnapshotsUsingPositionalArgs(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        String tableName = "positional_args_table";
        try {
            assertUpdate(session, "CREATE TABLE IF NOT EXISTS " + tableName + " (id integer, value varchar)");
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(1, 'a')", 1);

            Table table = loadTable(tableName);
            Snapshot firstSnapshot = table.currentSnapshot();

            waitUntilAfter(firstSnapshot.timestampMillis());

            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(2, 'b')", 1);
            table.refresh();

            Snapshot secondSnapshot = table.currentSnapshot();
            String formattedDateTime = getTimestampString(secondSnapshot.timestampMillis(), zoneId);

            assertHasSize(table.snapshots(), 2);

            // expire without retainLast param
            String callStr = format("CALL system.expire_snapshots('%s', '%s', TIMESTAMP '%s')", TEST_SCHEMA, tableName, formattedDateTime);
            assertUpdate(session, callStr);

            table.refresh();
            assertHasSize(table.snapshots(), 1);

            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(3, 'c')", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(4, 'd')", 1);
            assertQuery(session, "select * from " + tableName, "values(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");

            table.refresh();

            waitUntilAfter(table.currentSnapshot().timestampMillis());

            String currentTimestamp = getTimestampString(System.currentTimeMillis(), zoneId);

            assertHasSize(table.snapshots(), 3);

            // expire with retainLast param
            assertUpdate(session, format("CALL system.expire_snapshots('%s', '%s', TIMESTAMP '%s', %s)", TEST_SCHEMA, tableName, currentTimestamp, 2));

            table.refresh();
            assertHasSize(table.snapshots(), 2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "timezones")
    public void testExpireSnapshotUsingNamedArgsOfOlderThanAndRetainLast(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        String tableName = "named_args_table";
        try {
            assertUpdate(session, "CREATE TABLE IF NOT EXISTS " + tableName + " (id integer, data varchar)");

            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(1, 'a')", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(2, 'b')", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(3, 'c')", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(4, 'd')", 1);

            Table table = loadTable(tableName);
            assertHasSize(table.snapshots(), 4);

            // By default, we would only drop the snapshots 5 days again. So there isn't any snapshot dropped here.
            assertUpdate(session, format("CALL system.expire_snapshots(retain_last => %s, table_name => '%s', schema => '%s')",
                    2, tableName, TEST_SCHEMA));
            table.refresh();
            assertHasSize(table.snapshots(), 4);

            waitUntilAfter(table.currentSnapshot().timestampMillis());
            String formattedDateTime = getTimestampString(System.currentTimeMillis(), zoneId);

            // Indicate to retain the last 2 snapshots regardless of `older_than`
            assertUpdate(session, format("CALL system.expire_snapshots(retain_last => %s, older_than => TIMESTAMP '%s', table_name => '%s', schema => '%s')",
                    2, formattedDateTime, tableName, TEST_SCHEMA));
            table.refresh();
            assertHasSize(table.snapshots(), 2);

            // By default, we would retain the last 1 snapshot
            assertUpdate(session, format("CALL system.expire_snapshots(older_than => TIMESTAMP '%s', table_name => '%s', schema => '%s')",
                    formattedDateTime, tableName, TEST_SCHEMA));
            table.refresh();
            assertHasSize(table.snapshots(), 1);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testExpireSnapshotUsingNamedArgsOfSnapshotIds()
    {
        Session session = getSession();
        String tableName = "named_args_snapshot_ids_table";
        try {
            assertUpdate(session, "CREATE TABLE IF NOT EXISTS " + tableName + " (id integer, data varchar)");

            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(1, 'a')", 1);
            Table table = loadTable(tableName);
            long snapshotId1 = table.currentSnapshot().snapshotId();
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(2, 'b')", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(3, 'c')", 1);
            table.refresh();
            long snapshotId2 = table.currentSnapshot().snapshotId();
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES(4, 'd')", 1);

            table.refresh();
            assertHasSize(table.snapshots(), 4);

            // Drop the indicated 2 snapshots
            assertUpdate(session, format("CALL system.expire_snapshots(table_name => '%s', schema => '%s', snapshot_ids => %s)",
                    tableName, TEST_SCHEMA, "array[" + snapshotId1 + ", " + snapshotId2 + "]"));
            table.refresh();
            assertHasSize(table.snapshots(), 2);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidExpireSnapshotsCases()
    {
        assertQueryFails("CALL system.expire_snapshots('n', table_name => 't')", ".*Named and positional arguments cannot be mixed");
        assertQueryFails("CALL custom.expire_snapshots('n', 't')", "Procedure not registered: custom.expire_snapshots");
        assertQueryFails("CALL system.expire_snapshots()", ".*Required procedure argument 'schema' is missing");
        assertQueryFails("CALL system.expire_snapshots('s', 'n', 2.2)", ".*Cannot cast type decimal\\(2,1\\) to timestamp");
        assertQueryFails("CALL system.expire_snapshots('', '')", "schemaName is empty");
    }

    private String getTimestampString(long timeMillsUtc, String zoneId)
    {
        Instant instant = Instant.ofEpochMilli(timeMillsUtc);
        LocalDateTime localDateTime = instant
                .atZone(ZoneId.of(zoneId))
                .toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        formatter = formatter.withZone(ZoneId.of(zoneId));
        return localDateTime.format(formatter);
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(HadoopCatalog.class.getName(), ICEBERG_CATALOG, getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    private long waitUntilAfter(long snapshotTimeMillis)
    {
        long currentTimeMillis = System.currentTimeMillis();
        assertTrue(snapshotTimeMillis - currentTimeMillis <= 10,
                format("Snapshot time %s is greater than the current time %s by more than 10ms", snapshotTimeMillis, currentTimeMillis));

        while (currentTimeMillis <= snapshotTimeMillis) {
            currentTimeMillis = System.currentTimeMillis();
        }
        return currentTimeMillis;
    }

    private void assertHasSize(Iterable iterable, int size)
    {
        AtomicInteger count = new AtomicInteger(0);
        iterable.forEach(obj -> count.incrementAndGet());
        assertEquals(count.get(), size);
    }

    private Session sessionForTimezone(String zoneId, boolean legacyTimestamp)
    {
        SessionBuilder sessionBuilder = Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, String.valueOf(legacyTimestamp));
        if (legacyTimestamp) {
            sessionBuilder.setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId));
        }
        return sessionBuilder.build();
    }
}
