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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestRollbackToTimestampProcedure
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), HADOOP, ImmutableMap.of());
    }

    @DataProvider(name = "timezones")
    public Object[][] timezones()
    {
        return new Object[][] {
                {"UTC", true},
                {"America/Los_Angeles", true},
                {"Asia/Kolkata", true},
                {"UTC", false}};
    }

    private void createTable(String tableName)
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " (id integer, value varchar)");
    }

    @Test(dataProvider = "timezones")
    public void testRollbackToTimestampUsingPositionalArgs(String zoneId, boolean legacyTimestamp)
    {
        String tableName = "test_rollback_to_timestamp_table";
        createTable(tableName);
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(1, 'a')", 1);

            Table table = loadTable(tableName);
            Snapshot firstSnapshot = table.currentSnapshot();
            String firstSnapshotTimestampString = getTimestampString(System.currentTimeMillis(), zoneId);

            waitUntilAfter(firstSnapshot.timestampMillis());

            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(2, 'b')", 1);
            assertQuery(session, "select * from " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName, "values(1, 'a'), (2, 'b')");

            table.refresh();

            // Rollback to first snapshot timestamp
            @Language("SQL") String callProcedureString = format(
                    "CALL " + ICEBERG_CATALOG + ".system.rollback_to_timestamp('%s', '%s', TIMESTAMP '%s')",
                    TEST_SCHEMA,
                    tableName,
                    firstSnapshotTimestampString);

            assertUpdate(session, callProcedureString);

            // Rollback should be successful
            assertQuery(session, "select * from " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName, "values(1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "timezones")
    public void testRollbackToTimestampUsingNamedArgs(String zoneId, boolean legacyTimestamp)
    {
        String tableName = "test_rollback_to_timestamp_args_table";
        createTable(tableName);
        Session session = sessionForTimezone(zoneId, legacyTimestamp);

        try {
            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(1, 'a')", 1);

            Table table = loadTable(tableName);
            Snapshot firstSnapshot = table.currentSnapshot();
            String firstSnapshotTimestampString = getTimestampString(System.currentTimeMillis(), zoneId);

            waitUntilAfter(firstSnapshot.timestampMillis());

            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(2, 'b')", 1);
            assertQuery(session, "select * from " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName, "values(1, 'a'), (2, 'b')");

            table.refresh();

            // Rollback to first snapshot timestamp
            @Language("SQL") String callProcedureString = format(
                    "CALL " + ICEBERG_CATALOG + ".system.rollback_to_timestamp(timestamp => TIMESTAMP '%s', table_name => '%s', schema => '%s')",
                    firstSnapshotTimestampString,
                    tableName,
                    TEST_SCHEMA);

            assertUpdate(session, callProcedureString);

            // Rollback should be successful
            assertQuery(session, "select * from " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName, "values(1, 'a')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testNoValidSnapshotFound()
    {
        String tableName = "test_no_valid_snapshot";
        createTable(tableName);
        Session session = sessionForTimezone("UTC", false);

        try {
            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(1, 'a')", 1);

            Table table = loadTable(tableName);
            Snapshot firstSnapshot = table.currentSnapshot();

            waitUntilAfter(firstSnapshot.timestampMillis());

            assertUpdate(session, "INSERT INTO " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + " VALUES(2, 'b')", 1);
            assertQuery(session, "select * from " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName, "values(1, 'a'), (2, 'b')");

            table.refresh();

            // Try to roll back to snapshot older than first snapshot
            String olderThamfirstSnapshotTimestampString = "TIMESTAMP '1995-04-26 10:15:30.000'";

            @Language("SQL") String callProcedureString = format(
                    "CALL " + ICEBERG_CATALOG + ".system.rollback_to_timestamp(timestamp => %s, table_name => '%s', schema => '%s')",
                    olderThamfirstSnapshotTimestampString,
                    tableName,
                    TEST_SCHEMA);

            assertQueryFails(callProcedureString, ".*Cannot roll back, no valid snapshot older than.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidRollbackToTimestampCases()
    {
        String timestamp = "TIMESTAMP '1995-04-26 10:15:30.000'";

        assertQueryFails(
                format("CALL system.rollback_to_timestamp('n', table_name => 't', %s)", timestamp),
                ".*Named and positional arguments cannot be mixed");

        assertQueryFails(
                format("CALL custom.rollback_to_timestamp('n', 't', %s)", timestamp),
                "Procedure not registered: custom.rollback_to_timestamp");

        assertQueryFails(
                "CALL system.rollback_to_timestamp('n', 't')",
                ".*Required procedure argument 'timestamp' is missing");

        assertQueryFails(
                format("CALL system.rollback_to_timestamp(table_name => 't', timestamp => %s)", timestamp),
                ".*Required procedure argument 'schema' is missing");

        assertQueryFails(
                format("CALL system.rollback_to_timestamp('n', 't', %s, 'extra')", timestamp),
                ".*Too many arguments for procedure");

        assertQueryFails(
                "CALL system.rollback_to_timestamp('s', 't', 2.2)",
                ".*Cannot cast type decimal\\(2,1\\) to timestamp");

        assertQueryFails(
                format("CALL system.rollback_to_timestamp('', 't', %s)", timestamp),
                "schemaName is empty");

        assertQueryFails(
                format("CALL system.rollback_to_timestamp('s', '', %s)", timestamp),
                "tableName is empty");
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName);
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

    private static String getTimestampString(long timeMillsUtc, String zoneId)
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
        Catalog catalog = CatalogUtil.loadCatalog(HADOOP.getCatalogImpl(), ICEBERG_CATALOG, getProperties(), new Configuration());
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

    private static long waitUntilAfter(long snapshotTimeMillis)
    {
        long currentTimeMillis = System.currentTimeMillis();
        assertTrue(snapshotTimeMillis - currentTimeMillis <= 10,
                format("Snapshot time %s is greater than the current time %s by more than 10ms", snapshotTimeMillis, currentTimeMillis));

        while (currentTimeMillis <= snapshotTimeMillis) {
            currentTimeMillis = System.currentTimeMillis();
        }
        return currentTimeMillis;
    }
}
