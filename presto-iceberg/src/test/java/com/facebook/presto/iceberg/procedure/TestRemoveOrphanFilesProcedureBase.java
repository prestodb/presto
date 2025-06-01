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
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.Table;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergUtil.dataLocation;
import static com.facebook.presto.iceberg.IcebergUtil.metadataLocation;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.getFileSystem;
import static com.google.common.io.Files.createTempDir;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class TestRemoveOrphanFilesProcedureBase
        extends AbstractTestQueryFramework
{
    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_SCHEMA = "tpch";

    private final CatalogType catalogType;
    private final Map<String, String> extraConnectorProperties;

    protected TestRemoveOrphanFilesProcedureBase(CatalogType catalogType, Map<String, String> extraConnectorProperties)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.extraConnectorProperties = requireNonNull(extraConnectorProperties, "extraConnectorProperties is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(catalogType)
                .setExtraConnectorProperties(extraConnectorProperties)
                .build().getQueryRunner();
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
    public void testRemoveOrphanFilesInEmptyTable()
    {
        String tableName = "test_empty_remove_orphan_files_table";
        Session session = getQueryRunner().getDefaultSession();
        try {
            assertUpdate(format("create table %s (a int, b varchar)", tableName));

            Table table = loadTable(tableName);
            int metadataFilesCountBefore = allMetadataFilesCount(session, table);
            int dataFilesCountBefore = allDataFilesCount(session, table);
            assertUpdate(format("call iceberg.system.remove_orphan_files('%s', '%s')", TEST_SCHEMA, tableName));

            int metadataFilesCountAfter = allMetadataFilesCount(session, table);
            int dataFilesCountAfter = allDataFilesCount(session, table);
            assertEquals(metadataFilesCountBefore, metadataFilesCountAfter);
            assertEquals(dataFilesCountBefore, dataFilesCountAfter);
            assertQuery("select count(*) from " + tableName, "values(0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "timezones")
    public void testRemoveOrphanFilesInMetadataAndDataFolder(String zoneId, boolean legacyTimestamp)
    {
        String tableName = "test_remove_orphan_files_table";
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        try {
            assertUpdate(session, format("create table %s (a int, b varchar)", tableName));
            assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002')", tableName), 2);
            assertUpdate(session, format("insert into %s values(3, '1003'), (4, '1004')", tableName), 2);
            assertUpdate(session, format("insert into %s values(5, '1005'), (6, '1006')", tableName), 2);
            assertUpdate(session, format("delete from %s where a between 5 and 6", tableName), 2);
            assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

            Table table = loadTable(tableName);
            int metadataFilesCountBefore = allMetadataFilesCount(session, table);
            int dataFilesCountBefore = allDataFilesCount(session, table);

            // Generate 3 files in iceberg table data location
            assertTrue(generateFile(session, TEST_SCHEMA, tableName, dataLocation(table), "file_1.test"));
            assertTrue(generateFile(session, TEST_SCHEMA, tableName, dataLocation(table), "file_2.test"));
            assertTrue(generateFile(session, TEST_SCHEMA, tableName, dataLocation(table), "file_3.test"));

            // Generate 2 files in iceberg table metadata location
            assertTrue(generateFile(session, TEST_SCHEMA, tableName, metadataLocation(table), "file_4.test"));
            assertTrue(generateFile(session, TEST_SCHEMA, tableName, metadataLocation(table), "file_5.test"));

            int metadataFilesCountMiddle = allMetadataFilesCount(session, table);
            int dataFilesCountMiddle = allDataFilesCount(session, table);

            // Remove all orphan files older than now
            waitUntilAfter(System.currentTimeMillis());
            String formattedDateTime = getTimestampString(System.currentTimeMillis(), zoneId);
            assertUpdate(session, format("call iceberg.system.remove_orphan_files('%s', '%s', timestamp '%s')", TEST_SCHEMA, tableName, formattedDateTime));

            int metadataFilesCountAfter = allMetadataFilesCount(session, table);
            int dataFilesCountAfter = allDataFilesCount(session, table);

            assertEquals(metadataFilesCountBefore, metadataFilesCountAfter);
            assertEquals(dataFilesCountBefore, dataFilesCountAfter);

            assertEquals(metadataFilesCountBefore + 2, metadataFilesCountMiddle);
            assertEquals(dataFilesCountBefore + 3, dataFilesCountMiddle);

            assertEquals(metadataFilesCountMiddle, metadataFilesCountAfter + 2);
            assertEquals(dataFilesCountMiddle, dataFilesCountAfter + 3);
            assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test(dataProvider = "timezones")
    public void testRemoveOrphanFilesWithNonDefaultMetadataPath(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        String tempTableName = "temp_test_table_with_specified_metadata_path";
        String tableName = "test_table_with_specified_metadata_path";
        String tableTargetPath = createTempDir().toURI().toString();
        File metadataDir = new File(createTempDir().getAbsolutePath(), "metadata");
        metadataDir.mkdir();
        String specifiedMetadataPath = metadataDir.getAbsolutePath();

        // Create an iceberg table using specified table properties
        Table table = createTable(tempTableName, tableTargetPath,
                ImmutableMap.of(WRITE_METADATA_LOCATION, specifiedMetadataPath,
                                DELETE_MODE, "merge-on-read"));
        assertNotNull(table.properties().get(WRITE_METADATA_LOCATION));
        assertEquals(table.properties().get(WRITE_METADATA_LOCATION), specifiedMetadataPath);

        assertUpdate(session, format("CALL system.register_table('%s', '%s', '%s')", TEST_SCHEMA, tableName, metadataLocation(table)));
        assertUpdate(session, "insert into " + tableName + " values(1, '1001'), (2, '1002')", 2);
        assertUpdate(session, "insert into " + tableName + " values(3, '1003'), (4, '1004')", 2);
        assertUpdate(session, "delete from " + tableName + " where a between 3 and 4", 2);
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002')");

        int metadataFilesCountBefore = allMetadataFilesCount(session, table);
        int dataFilesCountBefore = allDataFilesCount(session, table);
        assertTrue(generateFile(session, TEST_SCHEMA, tableName, specifiedMetadataPath, "metadata_file_1.test"));
        assertTrue(generateFile(session, TEST_SCHEMA, tableName, specifiedMetadataPath, "metadata_file_2.test"));

        int metadataFilesCountMiddle = allMetadataFilesCount(session, table);
        int dataFilesCountMiddle = allDataFilesCount(session, table);

        // Remove all orphan files older than now
        waitUntilAfter(System.currentTimeMillis());
        String formattedDateTime = getTimestampString(System.currentTimeMillis(), zoneId);
        assertUpdate(session, format("call system.remove_orphan_files('%s', '%s', timestamp '%s')", TEST_SCHEMA, tableName, formattedDateTime));

        int metadataFilesCountAfter = allMetadataFilesCount(session, table);
        int dataFilesCountAfter = allDataFilesCount(session, table);

        assertEquals(metadataFilesCountBefore, metadataFilesCountAfter);
        assertEquals(dataFilesCountBefore, dataFilesCountAfter);

        assertEquals(metadataFilesCountBefore + 2, metadataFilesCountMiddle);
        assertEquals(dataFilesCountBefore, dataFilesCountMiddle);

        assertEquals(metadataFilesCountMiddle, metadataFilesCountAfter + 2);
        assertEquals(dataFilesCountMiddle, dataFilesCountAfter);
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002')");

        assertUpdate(format("CALL system.unregister_table('%s', '%s')", TEST_SCHEMA, tableName));
        dropTableFromCatalog(tempTableName);
    }

    @Test(dataProvider = "timezones")
    public void testRemoveOrphanFilesWithNonDefaultDataPath(String zoneId, boolean legacyTimestamp)
    {
        Session session = sessionForTimezone(zoneId, legacyTimestamp);
        String tempTableName = "temp_test_table_with_specified_data_path";
        String tableName = "test_table_with_specified_data_path";
        String tableTargetPath = createTempDir().toURI().toString();
        File dataDir = new File(createTempDir().getAbsolutePath(), "metadata");
        dataDir.mkdir();
        String specifiedDataPath = dataDir.getAbsolutePath();

        // Create an iceberg table using specified table properties
        Table table = createTable(tempTableName, tableTargetPath,
                ImmutableMap.of(WRITE_DATA_LOCATION, specifiedDataPath,
                                DELETE_MODE, "merge-on-read"));
        assertNotNull(table.properties().get(WRITE_DATA_LOCATION));
        assertEquals(table.properties().get(WRITE_DATA_LOCATION), specifiedDataPath);

        assertUpdate(session, format("CALL system.register_table('%s', '%s', '%s')", TEST_SCHEMA, tableName, metadataLocation(table)));
        assertUpdate(session, "insert into " + tableName + " values(1, '1001'), (2, '1002')", 2);
        assertUpdate(session, "insert into " + tableName + " values(3, '1003'), (4, '1004')", 2);
        assertUpdate(session, "delete from " + tableName + " where a between 3 and 4", 2);
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002')");

        int metadataFilesCountBefore = allMetadataFilesCount(session, table);
        int dataFilesCountBefore = allDataFilesCount(session, table);
        assertTrue(generateFile(session, TEST_SCHEMA, tableName, specifiedDataPath, "metadata_file_1.test"));
        assertTrue(generateFile(session, TEST_SCHEMA, tableName, specifiedDataPath, "metadata_file_2.test"));

        int metadataFilesCountMiddle = allMetadataFilesCount(session, table);
        int dataFilesCountMiddle = allDataFilesCount(session, table);

        // Remove all orphan files older than now
        waitUntilAfter(System.currentTimeMillis());
        String formattedDateTime = getTimestampString(System.currentTimeMillis(), zoneId);
        assertUpdate(session, format("call system.remove_orphan_files('%s', '%s', timestamp '%s')", TEST_SCHEMA, tableName, formattedDateTime));

        int metadataFilesCountAfter = allMetadataFilesCount(session, table);
        int dataFilesCountAfter = allDataFilesCount(session, table);

        assertEquals(metadataFilesCountBefore, metadataFilesCountAfter);
        assertEquals(dataFilesCountBefore, dataFilesCountAfter);

        assertEquals(metadataFilesCountBefore, metadataFilesCountMiddle);
        assertEquals(dataFilesCountBefore + 2, dataFilesCountMiddle);

        assertEquals(metadataFilesCountMiddle, metadataFilesCountAfter);
        assertEquals(dataFilesCountMiddle, dataFilesCountAfter + 2);
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002')");

        assertUpdate(format("CALL system.unregister_table('%s', '%s')", TEST_SCHEMA, tableName));
        dropTableFromCatalog(tempTableName);
    }

    abstract Table createTable(String tableName, String targetPath, Map<String, String> tableProperties);

    abstract Table loadTable(String tableName);

    abstract void dropTableFromCatalog(String tableName);

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + tableName);
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

    protected File getCatalogDirectory(CatalogType type)
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(dataDirectory, type.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    protected static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
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

    private static int allMetadataFilesCount(Session session, Table table)
    {
        return allFilesCount(session.toConnectorSession(), TEST_SCHEMA, table.name(), metadataLocation(table));
    }

    private static int allDataFilesCount(Session session, Table table)
    {
        return allFilesCount(session.toConnectorSession(), TEST_SCHEMA, table.name(), dataLocation(table));
    }

    private static int allFilesCount(ConnectorSession session, String schema, String table, String folderFullPath)
    {
        try {
            org.apache.hadoop.fs.Path fullPath = new org.apache.hadoop.fs.Path(folderFullPath);
            FileSystem fileSystem = getFileSystem(session, getHdfsEnvironment(), new SchemaTableName(schema, table), fullPath);
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(fullPath, true);
            int count = 0;
            while (allFiles.hasNext()) {
                allFiles.next();
                count++;
            }
            return count;
        }
        catch (Exception e) {
            return 0;
        }
    }

    private static boolean generateFile(Session session, String schema, String table, String path, String fileName)
    {
        try {
            ConnectorSession connectorSession = session.toConnectorSession();
            FileSystem fileSystem = getFileSystem(connectorSession, getHdfsEnvironment(), new SchemaTableName(schema, table), new org.apache.hadoop.fs.Path(path));
            fileSystem.createNewFile(new org.apache.hadoop.fs.Path(path, fileName));
        }
        catch (IOException e) {
            return false;
        }
        return true;
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
}
