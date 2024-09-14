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
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.FileSystem;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.METADATA_FOLDER_NAME;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.getFileSystem;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.parseMetadataVersionFromFileName;
import static com.facebook.presto.iceberg.procedure.RegisterTableProcedure.resolveLatestMetadataLocation;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestIcebergRegisterAndUnregisterProcedure
        extends AbstractTestQueryFramework
{
    private Session session;

    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_DATA_DIRECTORY = "iceberg_data";
    public static final String TEST_CATALOG_DIRECTORY = "catalog";
    public static final String TEST_SCHEMA = "register";
    public static final String TEST_TABLE_NAME = "iceberg_test";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        session = testSessionBuilder()
            .setCatalog(ICEBERG_CATALOG)
            .setSchema(TEST_SCHEMA)
            .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        File metastoreDir = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile();
        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.file-format", new IcebergConfig().getFileFormat().name())
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);
        queryRunner.execute("CREATE SCHEMA " + TEST_SCHEMA);

        return queryRunner;
    }

    public void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg." + TEST_SCHEMA + "." + tableName);
    }

    @Test
    public void testRegisterWithMetadataLocation()
    {
        String tableName = "metadata_loc";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        List<String> metadataLocations = Arrays.asList(
                metadataLocation,  // without trailing slash
                format("%s/", metadataLocation),  // with training slash
                format("%s/metadata", metadataLocation),  // with metadata folder specified
                format("%s/metadata/", metadataLocation));

        for (String validLocation : metadataLocations) {
            dropTableFromMetastore(TEST_SCHEMA, tableName);

            assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + validLocation + "')");
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");
        }

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithNamedArguments()
    {
        String tableName = "metadata_loc_named_arguments";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
            assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

            String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
            List<String> metadataLocations = Arrays.asList(
                    metadataLocation,  // without trailing slash
                    format("%s/", metadataLocation),  // with training slash
                    format("%s/metadata", metadataLocation),  // with metadata folder specified
                    format("%s/metadata/", metadataLocation));

            for (String validLocation : metadataLocations) {
                dropTableFromMetastore(TEST_SCHEMA, tableName);

                assertUpdate("CALL system.register_table(metadata_location => '" + validLocation + "', table_name => '" + tableName + "', schema => '" + TEST_SCHEMA + "')");
                assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRegisterWithMetadataLocationShowCreate()
    {
        String tableName = "show_create";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String showCreateTable = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')");
        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();

        assertThat(showCreateTable).isEqualTo(showCreateTableNew);

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithMetadataLocationMultipleVersions()
    {
        String tableName = "multiple_versions";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Create new snapshot
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);
        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithTableDataAndMetadataNotCoLocated()
    {
        String tableName = "original_table";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = getMetadataFileLocation(session.toConnectorSession(), TEST_SCHEMA, tableName, metadataLocation);
        String newTableName = "new_table";

        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + newTableName + "', '" + metadataLocation + "', '" + metadataFileName + "')");
        assertUpdate("INSERT INTO " + newTableName + " VALUES(2, 2)", 1);

        assertQuery("SELECT * FROM " + newTableName, "VALUES (1, 1), (2, 2)");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithMetadataLocationDuplicateVersions()
    {
        String tableName = "duplicate_versions";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Create new snapshots
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 2)", 1);
        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = getMetadataFileLocation(session.toConnectorSession(), TEST_SCHEMA, tableName, metadataLocation);

        // Rename file to spoof same version as previous file
        File metadataFile = new File(format("%s/%s/%s", metadataLocation, METADATA_FOLDER_NAME, metadataFileName));
        String newFileName = metadataFileName.replace("2", "1");
        metadataFile.renameTo(new File(format("%s/%s/%s", metadataLocation, METADATA_FOLDER_NAME, newFileName)));

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        // Ensure most recently modified version is chosen
        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1), (2, 2)");

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithMetadataFileName()
    {
        String tableName = "metadata_file";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = getMetadataFileLocation(session.toConnectorSession(), TEST_SCHEMA, tableName, metadataLocation);

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "', '" + metadataFileName + "')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithMetadataFileNameMultipleVersions()
    {
        String tableName = "multiple_files";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Create new snapshot and get metadata filename for this snapshot
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);
        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = getMetadataFileLocation(session.toConnectorSession(), TEST_SCHEMA, tableName, metadataLocation);

        // Create new snapshot
        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 2)", 1);

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        // Register table using intermediate snapshot (not latest)
        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "', '" + metadataFileName + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithInvalidParameters()
    {
        @Language("RegExp") String errorMessage = "line 1:1: Required procedure argument 'schema' is missing";
        assertQueryFails("CALL system.register_table()", errorMessage);

        errorMessage = "line 1:1: Required procedure argument 'table_name' is missing";
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "')", errorMessage);

        errorMessage = "line 1:1: Required procedure argument 'metadata_location' is missing";
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "', '" + TEST_TABLE_NAME + "')", errorMessage);

        errorMessage = "schemaName is empty";
        assertQueryFails("CALL system.register_table('', '', '')", errorMessage);

        errorMessage = "tableName is empty";
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "', '', '')", errorMessage);

        errorMessage = "path must not be null or empty";
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "', '" + TEST_TABLE_NAME + "', '')", errorMessage);

        errorMessage = "line 1:1: Too many arguments for procedure";
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "', '" + TEST_TABLE_NAME + "', '/dummy/location', 'dummyfile.json', '')", errorMessage);
    }

    @Test
    public void testRegisterWithInvalidSchema()
    {
        String invalidSchemaName = "invalid_schema";
        @Language("RegExp") String errorMessage = format("Schema %s not found", invalidSchemaName);
        assertQueryFails("CALL system.register_table('" + invalidSchemaName + "', '" + TEST_TABLE_NAME + "', '/dummy/metadata')", errorMessage);
    }

    @Test
    public void testRegisterWithExistingTable()
    {
        String tableName = "existing_table";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);

        @Language("RegExp") String errorMessage = format("Table already exists: '%s.%s'", TEST_SCHEMA, tableName);
        assertQueryFails("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')", errorMessage);
        dropTable(tableName);
    }

    @Test
    public void testRegisterWithInvalidMetadataScheme()
    {
        String tableName = "invalid_scheme";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String invalidMetadataLocation = format("invalidscheme://%s/%s", metadataLocation, METADATA_FOLDER_NAME);

        @Language("RegExp") String errorMessage = format("Error getting file system at path invalidscheme:%s/%s", metadataLocation, METADATA_FOLDER_NAME);
        assertQueryFails("CALL system.register_table ('" + TEST_SCHEMA + "', '" + tableName + "', '" + invalidMetadataLocation + "')", errorMessage);

        dropTable(tableName);
    }

    @Test
    public void testRegisterWithMissingMetadata()
    {
        String tableName = "missing_md";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = getMetadataFileLocation(session.toConnectorSession(), TEST_SCHEMA, tableName, metadataLocation);

        File metadataFile = new File(format("%s/%s/%s", metadataLocation, METADATA_FOLDER_NAME, metadataFileName));
        metadataFile.delete();

        @Language("RegExp") String errorMessage = format("No metadata found at location %s/%s", metadataLocation, METADATA_FOLDER_NAME);
        assertQueryFails("CALL system.register_table ('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')", errorMessage);
    }

    @Test
    public void testRegisterWithInvalidMetadataFilename()
    {
        String tableName = "invalid_loc";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String metadataFileName = "00001-invalid-metadata-file.metadata.json";

        dropTableFromMetastore(TEST_SCHEMA, tableName);

        @Language("RegExp") String errorMessage = format("Unable to read metadata file %s/%s/%s", metadataLocation, METADATA_FOLDER_NAME, metadataFileName);
        assertQueryFails("CALL system.register_table ('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "', '" + metadataFileName + "')", errorMessage);

        dropTable(tableName);
    }

    @Test
    public void testRegisterTableWithInvalidLocation()
    {
        String tableName = "invalid_loc_after_drop";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);
        String newTableName = tableName + "_new";

        // Drop table to remove underlying data and metadata
        assertUpdate(format("DROP TABLE %s", tableName));

        @Language("RegExp") String errorMessage = format("Unable to find metadata at location %s/%s", metadataLocation, METADATA_FOLDER_NAME);
        assertQueryFails("CALL system.register_table ('" + TEST_SCHEMA + "', '" + newTableName + "', '" + metadataLocation + "')", errorMessage);

        dropTable(tableName);
    }

    @Test
    public void testUnregisterTable()
    {
        String tableName = "unregister_positional_args";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Unregister table with procedure
        assertUpdate("CALL system.unregister_table('" + TEST_SCHEMA + "', '" + tableName + "')");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testUnregisterTableWithNamedArguments()
    {
        String tableName = "unregister_named_args";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Unregister table with procedure
        assertUpdate("CALL system.unregister_table(table_name => '" + tableName + "', schema => '" + TEST_SCHEMA + "')");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testRegisterAndUnregisterTable()
    {
        String tableName = "register_unregister";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getMetadataLocation(TEST_SCHEMA, tableName);

        // Unregister table with procedure
        assertUpdate("CALL system.unregister_table('" + TEST_SCHEMA + "', '" + tableName + "')");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Register table again
        assertUpdate("CALL system.register_table('" + TEST_SCHEMA + "', '" + tableName + "', '" + metadataLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        // Unregister table with procedure
        assertUpdate("CALL system.unregister_table('" + TEST_SCHEMA + "', '" + tableName + "')");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails(
                "CALL system.unregister_table ('" + TEST_SCHEMA + "', '" + tableName + "')",
                format("Table '%s.%s' not found", TEST_SCHEMA, tableName));
    }

    @Test
    public void testUnregisterSchemaNonExistent()
    {
        String schemaName = "invalid_schema";
        String tableName = "unregister_invalid_schema";

        // Unregister table with procedure
        @Language("RegExp") String errorMessage = format("Schema %s not found", schemaName);
        assertQueryFails("CALL system.unregister_table ('" + schemaName + "', '" + tableName + "')", errorMessage);
    }

    @Test
    public void testUnregisterTableNonExistent()
    {
        String tableName = "unregister_invalid_table";

        // Unregister table with procedure
        @Language("RegExp") String errorMessage = format("Table '%s.%s' not found", TEST_SCHEMA, tableName);
        assertQueryFails("CALL system.unregister_table ('" + TEST_SCHEMA + "', '" + tableName + "')", errorMessage);
    }

    @Test
    public void testUnregisterWithInvalidParameters()
    {
        @Language("RegExp") String errorMessage = "line 1:1: Required procedure argument 'schema' is missing";
        assertQueryFails("CALL system.register_table()", errorMessage);

        errorMessage = "line 1:1: Required procedure argument 'table_name' is missing";
        assertQueryFails("CALL system.unregister_table('" + TEST_SCHEMA + "')", errorMessage);

        errorMessage = "schemaName is empty";
        assertQueryFails("CALL system.unregister_table('', '')", errorMessage);

        errorMessage = "tableName is empty";
        assertQueryFails("CALL system.unregister_table('" + TEST_SCHEMA + "', '')", errorMessage);

        errorMessage = "line 1:1: Too many arguments for procedure";
        assertQueryFails("CALL system.unregister_table('" + TEST_SCHEMA + "', '" + TEST_TABLE_NAME + "', '')", errorMessage);
    }

    @Test
    public void testParseMetadataVersionFromFileName()
    {
        // Hive metadata file names and compressions
        assertEquals(parseMetadataVersionFromFileName("00000-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), 0);
        assertEquals(parseMetadataVersionFromFileName("00001-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), 1);
        assertEquals(parseMetadataVersionFromFileName("00010-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), 10);
        assertEquals(parseMetadataVersionFromFileName("00110-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), 110);
        assertEquals(parseMetadataVersionFromFileName("99999-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), 99999);
        assertEquals(parseMetadataVersionFromFileName("00000-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json.gz"), 0);
        assertEquals(parseMetadataVersionFromFileName("00000-7a73c190-2c6e-4924-b1b7-d4e840cbcef2.gz.metadata.json"), 0);

        // Hadoop metadata file names and compressions
        assertEquals(parseMetadataVersionFromFileName("v0.metadata.json"), 0);
        assertEquals(parseMetadataVersionFromFileName("v1.metadata.json"), 1);
        assertEquals(parseMetadataVersionFromFileName("v10.metadata.json"), 10);
        assertEquals(parseMetadataVersionFromFileName("v110.metadata.json"), 110);
        assertEquals(parseMetadataVersionFromFileName("v99999.metadata.json"), 99999);
        assertEquals(parseMetadataVersionFromFileName("v0.gz.metadata.json"), 0);
        assertEquals(parseMetadataVersionFromFileName("v0.metadata.json.gz"), 0);

        // Not a match
        assertEquals(parseMetadataVersionFromFileName("00000-7a73c190-2c6e-4924-b1b7-d4e840cbcef2"), -1);
        assertEquals(parseMetadataVersionFromFileName("9ed98089-c46a-48f3-b20f-a90344f8df48-m0.avro"), -1);
        assertEquals(parseMetadataVersionFromFileName("00000_7a73c190-2c6e-4924-b1b7-d4e840cbcef2.metadata.json"), -1);
        assertEquals(parseMetadataVersionFromFileName("v-0_metadata.json"), -1);
        assertEquals(parseMetadataVersionFromFileName("v0_metadata.json"), -1);
    }

    protected Path getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        return getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false);
    }

    private String getMetadataLocation(String schema, String table)
    {
        Path catalogDirectory = getCatalogDirectory();
        return format("%s/%s/%s", stripTrailingSlash(catalogDirectory.toString()), schema, table);
    }

    public static String getMetadataFileLocation(ConnectorSession session, String schema, String table, String metadataLocation)
    {
        metadataLocation = stripTrailingSlash(metadataLocation);
        org.apache.hadoop.fs.Path metadataDir = new org.apache.hadoop.fs.Path(metadataLocation, METADATA_FOLDER_NAME);
        FileSystem fileSystem = getFileSystem(session, getHdfsEnvironment(), new SchemaTableName(schema, table), metadataDir);
        return resolveLatestMetadataLocation(
                session,
                fileSystem,
                metadataDir).getName();
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
    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toFile().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    protected void dropTableFromMetastore(String schemaName, String tableName)
    {
        ExtendedHiveMetastore metastore = getFileHiveMetastore();
        ConnectorSession connectorSession = session.toConnectorSession();
        MetastoreContext metastoreContext = new MetastoreContext(connectorSession.getIdentity(), connectorSession.getQueryId(), connectorSession.getClientInfo(), connectorSession.getClientTags(), connectorSession.getSource(), getMetastoreHeaders(connectorSession), isUserDefinedTypeEncodingEnabled(connectorSession), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, connectorSession.getWarningCollector(), connectorSession.getRuntimeStats());

        metastore.dropTableFromMetastore(metastoreContext, schemaName, tableName);
    }
}
