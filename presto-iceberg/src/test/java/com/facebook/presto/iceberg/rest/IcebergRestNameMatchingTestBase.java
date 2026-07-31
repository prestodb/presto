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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.Session;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class IcebergRestNameMatchingTestBase
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "tpch";

    protected TestingHttpServer restServer;
    protected String serverUri;
    protected File warehouseLocation;

    @BeforeClass
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    protected Session testSession()
    {
        return testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA)
                .build();
    }

    /**
     * Builds the {@link IcebergNativeCatalogFactory} for direct Iceberg metadata access.
     * Subclasses set {@code setCaseSensitiveNameMatching} as appropriate.
     */
    protected abstract IcebergNativeCatalogFactory getCatalogFactory();

    /**
     * Returns the raw Iceberg {@link Table} from the REST catalog, bypassing Presto's
     * serialisation layer — allows direct inspection of PartitionSpec, Schema, etc.
     */
    protected Table getIcebergTable(String schema, String tableName)
    {
        return getNativeIcebergTable(getCatalogFactory(), SESSION, SchemaTableName.valueOf(schema + "." + tableName));
    }

    @Test
    public void testShowSchemas()
    {
        assertQuerySucceeds(testSession(), "SHOW SCHEMAS");
        assertQuery(testSession(), "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'tpch'", "VALUES 'tpch'");
    }

    /**
     * Double-quotes allow special characters (hyphens, spaces) and reserved keywords in identifiers.
     * These names are stored as-is in case-sensitive mode and lowercased in case-insensitive mode.
     * Since the names used here are already lowercase, the result is identical in both modes.
     */
    @Test
    public void testSpecialCharacterIdentifiersViaDoubleQuotes()
    {
        // Columns with hyphen, space, and reserved keyword — all require double-quotes.
        assertQuerySucceeds(testSession(), "CREATE TABLE specialcharcols (\"first-name\" varchar, \"last name\" varchar, \"order\" bigint)");
        assertQuerySucceeds(testSession(), "INSERT INTO specialcharcols VALUES ('Alice', 'Smith', 1)");
        assertQuery(testSession(), "SELECT \"first-name\", \"last name\", \"order\" FROM specialcharcols", "VALUES ('Alice', 'Smith', 1)");

        String result = getQueryRunner().execute(testSession(), "SHOW COLUMNS FROM specialcharcols").toString();
        assertTrue(result.contains("first-name"), "Expected 'first-name' in SHOW COLUMNS output");
        assertTrue(result.contains("last name"), "Expected 'last name' in SHOW COLUMNS output");
        assertTrue(result.contains("order"), "Expected 'order' in SHOW COLUMNS output");

        assertQuerySucceeds(testSession(), "DROP TABLE specialcharcols");

        // Partition column with hyphen — must embed double-quotes in the raw array string.
        assertQuerySucceeds(testSession(), "CREATE TABLE specialcharpart (\"region-id\" bigint, name varchar) WITH (partitioning = ARRAY['\"region-id\"'])");
        assertQuerySucceeds(testSession(), "INSERT INTO specialcharpart VALUES (10, 'north'), (20, 'south')");
        assertQuery(testSession(), "SELECT \"region-id\", name FROM specialcharpart ORDER BY \"region-id\"", "VALUES (10, 'north'), (20, 'south')");
        assertQuerySucceeds(testSession(), "DROP TABLE specialcharpart");

        // Reserved keyword 'year' as a partition column — quoting required in the array string
        // so the parser doesn't confuse it with the year() transform function.
        assertQuerySucceeds(testSession(), "CREATE TABLE keywordpart (\"year\" bigint, name varchar) WITH (partitioning = ARRAY['\"year\"'])");
        assertQuerySucceeds(testSession(), "INSERT INTO keywordpart VALUES (2024, 'a'), (2025, 'b')");
        assertQuery(testSession(), "SELECT \"year\", name FROM keywordpart ORDER BY \"year\"", "VALUES (2024, 'a'), (2025, 'b')");
        assertQuerySucceeds(testSession(), "DROP TABLE keywordpart");
    }

    /**
     * Partition transform keywords (year, month, bucket, truncate …) are accepted in any case
     * because PartitionFields uses {@code (?i:year)} patterns. The Iceberg library always stores
     * transforms in lowercase ({@code field.transform().toString()} is hardcoded in the Iceberg
     * spec), so SHOW CREATE TABLE and the raw metadata both show lowercase regardless of input.
     */
    @Test
    public void testPartitionTransformKeywordCaseInsensitive()
    {
        // Uppercase transform keywords accepted
        assertQuerySucceeds(testSession(), "CREATE TABLE transformcase (ts timestamp, id bigint, val varchar) WITH (partitioning = ARRAY['YEAR(ts)', 'BUCKET(id, 4)'])");
        assertQuerySucceeds(testSession(), "INSERT INTO transformcase VALUES (TIMESTAMP '2024-03-15 10:00:00', 1, 'a')");
        assertQuerySucceeds(testSession(), "SELECT val FROM transformcase");

        // 1. SHOW CREATE TABLE serialises transforms in lowercase
        String showCreate = getQueryRunner().execute(testSession(), "SHOW CREATE TABLE transformcase").toString();
        assertTrue(showCreate.contains("year(ts)"), "Expected lowercase 'year(ts)' in SHOW CREATE TABLE output, got: " + showCreate);
        assertTrue(showCreate.contains("bucket(id, 4)"), "Expected lowercase 'bucket(id, 4)' in SHOW CREATE TABLE output, got: " + showCreate);

        // 2. Raw Iceberg catalog metadata also stores transforms in lowercase
        List<PartitionField> fields = getIcebergTable(SCHEMA, "transformcase").spec().fields();
        assertEquals(fields.size(), 2);
        assertEquals(fields.get(0).transform().toString(), "year", "Iceberg must store transform as lowercase 'year'");
        assertEquals(fields.get(1).transform().toString(), "bucket[4]", "Iceberg must store transform as lowercase 'bucket[4]'");

        assertQuerySucceeds(testSession(), "DROP TABLE transformcase");

        // Mixed-case transform keywords accepted
        assertQuerySucceeds(testSession(), "CREATE TABLE transformcasemixed (ts timestamp, val varchar) WITH (partitioning = ARRAY['Month(ts)', 'Truncate(val, 8)'])");

        String showCreate2 = getQueryRunner().execute(testSession(), "SHOW CREATE TABLE transformcasemixed").toString();
        assertTrue(showCreate2.contains("month(ts)"), "Expected lowercase 'month(ts)' in SHOW CREATE TABLE output, got: " + showCreate2);
        assertTrue(showCreate2.contains("truncate(val, 8)"), "Expected lowercase 'truncate(val, 8)' in SHOW CREATE TABLE output, got: " + showCreate2);

        List<PartitionField> fields2 = getIcebergTable(SCHEMA, "transformcasemixed").spec().fields();
        assertEquals(fields2.size(), 2);
        assertEquals(fields2.get(0).transform().toString(), "month", "Iceberg must store transform as lowercase 'month'");
        assertEquals(fields2.get(1).transform().toString(), "truncate[8]", "Iceberg must store transform as lowercase 'truncate[8]'");

        assertQuerySucceeds(testSession(), "DROP TABLE transformcasemixed");
    }

    protected IcebergNativeCatalogFactory buildCatalogFactory(IcebergConfig icebergConfig)
    {
        return new IcebergRestCatalogFactory(
                icebergConfig,
                new IcebergRestConfig().setServerUri(serverUri),
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                new HiveAzureConfigurationInitializer(new HiveAzureConfig()),
                new NodeVersion("test_version"));
    }
}
