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
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test
public class TestIcebergSmokeRestNestedNamespace
        extends TestIcebergSmokeRest
{
    private static final String ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG = "iceberg_without_nested_namespaces";

    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    public TestIcebergSmokeRestNestedNamespace()
    {
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        String namespaceSeparatorEscaped = "\\.";
        String schemaPath = schema.replaceAll(namespaceSeparatorEscaped, "/");

        return format("%s/%s/%s", warehouseLocation, schemaPath, table);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> restConnectorProperties = restConnectorProperties(serverUri);
        IcebergQueryRunner icebergQueryRunner = IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(serverUri))
                        .put("iceberg.rest.nested.namespace.enabled", "true")
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("ns1.ns2")
                .build();

        // additional catalog for testing nested namespace disabled
        icebergQueryRunner.addCatalog(ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG,
                new ImmutableMap.Builder<String, String>()
                        .putAll(restConnectorProperties)
                        .put("iceberg.catalog.type", REST.name())
                        .put("iceberg.rest.nested.namespace.enabled", "false")
                        .build());

        return icebergQueryRunner.getQueryRunner();
    }

    protected IcebergNativeCatalogFactory getCatalogFactory(IcebergRestConfig restConfig)
    {
        IcebergConfig icebergConfig = new IcebergConfig()
                .setCatalogType(REST)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath());

        return new IcebergRestCatalogFactory(
                icebergConfig,
                restConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                new HiveAzureConfigurationInitializer(new HiveAzureConfig()),
                new NodeVersion("test_version"));
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergRestConfig restConfig = new IcebergRestConfig().setServerUri(serverUri);
        return getNativeIcebergTable(getCatalogFactory(restConfig),
                session,
                new SchemaTableName(schema, tableName));
    }

    @Test
    @Override // override due to double quotes around nested namespace
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().get();

        validateShowCreateTable("iceberg", "\"" + schemaName + "\"", "orders",
                ImmutableList.of(
                        columnDefinition("orderkey", "bigint"),
                        columnDefinition("custkey", "bigint"),
                        columnDefinition("orderstatus", "varchar"),
                        columnDefinition("totalprice", "double"),
                        columnDefinition("orderdate", "date"),
                        columnDefinition("orderpriority", "varchar"),
                        columnDefinition("clerk", "varchar"),
                        columnDefinition("shippriority", "integer"),
                        columnDefinition("comment", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(schemaName, "orders") + "'")));
    }

    @Test
    public void testShowCreateTableWithSpecifiedWriteDataLocation()
            throws IOException
    {
        String tableName = "test_table_with_specified_write_data_location";
        String dataWriteLocation = createTempDirectory("test1").toAbsolutePath().toString();
        try {
            assertUpdate(format("CREATE TABLE %s(a int, b varchar) with (\"write.data.path\" = '%s')", tableName, dataWriteLocation));
            String schemaName = getSession().getSchema().get();
            String location = getLocation(schemaName, tableName);

            validateShowCreateTable("iceberg", "\"" + schemaName + "\"", tableName,
                    ImmutableList.of(
                            columnDefinition("a", "integer"),
                            columnDefinition("b", "varchar")),
                    null,
                    getCustomizedTableProperties(ImmutableMap.of(
                            "location", "'" + location + "'",
                            "write.data.path", "'" + dataWriteLocation + "'")));
        }
        finally {
            assertUpdate(("DROP TABLE IF EXISTS " + tableName));
        }
    }

    @Test
    @Override // override due to double quotes around nested namespace
    public void testTableComments()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        @Language("SQL") String createTable = "" +
                "CREATE TABLE iceberg.\"%s\".test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   \"write.format.default\" = 'ORC',\n" +
                "   \"format-version\" = '2'\n" +
                ")";

        assertUpdate(format(createTable, schemaName, "test table comment"));

        validateShowCreateTable("iceberg", "\"" + schemaName + "\"", "test_table_comments",
                ImmutableList.of(columnDefinition("_x", "bigint")),
                "test table comment",
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.format.default", "'ORC'",
                        "location", "'" + getLocation(schemaName, "test_table_comments") + "'")));

        dropTable(session, "test_table_comments");
    }

    @Override // override due to double quotes around nested namespace
    protected void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        String fileFormatString = fileFormat.toString().toLowerCase(ENGLISH);
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as_%s " +
                "WITH (" +
                "\"write.format.default\" = '%s', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, format(createTable, fileFormatString, fileFormat), "SELECT count(*) from orders");

        validateShowCreateTable(getSession().getCatalog().get(),
                        format("\"%s\"", getSession().getSchema().get()),
                        "test_create_partitioned_table_as_" + fileFormatString,
                ImmutableList.of(
                        columnDefinition("order_key", "bigint"),
                        columnDefinition("ship_priority", "integer"),
                        columnDefinition("order_status", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.format.default", "'" + fileFormat + "'",
                        "location", "'" + getLocation(getSession().getSchema().get(), "test_create_partitioned_table_as_" + fileFormatString) + "'",
                        "partitioning", "ARRAY['order_status','ship_priority','bucket(order_key, 9)']")));

        assertQuery(session, "SELECT * from test_create_partitioned_table_as_" + fileFormatString,
                "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, "test_create_partitioned_table_as_" + fileFormatString);
    }

    @Test
    @Override
    public void testCreateTableWithFormatVersion()
    {
        // v1 table create fails due to Iceberg REST catalog bug (see: https://github.com/apache/iceberg/issues/8756)
        assertThatThrownBy(() -> testCreateTableWithFormatVersion("1", "copy-on-write"))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Cannot downgrade v2 table to v1");

        // v2 succeeds
        testCreateTableWithFormatVersion("2", "merge-on-read");
    }

    @Override // override due to double quotes around nested namespace
    protected void testCreateTableWithFormatVersion(String formatVersion, String defaultDeleteMode)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_table_with_format_version_%s " +
                "WITH (" +
                "\"write.format.default\" = 'PARQUET', " +
                "\"format-version\" = '%s'" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        Session session = getSession();

        assertUpdate(session, format(createTable, formatVersion, formatVersion), "SELECT count(*) from orders");

        validateShowCreateTable(getSession().getCatalog().get(),
                "\"" + getSession().getSchema().get() + "\"",
                "test_create_table_with_format_version_" + formatVersion,
                ImmutableList.of(
                        columnDefinition("order_key", "bigint"),
                        columnDefinition("ship_priority", "integer"),
                        columnDefinition("order_status", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.delete.mode", "'" + defaultDeleteMode + "'",
                        "format-version", "'" + formatVersion + "'",
                        "location", "'" + getLocation(getSession().getSchema().get(), "test_create_table_with_format_version_" + formatVersion) + "'",
                        "write.update.mode", "'" + defaultDeleteMode + "'")));

        dropTable(session, "test_create_table_with_format_version_" + formatVersion);
    }

    @Test
    public void testView()
    {
        Session session = getSession();
        String schemaName = getSession().getSchema().get();

        assertUpdate(session, "CREATE VIEW view_orders AS SELECT * from orders");
        assertQuery(session, "SELECT * FROM view_orders", "SELECT * from orders");
        assertThat(computeActual("SHOW CREATE VIEW view_orders").getOnlyValue())
                .isEqualTo(format("CREATE VIEW iceberg.\"%s\".view_orders SECURITY %s AS\n" +
                                "SELECT *\n" +
                                "FROM\n" +
                                "  orders",
                        schemaName,
                        "DEFINER"));
        assertUpdate(session, "DROP VIEW view_orders");
    }

    @Test
    void testNestedNamespaceDisabled()
    {
        assertQuery(format("SHOW SCHEMAS FROM %s", ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG),
                "VALUES 'ns1', 'tpch', 'tpcds', 'information_schema'");

        assertQueryFails(format("CREATE SCHEMA %s.\"ns1.ns2.ns3\"", ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG),
                "Nested namespaces are disabled. Schema ns1.ns2.ns3 is not valid");
        assertQueryFails(format("CREATE TABLE %s.\"ns1.ns2\".test_table(a int)", ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG),
                "Nested namespaces are disabled. Schema ns1.ns2 is not valid");
        assertQueryFails(format("SELECT * FROM %s.\"ns1.ns2\".orders", ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG),
                "Nested namespaces are disabled. Schema ns1.ns2 is not valid");
        assertQueryFails(format("SHOW TABLES IN %s.\"ns1.ns2\"", ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG),
                "line 1:1: Schema 'ns1.ns2' does not exist");
    }
}
