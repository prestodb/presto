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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
        DistributedQueryRunner icebergQueryRunner = IcebergQueryRunner.createIcebergQueryRunner(
                ImmutableMap.of(),
                restConnectorProperties,
                PARQUET,
                true,
                false,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.of(warehouseLocation.toPath()),
                false,
                Optional.of("ns1.ns2"));

        // additional catalog for testing nested namespace disabled
        icebergQueryRunner.createCatalog(ICEBERG_NESTED_NAMESPACE_DISABLED_CATALOG, "iceberg",
                new ImmutableMap.Builder<String, String>()
                .putAll(restConnectorProperties)
                .put("iceberg.rest.nested.namespace.enabled", "false")
                .build());

        return icebergQueryRunner;
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
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(format("CREATE TABLE iceberg.\"%s\".orders (\n" +
                        "   \"orderkey\" bigint,\n" +
                        "   \"custkey\" bigint,\n" +
                        "   \"orderstatus\" varchar,\n" +
                        "   \"totalprice\" double,\n" +
                        "   \"orderdate\" date,\n" +
                        "   \"orderpriority\" varchar,\n" +
                        "   \"clerk\" varchar,\n" +
                        "   \"shippriority\" integer,\n" +
                        "   \"comment\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   delete_mode = 'merge-on-read',\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = '2',\n" +
                        "   location = '%s',\n" +
                        "   metadata_delete_after_commit = false,\n" +
                        "   metadata_previous_versions_max = 100,\n" +
                        "   metrics_max_inferred_column = 100\n" +
                        ")", schemaName, getLocation(schemaName, "orders")));
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
                "   format = 'ORC',\n" +
                "   format_version = '2'\n" +
                ")";

        assertUpdate(format(createTable, schemaName, "test table comment"));

        String createTableTemplate = "" +
                "CREATE TABLE iceberg.\"%s\".test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100\n" +
                ")";
        String createTableSql = format(createTableTemplate, schemaName, "test table comment", getLocation(schemaName, "test_table_comments"));

        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()), createTableSql);

        dropTable(session, "test_table_comments");
    }

    @Override // override due to double quotes around nested namespace
    protected void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        String fileFormatString = fileFormat.toString().toLowerCase(ENGLISH);
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as_%s " +
                "WITH (" +
                "format = '%s', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, format(createTable, fileFormatString, fileFormat), "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.\"%s\".%s (\n" +
                        "   \"order_key\" bigint,\n" +
                        "   \"ship_priority\" integer,\n" +
                        "   \"order_status\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   delete_mode = 'merge-on-read',\n" +
                        "   format = '%s',\n" +
                        "   format_version = '2',\n" +
                        "   location = '%s',\n" +
                        "   metadata_delete_after_commit = false,\n" +
                        "   metadata_previous_versions_max = 100,\n" +
                        "   metrics_max_inferred_column = 100,\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_partitioned_table_as_" + fileFormatString,
                fileFormat,
                getLocation(getSession().getSchema().get(), "test_create_partitioned_table_as_" + fileFormatString));

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_partitioned_table_as_" + fileFormatString);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

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
                "format = 'PARQUET', " +
                "format_version = '%s'" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        Session session = getSession();

        assertUpdate(session, format(createTable, formatVersion, formatVersion), "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.\"%s\".%s (\n" +
                        "   \"order_key\" bigint,\n" +
                        "   \"ship_priority\" integer,\n" +
                        "   \"order_status\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   delete_mode = '%s',\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = '%s',\n" +
                        "   location = '%s',\n" +
                        "   metadata_delete_after_commit = false,\n" +
                        "   metadata_previous_versions_max = 100,\n" +
                        "   metrics_max_inferred_column = 100\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_table_with_format_version_" + formatVersion,
                defaultDeleteMode,
                formatVersion,
                getLocation(getSession().getSchema().get(), "test_create_table_with_format_version_" + formatVersion));

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_table_with_format_version_" + formatVersion);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

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
                .isEqualTo(format("CREATE VIEW iceberg.\"%s\".view_orders AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  orders", schemaName));
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
