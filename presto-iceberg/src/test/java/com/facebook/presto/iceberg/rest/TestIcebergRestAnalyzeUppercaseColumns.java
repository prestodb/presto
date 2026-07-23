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
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;

/**
 * E2E test verifying an Iceberg table whose schema was written by an external engine (e.g. Spark on Hadoop) with
 * uppercase column names, and then registered into a Presto REST catalog via the register_table procedure.
 */
@Test
public class TestIcebergRestAnalyzeUppercaseColumns
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";
    private static final String TABLE_NAME = "test_uppercase_columns";

    private File restWarehouseLocation;
    private File hadoopWarehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @BeforeClass
    public void init()
            throws Exception
    {
        // Separate temp dirs: REST catalog warehouse vs. the "external" Hadoop warehouse
        restWarehouseLocation = Files.newTemporaryFolder();
        hadoopWarehouseLocation = Files.newTemporaryFolder();

        restServer = getRestServer(restWarehouseLocation.getAbsolutePath());
        restServer.start();
        serverUri = restServer.getBaseUrl().toString();

        super.init();
        assertQuerySucceeds("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);

        // Use the Iceberg Hadoop Catalog API (simulating Spark or another external engine)
        // to create a table with UPPERCASE column names and write some data into it.
        Catalog hadoopCatalog = loadHadoopCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, TABLE_NAME);

        Schema schema = new Schema(Types.NestedField.required(1, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(2, "VALUE", Types.StringType.get()));

        Table table = hadoopCatalog.createTable(tableId, schema, PartitionSpec.unpartitioned());

        writeRecord(table, GenericRecord.create(schema).copy("ID", 1, "VALUE", "one"));
        table.refresh();
        writeRecord(table, GenericRecord.create(schema).copy("ID", 2, "VALUE", "two"));
        table.refresh();
        writeRecord(table, GenericRecord.create(schema).copy("ID", 3, "VALUE", "three"));

        // Register the externally-created table into the REST catalog using the metadata location
        String metadataLocation = table.location();
        assertQuerySucceeds(format("CALL %s.system.register_table('%s', '%s', '%s')", ICEBERG_CATALOG, TEST_SCHEMA, TABLE_NAME, metadataLocation));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        try {
            assertQuerySucceeds(format("CALL %s.system.unregister_table('%s', '%s')", ICEBERG_CATALOG, TEST_SCHEMA, TABLE_NAME));
        }
        catch (Exception ignored) {
        }
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(restWarehouseLocation.toPath(), ALLOW_INSECURE);
        deleteRecursively(hadoopWarehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(restWarehouseLocation.toPath()))
                .build()
                .getQueryRunner();
    }

    @Test
    public void testAnalyzeSucceedsWithUppercaseColumnNames()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, TABLE_NAME));
    }

    @Test(dependsOnMethods = "testAnalyzeSucceedsWithUppercaseColumnNames")
    public void testShowStatsAfterAnalyzeWithUppercaseColumnNames()
    {
        assertQuerySucceeds(format("ANALYZE %s.%s", TEST_SCHEMA, TABLE_NAME));

        MaterializedResult stats = getQueryRunner().execute(format("SHOW STATS FOR %s.%s", TEST_SCHEMA, TABLE_NAME));
        List<MaterializedRow> rows = stats.getMaterializedRows();

        // Find the row for the 'id' column (Presto lowercases the column name)
        MaterializedRow idRow = rows.stream().filter(row -> "id".equalsIgnoreCase((String) row.getField(0)))
                .findFirst()
                .orElse(null);

        assertNotNull(idRow, "SHOW STATS FOR must return a row for the 'id' column after ANALYZE");

        // distinct_values_count is field index 2; it must be non-null after ANALYZE
        assertNotNull(idRow.getField(2), "distinct_values_count for 'id' must be populated after ANALYZE " + "on a table whose schema has uppercase column names");
    }

    @Test
    public void testSelectFromTableWithUppercaseColumnNames()
    {
        assertQuery(format("SELECT count(*) FROM %s.%s", TEST_SCHEMA, TABLE_NAME), "VALUES (3)");
    }

    @Test(dependsOnMethods = "testAnalyzeSucceedsWithUppercaseColumnNames")
    public void testQueryWithFilterAfterAnalyzeOnUppercaseColumnsTable()
    {
        assertQuery(format("SELECT id FROM %s.%s WHERE id > 1 ORDER BY id", TEST_SCHEMA, TABLE_NAME), "VALUES (2), (3)");
    }

    /**
     * Load an Iceberg HadoopCatalog pointing at the separate "external" warehouse dir.
     * This simulates an external engine (Spark, Flink, …) that writes uppercase-column tables.
     */
    private Catalog loadHadoopCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(),
                "hadoop_test",
                ImmutableMap.of("warehouse", hadoopWarehouseLocation.toURI().toString()),
                new Configuration());
    }

    /**
     * Write a single record to the table as a new Parquet data file and commit it as a
     * new snapshot — the same write path used by Spark internally.
     */
    private static void writeRecord(Table table, Record record)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);

        DataWriter<Record> writer = Parquet
                .writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
        try {
            writer.write(record);
        }
        finally {
            writer.close();
        }

        table.newAppend()
                .appendFile(writer.toDataFile())
                .commit();
    }
}
