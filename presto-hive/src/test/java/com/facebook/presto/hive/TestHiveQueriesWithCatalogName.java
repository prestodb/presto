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
package com.facebook.presto.hive;

import com.facebook.presto.hive.containers.HiveMinIODataLake;
import com.facebook.presto.hive.s3.S3HiveQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestHiveQueriesWithCatalogName
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA_1 = "hive_test_schema_1";
    private static final String HIVE_TEST_SCHEMA_2 = "hive_test_schema_2";
    private static final String HIVE_CATALOG = "hive";
    private static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";

    private String bucketName;
    private HiveMinIODataLake dockerizedS3DataLake;

    private final String hiveHadoopImage;

    public TestHiveQueriesWithCatalogName(String hiveHadoopImage)
    {
        this.hiveHadoopImage = requireNonNull(hiveHadoopImage, "hiveHadoopImage is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-schema-with-hive-catalog-name-" + randomTableSuffix();
        this.dockerizedS3DataLake = new HiveMinIODataLake(bucketName, ImmutableMap.of(), hiveHadoopImage);
        this.dockerizedS3DataLake.start();
        return S3HiveQueryRunner.create(
                this.dockerizedS3DataLake.getHiveHadoop().getHiveMetastoreEndpoint(),
                this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint(),
                HiveMinIODataLake.ACCESS_KEY,
                HiveMinIODataLake.SECRET_KEY,
                ImmutableMap.<String, String>builder()
                        // This is required when using MinIO which requires path style access
                        .put("hive.s3.path-style-access", "true")
                        .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        // This new conf is added to pass the catalog information to metastore
                        .put("hive.metastore.catalog.name", HIVE_CATALOG)
                        .build());
    }

    @BeforeClass
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA_1,
                bucketName));
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        closeAllRuntimeException(dockerizedS3DataLake);
    }

    @Test
    public void testInsertOverwritePartitionedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedExternalTable()
    {
        String testTable = getTestTableName();
        // Store table data in data lake bucket
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);

        String tableName = testTable.substring(testTable.lastIndexOf('.') + 1);
        // Map this table as external table
        String externalTableName = testTable + "_ext";
        computeActual(getCreateTableStatement(
                externalTableName,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3",
                format("external_location = 's3a://%s/%s/%s/'", this.bucketName, HIVE_TEST_SCHEMA_1, tableName)));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(externalTableName);
    }

    protected void assertOverwritePartition(String testTable)
    {
        computeActual(format(
                "INSERT INTO %s VALUES " +
                        "('POLAND', 'Test Data', 25, 5), " +
                        "('CZECH', 'Test Data', 26, 5)",
                testTable));

        String oldPartitionPath = getPartitionPath(testTable);
        assertQuery(format("SELECT count(*) FROM %s WHERE regionkey = 5", testTable), "SELECT 2");

        computeActual(format("INSERT INTO %s values('POLAND', 'Overwrite', 25, 5)", testTable));

        String newPartitionPath = getPartitionPath(testTable);
        assertQuery(format("SELECT count(*) FROM %s WHERE regionkey = 5", testTable), "SELECT 1");

        assertEquals(oldPartitionPath, newPartitionPath);
        computeActual(format("DROP TABLE %s", testTable));
    }

    private String getPartitionPath(String testTable)
    {
        MaterializedResult result = computeActual(format("SELECT \"$PATH\" from %s where regionkey = 5", testTable));
        assertTrue(result.getMaterializedRows().size() > 0);
        String path = result.getMaterializedRows().get(0).getField(0).toString();
        return path.substring(0, path.lastIndexOf("/"));
    }

    protected String getTestTableName()
    {
        return format("hive.%s.%s", HIVE_TEST_SCHEMA_1, "nation_" + randomTableSuffix());
    }

    protected String getCreateTableStatement(String tableName, String... propertiesEntries)
    {
        return getCreateTableStatement(tableName, Arrays.asList(propertiesEntries));
    }

    protected String getCreateTableStatement(String tableName, List<String> propertiesEntries)
    {
        return format(
                "CREATE TABLE %s (" +
                        "    name varchar(25), " +
                        "    comment varchar(152),  " +
                        "    nationkey bigint, " +
                        "    regionkey bigint) " +
                        (propertiesEntries.isEmpty() ? "" : propertiesEntries
                                .stream()
                                .collect(joining(",", "WITH (", ")"))),
                tableName);
    }

    protected void copyTpchNationToTable(String testTable)
    {
        computeActual(format("INSERT INTO " + testTable + " SELECT name, comment, nationkey, regionkey FROM tpch.tiny.nation"));
    }

    @Test
    public void testListSchemasAndListTablesAcrossCatalogs()
    {
        // Create two schemas in different locations
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA_1,
                bucketName));

        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA_2,
                bucketName));

        // Create tables in both schemas
        String testTableSchema1 = format("hive.%s.%s", HIVE_TEST_SCHEMA_1, "nation_" + randomTableSuffix());
        String testTableSchema2 = format("hive.%s.%s", HIVE_TEST_SCHEMA_2, "region_" + randomTableSuffix());

        computeActual(getCreateTableStatement(testTableSchema1));
        computeActual(getCreateTableStatement(testTableSchema2));

        // Verify that listSchemas contains both schemas
        MaterializedResult schemas = computeActual("SHOW SCHEMAS FROM hive");
        List<String> schemaNames = schemas.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        assertTrue(schemaNames.contains(HIVE_TEST_SCHEMA_1), "Schema 1 is missing");
        assertTrue(schemaNames.contains(HIVE_TEST_SCHEMA_2), "Schema 2 is missing");

        // Verify that each table is listed under its own schema
        MaterializedResult tablesSchema1 = computeActual(format("SHOW TABLES FROM hive.%s", HIVE_TEST_SCHEMA_1));
        MaterializedResult tablesSchema2 = computeActual(format("SHOW TABLES FROM hive.%s", HIVE_TEST_SCHEMA_2));

        List<String> tableNamesSchema1 = tablesSchema1.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        List<String> tableNamesSchema2 = tablesSchema2.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        assertTrue(tableNamesSchema1.contains(testTableSchema1.substring(testTableSchema1.lastIndexOf('.') + 1)),
                "Table in Schema 1 is missing");

        assertTrue(tableNamesSchema2.contains(testTableSchema2.substring(testTableSchema2.lastIndexOf('.') + 1)),
                "Table in Schema 2 is missing");

        // Cleanup tables
        computeActual(format("DROP TABLE %s", testTableSchema1));
        computeActual(format("DROP TABLE %s", testTableSchema2));
    }


    @Test
    public void testUniqueSchemaAndListTablesAcrossCatalogs()
    {
        computeActual(format(
                "CREATE SCHEMA %1$s.%2$s WITH (location='s3a://%3$s/%2$s')",
                HIVE_CATALOG,
                HIVE_TEST_SCHEMA_1,
                bucketName));

        computeActual(format(
                "CREATE SCHEMA %1$s.%2$s WITH (location='s3a://%3$s/%2$s')",
                HIVE_BUCKETED_CATALOG,
                HIVE_TEST_SCHEMA_1,
                bucketName));

        String testTableSchema1 = format("%s.%s.%s", HIVE_CATALOG, HIVE_TEST_SCHEMA_1, "nation_" + randomTableSuffix());
        String testTableSchema2 = format("%s.%s.%s", HIVE_BUCKETED_CATALOG, HIVE_TEST_SCHEMA_1, "region_" + randomTableSuffix());

        computeActual(getCreateTableStatement(testTableSchema1));
        computeActual(getCreateTableStatement(testTableSchema2));

        MaterializedResult schemas = computeActual("SHOW SCHEMAS FROM hive");
        List<String> schemaNames = schemas.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        assertTrue(schemaNames.contains(HIVE_TEST_SCHEMA_1), "Schema 1 is missing");

        MaterializedResult tablesSchema1 = computeActual(format("SHOW TABLES FROM %s.%s", HIVE_CATALOG, HIVE_TEST_SCHEMA_1));
        MaterializedResult tablesSchema2 = computeActual(format("SHOW TABLES FROM %s.%s", HIVE_BUCKETED_CATALOG, HIVE_TEST_SCHEMA_1));

        List<String> tableNamesSchema1 = tablesSchema1.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        List<String> tableNamesSchema2 = tablesSchema2.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());

        assertTrue(tableNamesSchema1.contains(testTableSchema1.substring(testTableSchema1.lastIndexOf('.') + 1)),
                "Table in Schema 1 is missing");

        assertTrue(tableNamesSchema2.contains(testTableSchema2.substring(testTableSchema2.lastIndexOf('.') + 1)),
                "Table in Schema 2 is missing");
        computeActual(format("DROP TABLE %s", testTableSchema1));
        computeActual(format("DROP TABLE %s", testTableSchema2));
    }
}
