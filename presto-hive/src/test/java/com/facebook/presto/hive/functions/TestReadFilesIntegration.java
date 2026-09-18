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
package com.facebook.presto.hive.functions;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestReadFilesIntegration
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "read_files_schema";

    private Path tempRoot;
    private String singleFileLocation;
    private String multiFileMergeableLocation;
    private String multiFileConflictLocation;
    private String emptyLocation;
    private String nestedDirectoryLocation;
    private String hiddenFilesLocation;
    private String nestedTypesLocation;
    private String threeFileLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session bootstrapSession = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(bootstrapSession)
                .setExtraProperties(ImmutableMap.of())
                .build();

        queryRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.storage-format", "PARQUET")
                .build();
        queryRunner.createCatalog(CATALOG, CATALOG, properties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));

        return queryRunner;
    }

    @BeforeClass
    public void setUpFixtures()
            throws IOException
    {
        tempRoot = Files.createTempDirectory("read_files_fixtures_");

        Path singleFile = sourceParquetFile(
                "single_src",
                "CAST(id AS BIGINT) AS id, CAST(name AS VARCHAR) AS name",
                "(1, 'a'), (2, 'b'), (3, 'c')",
                "t(id, name)");
        Path singleFileDir = tempRoot.resolve("single");
        Files.createDirectories(singleFileDir);
        Files.copy(singleFile, singleFileDir.resolve("file.parquet"), StandardCopyOption.REPLACE_EXISTING);
        singleFileLocation = singleFileDir.toUri().toString();

        Path intFile = sourceParquetFile(
                "merge_int_src",
                "CAST(n AS INTEGER) AS n",
                "(1), (2)",
                "t(n)");
        Path bigintFile = sourceParquetFile(
                "merge_bigint_src",
                "CAST(n AS BIGINT) AS n",
                "(100000000000)",
                "t(n)");
        Path mergeableDir = tempRoot.resolve("mergeable");
        Files.createDirectories(mergeableDir);
        Files.copy(intFile, mergeableDir.resolve("int.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(bigintFile, mergeableDir.resolve("bigint.parquet"), StandardCopyOption.REPLACE_EXISTING);
        multiFileMergeableLocation = mergeableDir.toUri().toString();

        Path conflictBigintFile = sourceParquetFile(
                "conflict_bigint_src",
                "CAST(x AS BIGINT) AS x",
                "(1)",
                "t(x)");
        Path conflictVarcharFile = sourceParquetFile(
                "conflict_varchar_src",
                "CAST(x AS VARCHAR) AS x",
                "('not_a_number')",
                "t(x)");
        Path conflictDir = tempRoot.resolve("conflict");
        Files.createDirectories(conflictDir);
        Files.copy(conflictBigintFile, conflictDir.resolve("a.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(conflictVarcharFile, conflictDir.resolve("b.parquet"), StandardCopyOption.REPLACE_EXISTING);
        multiFileConflictLocation = conflictDir.toUri().toString();

        Path emptyDir = tempRoot.resolve("empty");
        Files.createDirectories(emptyDir);
        emptyLocation = emptyDir.toUri().toString();

        Path nestedDir = tempRoot.resolve("nested");
        Path subA = nestedDir.resolve("part=A");
        Path subB = nestedDir.resolve("part=B");
        Files.createDirectories(subA);
        Files.createDirectories(subB);
        Path nestedFileA = sourceParquetFile(
                "nested_a_src",
                "CAST(id AS BIGINT) AS id",
                "(10), (20)",
                "t(id)");
        Path nestedFileB = sourceParquetFile(
                "nested_b_src",
                "CAST(id AS BIGINT) AS id",
                "(30)",
                "t(id)");
        Files.copy(nestedFileA, subA.resolve("data.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(nestedFileB, subB.resolve("data.parquet"), StandardCopyOption.REPLACE_EXISTING);
        nestedDirectoryLocation = nestedDir.toUri().toString();

        Path hiddenDir = tempRoot.resolve("hidden");
        Files.createDirectories(hiddenDir);
        Path realFile = sourceParquetFile(
                "hidden_real_src",
                "CAST(id AS BIGINT) AS id",
                "(1), (2)",
                "t(id)");
        Files.copy(realFile, hiddenDir.resolve("real.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(realFile, hiddenDir.resolve("_meta.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(realFile, hiddenDir.resolve(".staging.parquet"), StandardCopyOption.REPLACE_EXISTING);
        Path hiddenSubDir = hiddenDir.resolve(".prestoPermissions");
        Files.createDirectories(hiddenSubDir);
        Files.copy(realFile, hiddenSubDir.resolve("user_token"), StandardCopyOption.REPLACE_EXISTING);
        hiddenFilesLocation = hiddenDir.toUri().toString();

        Path threeDir = tempRoot.resolve("three");
        Files.createDirectories(threeDir);
        for (int i = 0; i < 3; i++) {
            Path source = sourceParquetFile(
                    "three_src_" + i,
                    "CAST(v AS BIGINT) AS v",
                    "(" + i + ")",
                    "t(v)");
            Files.copy(source, threeDir.resolve("part_" + i + ".parquet"), StandardCopyOption.REPLACE_EXISTING);
        }
        threeFileLocation = threeDir.toUri().toString();

        Path nestedTypesDir = tempRoot.resolve("nested_types");
        Files.createDirectories(nestedTypesDir);
        Path nestedTypesFile = sourceParquetFile(
                "nested_types_src",
                "CAST(ROW(1, 'a') AS ROW(x BIGINT, y VARCHAR)) AS r, " +
                        "CAST(ARRAY[1, 2, 3] AS ARRAY(BIGINT)) AS arr, " +
                        "CAST(MAP(ARRAY['k1'], ARRAY[100]) AS MAP(VARCHAR, BIGINT)) AS m",
                "(1)",
                "t(dummy)");
        Files.copy(nestedTypesFile, nestedTypesDir.resolve("nested.parquet"), StandardCopyOption.REPLACE_EXISTING);
        nestedTypesLocation = nestedTypesDir.toUri().toString();
    }

    private Path sourceParquetFile(String tableName, String projection, String values, String columnAlias)
    {
        String fqn = format("%s.%s.%s", CATALOG, SCHEMA, tableName);
        getQueryRunner().execute(format(
                "CREATE TABLE %s AS SELECT %s FROM (VALUES %s) %s",
                fqn, projection, values, columnAlias));
        String path = (String) computeActual(format("SELECT \"$path\" FROM %s LIMIT 1", fqn)).getOnlyValue();
        return Paths.get(URI.create(path));
    }

    @Test
    public void testSelectFromPtf()
    {
        MaterializedResult result = computeActual(format(
                "SELECT id, name FROM TABLE(%s.system.read_files('%s', 'parquet')) ORDER BY id",
                CATALOG, singleFileLocation));

        assertEquals(result.getRowCount(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "a");
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "c");
    }

    @Test
    public void testCtasFromPtf()
    {
        String targetTable = format("%s.%s.ctas_target", CATALOG, SCHEMA);
        try {
            getQueryRunner().execute(format(
                    "CREATE TABLE %s AS SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                    targetTable, CATALOG, singleFileLocation));

            MaterializedResult rows = computeActual(format("SELECT id, name FROM %s ORDER BY id", targetTable));
            assertEquals(rows.getRowCount(), 3);

            MaterializedResult schema = computeActual(format("DESCRIBE %s", targetTable));
            assertEquals(schema.getMaterializedRows().get(0).getField(0), "id");
            assertEquals(schema.getMaterializedRows().get(0).getField(1), "bigint");
            assertEquals(schema.getMaterializedRows().get(1).getField(0), "name");
            assertEquals(schema.getMaterializedRows().get(1).getField(1), "varchar");
        }
        finally {
            getQueryRunner().execute(format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertIntoFromPtf()
    {
        String targetTable = format("%s.%s.insert_target", CATALOG, SCHEMA);
        try {
            getQueryRunner().execute(format(
                    "CREATE TABLE %s (id BIGINT, name VARCHAR)", targetTable));
            getQueryRunner().execute(format(
                    "INSERT INTO %s SELECT id, name FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                    targetTable, CATALOG, singleFileLocation));
            MaterializedResult rows = computeActual(format("SELECT count(*) FROM %s", targetTable));
            assertEquals(rows.getOnlyValue(), 3L);
        }
        finally {
            getQueryRunner().execute(format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testExplainShowsTableScan()
    {
        String plan = (String) computeActual(format(
                "EXPLAIN SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                CATALOG, singleFileLocation)).getOnlyValue();

        assertTrue(plan.contains("TableScan"), "Expected TableScan in plan, got:\n" + plan);
        assertFalse(plan.contains("TableFunctionProcessor"), "Expected no TableFunctionProcessor in plan, got:\n" + plan);
    }

    @Test
    public void testRejectsAvro()
    {
        assertFormatRejected("avro");
    }

    @Test
    public void testRejectsCsv()
    {
        assertFormatRejected("csv");
    }

    @Test
    public void testRejectsJson()
    {
        assertFormatRejected("json");
    }

    @Test
    public void testNoFilesAtLocationErrors()
    {
        try {
            computeActual(format("SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet'))", CATALOG, emptyLocation));
            fail("Expected error on empty location");
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage() != null && e.getMessage().contains(emptyLocation),
                    "Expected error to mention location, got: " + e.getMessage());
        }
    }

    @Test
    public void testMultiFileMergeable()
    {
        MaterializedResult schema = computeActual(format(
                "SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet')) ORDER BY n",
                CATALOG, multiFileMergeableLocation));
        assertEquals(schema.getRowCount(), 3);
        assertEquals(schema.getMaterializedRows().get(2).getField(0), 100000000000L);
    }

    @Test
    public void testMultiFileIncompatibleFails()
    {
        try {
            computeActual(format(
                    "SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                    CATALOG, multiFileConflictLocation));
            fail("Expected merge conflict error");
        }
        catch (RuntimeException e) {
            String message = e.getMessage();
            assertTrue(message != null && message.contains("'x'"),
                    "Expected error to name the conflicting column 'x', got: " + message);
            assertTrue(message != null && (message.contains("a.parquet") || message.contains("b.parquet")),
                    "Expected error to name one of the offending files, got: " + message);
        }
    }

    @Test
    public void testWhereClauseFilters()
    {
        MaterializedResult result = computeActual(format(
                "SELECT id, name FROM TABLE(%s.system.read_files('%s', 'parquet')) WHERE id >= 2 ORDER BY id",
                CATALOG, singleFileLocation));
        assertEquals(result.getRowCount(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 2L);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 3L);
    }

    @Test
    public void testAggregation()
    {
        MaterializedResult result = computeActual(format(
                "SELECT count(*), sum(id) FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                CATALOG, singleFileLocation));
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 6L);
    }

    @Test
    public void testColumnProjection()
    {
        MaterializedResult result = computeActual(format(
                "SELECT name FROM TABLE(%s.system.read_files('%s', 'parquet')) ORDER BY name",
                CATALOG, singleFileLocation));
        assertEquals(result.getRowCount(), 3);
        assertEquals(result.getTypes().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "a");
    }

    @Test
    public void testRecursiveSubdirectoryListing()
    {
        MaterializedResult result = computeActual(format(
                "SELECT sum(id) FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                CATALOG, nestedDirectoryLocation));
        assertEquals(result.getOnlyValue(), 60L);
    }

    @Test
    public void testHiddenFilesSkipped()
    {
        MaterializedResult result = computeActual(format(
                "SELECT count(*) FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                CATALOG, hiddenFilesLocation));
        assertEquals(result.getOnlyValue(), 2L);
    }

    @Test
    public void testNestedTypesRoundTrip()
    {
        MaterializedResult result = computeActual(format(
                "SELECT r.x, r.y, arr[2], m['k1'] FROM TABLE(%s.system.read_files('%s', 'parquet'))",
                CATALOG, nestedTypesLocation));
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "a");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 2L);
        assertEquals(result.getMaterializedRows().get(0).getField(3), 100L);
    }

    @Test
    public void testJoinWithRegularTable()
    {
        String dimTable = format("%s.%s.join_dim", CATALOG, SCHEMA);
        try {
            getQueryRunner().execute(format(
                    "CREATE TABLE %s AS SELECT * FROM (VALUES (1, 'one'), (2, 'two')) t(id, label)",
                    dimTable));
            MaterializedResult result = computeActual(format(
                    "SELECT d.label FROM TABLE(%s.system.read_files('%s', 'parquet')) p " +
                            "JOIN %s d ON p.id = d.id ORDER BY p.id",
                    CATALOG, singleFileLocation, dimTable));
            assertEquals(result.getRowCount(), 2);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "one");
            assertEquals(result.getMaterializedRows().get(1).getField(0), "two");
        }
        finally {
            getQueryRunner().execute(format("DROP TABLE IF EXISTS %s", dimTable));
        }
    }

    @Test
    public void testMaxFileCountCapsScanRows()
    {
        MaterializedResult capped = computeActual(format(
                "SELECT count(*) FROM TABLE(%s.system.read_files('%s', 'parquet', 1))",
                CATALOG, threeFileLocation));
        assertEquals(capped.getOnlyValue(), 1L);

        MaterializedResult uncapped = computeActual(format(
                "SELECT count(*) FROM TABLE(%s.system.read_files('%s', 'parquet', 3))",
                CATALOG, threeFileLocation));
        assertEquals(uncapped.getOnlyValue(), 3L);
    }

    @Test
    public void testBadLocationError()
    {
        String bogus = "file:///tmp/this_path_should_not_exist_" + System.nanoTime() + "/";
        try {
            computeActual(format("SELECT * FROM TABLE(%s.system.read_files('%s', 'parquet'))", CATALOG, bogus));
            fail("Expected error on bogus location");
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage() != null && e.getMessage().contains(bogus),
                    "Expected error to mention the bad location, got: " + e.getMessage());
        }
    }

    private void assertFormatRejected(String formatString)
    {
        try {
            computeActual(format("SELECT * FROM TABLE(%s.system.read_files('%s', '%s'))", CATALOG, singleFileLocation, formatString));
            fail("Expected format rejection for: " + formatString);
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage() != null && e.getMessage().toUpperCase().contains("PARQUET"),
                    "Expected error to mention PARQUET as supported format, got: " + e.getMessage());
        }
    }
}
