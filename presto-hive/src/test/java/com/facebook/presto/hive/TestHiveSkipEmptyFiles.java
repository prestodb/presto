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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.parquet.ParquetTester;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.testng.Assert.assertEquals;

@Test
public class TestHiveSkipEmptyFiles
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "skip_empty_files_schema";
    private DistributedQueryRunner queryRunner;
    private DistributedQueryRunner queryFailRunner;
    private DistributedQueryRunner queryBucketRunner;
    private DistributedQueryRunner queryBucketFailRunner;
    private File temporaryDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        this.queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        this.queryRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = this.queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.storage-format", "PARQUET")
                .put("hive.bucket-execution", "false")
                .put("hive.skip-empty-files", "true")
                .build();

        this.queryRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));

        return this.queryRunner;
    }

    @BeforeClass
    private void generateMetadataDirectory()
            throws Exception
    {
        temporaryDirectory = createTempDir();
        generateMetadata(queryRunner, "skip_empty_files_success");
        generateMetadata(queryFailRunner, "skip_empty_files_fail");
        generateBucketedMetadataWithEmptyFiles(queryBucketRunner, "skip_empty_files_bucket_success", false);
        generateBucketedMetadataWithEmptyFiles(queryBucketFailRunner, "skip_empty_files_bucket_insert_fail", false);
        generateBucketedMetadataWithEmptyFiles(queryBucketFailRunner, "skip_empty_files_bucket_replace_fail", true);
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
            throws IOException
    {
        deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
    }

    @BeforeClass
    private void createQueryFailRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        this.queryFailRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        this.queryFailRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = this.queryFailRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.storage-format", "PARQUET")
                .put("hive.bucket-execution", "false")
                .put("hive.skip-empty-files", "false")
                .build();

        this.queryFailRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryFailRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
    }

    @BeforeClass
    private void createQueryBucketRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        this.queryBucketRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        this.queryBucketRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = this.queryBucketRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.storage-format", "PARQUET")
                .put("hive.bucket-execution", "true")
                .put("hive.skip-empty-files", "true")
                .build();

        this.queryBucketRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryBucketRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
    }

    @BeforeClass
    private void createQueryBucketFailRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        this.queryBucketFailRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        this.queryBucketFailRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = this.queryBucketFailRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.storage-format", "PARQUET")
                .put("hive.bucket-execution", "true")
                .put("hive.skip-empty-files", "false")
                .build();

        this.queryBucketFailRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryBucketFailRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
    }

    /**
     * Generates a temporary directory and creates two parquet files inside, one is empty and the other is not
     *
     * @param tableName a {@link String} containing the desired table name
     */
    private void generateMetadata(DistributedQueryRunner queryRunner, String tableName)
            throws Exception
    {
        Path tempDirectory = Files.createDirectory(Paths.get(temporaryDirectory.getPath(), tableName));
        @Language("SQL") String createQuery = format(
                "CREATE TABLE %s.\"%s\".\"%s\" (field %s) WITH (external_location = '%s')",
                CATALOG,
                SCHEMA,
                tableName,
                IntegerType.INTEGER,
                temporaryDirectory.toURI() + tableName);
        queryRunner.execute(createQuery);
        Path firstParquetFile = createTempFile(tempDirectory, randomUUID().toString(), randomUUID().toString());
        ParquetTester.writeParquetFileFromPresto(firstParquetFile.toFile(),
                ImmutableList.of(IntegerType.INTEGER),
                Collections.singletonList("field"),
                new Iterable[] {Collections.singleton(1)},
                1,
                GZIP,
                PARQUET_2_0);
        createTempFile(tempDirectory, randomUUID().toString(), randomUUID().toString());
    }

    /**
     * Generates a temporary directory and inserts data in every partition of the bucketed table, including an empty file in the first partition
     *
     * @param queryRunner a {@link QueryRunner} with the desired configuration properties
     * @param tableName a {@link String} containing the desired table name
     * @param replaceDataFileByEmptyFile a {@code true} if it is necessary to delete a partition file
     */
    private void generateBucketedMetadataWithEmptyFiles(DistributedQueryRunner queryRunner, String tableName, boolean replaceDataFileByEmptyFile) throws Exception
    {
        Path tempDirectory = Files.createDirectory(Paths.get(temporaryDirectory.getPath(), tableName));
        @Language("SQL") String createQuery = format("CREATE TABLE %s.\"%s\".\"%s\" (id %s, field %s) WITH (external_location = '%s'," +
                        "format = 'Parquet',partitioned_by = ARRAY['field']," +
                        "  bucketed_by = ARRAY['id']," +
                        "  bucket_count = 3)",
                CATALOG, SCHEMA, tableName, IntegerType.INTEGER, VarcharType.VARCHAR, tempDirectory.toUri());
        queryRunner.execute(createQuery);
        Path partitionDirectory = Paths.get(tempDirectory + "/field=field1");
        String partitionName = "field";
        @Language("SQL") String insertQuery;
        for (int i = 1; i <= 5; i++) {
            insertQuery = format("INSERT INTO %s.\"%s\".\"%s\" VALUES (%s,'%s')",
                    CATALOG, SCHEMA, tableName, i, partitionName + i);
            queryRunner.execute(insertQuery);
        }
        if (replaceDataFileByEmptyFile) {
            FilenameFilter filenameFilter = (dir, name) -> !name.endsWith(".crc");
            File[] filteredFiles = partitionDirectory.toFile().listFiles(filenameFilter);
            Files.delete(Arrays.stream(requireNonNull(filteredFiles)).iterator().next().toPath());
        }
        createTempFile(partitionDirectory, randomUUID().toString(), randomUUID().toString());
    }

    /**
     * Tries a table with the configuration property desired. If succeeds, tests the output.
     * Finally, it drops the table.
     */
    @Test
    public void testSkipEmptyFilesSuccessful()
    {
        try {
            @Language("SQL") String selectQuery = format("SELECT \"$path\" FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, "skip_empty_files_success");
            MaterializedResult result = queryRunner.execute(selectQuery);
            assertEquals(1, result.getRowCount());
        }
        finally {
            dropTable(queryRunner, "skip_empty_files_success");
        }
    }

    @Test
    public void testSkipEmptyFilesError()
    {
        try {
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, "skip_empty_files_fail");
            assertQueryFails(queryFailRunner, selectQuery, ".* is not a valid Parquet File");
        }
        finally {
            dropTable(queryFailRunner, "skip_empty_files_fail");
        }
    }

    @Test
    public void testSkipEmptyFilesBucketSuccessful()
    {
        try {
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, "skip_empty_files_bucket_success");
            MaterializedResult result = queryBucketRunner.execute(selectQuery);
            assertEquals(5, result.getRowCount());
        }
        finally {
            dropTable(queryBucketRunner, "skip_empty_files_bucket_success");
        }
    }

    @Test
    public void testSkipEmptyFilesBucketInsertFileFail()
    {
        try {
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, "skip_empty_files_bucket_insert_fail");
            assertQueryFails(queryBucketFailRunner, selectQuery, ".* is corrupt.* does not match the standard naming pattern," +
                    " and the number of files in the directory .* does not match the declared bucket count.*");
        }
        finally {
            dropTable(queryBucketFailRunner, "skip_empty_files_bucket_insert_fail");
        }
    }

    @Test
    public void testSkipEmptyFilesBucketReplaceFileFail()
    {
        try {
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, "skip_empty_files_bucket_replace_fail");
            assertQueryFails(queryBucketFailRunner, selectQuery, ".* is not a valid Parquet File");
        }
        finally {
            dropTable(queryBucketFailRunner, "skip_empty_files_bucket_replace_fail");
        }
    }

    private void dropTable(DistributedQueryRunner queryRunner, String tableName)
    {
        @Language("SQL") String dropQuery = format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", CATALOG,
                SCHEMA, tableName);
        queryRunner.execute(dropQuery);
    }
}
