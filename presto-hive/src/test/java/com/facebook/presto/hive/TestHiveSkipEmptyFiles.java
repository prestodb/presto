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
import com.facebook.presto.hive.parquet.ParquetTester;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
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
                .put("hive.skip-empty-files", "true")
                .build();

        this.queryRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));

        return this.queryRunner;
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
                .put("hive.skip-empty-files", "false")
                .build();

        this.queryFailRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryFailRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
    }

    /**
     * Generates a temporary directory and creates two parquet files inside, one is empty and the other is not
     *
     * @return a {@link File} pointing to the newly created temporary directory
     */
    private static File generateMetadata(String tableName)
            throws Exception
    {
        // obtains the root resource directory in order to create temporary tables
        URL url = TestHiveSkipEmptyFiles.class.getClassLoader().getResource(".");
        if (url == null) {
            throw new RuntimeException("Could not obtain resource URL");
        }
        File temporaryDirectory = new File(url.getPath(), tableName);
        boolean created = temporaryDirectory.mkdirs();
        if (!created) {
            throw new RuntimeException("Could not create resource directory: " + temporaryDirectory.getPath());
        }
        File firstParquetFile = new File(temporaryDirectory, randomUUID().toString());
        ParquetTester.writeParquetFileFromPresto(firstParquetFile,
                ImmutableList.of(IntegerType.INTEGER),
                Collections.singletonList("field"),
                new Iterable[] {Collections.singleton(1)},
                1,
                GZIP,
                PARQUET_2_0);
        File secondParquetFile = new File(temporaryDirectory, randomUUID().toString());
        if (!secondParquetFile.createNewFile()) {
            throw new RuntimeException("Could not create empty file");
        }
        return temporaryDirectory;
    }

    /**
     * Deletes the given directory and all of its contents recursively
     * Does not follow symbolic links
     *
     * @param temporaryDirectory a {@link File} pointing to the directory to delete
     */
    private static void deleteMetadata(File temporaryDirectory)
    {
        File[] metadataFiles = temporaryDirectory.listFiles();
        if (metadataFiles != null) {
            for (File file : metadataFiles) {
                if (!Files.isSymbolicLink(file.toPath())) {
                    deleteMetadata(file);
                }
            }
        }
        if (!temporaryDirectory.delete()) {
            throw new RuntimeException("Could not to delete metadata");
        }
    }

    @Test
    public void testSkipEmptyFilesSuccessful()
            throws Exception
    {
        String tableName = "skip_empty_files_success";
        File resourcesLocation = generateMetadata(tableName);
        executeCreationTestAndDropCycle(queryRunner, tableName, getResourceUrl(tableName), false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testSkipEmptyFilesError()
            throws Exception
    {
        String tableName = "skip_empty_files_fail";
        File resourcesLocation = generateMetadata(tableName);
        executeCreationTestAndDropCycle(queryFailRunner, tableName, getResourceUrl(tableName), true,
                ".* is not a valid Parquet File");
        deleteMetadata(resourcesLocation);
    }

    /**
     * Obtains the external location from the local resources directory of the project
     *
     * @param tableName a {@link String} containing the directory name to search for
     * @return a {@link String} with the external location for the given table_name
     */
    private static String getResourceUrl(String tableName)
    {
        URL resourceUrl = TestHiveSkipEmptyFiles.class.getClassLoader().getResource(tableName);
        if (resourceUrl == null) {
            throw new RuntimeException("Cannot find resource path for table name: " + tableName);
        }
        return resourceUrl.toString();
    }

    /**
     * Tries a table with the configuration property desired. If succeeds, tests the output.
     * Finally, it drops the table.
     *
     * @param queryRunner a {@link QueryRunner} with the desired configuration properties
     * @param tableName a {@link String} containing the desired table name
     * @param externalLocation a {@link String} with the external location to create the table against it
     * @param shouldFail {@code true} if the table creation should fail, {@code false} otherwise
     * @param errorMessage a {@link String} containing the expected error message. Will be checked if {@code shouldFail} is {@code true}
     */
    private void executeCreationTestAndDropCycle(DistributedQueryRunner queryRunner, String tableName, String externalLocation, boolean shouldFail, @Language("RegExp") String errorMessage)
    {
        try {
            @Language("SQL") String createQuery = format(
                    "CREATE TABLE %s.\"%s\".\"%s\" (field %s) WITH (external_location = '%s')",
                    CATALOG,
                    SCHEMA,
                    tableName,
                    IntegerType.INTEGER,
                    externalLocation);
            queryRunner.execute(createQuery);
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, tableName);
            if (shouldFail) {
                assertQueryFails(queryRunner, selectQuery, errorMessage);
            }
            else {
                MaterializedResult result = queryRunner.execute(selectQuery);
                assertEquals(1, result.getRowCount());
            }
        }
        finally {
            @Language("SQL") String dropQuery = format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, tableName);
            queryRunner.execute(dropQuery);
        }
    }
}
