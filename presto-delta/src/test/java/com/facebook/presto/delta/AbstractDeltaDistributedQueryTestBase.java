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
package com.facebook.presto.delta;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.ITest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractDeltaDistributedQueryTestBase
        extends AbstractTestQueryFramework implements ITest
{
    public static final String DELTA_CATALOG = "delta";
    public static final String HIVE_CATALOG = "hive";
    public static final String PATH_SCHEMA = "$path$";
    public static final String DELTA_SCHEMA = "deltaTables"; // Schema in Hive which has test Delta tables
    protected static final String DELTA_V1 = "delta_v1";
    protected static final String DELTA_V3 = "delta_v3";

    protected static final String[] DELTA_VERSIONS = {DELTA_V1, DELTA_V3};

    /**
     * List of tables present in the test resources directory.
     */
    private static final String[] DELTA_TEST_TABLE_NAMES_LIST = {
            "data-reader-primitives",
            "data-reader-array-primitives",
            "data-reader-map",
            "snapshot-data3",
            "checkpointed-delta-table",
            "time-travel-partition-changes-b",
            "deltatbl-partition-prune",
            "data-reader-partition-values",
            "data-reader-partition-values-end-keys",
            "data-reader-nested-struct",
            "test-lowercase",
            "test-partitions-lowercase",
            "test-uppercase",
            "test-partitions-uppercase",
            "test-typing",
            "simple-partitioned-table",
            "simple-partitioned-table-end-keys"
    };

    /**
     * List of tables present in the test resources directory. Each table is replicated in reader version 1 and 3
     */
    public static final String[] DELTA_TEST_TABLE_LIST =
            new String[DELTA_VERSIONS.length * DELTA_TEST_TABLE_NAMES_LIST.length];
    static {
        for (int i = 0; i < DELTA_VERSIONS.length; i++) {
            for (int j = 0; j < DELTA_TEST_TABLE_NAMES_LIST.length; j++) {
                DELTA_TEST_TABLE_LIST[i * DELTA_TEST_TABLE_NAMES_LIST.length + j] = DELTA_VERSIONS[i] +
                        FileSystems.getDefault().getSeparator() + DELTA_TEST_TABLE_NAMES_LIST[j];
            }
        }
    }

    private static Path localDataDir;
    private final ThreadLocal<String> testName = new ThreadLocal<>();

    @DataProvider
    protected static Object[][] deltaReaderVersions()
    {
        return new Object[][] {{DELTA_V1}, {DELTA_V3}};
    }

    @Override
    public String getTestName()
    {
        return this.testName.get();
    }

    protected static String getVersionPrefix(String version)
    {
        return version + FileSystems.getDefault().getSeparator();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DeltaQueryRunner.builder().setExtraProperties(ImmutableMap.of(
                "experimental.pushdown-subfields-enabled", "true",
                "experimental.pushdown-dereference-enabled", "true")).build().getQueryRunner();

        // Create the test Delta tables in HMS
        for (String deltaTestTable : DELTA_TEST_TABLE_LIST) {
            registerDeltaTableInHMS(queryRunner, deltaTestTable, deltaTestTable);
        }

        return queryRunner;
    }

    @AfterClass
    public void deleteTestDeltaTables()
    {
        QueryRunner queryRunner = getQueryRunner();
        if (queryRunner != null) {
            // Remove the test Delta tables from HMS
            for (String deltaTestTable : DELTA_TEST_TABLE_LIST) {
                unregisterDeltaTableInHMS(queryRunner, deltaTestTable);
            }
        }
    }

    protected String goldenTablePath(String tableName)
    {
        return AbstractDeltaDistributedQueryTestBase.class.getClassLoader().getResource(tableName).toString();
    }

    protected static String extractedGoldenTablePath(String tableName)
    {
        try {
            URL resourceUrl = AbstractDeltaDistributedQueryTestBase.class.getClassLoader().getResource(tableName);
            if (resourceUrl == null) {
                throw new RuntimeException("Resource not found: " + tableName);
            }

            URI resourceUri = resourceUrl.toURI();

            // If resource is in a JAR, extract it to a temporary directory
            if ("jar".equals(resourceUri.getScheme())) {
                return extractFromJar(tableName, resourceUri);
            }

            // Resource is already on the filesystem
            return resourceUri.toString();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get path for table: " + tableName + " - " + e.toString(), e);
        }
    }

    private static String extractFromJar(String tableName, URI jarResourceUri)
            throws IOException
    {
        synchronized (AbstractDeltaDistributedQueryTestBase.class) {
            if (localDataDir == null) {
                localDataDir = Files.createTempDirectory("delta-table-");
            }

            Path targetPath = localDataDir.resolve(tableName);
            if (Files.exists(targetPath)) {
                return targetPath.toUri().toString();
            }

            String jarUriString = jarResourceUri.toString();
            int separatorIndex = jarUriString.indexOf("!/");
            URI jarUri = URI.create(jarUriString.substring(0, separatorIndex));

            FileSystem fs;
            try {
                fs = FileSystems.getFileSystem(jarUri);
            }
            catch (Exception e) {
                fs = FileSystems.newFileSystem(jarUri, Collections.emptyMap());
            }

            Path sourcePath = fs.getPath("/" + tableName);
            copyRecursively(sourcePath, targetPath);

            return targetPath.toUri().toString();
        }
    }

    private static void copyRecursively(Path source, Path target)
            throws IOException
    {
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");

        if (Files.isDirectory(source)) {
            Files.createDirectories(target);
            try (Stream<Path> entries = Files.list(source)) {
                entries.forEach(entry -> {
                    try {
                        copyRecursively(entry, target.resolve(entry.getFileName().toString()));
                    }
                    catch (IOException e) {
                        throw new RuntimeException("Failed to copy: " + entry + " - " + e.toString(), e);
                    }
                });
            }
        }
        else {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            if (!Files.exists(target)) {
                Files.copy(source, target);
            }
        }
    }

    protected String goldenTablePathWithPrefix(String prefix, String tableName)
    {
        return goldenTablePath(prefix + FileSystems.getDefault().getSeparator() + tableName);
    }

    /**
     * Register the given <i>deltaTableName</i> as <i>hiveTableName</i> in HMS using the Delta catalog.
     * Hive and Delta catalogs share the same HMS in this test.
     *
     * @param queryRunner
     * @param deltaTableName Name of the delta table which is on the classpath.
     * @param hiveTableName Name of the Hive table that the Delta table is to be registered as in HMS
     */
    protected void registerDeltaTableInHMS(QueryRunner queryRunner, String deltaTableName, String hiveTableName)
    {
        queryRunner.execute(format(
                "CREATE TABLE %s.\"%s\".\"%s\" (dummyColumn INT) WITH (external_location = '%s')",
                DELTA_CATALOG,
                DELTA_SCHEMA,
                hiveTableName,
                goldenTablePath(deltaTableName)));
    }

    /**
     * Drop the given table from HMS
     */
    private static void unregisterDeltaTableInHMS(QueryRunner queryRunner, String hiveTableName)
    {
        queryRunner.execute(format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", DELTA_CATALOG, DELTA_SCHEMA, hiveTableName));
    }
}
