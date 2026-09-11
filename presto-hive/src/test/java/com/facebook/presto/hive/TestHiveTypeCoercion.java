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
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.hive.parquet.ParquetTester;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.testng.Assert.assertEquals;

/**
 * Verifies that a Parquet DOUBLE column can be read through a Hive table whose column is declared
 * as VARCHAR / VARCHAR(n) / CHAR(n). Exercises both the row-at-a-time reader
 * ({@link com.facebook.presto.parquet.reader.DoubleColumnReader}) and the batch reader path
 * (Int64FlatBatchReader + {@link com.facebook.presto.parquet.reader.ParquetReader#typeCoercion}).
 */
@Test
public class TestHiveTypeCoercion
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "type_coercion_schema";
    private static final List<Double> DOUBLE_VALUES = ImmutableList.of(
            0.0d,
            -1.5d,
            4124.1324213412341241242134243d,
            Double.MAX_VALUE,
            Double.NaN,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY);

    private DistributedQueryRunner queryRunner;

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
                .build();
        this.queryRunner.createCatalog(CATALOG, CATALOG, properties);
        this.queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
        return this.queryRunner;
    }

    @DataProvider(name = "batchReadOptimizationEnabled")
    public Object[][] batchReadOptimizationEnabled()
    {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "batchReadOptimizationEnabled")
    public void testDoubleToUnboundedVarchar(boolean batchReadEnabled)
            throws Exception
    {
        List<String> expected = DOUBLE_VALUES.stream().map(String::valueOf).collect(Collectors.toList());
        runCoercionTest("double_to_varchar_" + batchReadEnabled, "VARCHAR", expected, batchReadEnabled);
    }

    @Test(dataProvider = "batchReadOptimizationEnabled")
    public void testDoubleToBoundedVarchar(boolean batchReadEnabled)
            throws Exception
    {
        int maxLength = 5;
        List<String> expected = DOUBLE_VALUES.stream()
                .map(value -> truncate(String.valueOf(value), maxLength))
                .collect(Collectors.toList());
        runCoercionTest("double_to_varchar5_" + batchReadEnabled, "VARCHAR(" + maxLength + ")", expected, batchReadEnabled);
    }

    @Test(dataProvider = "batchReadOptimizationEnabled")
    public void testDoubleToChar(boolean batchReadEnabled)
            throws Exception
    {
        int length = 6;
        // CHAR(n) truncates to n characters, then trims trailing spaces before writing.
        // Reading requires padding back to length n.
        List<String> expected = DOUBLE_VALUES.stream()
                .map(value -> padRight(truncate(String.valueOf(value), length), length))
                .collect(Collectors.toList());
        runCoercionTest("double_to_char" + length + "_" + batchReadEnabled, "CHAR(" + length + ")", expected, batchReadEnabled);
    }

    private void runCoercionTest(String tableName, String declaredType, List<String> expectedRows, boolean batchReadEnabled)
            throws Exception
    {
        File resourcesLocation = generateMetadata(tableName);
        try {
            @Language("SQL") String createQuery = format(
                    "CREATE TABLE %s.\"%s\".\"%s\" (field %s) WITH (external_location = '%s')",
                    CATALOG, SCHEMA, tableName, declaredType, getResourceUrl(tableName));
            this.queryRunner.execute(createQuery);

            Session session = Session.builder(this.queryRunner.getDefaultSession())
                    .setCatalogSessionProperty(CATALOG, PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, String.valueOf(batchReadEnabled))
                    .build();
            @Language("SQL") String selectQuery = format("SELECT field FROM %s.\"%s\".\"%s\"", CATALOG,
                SCHEMA, tableName);
            MaterializedResult result = this.queryRunner.execute(session, selectQuery);
            List<String> actualResults = result.getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .sorted()
                    .collect(Collectors.toList());
            List<String> sortedExpected = expectedRows.stream().sorted().collect(Collectors.toList());
            assertEquals(actualResults, sortedExpected, "Rows read with batchReadEnabled=" + batchReadEnabled);
        }
        finally {
            @Language("SQL") String dropQuery = format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", CATALOG,
                SCHEMA, tableName);
            this.queryRunner.execute(dropQuery);
            deleteMetadata(resourcesLocation);
        }
    }

    private static File generateMetadata(String tableName)
            throws Exception
    {
        URL url = TestHiveTypeCoercion.class.getClassLoader().getResource(".");
        if (url == null) {
            throw new RuntimeException("Could not obtain resource URL");
        }
        File temporaryDirectory = new File(url.getPath(), tableName);
        if (!temporaryDirectory.mkdirs()) {
            throw new RuntimeException("Could not create resource directory: " + temporaryDirectory.getPath());
        }
        File parquetFile = new File(temporaryDirectory, randomUUID().toString());
        ParquetTester.writeParquetFileFromPresto(parquetFile,
                ImmutableList.of(DoubleType.DOUBLE),
                ImmutableList.of("field"),
                new Iterable[] {DOUBLE_VALUES},
                DOUBLE_VALUES.size(),
                GZIP,
                PARQUET_1_0);
        return temporaryDirectory;
    }

    private static String getResourceUrl(String tableName)
    {
        URL resourceUrl = TestHiveTypeCoercion.class.getClassLoader().getResource(tableName);
        if (resourceUrl == null) {
            throw new RuntimeException("Cannot find resource path for table name: " + tableName);
        }
        return resourceUrl.toString();
    }

    private static void deleteMetadata(File file)
    {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                if (!Files.isSymbolicLink(child.toPath())) {
                    deleteMetadata(child);
                }
            }
        }
        file.delete();
    }

    private static String truncate(String value, int maxLength)
    {
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private static String padRight(String value, int length)
    {
        if (value.length() >= length) {
            return value;
        }
        StringBuilder builder = new StringBuilder(length);
        builder.append(value);
        for (int i = value.length(); i < length; i++) {
            builder.append(' ');
        }
        return builder.toString();
    }
}
