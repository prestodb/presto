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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.parquet.ParquetTester;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestHiveTypeWidening
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.getLogger(TestHiveTypeWidening.class);
    private static final String CATALOG = "hive";
    private static final String SCHEMA = "type_widening_schema";
    private static final String INTEGER = "INTEGER";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String DOUBLE = "DOUBLE";
    private DistributedQueryRunner queryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        logger.info("Creating 'QueryRunner'");
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).setTimeZoneKey(TimeZoneKey.UTC_KEY).build();
        this.queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(ImmutableMap.<String, String>builder().build()).build();

        logger.info("  |-- Installing Plugin: " + CATALOG);
        this.queryRunner.installPlugin(new HivePlugin(CATALOG));
        Path catalogDirectory = this.queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").getParent().resolve("catalog");
        logger.info("  |-- Obtained catalog directory: " + catalogDirectory.toFile().toURI());
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.compression-codec", "GZIP")
                .put("hive.storage-format", "PARQUET")
                .build();
        logger.info("  |-- Properties loaded");

        logger.info("  |-- Creating catalog '" + CATALOG + "' using plugin '" + CATALOG + '\'');
        this.queryRunner.createCatalog(CATALOG, CATALOG, properties);
        logger.info("  |-- Catalog '" + CATALOG + "' created");
        logger.info("  |-- Creating schema '" + SCHEMA + "' on catalog '" + CATALOG + '\'');
        this.queryRunner.execute(format("CREATE SCHEMA %s.%s", CATALOG, SCHEMA));
        logger.info("  |-- Schema '" + SCHEMA + "' created");

        logger.info("'QueryRunner' created succesfully");
        return this.queryRunner;
    }

    /**
     * Generates a temporary directory and creates two parquet files inside, one with data of each type
     * @param baseType
     * @param widenedType
     * @throws Exception if an error occurs
     * @return a {@link File} pointing to the newly created temporary directory
     */
    private static File generateMetadata(String baseType, String widenedType)
            throws Exception
    {
        // obtains the root resouce directory in order to create temporary tables
        URL url = TestHiveTypeWidening.class.getClassLoader().getResource(".");
        if (url == null) {
            throw new RuntimeException("Could not obtain resource URL");
        }
        File temporaryDirectory = new File(url.getPath(), getTableName(baseType, widenedType));
        boolean created = temporaryDirectory.mkdirs();
        if (!created) {
            throw new RuntimeException("Could not create resource directory: " + temporaryDirectory.getPath());
        }
        logger.info("Created temporary directory: " + temporaryDirectory.toPath());
        File firstParquetFile = new File(temporaryDirectory, randomUUID().toString());
        ParquetTester.writeParquetFileFromPresto(firstParquetFile,
                Collections.singletonList(toType(baseType)),
                Collections.singletonList("field"),
                new Iterable[] {Collections.singletonList(getExpectedValueForType(baseType))},
                1,
                GZIP,
                PARQUET_1_0);
        logger.info("First file written");
        File secondParquetFile = new File(temporaryDirectory, randomUUID().toString());
        ParquetTester.writeParquetFileFromPresto(secondParquetFile,
                Collections.singletonList(toType(widenedType)),
                Collections.singletonList("field"),
                new Iterable[] {Collections.singletonList(getExpectedValueForType(widenedType))},
                1,
                GZIP,
                PARQUET_1_0);
        logger.info("Second file written");
        return temporaryDirectory;
    }

    /**
     * Returns the presto type for the given type name
     * @param type a {@link String} containing the type name
     * @return a {@link Type} matching the given type name
     */
    private static Type toType(String type)
    {
        switch (type) {
            case INTEGER:
                return IntegerType.INTEGER;
            case BIGINT:
                return BigintType.BIGINT;
            case REAL:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            default:
                throw new RuntimeException("Type not supported: " + type);
        }
    }

    /**
     * Deletes the given directory and all of its contents recursively
     * Does not follow symbolic links
     * @param temporaryDirectory a {@link File} pointing to the directory to delete
     */
    private static void deleteMetadata(File temporaryDirectory)
    {
        File[] data = temporaryDirectory.listFiles();
        if (data != null) {
            for (File f : data) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    deleteMetadata(f);
                }
            }
        }
        deleteAndLog(temporaryDirectory);
    }

    private static void deleteAndLog(File file)
    {
        String filePath = file.getAbsolutePath();
        boolean isDirectory = file.isDirectory();
        if (file.delete()) {
            if (isDirectory) {
                logger.info("   deleted temporary directory: " + filePath);
            }
            else {
                logger.info("   deleted temporary file: " + filePath);
            }
        }
        else {
            logger.info("   could not delete temporary element: " + filePath);
        }
    }

    // Integer type widenings

    @Test
    public void testTypeWideningTableCreationIntegerToInteger()
            throws Exception
    {
        File resourcesLocation = generateMetadata(INTEGER, INTEGER);
        String tableName = getTableName(INTEGER, INTEGER);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), INTEGER, INTEGER, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationIntegerToBigint()
            throws Exception
    {
        File resourcesLocation = generateMetadata(INTEGER, BIGINT);
        String tableName = getTableName(INTEGER, BIGINT);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), INTEGER, BIGINT, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationIntegerToReal()
            throws Exception
    {
        File resourcesLocation = generateMetadata(INTEGER, REAL);
        String tableName = getTableName(INTEGER, REAL);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), INTEGER, REAL, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationIntegerToDouble()
            throws Exception
    {
        File resourcesLocation = generateMetadata(INTEGER, DOUBLE);
        String tableName = getTableName(INTEGER, DOUBLE);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), INTEGER, DOUBLE, false, null);
        deleteMetadata(resourcesLocation);
    }

    // Bigint type widenings

    @Test
    public void testTypeWideningTableCreationBigintToInteger()
            throws Exception
    {
        File resourcesLocation = generateMetadata(BIGINT, INTEGER);
        String tableName = getTableName(BIGINT, INTEGER);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), BIGINT, INTEGER, true,
                "The column field of table type_widening_schema\\.bigint_to_integer is declared as type int, but the Parquet file (.*) declares the column as type INT64");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationBigintToBigint()
            throws Exception
    {
        File resourcesLocation = generateMetadata(BIGINT, BIGINT);
        String tableName = getTableName(BIGINT, BIGINT);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), BIGINT, BIGINT, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationBigintToReal()
            throws Exception
    {
        File resourcesLocation = generateMetadata(BIGINT, REAL);
        String tableName = getTableName(BIGINT, REAL);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), BIGINT, REAL, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationBigintToDouble()
            throws Exception
    {
        File resourcesLocation = generateMetadata(BIGINT, DOUBLE);
        String tableName = getTableName(BIGINT, DOUBLE);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), BIGINT, DOUBLE, false, null);
        deleteMetadata(resourcesLocation);
    }

    // Real type widenings

    @Test
    public void testTypeWideningTableCreationRealToInteger()
            throws Exception
    {
        File resourcesLocation = generateMetadata(REAL, INTEGER);
        String tableName = getTableName(REAL, INTEGER);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), REAL, INTEGER, true,
                "The column field of table type_widening_schema\\.real_to_integer is declared as type int, but the Parquet file (.*) declares the column as type FLOAT");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationRealToBigint()
            throws Exception
    {
        File resourcesLocation = generateMetadata(REAL, BIGINT);
        String tableName = getTableName(REAL, BIGINT);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), REAL, BIGINT, true,
                "The column field of table type_widening_schema\\.real_to_bigint is declared as type bigint, but the Parquet file (.*) declares the column as type FLOAT");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationRealToReal()
            throws Exception
    {
        File resourcesLocation = generateMetadata(REAL, REAL);
        String tableName = getTableName(REAL, REAL);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), REAL, REAL, false, null);
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationRealToDouble()
            throws Exception
    {
        File resourcesLocation = generateMetadata(REAL, DOUBLE);
        String tableName = getTableName(REAL, DOUBLE);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), REAL, DOUBLE, false, null);
        deleteMetadata(resourcesLocation);
    }

    // Double type widenings

    @Test
    public void testTypeWideningTableCreationDoubleToInteger()
            throws Exception
    {
        File resourcesLocation = generateMetadata(DOUBLE, INTEGER);
        String tableName = getTableName(DOUBLE, INTEGER);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), DOUBLE, INTEGER, true,
                "The column field of table type_widening_schema\\.double_to_integer is declared as type int, but the Parquet file (.*) declares the column as type DOUBLE");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationDoubleToBigint()
            throws Exception
    {
        File resourcesLocation = generateMetadata(DOUBLE, BIGINT);
        String tableName = getTableName(DOUBLE, BIGINT);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), DOUBLE, BIGINT, true,
                "The column field of table type_widening_schema\\.double_to_bigint is declared as type bigint, but the Parquet file (.*) declares the column as type DOUBLE");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationDoubleToReal()
            throws Exception
    {
        File resourcesLocation = generateMetadata(DOUBLE, REAL);
        String tableName = getTableName(DOUBLE, REAL);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), DOUBLE, REAL, true,
                "The column field of table type_widening_schema\\.double_to_real is declared as type float, but the Parquet file (.*) declares the column as type DOUBLE");
        deleteMetadata(resourcesLocation);
    }

    @Test
    public void testTypeWideningTableCreationDoubleToDouble()
            throws Exception
    {
        File resourcesLocation = generateMetadata(DOUBLE, DOUBLE);
        String tableName = getTableName(DOUBLE, DOUBLE);
        executeCreationTestAndDropCycle(tableName, getResourceUrl(tableName), DOUBLE, DOUBLE, false, null);
        deleteMetadata(resourcesLocation);
    }

    private static String getTableName(String baseType, String targetType)
    {
        return baseType.toLowerCase(Locale.ENGLISH) + "_to_" + targetType.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Obtains the external location from the local resources directory of the project
     * @param tableName a {@link String} containting the directory name to search for
     * @return a {@link String} with the external location for the given table_name
     */
    private static String getResourceUrl(String tableName)
    {
        URL resourceUrl = TestHiveTypeWidening.class.getClassLoader().getResource(tableName);
        if (resourceUrl == null) {
            throw new RuntimeException("Cannot find resource path for table name: " + tableName);
        }
        logger.info("resource url: " + resourceUrl.toString());
        return resourceUrl.toString();
    }

    /**
     * Tries a table with the type defined in {@code widenedType}. If succeeds, tests the output.
     * Finally, it drops the table.
     * @param tableName a {@link String} containing the desired table name
     * @param externalLocation a {@link String} with the external location to create the table against it
     * @param baseType a {@link String} containing the type of the files to read
     * @param widenedType a {@link String} containing the type of the created table
     * @param shouldFail {@code true} if the table creation should fail, {@code false} otherwise
     * @param errorMessage a {@link String} containing the expected error message. Will be checked if {@code shouldFail} is {@code true}
     */
    private void executeCreationTestAndDropCycle(String tableName, String externalLocation, String baseType,
            String widenedType, boolean shouldFail, @Language("RegExp") String errorMessage)
    {
        logger.info("Executing Create - Test - Drop for: " + tableName);
        try {
            @Language("SQL") String createQuery = format(
                    "CREATE TABLE %s.\"%s\".\"%s\" (field %s) WITH (external_location = '%s')",
                    CATALOG,
                    SCHEMA,
                    tableName,
                    widenedType,
                    externalLocation);
            logger.info("Creating table: " + createQuery);
            this.queryRunner.execute(createQuery);
            @Language("SQL") String selectQuery = format("SELECT * FROM %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, tableName);
            logger.info("Executing query: " + selectQuery);
            if (shouldFail) {
                assertQueryFails(selectQuery, errorMessage);
            }
            else {
                MaterializedResult result = this.queryRunner.execute(selectQuery);
                assertEquals(1, result.getTypes().size());
                assertEquals(widenedType, result.getTypes().get(0).toString().toUpperCase());
                List<Object> fieldsValues = new ArrayList<>(0);
                for (MaterializedRow mr : result.getMaterializedRows()) {
                    fieldsValues.addAll(mr.getFields());
                }
                for (Object o : fieldsValues) {
                    logger.info(o.getClass().toString() + "     " + o);
                }
                Number genericTypeValue = getExpectedValueForType(widenedType);
                Number specificTypeValue = getExpectedValueCastedForType(getExpectedValueForType(baseType), widenedType);
                logger.info("Checking for existence of type '" + widenedType + "' value: " + genericTypeValue.toString());
                assertTrue(fieldsValues.contains(genericTypeValue));
                logger.info("Checking for existence of type '" + widenedType + "' value: " + specificTypeValue.toString());
                assertTrue(fieldsValues.contains(specificTypeValue));
            }
        }
        finally {
            @Language("SQL") String dropQuery = format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", CATALOG,
                    SCHEMA, tableName);
            logger.info("Dropping table: " + dropQuery);
            this.queryRunner.execute(dropQuery);
        }
    }

    /**
     * Gives the desired output value from each type
     * @param typeName a {@link String} with the target type
     * @return the expected value for each type
     */
    private static Number getExpectedValueForType(String typeName)
    {
        switch (typeName) {
            case INTEGER:
                return Integer.valueOf(1);
            case BIGINT:
                return Long.valueOf(1000000000000L);
            case REAL:
                return Float.valueOf(0.04f);
            case DOUBLE:
                return Double.valueOf(4124.1324213412341241242134243d);
            default:
                throw new RuntimeException("Type not supported: " + typeName);
        }
    }

    /**
     * Casts the desired value to the desired type
     * @param typeName a {@link String} with the target type
     * @return the expected value converted to the given type
     */
    private static Number getExpectedValueCastedForType(Number value, String typeName)
    {
        switch (typeName) {
            case INTEGER:
                return value.intValue();
            case BIGINT:
                return value.longValue();
            case REAL:
                return value.floatValue();
            case DOUBLE:
                return value.doubleValue();
            default:
                throw new RuntimeException("Type not supported: " + typeName);
        }
    }
}
