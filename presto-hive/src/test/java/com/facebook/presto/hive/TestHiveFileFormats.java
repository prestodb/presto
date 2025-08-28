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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.orc.DwrfBatchPageSourceFactory;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetFileWriterFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.HiveDwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.CSV;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.JSON;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.rowBlockOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static io.airlift.slice.Slices.utf8Slice;
import static java.io.File.createTempFile;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    private static final FileFormatDataSourceStats STATS = new FileFormatDataSourceStats();
    private static final MetadataReader METADATA_READER = new MetadataReader();
    private static TestingConnectorSession parquetPageSourceSession = new TestingConnectorSession(getAllSessionProperties(new HiveClientConfig(), createParquetHiveCommonClientConfig(false)));
    private static TestingConnectorSession parquetPageSourceSessionUseName = new TestingConnectorSession(getAllSessionProperties(new HiveClientConfig(), createParquetHiveCommonClientConfig(true)));

    private static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    @DataProvider(name = "rowCount")
    public static Object[][] rowCountProvider()
    {
        return new Object[][] {{0}, {1000}};
    }

    @BeforeClass(alwaysRun = true)
    public void setUp()
    {
        // ensure the expected timezone is configured for this VM
        assertEquals(TimeZone.getDefault().getID(),
                "America/Bahia_Banderas",
                "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
    }

    @Test(dataProvider = "rowCount")
    public void testTextFile(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(column -> !column.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        assertThatFileFormat(TEXTFILE)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testCsvFile(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // CSV table only support Hive string columns. Notice that CSV does not allow to store null, it uses an empty string instead.
                .filter(column -> column.isPartitionKey() || ("string".equals(column.getType()) && !column.getName().contains("_null_")))
                .collect(toImmutableList());

        assertTrue(testColumns.size() > 5);

        assertThatFileFormat(CSV)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test
    public void testCsvFileWithNullAndValue()
            throws Exception
    {
        assertThatFileFormat(CSV)
                .withColumns(ImmutableList.of(
                        new TestColumn("t_null_string", javaStringObjectInspector, null, Slices.utf8Slice("")), // null was converted to empty string!
                        new TestColumn("t_string", javaStringObjectInspector, "test", Slices.utf8Slice("test"))))
                .withRowsCount(2)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testJson(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // binary is not supported
                .filter(column -> !column.getName().equals("t_binary"))
                // non-string map keys are not supported
                .filter(column -> !column.getName().equals("t_map_tinyint"))
                .filter(column -> !column.getName().equals("t_map_smallint"))
                .filter(column -> !column.getName().equals("t_map_int"))
                .filter(column -> !column.getName().equals("t_map_bigint"))
                .filter(column -> !column.getName().equals("t_map_float"))
                .filter(column -> !column.getName().equals("t_map_double"))
                // null map keys are not supported
                .filter(column -> !column.getName().equals("t_map_null_key"))
                .filter(column -> !column.getName().equals("t_map_null_key_complex_key_value"))
                .filter(column -> !column.getName().equals("t_map_null_key_complex_value"))
                // decimal(38) is broken or not supported
                .filter(column -> !column.getName().equals("t_decimal_precision_38"))
                .filter(column -> !column.getName().equals("t_map_decimal_precision_38"))
                .filter(column -> !column.getName().equals("t_array_decimal_precision_38"))
                .collect(toList());

        assertThatFileFormat(JSON)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testRCText(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = ImmutableList.copyOf(filter(TEST_COLUMNS, testColumn -> {
            // TODO: This is a bug in the RC text reader
            // RC file does not support complex type as key of a map
            return !testColumn.getName().equals("t_struct_null")
                    && !testColumn.getName().equals("t_map_null_key_complex_key_value");
        }));
        assertThatFileFormat(RCTEXT)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testRcTextPageSource(int rowCount)
            throws Exception
    {
        assertThatFileFormat(RCTEXT)
                .withColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testRcTextOptimizedWriter(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Presto can not produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toImmutableList());

        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(createRcFileHiveClientConfig(true), new HiveCommonClientConfig());
        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);

        assertThatFileFormat(RCTEXT)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new RcFileFileWriterFactory(HDFS_ENVIRONMENT, FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testRCBinary(int rowCount)
            throws Exception
    {
        // RCBinary does not support complex type as key of a map and interprets empty VARCHAR as nulls
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> {
                    String name = testColumn.getName();
                    return !name.equals("t_map_null_key_complex_key_value") && !name.equals("t_empty_varchar");
                }).collect(toList());
        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test(dataProvider = "rowCount")
    public void testRcBinaryPageSource(int rowCount)
            throws Exception
    {
        // RCBinary does not support complex type as key of a map and interprets empty VARCHAR as nulls
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_empty_varchar"))
                .collect(toList());

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testRcBinaryOptimizedWriter(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // RCBinary interprets empty VARCHAR as nulls
                .filter(testColumn -> !testColumn.getName().equals("t_empty_varchar"))
                // t_map_null_key_* must be disabled because Presto can not produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toList());

        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(createRcFileHiveClientConfig(true), new HiveCommonClientConfig());
        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new RcFileFileWriterFactory(HDFS_ENVIRONMENT, FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testOrc(int rowCount)
            throws Exception
    {
        assertThatFileFormat(ORC)
                .withColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcOptimizedWriter(int rowCount)
            throws Exception
    {
        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(
                new HiveClientConfig(),
                createOrcHiveCommonClientConfig(true, 100.0));
        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);

        // A Presto page can not contain a map with null keys, so a page based writer can not write null keys
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_map_null_key") && !testColumn.getName().equals("t_map_null_key_complex_value") && !testColumn.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        assertThatFileFormat(ORC)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new OrcFileWriterFactory(HDFS_ENVIRONMENT, new OutputStreamDataSinkFactory(), FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS, new OrcFileWriterConfig(), NO_ENCRYPTION))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())));
    }

    @Test(dataProvider = "rowCount")
    public void testOptimizedParquetWriter(int rowCount)
            throws Exception
    {
        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(
                new HiveClientConfig(),
                new ParquetFileWriterConfig().setParquetOptimizedWriterEnabled(true),
                createOrcHiveCommonClientConfig(true, 100.0));
        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);

        // A Presto page can not contain a map with null keys, so a page based writer can not write null keys
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_map_null_key") && !testColumn.getName().equals("t_map_null_key_complex_value") && !testColumn.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        assertThatFileFormat(PARQUET)
                .withSession(session)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withFileWriterFactory(new ParquetFileWriterFactory(HDFS_ENVIRONMENT, FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE))
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcUseColumnNames(int rowCount)
            throws Exception
    {
        TestingConnectorSession session = new TestingConnectorSession(getAllSessionProperties(
                new HiveClientConfig(),
                new HiveCommonClientConfig()
                    .setUseOrcColumnNames(true)));

        assertThatFileFormat(ORC)
                .withWriteColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .withReadColumns(Lists.reverse(TEST_COLUMNS))
                .withSession(session)
                .isReadableByPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcUseColumnNamesCompatibility(int rowCount)
            throws Exception
    {
        // test hive.orc.use-column-names can fallback to use hive column names, if in orc file has no real column names
        // only have old hive style name _col1, _col2, _col3
        TestingConnectorSession session = new TestingConnectorSession(getAllSessionProperties(
                new HiveClientConfig(),
                new HiveCommonClientConfig()));

        assertThatFileFormat(ORC)
                .withWriteColumns(getHiveColumnNameColumns())
                .withRowsCount(rowCount)
                .withReadColumns(TEST_COLUMNS)
                .withSession(session)
                .isReadableByPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())));
    }

    private static List<TestColumn> getHiveColumnNameColumns()
    {
        // Creates a new list of TestColumn objects with Hive-style column names based on their indices.
        // Each column's name is replaced with "_col" followed by its index in the list.
        return IntStream.range(0, TEST_COLUMNS.size())
                .mapToObj(index -> TEST_COLUMNS.get(index).withName("_col" + index))
                .collect(toList());
    }

    @Test(dataProvider = "rowCount")
    public void testAvro(int rowCount)
            throws Exception
    {
        assertThatFileFormat(AVRO)
                .withColumns(getTestColumnsSupportedByAvro())
                .withRowsCount(rowCount)
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    private static List<TestColumn> getTestColumnsSupportedByAvro()
    {
        // Avro only supports String for Map keys, and doesn't support smallint or tinyint.
        return TEST_COLUMNS.stream()
                .filter(column -> !column.getName().startsWith("t_map_") || column.getName().equals("t_map_string"))
                .filter(column -> !column.getName().endsWith("_smallint"))
                .filter(column -> !column.getName().endsWith("_tinyint"))
                .collect(toList());
    }

    @Test(dataProvider = "rowCount")
    public void testParquetPageSource(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(parquetPageSourceSession)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));
    }

    @Test(dataProvider = "rowCount")
    public void testParquetPageSourceGzip(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(parquetPageSourceSession)
                .withCompressionCodec(HiveCompressionCodec.GZIP)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));
    }

    @Test(dataProvider = "rowCount")
    public void testParquetPageSourceSchemaEvolution(int rowCount)
            throws Exception
    {
        List<TestColumn> writeColumns = getTestColumnsSupportedByParquet();

        // test index-based access
        List<TestColumn> readColumns = writeColumns.stream()
                .map(column -> new TestColumn(
                        column.getName() + "_new",
                        column.getObjectInspector(),
                        column.getWriteValue(),
                        column.getExpectedValue(),
                        column.isPartitionKey()))
                .collect(toList());
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withSession(parquetPageSourceSession)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        // test name-based access
        readColumns = Lists.reverse(writeColumns);
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withSession(parquetPageSourceSessionUseName)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));
    }

    @Test
    public void testParquetLogicalTypes() throws IOException
    {
        HiveFileWriterFactory parquetFileWriterFactory = new ParquetFileWriterFactory(HDFS_ENVIRONMENT, FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE);

        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(
                new HiveClientConfig(),
                new ParquetFileWriterConfig().setParquetOptimizedWriterEnabled(true),
                createOrcHiveCommonClientConfig(true, 100.0));

        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);
        File file = createTempFile("logicaltest", ".parquet");
        long timestamp = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();

        try {
            createTestFile(
                    file.getAbsolutePath(),
                    PARQUET,
                    HiveCompressionCodec.NONE,
                    ImmutableList.of(new TestColumn("t_timestamp", javaTimestampObjectInspector, new Timestamp(timestamp), timestamp)),
                    session,
                    3,
                    parquetFileWriterFactory);

            FileParquetDataSource dataSource = new FileParquetDataSource(file);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    file.length(),
                    Optional.empty(),
                    false).getParquetMetadata();

            MessageType writtenSchema = parquetMetadata.getFileMetaData().getSchema();
            Type timestampType = writtenSchema.getType("t_timestamp");
            if (timestampType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation annotation = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) timestampType.getLogicalTypeAnnotation();
                assertFalse(annotation.isAdjustedToUTC());
            }
            else {
                fail("the logical type annotation saved was not of type TimestampLogicalTypeAnnotation");
            }
        }
        finally {
            file.delete();
        }
    }

    private static List<TestColumn> getTestColumnsSupportedByParquet()
    {
        // Write of complex hive data to Parquet is broken
        // TODO: empty arrays or maps with null keys don't seem to work
        // Parquet does not support DATE
        return TEST_COLUMNS.stream()
                .filter(column -> !ImmutableSet.of("t_null_array_int", "t_array_empty", "t_map_null_key", "t_map_null_key_complex_value", "t_map_null_key_complex_key_value")
                        .contains(column.getName()))
                .filter(column -> column.isPartitionKey() || (
                        !hasType(column.getObjectInspector(), PrimitiveCategory.DATE)) &&
                        !hasType(column.getObjectInspector(), PrimitiveCategory.SHORT) &&
                        !hasType(column.getObjectInspector(), PrimitiveCategory.BYTE))
                .collect(toList());
    }

    @Test(dataProvider = "rowCount")
    public void testDwrf(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !hasType(testColumn.getObjectInspector(), PrimitiveCategory.DATE, PrimitiveCategory.VARCHAR, PrimitiveCategory.CHAR, PrimitiveCategory.DECIMAL))
                .collect(Collectors.toList());
        assertThatFileFormat(DWRF)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new DwrfBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HIVE_CLIENT_CONFIG, HDFS_ENVIRONMENT, STATS, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), NO_ENCRYPTION));
    }

    @Test(dataProvider = "rowCount")
    public void testDwrfOptimizedWriter(int rowCount)
            throws Exception
    {
        List<PropertyMetadata<?>> allSessionProperties = getAllSessionProperties(
                new HiveClientConfig(),
                createOrcHiveCommonClientConfig(true, 100.0));
        TestingConnectorSession session = new TestingConnectorSession(allSessionProperties);

        // DWRF does not support modern Hive types
        // A Presto page can not contain a map with null keys, so a page based writer can not write null keys
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !hasType(testColumn.getObjectInspector(), PrimitiveCategory.DATE, PrimitiveCategory.VARCHAR, PrimitiveCategory.CHAR, PrimitiveCategory.DECIMAL))
                .filter(testColumn -> !testColumn.getName().equals("t_map_null_key") && !testColumn.getName().equals("t_map_null_key_complex_value") && !testColumn.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        assertThatFileFormat(DWRF)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new OrcFileWriterFactory(HDFS_ENVIRONMENT, new OutputStreamDataSinkFactory(), FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS, new OrcFileWriterConfig(), NO_ENCRYPTION))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new DwrfBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HIVE_CLIENT_CONFIG, HDFS_ENVIRONMENT, STATS, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), NO_ENCRYPTION));
    }

    @Test
    public void testTruncateVarcharColumn()
            throws Exception
    {
        TestColumn writeColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(4)), new HiveVarchar("test", 4), utf8Slice("test"));
        TestColumn readColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), new HiveVarchar("tes", 3), utf8Slice("tes"));

        assertThatFileFormat(RCTEXT)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(RCBINARY)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(ORC)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        assertThatFileFormat(AVRO)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(SEQUENCEFILE)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(TEXTFILE)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));
    }

    @Test
    public void testFailForLongVarcharPartitionColumn()
            throws Exception
    {
        TestColumn partitionColumn = new TestColumn("partition_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), "test", utf8Slice("tes"), true);
        TestColumn varcharColumn = new TestColumn("varchar_column", getPrimitiveJavaObjectInspector(new VarcharTypeInfo(3)), new HiveVarchar("tes", 3), utf8Slice("tes"));

        List<TestColumn> columns = ImmutableList.of(partitionColumn, varcharColumn);

        HiveErrorCode expectedErrorCode = HIVE_INVALID_PARTITION_VALUE;
        String expectedMessage = "Invalid partition value 'test' for varchar\\(3\\) partition key: partition_column";

        assertThatFileFormat(RCTEXT)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(RCBINARY)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(ORC)
                .withColumns(columns)
                .isFailingForPageSource(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, HDFS_ENVIRONMENT, STATS, 100, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())), expectedErrorCode, expectedMessage);

        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessage);

        assertThatFileFormat(SEQUENCEFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(TEXTFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);
    }

    @Test
    public void testSchemaMismatch()
            throws Exception
    {
        TestColumn floatColumn = new TestColumn("column_name", javaFloatObjectInspector, 5.1f, 5.1f);
        TestColumn doubleColumn = new TestColumn("column_name", javaDoubleObjectInspector, 5.1, 5.1);
        TestColumn booleanColumn = new TestColumn("column_name", javaBooleanObjectInspector, true, true);
        TestColumn stringColumn = new TestColumn("column_name", javaStringObjectInspector, "test", utf8Slice("test"));
        TestColumn intColumn = new TestColumn("column_name", javaIntObjectInspector, 3, 3);
        TestColumn longColumn = new TestColumn("column_name", javaLongObjectInspector, 4L, 4L);
        TestColumn longStoredAsIntColumn = new TestColumn("column_name", javaIntObjectInspector, 4, 4);
        TestColumn timestampColumn = new TestColumn("column_name", javaTimestampObjectInspector, 4L, 4L);
        TestColumn mapLongColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(javaLongObjectInspector, javaLongObjectInspector),
                ImmutableMap.of(4L, 4L),
                mapBlockOf(BIGINT, BIGINT, 4L, 4L));
        TestColumn mapDoubleColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(javaDoubleObjectInspector, javaDoubleObjectInspector),
                ImmutableMap.of(5.1, 5.2),
                mapBlockOf(DOUBLE, DOUBLE, 5.1, 5.2));
        TestColumn arrayStringColumn = new TestColumn("column_name",
                getStandardListObjectInspector(javaStringObjectInspector),
                ImmutableList.of("test"),
                arrayBlockOf(createUnboundedVarcharType(), "test"));
        TestColumn arrayBooleanColumn = new TestColumn("column_name",
                getStandardListObjectInspector(javaBooleanObjectInspector),
                ImmutableList.of(true),
                arrayBlockOf(BOOLEAN, true));
        TestColumn rowLongColumn = new TestColumn("column_name",
                getStandardStructObjectInspector(ImmutableList.of("s_bigint"), ImmutableList.of(javaLongObjectInspector)),
                new Long[] {1L},
                rowBlockOf(ImmutableList.of(BIGINT), 1));
        TestColumn nestColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        ImmutableList.of("s_int"),
                                        ImmutableList.of(javaIntObjectInspector)))),
                ImmutableMap.of("test", ImmutableList.<Object>of(new Integer[] {1})),
                mapBlockOf(createUnboundedVarcharType(), new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER))),
                        "test", arrayBlockOf(RowType.anonymous(ImmutableList.of(INTEGER)), rowBlockOf(ImmutableList.of(INTEGER), 1L))));

        HiveErrorCode expectedErrorCode = HIVE_PARTITION_SCHEMA_MISMATCH;

        // Make sure INT64 is still readable as Timestamp see https://github.com/prestodb/presto/issues/13855
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(longColumn))
                .withReadColumns(ImmutableList.of(timestampColumn))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        // make sure INT64 (declared in Hive schema) stored as INT32 in file is still readable
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(longStoredAsIntColumn))
                .withReadColumns(ImmutableList.of(longColumn))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(floatColumn))
                .withReadColumns(ImmutableList.of(doubleColumn))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        String expectedMessageDoubleLong = "The column column_name of table schema.table is declared as type bigint, but the Parquet file ((.*?)) declares the column as type DOUBLE";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(doubleColumn))
                .withReadColumns(ImmutableList.of(longColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageDoubleLong);

        String expectedMessageFloatInt = "The column column_name of table schema.table is declared as type int, but the Parquet file ((.*?)) declares the column as type FLOAT";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(floatColumn))
                .withReadColumns(ImmutableList.of(intColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageFloatInt);

        String expectedMessageIntBoolean = "The column column_name of table schema.table is declared as type boolean, but the Parquet file ((.*?)) declares the column as type INT32";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(intColumn))
                .withReadColumns(ImmutableList.of(booleanColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageIntBoolean);

        String expectedMessageStringLong = "The column column_name of table schema.table is declared as type string, but the Parquet file ((.*?)) declares the column as type INT64";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(longColumn))
                .withReadColumns(ImmutableList.of(stringColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageStringLong);

        String expectedMessageIntString = "The column column_name of table schema.table is declared as type int, but the Parquet file ((.*?)) declares the column as type BINARY";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(stringColumn))
                .withReadColumns(ImmutableList.of(intColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageIntString);

        String expectedMessageMapLongLong = "The column column_name of table schema.table is declared as type map<bigint,bigint>, but the Parquet file ((.*?)) declares the column as type INT64";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(longColumn))
                .withReadColumns(ImmutableList.of(mapLongColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageMapLongLong);

        String expectedMessageMapLongMapDouble = "The column column_name of table schema.table is declared as type map<bigint,bigint>, but the Parquet file ((.*?)) declares the column as type optional group column_name \\(MAP\\) \\{\n"
                + "  repeated group key_value \\(MAP_KEY_VALUE\\) \\{\n"
                + "    required double key;\n"
                + "    optional double value;\n"
                + "  \\}\n"
                + "\\}";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(mapDoubleColumn))
                .withReadColumns(ImmutableList.of(mapLongColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageMapLongMapDouble);

        String expectedMessageArrayStringArrayBoolean = "The column column_name of table schema.table is declared as type array<string>, but the Parquet file ((.*?)) declares the column as type optional group column_name \\(LIST\\) \\{\n"
                + "  repeated group bag \\{\n"
                + "    optional boolean array_element;\n"
                + "  \\}\n"
                + "\\}";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(arrayBooleanColumn))
                .withReadColumns(ImmutableList.of(arrayStringColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageArrayStringArrayBoolean);

        String expectedMessageBooleanArrayBoolean = "The column column_name of table schema.table is declared as type array<boolean>, but the Parquet file ((.*?)) declares the column as type BOOLEAN";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(booleanColumn))
                .withReadColumns(ImmutableList.of(arrayBooleanColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageBooleanArrayBoolean);

        String expectedMessageRowLongLong = "The column column_name of table schema.table is declared as type bigint, but the Parquet file ((.*?)) declares the column as type optional group column_name \\{\n"
                + "  optional int64 s_bigint;\n"
                + "\\}";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(rowLongColumn))
                .withReadColumns(ImmutableList.of(longColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageRowLongLong);

        TestColumn rowLongColumnReadOnMap = new TestColumn("column_name",
                getStandardStructObjectInspector(ImmutableList.of("s_bigint"), ImmutableList.of(javaLongObjectInspector)),
                new Long[] {1L},
                null);
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(mapLongColumn))
                .withReadColumns(ImmutableList.of(rowLongColumnReadOnMap))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        String expectedMessageRowLongNest = "The column column_name of table schema.table is declared as type map<string,array<struct<s_int:int>>>, but the Parquet file ((.*?)) declares the column as type optional group column_name \\{\n"
                + "  optional int64 s_bigint;\n"
                + "\\}";

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(rowLongColumn))
                .withReadColumns(ImmutableList.of(nestColumn))
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageRowLongNest);
    }

    @Test
    public void testSchemaMismatchOnNestedStruct()
            throws Exception
    {
        //test out of order fields in nested Row type
        TestColumn writeColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardStructObjectInspector(
                                ImmutableList.of("s_int", "s_double"),
                                ImmutableList.of(javaIntObjectInspector, javaDoubleObjectInspector))),
                ImmutableMap.of("test", Arrays.asList(1, 5.0)),
                mapBlockOf(createUnboundedVarcharType(), RowType.anonymous(ImmutableList.of(INTEGER, DOUBLE)),
                        "test", rowBlockOf(ImmutableList.of(INTEGER, DOUBLE), 1L, 5.0)));
        TestColumn readColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardStructObjectInspector(
                                ImmutableList.of("s_double", "s_int"),  //out of order
                                ImmutableList.of(javaDoubleObjectInspector, javaIntObjectInspector))),
                ImmutableMap.of("test", Arrays.asList(5.0, 1)),
                mapBlockOf(createUnboundedVarcharType(), RowType.anonymous(ImmutableList.of(DOUBLE, INTEGER)),
                        "test", rowBlockOf(ImmutableList.of(DOUBLE, INTEGER), 5.0, 1L)));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withRowsCount(1)
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        //test add/remove sub-fields
        readColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardStructObjectInspector(
                                ImmutableList.of("s_int", "s_int_new"),  //add sub-field s_int_new, remove s_double
                                ImmutableList.of(javaIntObjectInspector, javaIntObjectInspector))),
                ImmutableMap.of("test", Arrays.asList(1, 5.0)),
                mapBlockOf(createUnboundedVarcharType(), RowType.anonymous(ImmutableList.of(INTEGER, INTEGER)),
                        "test", rowBlockOf(ImmutableList.of(INTEGER, INTEGER), 1L, null))); //expected null for s_int_new

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withRowsCount(1)
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        //test field name case sensitivity in nested Row type
        readColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardStructObjectInspector(
                                ImmutableList.of("s_DOUBLE", "s_INT"),  //out of order
                                ImmutableList.of(javaDoubleObjectInspector, javaIntObjectInspector))),
                ImmutableMap.of("test", Arrays.asList(5.0, 1)),
                mapBlockOf(createUnboundedVarcharType(), RowType.anonymous(ImmutableList.of(DOUBLE, INTEGER)),
                        "test", rowBlockOf(ImmutableList.of(DOUBLE, INTEGER), 5.0, 1L)));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withRowsCount(1)
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER));

        //test sub-field type mismatch in nested Row type
        readColumn = new TestColumn("column_name",
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardStructObjectInspector(
                                ImmutableList.of("s_double", "s_int"),
                                ImmutableList.of(javaIntObjectInspector, javaIntObjectInspector))), //re-type a sub-field
                ImmutableMap.of("test", Arrays.asList(5, 1)),
                mapBlockOf(createUnboundedVarcharType(), RowType.anonymous(ImmutableList.of(INTEGER, INTEGER)),
                        "test", rowBlockOf(ImmutableList.of(INTEGER, INTEGER), 5L, 1L)));

        HiveErrorCode expectedErrorCode = HIVE_PARTITION_SCHEMA_MISMATCH;
        String expectedMessageRowLongNest = "The column column_name of table schema.table is declared as type map<string,struct<s_double:int,s_int:int>>, but the Parquet file ((.*?)) declares the column as type optional group column_name \\(MAP\\) \\{\n" +
                "  repeated group key_value \\(MAP_KEY_VALUE\\) \\{\n" +
                "    required binary key \\(STRING\\);\n" +
                "    optional group value \\{\n" +
                "      optional int32 s_int;\n" +
                "      optional double s_double;\n" +
                "    \\}\n" +
                "  \\}\n" +
                "\\}";
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withRowsCount(1)
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, HDFS_ENVIRONMENT, STATS, METADATA_READER), expectedErrorCode, expectedMessageRowLongNest);
    }

    private void testCursorProvider(HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testColumns,
            ConnectorSession session,
            int rowCount)
    {
        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(TestColumn::toHivePartitionKey)
                .collect(toList());

        List<HiveColumnHandle> partitionKeyColumnHandles = getColumnHandles(testColumns.stream().filter(TestColumn::isPartitionKey).collect(toImmutableList()));
        List<Column> tableDataColumns = testColumns.stream()
                .filter(column -> !column.isPartitionKey())
                .map(column -> new Column(column.getName(), HiveType.valueOf(column.getType()), Optional.empty(), Optional.empty()))
                .collect(toImmutableList());

        HiveFileSplit hiveFileSplit = new HiveFileSplit(
                split.getPath().toString(),
                split.getStart(),
                split.getLength(),
                split.getLength(),
                Instant.now().toEpochMilli(),
                Optional.empty(),
                ImmutableMap.of(),
                0);

        Configuration configuration = new Configuration();
        configuration.set("io.compression.codecs", LzoCodec.class.getName() + "," + LzopCodec.class.getName());
        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(cursorProvider),
                ImmutableSet.of(),
                configuration,
                session,
                hiveFileSplit,
                OptionalInt.empty(),
                new Storage(
                        StorageFormat.create(storageFormat.getSerDe(), storageFormat.getInputFormat(), storageFormat.getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                TupleDomain.all(),
                getColumnHandles(testColumns),
                ImmutableMap.of(),
                partitionKeys,
                DateTimeZone.getDefault(),
                FUNCTION_AND_TYPE_MANAGER,
                new SchemaTableName("schema", "table"),
                partitionKeyColumnHandles,
                tableDataColumns,
                ImmutableMap.of(),
                tableDataColumns.size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                DEFAULT_HIVE_FILE_CONTEXT,
                TRUE_CONSTANT,
                false,
                ROW_EXPRESSION_SERVICE,
                Optional.empty(),
                Optional.empty());

        RecordCursor cursor = ((RecordPageSource) pageSource.get()).getCursor();

        checkCursor(cursor, testColumns, rowCount);
    }

    private void testPageSourceFactory(HiveBatchPageSourceFactory sourceFactory,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testColumns,
            ConnectorSession session,
            int rowCount)
            throws IOException
    {
        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(TestColumn::toHivePartitionKey)
                .collect(toList());

        List<HiveColumnHandle> partitionKeyColumnHandles = getColumnHandles(testColumns.stream().filter(TestColumn::isPartitionKey).collect(toImmutableList()));
        List<Column> tableDataColumns = testColumns.stream()
                .filter(column -> !column.isPartitionKey())
                .map(column -> new Column(column.getName(), HiveType.valueOf(column.getType()), Optional.empty(), Optional.empty()))
                .collect(toImmutableList());

        List<HiveColumnHandle> columnHandles = getColumnHandles(testColumns);

        HiveFileSplit hiveFileSplit = new HiveFileSplit(
                split.getPath().toString(),
                split.getStart(),
                split.getLength(),
                split.getLength(),
                Instant.now().toEpochMilli(),
                Optional.empty(),
                ImmutableMap.of(),
                0);

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(),
                ImmutableSet.of(sourceFactory),
                new Configuration(),
                session,
                hiveFileSplit,
                OptionalInt.empty(),
                new Storage(
                        StorageFormat.create(storageFormat.getSerDe(), storageFormat.getInputFormat(), storageFormat.getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                TupleDomain.all(),
                columnHandles,
                ImmutableMap.of(),
                partitionKeys,
                DateTimeZone.getDefault(),
                FUNCTION_AND_TYPE_MANAGER,
                new SchemaTableName("schema", "table"),
                partitionKeyColumnHandles,
                tableDataColumns,
                ImmutableMap.of(),
                tableDataColumns.size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                DEFAULT_HIVE_FILE_CONTEXT,
                TRUE_CONSTANT,
                false,
                ROW_EXPRESSION_SERVICE,
                Optional.empty(),
                Optional.empty());

        assertTrue(pageSource.isPresent());

        checkPageSource(pageSource.get(), testColumns, getTypes(columnHandles), rowCount);
    }

    public static boolean hasType(ObjectInspector objectInspector, PrimitiveCategory... types)
    {
        if (objectInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) objectInspector;
            PrimitiveCategory primitiveCategory = primitiveInspector.getPrimitiveCategory();
            for (PrimitiveCategory type : types) {
                if (primitiveCategory == type) {
                    return true;
                }
            }
            return false;
        }
        if (objectInspector instanceof ListObjectInspector) {
            ListObjectInspector listInspector = (ListObjectInspector) objectInspector;
            return hasType(listInspector.getListElementObjectInspector(), types);
        }
        if (objectInspector instanceof MapObjectInspector) {
            MapObjectInspector mapInspector = (MapObjectInspector) objectInspector;
            return hasType(mapInspector.getMapKeyObjectInspector(), types) ||
                    hasType(mapInspector.getMapValueObjectInspector(), types);
        }
        if (objectInspector instanceof StructObjectInspector) {
            for (StructField field : ((StructObjectInspector) objectInspector).getAllStructFieldRefs()) {
                if (hasType(field.getFieldObjectInspector(), types)) {
                    return true;
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unknown object inspector type " + objectInspector);
    }

    private static boolean withoutNullMapKeyTests(TestColumn testColumn)
    {
        String name = testColumn.getName();
        return !name.equals("t_map_null_key") &&
                !name.equals("t_map_null_key_complex_key_value") &&
                !name.equals("t_map_null_key_complex_value");
    }

    private FileFormatAssertion assertThatFileFormat(HiveStorageFormat hiveStorageFormat)
    {
        return new FileFormatAssertion(hiveStorageFormat.name())
                .withStorageFormat(hiveStorageFormat);
    }

    private static HiveClientConfig createRcFileHiveClientConfig(boolean rcfileOptimizedWriterEnabled)
    {
        HiveClientConfig config = new HiveClientConfig();
        config.setRcfileOptimizedWriterEnabled(rcfileOptimizedWriterEnabled);
        return config;
    }

    private static HiveCommonClientConfig createParquetHiveCommonClientConfig(boolean useParquetColumnNames)
    {
        HiveCommonClientConfig config = new HiveCommonClientConfig();
        config.setUseParquetColumnNames(useParquetColumnNames);
        return config;
    }

    private static HiveCommonClientConfig createOrcHiveCommonClientConfig(boolean orcOptimizedWriterEnabled, double orcWriterValidationPercentage)
    {
        HiveCommonClientConfig config = new HiveCommonClientConfig();
        config.setOrcOptimizedWriterEnabled(orcOptimizedWriterEnabled);
        config.setOrcWriterValidationPercentage(orcWriterValidationPercentage);
        return config;
    }

    private class FileFormatAssertion
    {
        private final String formatName;
        private HiveStorageFormat storageFormat;
        private HiveCompressionCodec compressionCodec = HiveCompressionCodec.NONE;
        private List<TestColumn> writeColumns;
        private List<TestColumn> readColumns;
        private ConnectorSession session = SESSION;
        private int rowsCount = 1000;
        private HiveFileWriterFactory fileWriterFactory;

        private FileFormatAssertion(String formatName)
        {
            this.formatName = requireNonNull(formatName, "formatName is null");
        }

        public FileFormatAssertion withStorageFormat(HiveStorageFormat storageFormat)
        {
            this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
            return this;
        }

        public FileFormatAssertion withCompressionCodec(HiveCompressionCodec compressionCodec)
        {
            this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
            return this;
        }

        public FileFormatAssertion withFileWriterFactory(HiveFileWriterFactory fileWriterFactory)
        {
            this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
            return this;
        }

        public FileFormatAssertion withColumns(List<TestColumn> inputColumns)
        {
            withWriteColumns(inputColumns);
            withReadColumns(inputColumns);
            return this;
        }

        public FileFormatAssertion withWriteColumns(List<TestColumn> writeColumns)
        {
            this.writeColumns = requireNonNull(writeColumns, "writeColumns is null");
            return this;
        }

        public FileFormatAssertion withReadColumns(List<TestColumn> readColumns)
        {
            this.readColumns = requireNonNull(readColumns, "readColumns is null");
            return this;
        }

        public FileFormatAssertion withRowsCount(int rowsCount)
        {
            this.rowsCount = rowsCount;
            return this;
        }

        public FileFormatAssertion withSession(ConnectorSession session)
        {
            this.session = requireNonNull(session, "session is null");
            return this;
        }

        public FileFormatAssertion isReadableByPageSource(HiveBatchPageSourceFactory pageSourceFactory)
                throws Exception
        {
            assertRead(Optional.of(pageSourceFactory), Optional.empty());
            return this;
        }

        public FileFormatAssertion isReadableByRecordCursor(HiveRecordCursorProvider cursorProvider)
                throws Exception
        {
            assertRead(Optional.empty(), Optional.of(cursorProvider));
            return this;
        }

        public FileFormatAssertion isFailingForPageSource(HiveBatchPageSourceFactory pageSourceFactory, ErrorCodeSupplier expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.of(pageSourceFactory), Optional.empty(), expectedErrorCode, expectedMessage);
            return this;
        }

        public FileFormatAssertion isFailingForRecordCursor(HiveRecordCursorProvider cursorProvider, ErrorCodeSupplier expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.empty(), Optional.of(cursorProvider), expectedErrorCode, expectedMessage);
            return this;
        }

        private void assertRead(Optional<HiveBatchPageSourceFactory> pageSourceFactory, Optional<HiveRecordCursorProvider> cursorProvider)
                throws Exception
        {
            assertNotNull(storageFormat, "storageFormat must be specified");
            assertNotNull(writeColumns, "writeColumns must be specified");
            assertNotNull(readColumns, "readColumns must be specified");
            assertNotNull(session, "session must be specified");
            assertTrue(rowsCount >= 0, "rowsCount must be greater than zero");

            String compressionSuffix = compressionCodec.getCodec()
                    .map(codec -> {
                        try {
                            return codec.getConstructor().newInstance().getDefaultExtension();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .orElse("");

            File file = File.createTempFile("presto_test", formatName + compressionSuffix);
            file.delete();
            try {
                FileSplit split;
                if (fileWriterFactory != null) {
                    split = createTestFile(file.getAbsolutePath(), storageFormat, compressionCodec, writeColumns, session, rowsCount, fileWriterFactory);
                }
                else {
                    split = createTestFile(file.getAbsolutePath(), storageFormat, compressionCodec, writeColumns, rowsCount);
                }
                if (pageSourceFactory.isPresent()) {
                    testPageSourceFactory(pageSourceFactory.get(), split, storageFormat, readColumns, session, rowsCount);
                }
                if (cursorProvider.isPresent()) {
                    testCursorProvider(cursorProvider.get(), split, storageFormat, readColumns, session, rowsCount);
                }
            }
            finally {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
        }

        private void assertFailure(
                Optional<HiveBatchPageSourceFactory> pageSourceFactory,
                Optional<HiveRecordCursorProvider> cursorProvider,
                ErrorCodeSupplier expectedErrorCode,
                String expectedMessage)
                throws Exception
        {
            try {
                assertRead(pageSourceFactory, cursorProvider);
                fail("failure is expected");
            }
            catch (PrestoException prestoException) {
                assertEquals(prestoException.getErrorCode(), expectedErrorCode.toErrorCode());
                assertTrue(prestoException.getMessage().matches(expectedMessage));
            }
        }
    }
}
