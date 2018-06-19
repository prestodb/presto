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

import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.JSON;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.rowBlockOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    private static final FileFormatDataSourceStats STATS = new FileFormatDataSourceStats();
    private static TestingConnectorSession parquetCursorSession = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(false, false, false), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetCursorSessionUseName = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(false, false, true), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetCursorPushdownSession = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(false, true, false), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetCursorPushdownSessionUseName = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(false, true, true), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetPageSourceSession = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(true, false, false), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetPageSourceSessionUseName = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(true, false, true), new OrcFileWriterConfig()).getSessionProperties());
    private static TestingConnectorSession parquetPageSourcePushdown = new TestingConnectorSession(new HiveSessionProperties(createParquetHiveClientConfig(true, true, false), new OrcFileWriterConfig()).getSessionProperties());

    private static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("Asia/Katmandu");

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
                "Asia/Katmandu",
                "Timezone not configured correctly. Add -Duser.timezone=Asia/Katmandu to your JVM arguments");
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
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testRcTextOptimizedWriter(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                // t_map_null_key_* must be disabled because Presto can not produce maps with null keys so the writer will throw
                .filter(TestHiveFileFormats::withoutNullMapKeyTests)
                .collect(toImmutableList());

        TestingConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setRcfileOptimizedWriterEnabled(true), new OrcFileWriterConfig()).getSessionProperties());

        assertThatFileFormat(RCTEXT)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new RcFileFileWriterFactory(HDFS_ENVIRONMENT, TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
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
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
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

        TestingConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setRcfileOptimizedWriterEnabled(true), new OrcFileWriterConfig()).getSessionProperties());

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new RcFileFileWriterFactory(HDFS_ENVIRONMENT, TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testOrc(int rowCount)
            throws Exception
    {
        assertThatFileFormat(ORC)
                .withColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, false, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcOptimizedWriter(int rowCount)
            throws Exception
    {
        TestingConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig()
                                .setOrcOptimizedWriterEnabled(true)
                                .setOrcWriterValidationPercentage(100.0),
                        new OrcFileWriterConfig()).getSessionProperties());

        // A Presto page can not contain a map with null keys, so a page based writer can not write null keys
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> !testColumn.getName().equals("t_map_null_key") && !testColumn.getName().equals("t_map_null_key_complex_value") && !testColumn.getName().equals("t_map_null_key_complex_key_value"))
                .collect(toList());

        assertThatFileFormat(ORC)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(session)
                .withFileWriterFactory(new OrcFileWriterFactory(HDFS_ENVIRONMENT, TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS, new OrcWriterOptions()))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, false, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testOrcUseColumnNames(int rowCount)
            throws Exception
    {
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig(), new OrcFileWriterConfig()).getSessionProperties());

        assertThatFileFormat(ORC)
                .withWriteColumns(TEST_COLUMNS)
                .withRowsCount(rowCount)
                .withReadColumns(Lists.reverse(TEST_COLUMNS))
                .withSession(session)
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, true, HDFS_ENVIRONMENT, STATS));
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
    public void testParquet(int rowCount)
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorSession)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorPushdownSession)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testParquetCaseInsensitiveColumnLookup(int rowCount)
            throws Exception
    {
        List<TestColumn> writeColumns = ImmutableList.of(new TestColumn("column_name", javaStringObjectInspector, "test", utf8Slice("test"), false));
        List<TestColumn> readColumns = ImmutableList.of(new TestColumn("Column_Name", javaStringObjectInspector, "test", utf8Slice("test"), false));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorSessionUseName)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorPushdownSessionUseName)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
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
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(parquetPageSourcePushdown)
                .withRowsCount(rowCount)
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
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
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));

        // test name-based access
        readColumns = Lists.reverse(writeColumns);
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withSession(parquetPageSourceSessionUseName)
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testParquetUseColumnNames(int rowCount)
            throws Exception
    {
        List<TestColumn> writeColumns = getTestColumnsSupportedByParquet();
        List<TestColumn> readColumns = Lists.reverse(writeColumns);
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorSessionUseName)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .withRowsCount(rowCount)
                .withSession(parquetCursorPushdownSessionUseName)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
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
    public void testParquetThrift(int rowCount)
    {
        RowType nameType = RowType.anonymous(ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType()));
        RowType phoneType = RowType.anonymous(ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType()));
        RowType personType = RowType.anonymous(ImmutableList.of(nameType, INTEGER, createUnboundedVarcharType(), new ArrayType(phoneType)));

        List<TestColumn> testColumns = ImmutableList.of(
                new TestColumn(
                        "persons",
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        ImmutableList.of("name", "id", "email", "phones"),
                                        ImmutableList.of(
                                                getStandardStructObjectInspector(
                                                        ImmutableList.of("first_name", "last_name"),
                                                        ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)),
                                                javaIntObjectInspector,
                                                javaStringObjectInspector,
                                                getStandardListObjectInspector(
                                                        getStandardStructObjectInspector(
                                                                ImmutableList.of("number", "type"),
                                                                ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)))))),
                        null,
                        arrayBlockOf(personType,
                                rowBlockOf(ImmutableList.of(nameType, INTEGER, createUnboundedVarcharType(), new ArrayType(phoneType)),
                                        rowBlockOf(ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType()), "Bob", "Roberts"),
                                        0,
                                        "bob.roberts@example.com",
                                        arrayBlockOf(phoneType, rowBlockOf(ImmutableList.of(createUnboundedVarcharType(), createUnboundedVarcharType()), "1234567890", null))))));

        File file = new File(this.getClass().getClassLoader().getResource("addressbook.parquet").getPath());
        FileSplit split = new FileSplit(new Path(file.getAbsolutePath()), 0, file.length(), new String[0]);
        HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS);
        testCursorProvider(cursorProvider, split, PARQUET, testColumns, SESSION, 1);
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
                .isReadableByPageSource(new DwrfPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
    }

    @Test(dataProvider = "rowCount")
    public void testDwrfOptimizedWriter(int rowCount)
            throws Exception
    {
        TestingConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig()
                                .setOrcOptimizedWriterEnabled(true)
                                .setOrcWriterValidationPercentage(100.0),
                        new OrcFileWriterConfig()).getSessionProperties());

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
                .withFileWriterFactory(new OrcFileWriterFactory(HDFS_ENVIRONMENT, TYPE_MANAGER, new NodeVersion("test"), HIVE_STORAGE_TIME_ZONE, STATS, new OrcWriterOptions()))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT))
                .isReadableByPageSource(new DwrfPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
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
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(RCBINARY)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS))
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT));

        assertThatFileFormat(ORC)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, false, HDFS_ENVIRONMENT, STATS));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(parquetCursorSession)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(parquetCursorPushdownSession)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(parquetPageSourceSession)
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));
        assertThatFileFormat(PARQUET)
                .withWriteColumns(ImmutableList.of(writeColumn))
                .withReadColumns(ImmutableList.of(readColumn))
                .withSession(parquetPageSourcePushdown)
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS));

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

        HiveErrorCode expectedErrorCode = HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
        String expectedMessage = "Invalid partition value 'test' for varchar(3) partition key: partition_column";

        assertThatFileFormat(RCTEXT)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(RCBINARY)
                .withColumns(columns)
                .isFailingForPageSource(new RcFilePageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(ORC)
                .withColumns(columns)
                .isFailingForPageSource(new OrcPageSourceFactory(TYPE_MANAGER, false, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage);

        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(parquetCursorSession)
                .isFailingForRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage);
        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(parquetCursorPushdownSession)
                .isFailingForRecordCursor(new ParquetRecordCursorProvider(HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage);

        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(parquetPageSourceSession)
                .isFailingForPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage);
        assertThatFileFormat(PARQUET)
                .withColumns(columns)
                .withSession(parquetPageSourcePushdown)
                .isFailingForPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, HDFS_ENVIRONMENT, STATS), expectedErrorCode, expectedMessage);

        assertThatFileFormat(SEQUENCEFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);

        assertThatFileFormat(TEXTFILE)
                .withColumns(columns)
                .isFailingForRecordCursor(new GenericHiveRecordCursorProvider(HDFS_ENVIRONMENT), expectedErrorCode, expectedMessage);
    }

    private void testCursorProvider(HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testColumns,
            ConnectorSession session,
            int rowCount)
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, storageFormat.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, storageFormat.getSerDe());
        splitProperties.setProperty("columns", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getName)));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getType)));

        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                .collect(toList());

        Configuration configuration = new Configuration();
        configuration.set("io.compression.codecs", LzoCodec.class.getName() + "," + LzopCodec.class.getName());
        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(cursorProvider),
                ImmutableSet.of(),
                configuration,
                session,
                split.getPath(),
                OptionalInt.empty(),
                split.getStart(),
                split.getLength(),
                split.getLength(),
                splitProperties,
                TupleDomain.all(),
                getColumnHandles(testColumns),
                partitionKeys,
                DateTimeZone.getDefault(),
                TYPE_MANAGER,
                ImmutableMap.of(),
                Optional.empty());

        RecordCursor cursor = ((RecordPageSource) pageSource.get()).getCursor();

        checkCursor(cursor, testColumns, rowCount);
    }

    private void testPageSourceFactory(HivePageSourceFactory sourceFactory,
            FileSplit split,
            HiveStorageFormat storageFormat,
            List<TestColumn> testColumns,
            ConnectorSession session,
            int rowCount)
            throws IOException
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, storageFormat.getInputFormat());
        splitProperties.setProperty(SERIALIZATION_LIB, storageFormat.getSerDe());
        splitProperties.setProperty("columns", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getName)));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getType)));

        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), (String) input.getWriteValue()))
                .collect(toList());

        List<HiveColumnHandle> columnHandles = getColumnHandles(testColumns);

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(),
                ImmutableSet.of(sourceFactory),
                new Configuration(),
                session,
                split.getPath(),
                OptionalInt.empty(),
                split.getStart(),
                split.getLength(),
                split.getLength(),
                splitProperties,
                TupleDomain.all(),
                columnHandles,
                partitionKeys,
                DateTimeZone.getDefault(),
                TYPE_MANAGER,
                ImmutableMap.of(),
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

    private static HiveClientConfig createParquetHiveClientConfig(boolean enableOptimizedReader, boolean enablePredicatePushDown, boolean useParquetColumnNames)
    {
        HiveClientConfig config = new HiveClientConfig();
        config.setParquetOptimizedReaderEnabled(enableOptimizedReader)
                .setParquetPredicatePushdownEnabled(enablePredicatePushDown)
                .setUseParquetColumnNames(useParquetColumnNames);
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

        public FileFormatAssertion isReadableByPageSource(HivePageSourceFactory pageSourceFactory)
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

        public FileFormatAssertion isFailingForPageSource(HivePageSourceFactory pageSourceFactory, HiveErrorCode expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.of(pageSourceFactory), Optional.empty(), expectedErrorCode, expectedMessage);
            return this;
        }

        public FileFormatAssertion isFailingForRecordCursor(HiveRecordCursorProvider cursorProvider, HiveErrorCode expectedErrorCode, String expectedMessage)
                throws Exception
        {
            assertFailure(Optional.empty(), Optional.of(cursorProvider), expectedErrorCode, expectedMessage);
            return this;
        }

        private void assertRead(Optional<HivePageSourceFactory> pageSourceFactory, Optional<HiveRecordCursorProvider> cursorProvider)
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
                Optional<HivePageSourceFactory> pageSourceFactory,
                Optional<HiveRecordCursorProvider> cursorProvider,
                HiveErrorCode expectedErrorCode,
                String expectedMessage)
                throws Exception
        {
            try {
                assertRead(pageSourceFactory, cursorProvider);
                fail("failure is expected");
            }
            catch (PrestoException prestoException) {
                assertEquals(prestoException.getErrorCode(), expectedErrorCode.toErrorCode());
                assertEquals(prestoException.getMessage(), expectedMessage);
            }
        }
    }
}
