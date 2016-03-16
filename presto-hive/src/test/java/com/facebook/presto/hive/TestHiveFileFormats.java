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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.rowBlockOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    @BeforeClass(alwaysRun = true)
    public void setUp()
            throws Exception
    {
        // ensure the expected timezone is configured for this VM
        assertEquals(TimeZone.getDefault().getID(),
                "Asia/Katmandu",
                "Timezone not configured correctly. Add -Duser.timezone=Asia/Katmandu to your JVM arguments");
    }

    @Test
    public void testRCText()
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
                .isReadableByRecordCursor(new ColumnarTextHiveRecordCursorProvider())
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider());
    }

    @Test(enabled = false)
    public void testRcTextPageSource()
            throws Exception
    {
        assertThatFileFormat(RCTEXT)
                .withColumns(TEST_COLUMNS)
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER));
    }

    @Test
    public void testRCBinary()
            throws Exception
    {
        // RC file does not support complex type as key of a map and interprets empty VARCHAR as nulls
        List<TestColumn> testColumns = TEST_COLUMNS.stream()
                .filter(testColumn -> {
                    String name = testColumn.getName();
                    return !name.equals("t_map_null_key_complex_key_value") && !name.equals("t_empty_varchar");
                }).collect(toList());

        assertThatFileFormat(RCBINARY)
                .withColumns(testColumns)
                .isReadableByRecordCursor(new ColumnarBinaryHiveRecordCursorProvider())
                .isReadableByRecordCursor(new GenericHiveRecordCursorProvider());
    }

    @Test(enabled = false)
    public void testRcBinaryPageSource()
            throws Exception
    {
        assertThatFileFormat(RCBINARY)
                .withColumns(TEST_COLUMNS)
                .isReadableByPageSource(new RcFilePageSourceFactory(TYPE_MANAGER));
    }

    @Test
    public void testOrc()
            throws Exception
    {
        assertThatFileFormat(ORC)
                .withColumns(TEST_COLUMNS)
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, false));
    }

    @Test
    public void testOrcUseColumnNames()
            throws Exception
    {
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());

        assertThatFileFormat(ORC)
                .withWriteColumns(TEST_COLUMNS)
                .withReadColumns(Lists.reverse(TEST_COLUMNS))
                .withSession(session)
                .isReadableByPageSource(new OrcPageSourceFactory(TYPE_MANAGER, true));
    }

    @Test
    public void testParquet()
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();

        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(false));
    }

    @Test
    public void testParquetCaseInsensitiveColumnLookup()
            throws Exception
    {
        List<TestColumn> writeColumns = ImmutableList.of(new TestColumn("column_name", javaStringObjectInspector, "test", utf8Slice("test"), false));
        List<TestColumn> readColumns = ImmutableList.of(new TestColumn("Column_Name", javaStringObjectInspector, "test", utf8Slice("test"), false));

        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(true));
    }

    @Test
    public void testParquetPageSource()
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        testColumns = testColumns.stream()
                .filter(column -> column.getObjectInspector().getCategory() == Category.PRIMITIVE)
                .collect(toList());
        TestingConnectorSession session = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setParquetOptimizedReaderEnabled(true)).getSessionProperties());

        assertThatFileFormat(PARQUET)
                .withColumns(testColumns)
                .withSession(session)
                .isReadableByPageSource(new ParquetPageSourceFactory(TYPE_MANAGER, false));
    }

    @Test
    public void testParquetUseColumnNames()
            throws Exception
    {
        List<TestColumn> writeColumns = getTestColumnsSupportedByParquet();
        List<TestColumn> readColumns = Lists.reverse(writeColumns);

        assertThatFileFormat(PARQUET)
                .withWriteColumns(writeColumns)
                .withReadColumns(readColumns)
                .isReadableByRecordCursor(new ParquetRecordCursorProvider(true));
    }

    private static List<TestColumn> getTestColumnsSupportedByParquet()
    {
        // Write of complex hive data to Parquet is broken
        // TODO: empty arrays or maps with null keys don't seem to work
        // Parquet does not support DATE
        return TEST_COLUMNS.stream()
                .filter(column -> !ImmutableSet.of("t_array_empty", "t_map_null_key", "t_map_null_key_complex_value", "t_map_null_key_complex_key_value")
                        .contains(column.getName()))
                .filter(column -> column.isPartitionKey() || !hasType(column.getObjectInspector(), PrimitiveCategory.DATE))
                .collect(toList());
    }

    @Test
    public void testParquetThrift()
            throws Exception
    {
        RowType nameType = new RowType(ImmutableList.of(VARCHAR, VARCHAR), Optional.empty());
        RowType phoneType = new RowType(ImmutableList.of(VARCHAR, VARCHAR), Optional.empty());
        RowType personType = new RowType(ImmutableList.of(nameType, BIGINT, VARCHAR, new ArrayType(phoneType)), Optional.empty());

        List<TestColumn> testColumns = ImmutableList.of(
                new TestColumn(
                        "persons",
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        ImmutableList.of("name", "id", "email", "phones"),
                                        ImmutableList.of(
                                                getStandardStructObjectInspector(
                                                        ImmutableList.of("first_name", "last_name"),
                                                        ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)
                                                ),
                                                javaIntObjectInspector,
                                                javaStringObjectInspector,
                                                getStandardListObjectInspector(
                                                        getStandardStructObjectInspector(
                                                                ImmutableList.of("number", "type"),
                                                                ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)
                                                        )
                                                )
                                        )
                                )
                        ),
                        null,
                        arrayBlockOf(personType,
                                rowBlockOf(ImmutableList.of(nameType, BIGINT, VARCHAR, new ArrayType(phoneType)),
                                        rowBlockOf(ImmutableList.of(VARCHAR, VARCHAR), "Bob", "Roberts"),
                                        0,
                                        "bob.roberts@example.com",
                                        arrayBlockOf(phoneType, rowBlockOf(ImmutableList.of(VARCHAR, VARCHAR), "1234567890", null))
                                )
                        )
                )
        );

        InputFormat<?, ?> inputFormat = new MapredParquetInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ParquetHiveSerDe();
        File file = new File(this.getClass().getClassLoader().getResource("addressbook.parquet").getPath());
        FileSplit split = new FileSplit(new Path(file.getAbsolutePath()), 0, file.length(), new String[0]);
        HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider(false);
        testCursorProvider(cursorProvider, split, inputFormat, serde, testColumns, 1);
    }

    @Test
    public void testDwrf()
            throws Exception
    {
        List<TestColumn> testColumns = ImmutableList.copyOf(filter(TEST_COLUMNS, testColumn -> {
            ObjectInspector objectInspector = testColumn.getObjectInspector();
            return !hasType(objectInspector, PrimitiveCategory.DATE) && !hasType(objectInspector, PrimitiveCategory.DECIMAL)
                    && !hasType(objectInspector, PrimitiveCategory.VARCHAR);
        }));

        assertThatFileFormat(DWRF)
                .withColumns(testColumns)
                .isReadableByPageSource(new DwrfPageSourceFactory(TYPE_MANAGER));
    }

    private void testCursorProvider(HiveRecordCursorProvider cursorProvider,
            FileSplit split,
            InputFormat<?, ?> inputFormat,
            @SuppressWarnings("deprecation") SerDe serde,
            List<TestColumn> testColumns,
            int numRows)
            throws IOException
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, inputFormat.getClass().getName());
        splitProperties.setProperty(SERIALIZATION_LIB, serde.getClass().getName());
        splitProperties.setProperty("columns", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getName)));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getType)));

        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), HiveType.valueOf(input.getObjectInspector().getTypeName()), (String) input.getWriteValue()))
                .collect(toList());

        HiveRecordCursor cursor = cursorProvider.createHiveRecordCursor(
                "test",
                new Configuration(),
                SESSION,
                split.getPath(),
                split.getStart(),
                split.getLength(),
                splitProperties,
                getColumnHandles(testColumns),
                partitionKeys,
                TupleDomain.<HiveColumnHandle>all(),
                DateTimeZone.getDefault(),
                TYPE_MANAGER).get();

        checkCursor(cursor, testColumns, numRows);
    }

    private void testPageSourceFactory(HivePageSourceFactory sourceFactory,
            FileSplit split,
            InputFormat<?, ?> inputFormat,
            SerDe serde,
            List<TestColumn> testColumns,
            ConnectorSession session)
            throws IOException
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, inputFormat.getClass().getName());
        splitProperties.setProperty(SERIALIZATION_LIB, serde.getClass().getName());
        splitProperties.setProperty("columns", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getName)));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(transform(filter(testColumns, not(TestColumn::isPartitionKey)), TestColumn::getType)));

        List<HivePartitionKey> partitionKeys = testColumns.stream()
                .filter(TestColumn::isPartitionKey)
                .map(input -> new HivePartitionKey(input.getName(), HiveType.valueOf(input.getObjectInspector().getTypeName()), (String) input.getWriteValue()))
                .collect(toList());

        List<HiveColumnHandle> columnHandles = getColumnHandles(testColumns);

        Optional<? extends ConnectorPageSource> pageSource = sourceFactory.createPageSource(
                new Configuration(),
                session,
                split.getPath(),
                split.getStart(),
                split.getLength(),
                splitProperties,
                columnHandles,
                partitionKeys,
                TupleDomain.<HiveColumnHandle>all(),
                DateTimeZone.getDefault());

        assertTrue(pageSource.isPresent());

        checkPageSource(pageSource.get(), testColumns, getTypes(columnHandles));
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

    private FileFormatAssertion assertThatFileFormat(HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        return new FileFormatAssertion(hiveStorageFormat.name())
                .withInputFormat((InputFormat<?, ?>) Class.forName(hiveStorageFormat.getInputFormat()).newInstance())
                .withOutputFormat((HiveOutputFormat<?, ?>) Class.forName(hiveStorageFormat.getOutputFormat()).newInstance())
                .withSerde((SerDe) Class.forName(hiveStorageFormat.getSerDe()).newInstance());
    }

    private class FileFormatAssertion
    {
        private final String formatName;
        private HiveOutputFormat<?, ?> outputFormat;
        private InputFormat<?, ?> inputFormat;
        private SerDe serde;
        private Optional<String> compressionCodec = Optional.empty();
        private List<TestColumn> writeColumns;
        private List<TestColumn> readColumns;
        private ConnectorSession session = SESSION;
        private int rowsCount = NUM_ROWS;

        private FileFormatAssertion(String formatName)
        {
            this.formatName = requireNonNull(formatName, "formatName is null");
        }

        public FileFormatAssertion withOutputFormat(HiveOutputFormat<?, ?> outputFormat)
        {
            this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
            return this;
        }

        public FileFormatAssertion withInputFormat(InputFormat<?, ?> inputFormat)
        {
            this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
            return this;
        }

        public FileFormatAssertion withSerde(SerDe serde)
        {
            this.serde = requireNonNull(serde, "serde is null");
            return this;
        }

        public FileFormatAssertion withCompressionCodec(String compressionCodec)
        {
            this.compressionCodec = Optional.of(requireNonNull(compressionCodec, "compressionCodec is null"));
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
            testReadValues(ImmutableList.of(pageSourceFactory), ImmutableList.of());
            return this;
        }

        public FileFormatAssertion isReadableByRecordCursor(HiveRecordCursorProvider cursorProvider)
                throws Exception
        {
            testReadValues(ImmutableList.of(), ImmutableList.of(cursorProvider));
            return this;
        }

        private void testReadValues(List<HivePageSourceFactory> pageSourceFactories, List<HiveRecordCursorProvider> cursorProviders)
                throws Exception
        {
            assertNotNull(outputFormat, "outputFormat must be specified");
            assertNotNull(inputFormat, "inputFormat must be specified");
            assertNotNull(serde, "serde must be specified");
            assertNotNull(writeColumns, "writeColumns must be specified");
            assertNotNull(readColumns, "readColumns must be specified");
            assertNotNull(session, "session must be specified");
            assertTrue(rowsCount > 0, "rowsCount must be greater than zero");

            File file = File.createTempFile("presto_test", formatName);
            file.delete();
            try {
                FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, compressionCodec.orElse(null), writeColumns, rowsCount);
                for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
                    testPageSourceFactory(pageSourceFactory, split, inputFormat, serde, readColumns, session);
                }
                for (HiveRecordCursorProvider cursorProvider : cursorProviders) {
                    testCursorProvider(cursorProvider, split, inputFormat, serde, readColumns, rowsCount);
                }
            }
            finally {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
        }
    }
}
