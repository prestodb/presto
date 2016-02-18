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

import com.facebook.presto.hadoop.shaded.com.google.common.collect.Lists;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

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
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
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
            return !testColumn.getName().equals("t_struct_null") // TODO: This is a bug in the RC text reader
                    && !testColumn.getName().equals("t_map_null_key_complex_key_value"); // RC file does not support complex type as key of a map
        }));

        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new ColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-text");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            testCursorProvider(new ColumnarTextHiveRecordCursorProvider(), split, inputFormat, serde, testColumns, NUM_ROWS);
            testCursorProvider(new GenericHiveRecordCursorProvider(), split, inputFormat, serde, testColumns, NUM_ROWS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test(enabled = false)
    public void testRcTextPageSource()
            throws Exception
    {
        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new ColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS, NUM_ROWS);
            testPageSourceFactory(new RcFilePageSourceFactory(TYPE_MANAGER), split, inputFormat, serde, TEST_COLUMNS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testRCBinary()
            throws Exception
    {
        List<TestColumn> testColumns = ImmutableList.copyOf(filter(TEST_COLUMNS, testColumn -> {
            // RC file does not support complex type as key of a map
            return !testColumn.getName().equals("t_map_null_key_complex_key_value");
        }));

        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new LazyBinaryColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            testCursorProvider(new ColumnarBinaryHiveRecordCursorProvider(), split, inputFormat, serde, testColumns, NUM_ROWS);
            testCursorProvider(new GenericHiveRecordCursorProvider(), split, inputFormat, serde, testColumns, NUM_ROWS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test(enabled = false)
    public void testRcBinaryPageSource()
            throws Exception
    {
        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new LazyBinaryColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS, NUM_ROWS);
            testPageSourceFactory(new RcFilePageSourceFactory(TYPE_MANAGER), split, inputFormat, serde, TEST_COLUMNS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testOrc()
            throws Exception
    {
        HiveOutputFormat<?, ?> outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        InputFormat<?, ?> inputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new org.apache.hadoop.hive.ql.io.orc.OrcSerde();
        File file = File.createTempFile("presto_test", "orc");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS, NUM_ROWS);
            testPageSourceFactory(new OrcPageSourceFactory(TYPE_MANAGER, false), split, inputFormat, serde, TEST_COLUMNS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testOrcUseColumnNames()
            throws Exception
    {
        HiveOutputFormat<?, ?> outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        InputFormat<?, ?> inputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new org.apache.hadoop.hive.ql.io.orc.OrcSerde();
        File file = File.createTempFile("presto_test", "orc");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS, NUM_ROWS);
            // Reverse the order of the columns to test access by name, not by index
            List<TestColumn> reversedColumns = Lists.reverse(TEST_COLUMNS);
            TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());
            testPageSourceFactory(new OrcPageSourceFactory(TYPE_MANAGER, true), split, inputFormat, serde, reversedColumns, session);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testParquet()
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();

        HiveOutputFormat<?, ?> outputFormat = new MapredParquetOutputFormat();
        InputFormat<?, ?> inputFormat = new MapredParquetInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ParquetHiveSerDe();
        File file = File.createTempFile("presto_test", "parquet");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider(false);
            testCursorProvider(cursorProvider, split, inputFormat, serde, testColumns, NUM_ROWS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test(enabled = false)
    public void testParquetPageSource()
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();
        testColumns = testColumns.stream()
                .filter(column -> column.getObjectInspector().getCategory() == Category.PRIMITIVE)
                .collect(toList());

        HiveOutputFormat<?, ?> outputFormat = new MapredParquetOutputFormat();
        InputFormat<?, ?> inputFormat = new MapredParquetInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ParquetHiveSerDe();
        File file = File.createTempFile("presto_test", "parquet");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            TestingConnectorSession session = new TestingConnectorSession(
                    new HiveSessionProperties(new HiveClientConfig().setParquetOptimizedReaderEnabled(true)).getSessionProperties());
            testPageSourceFactory(new ParquetPageSourceFactory(TYPE_MANAGER, false), split, inputFormat, serde, testColumns, session);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testParquetUseColumnNames()
            throws Exception
    {
        List<TestColumn> testColumns = getTestColumnsSupportedByParquet();

        HiveOutputFormat<?, ?> outputFormat = new MapredParquetOutputFormat();
        InputFormat<?, ?> inputFormat = new MapredParquetInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ParquetHiveSerDe();
        File file = File.createTempFile("presto_test", "parquet");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            // Reverse the order of the columns to test access by name, not by index
            Collections.reverse(testColumns);
            HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider(true);
            testCursorProvider(cursorProvider, split, inputFormat, serde, testColumns, NUM_ROWS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
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

        List<TestColumn> testColumns = ImmutableList.<TestColumn>of(
            new TestColumn(
                "persons",
                getStandardListObjectInspector(
                    getStandardStructObjectInspector(
                        ImmutableList.of("name", "id", "email", "phones"),
                        ImmutableList.<ObjectInspector>of(
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
            return !hasType(objectInspector, PrimitiveCategory.DATE);
        }));

        HiveOutputFormat<?, ?> outputFormat = new com.facebook.hive.orc.OrcOutputFormat();
        InputFormat<?, ?> inputFormat = new com.facebook.hive.orc.OrcInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new com.facebook.hive.orc.OrcSerde();
        File file = File.createTempFile("presto_test", "dwrf");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns, NUM_ROWS);
            testPageSourceFactory(new DwrfPageSourceFactory(TYPE_MANAGER), split, inputFormat, serde, testColumns);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
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
            List<TestColumn> testColumns)
            throws IOException
    {
        testPageSourceFactory(sourceFactory, split, inputFormat, serde, testColumns, SESSION);
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
}
