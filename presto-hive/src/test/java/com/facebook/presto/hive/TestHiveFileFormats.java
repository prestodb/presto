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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.nameGetter;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.partitionKeyFilter;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.typeGetter;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    private static final TimeZoneKey TIME_ZONE_KEY = TimeZoneKey.getTimeZoneKey(DateTimeZone.getDefault().getID());
    private static final ConnectorSession SESSION = new ConnectorSession("user", "test", "catalog", "test", TIME_ZONE_KEY, Locale.ENGLISH, null, null);

    @BeforeMethod(alwaysRun = true)
    public void setup()
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
        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new ColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-text");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS);
            testCursorProvider(new ColumnarTextHiveRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
            testCursorProvider(new GenericHiveRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
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
        HiveOutputFormat<?, ?> outputFormat = new RCFileOutputFormat();
        InputFormat<?, ?> inputFormat = new RCFileInputFormat<>();
        @SuppressWarnings("deprecation")
        SerDe serde = new LazyBinaryColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS);
            testCursorProvider(new ColumnarBinaryHiveRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
            testCursorProvider(new GenericHiveRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void testDwrf()
            throws Exception
    {
        HiveOutputFormat<?, ?> outputFormat = new com.facebook.hive.orc.OrcOutputFormat();
        InputFormat<?, ?> inputFormat = new com.facebook.hive.orc.OrcInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new com.facebook.hive.orc.OrcSerde();
        File file = File.createTempFile("presto_test", "dwrf");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS);
            testCursorProvider(new DwrfRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
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
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, TEST_COLUMNS);
            testCursorProvider(new OrcRecordCursorProvider(), split, inputFormat, serde, TEST_COLUMNS);
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
        List<TestColumn> testColumns = ImmutableList.copyOf(filter(TEST_COLUMNS, new Predicate<TestColumn>()
        {
            @Override
            public boolean apply(TestColumn testColumn)
            {
                if (testColumn.getObjectInspector().getCategory() != Category.PRIMITIVE) {
                    return false;
                }
                PrimitiveObjectInspector objectInspector = (PrimitiveObjectInspector) testColumn.getObjectInspector();
                return objectInspector.getPrimitiveCategory() != PrimitiveCategory.TIMESTAMP &&
                        objectInspector.getPrimitiveCategory() != PrimitiveCategory.BINARY;
            }
        }));

        HiveOutputFormat<?, ?> outputFormat = new MapredParquetOutputFormat();
        InputFormat<?, ?> inputFormat = new MapredParquetInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ParquetHiveSerDe();
        File file = File.createTempFile("presto_test", "ord");
        file.delete();
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null, testColumns);
            HiveRecordCursorProvider cursorProvider = new ParquetRecordCursorProvider();
            testCursorProvider(cursorProvider, split, inputFormat, serde, testColumns);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    private void testCursorProvider(HiveRecordCursorProvider cursorProvider, FileSplit split, InputFormat<?, ?> inputFormat, @SuppressWarnings("deprecation") SerDe serde, List<TestColumn> testColumns)
            throws IOException
    {
        Properties splitProperties = new Properties();
        splitProperties.setProperty(FILE_INPUT_FORMAT, inputFormat.getClass().getName());
        splitProperties.setProperty(SERIALIZATION_LIB, serde.getClass().getName());
        splitProperties.setProperty("columns", Joiner.on(',').join(transform(filter(testColumns, not(partitionKeyFilter())), nameGetter())));
        splitProperties.setProperty("columns.types", Joiner.on(',').join(transform(filter(testColumns, not(partitionKeyFilter())), typeGetter())));

        List<HivePartitionKey> partitionKeys = ImmutableList.copyOf(transform(filter(testColumns, partitionKeyFilter()), new Function<TestColumn, HivePartitionKey>() {
            @Override
            public HivePartitionKey apply(TestColumn input)
            {
                return new HivePartitionKey(input.getName(), HiveType.getHiveType(input.getObjectInspector()), (String) input.getWriteValue());
            }
        }));

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
                DateTimeZone.getDefault()).get();

        checkCursor(cursor, testColumns);
    }
}
