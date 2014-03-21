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

import com.facebook.presto.spi.RecordCursor;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.testng.annotations.Test;

import parquet.hive.serde.ParquetHiveSerDe;
import parquet.hive.DeprecatedParquetInputFormat;
import parquet.hive.DeprecatedParquetOutputFormat;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

public class TestHiveFileFormats
        extends AbstractTestHiveFileFormats
{
    @Test
    public void testRCText()
            throws Exception
    {
        JobConf jobConf = new JobConf();
        RCFileOutputFormat outputFormat = new RCFileOutputFormat();
        @SuppressWarnings("rawtypes")
        RCFileInputFormat inputFormat = new RCFileInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new ColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-text");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null);
            @SuppressWarnings("unchecked")
            RecordReader<?, BytesRefArrayWritable> recordReader = (RecordReader<?, BytesRefArrayWritable>) inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
            Properties splitProperties = new Properties();
            splitProperties.setProperty("serialization.lib", "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
            splitProperties.setProperty("columns", COLUMN_NAMES_STRING);
            splitProperties.setProperty("columns.types", COLUMN_TYPES);
            RecordCursor cursor = new ColumnarTextHiveRecordCursor<>(recordReader, split.getLength(), splitProperties, new ArrayList<HivePartitionKey>(), getColumns());

            checkCursor(cursor);
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
        JobConf jobConf = new JobConf();
        RCFileOutputFormat outputFormat = new RCFileOutputFormat();
        @SuppressWarnings("rawtypes")
        RCFileInputFormat inputFormat = new RCFileInputFormat();
        @SuppressWarnings("deprecation")
        SerDe serde = new LazyBinaryColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null);
            @SuppressWarnings("unchecked")
            RecordReader<?, BytesRefArrayWritable> recordReader = (RecordReader<?, BytesRefArrayWritable>) inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
            Properties splitProperties = new Properties();
            splitProperties.setProperty("serialization.lib", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe");
            splitProperties.setProperty("columns", COLUMN_NAMES_STRING);
            splitProperties.setProperty("columns.types", COLUMN_TYPES);
            RecordCursor cursor = new ColumnarBinaryHiveRecordCursor<>(recordReader, split.getLength(), splitProperties, new ArrayList<HivePartitionKey>(), getColumns());

            checkCursor(cursor);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test(enabled = false)
    // The test fails with a Parquet-Hive-SerDe bug:
    // https://github.com/Parquet/parquet-mr/issues/218
    // will enable when the bug get fixed
    public void testParquet()
            throws Exception
    {
        JobConf jobConf = new JobConf();
        @SuppressWarnings("rawtypes")
        DeprecatedParquetOutputFormat outputFormat = new DeprecatedParquetOutputFormat();
        @SuppressWarnings("deprecation")
        DeprecatedParquetInputFormat inputFormat = new DeprecatedParquetInputFormat();
        ParquetHiveSerDe serde = new ParquetHiveSerDe();
        File file = File.createTempFile("presto_test", "parquet");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null);
            @SuppressWarnings("unchecked")
            RecordReader<java.lang.Void, org.apache.hadoop.io.ArrayWritable> recordReader =
                (RecordReader<java.lang.Void, org.apache.hadoop.io.ArrayWritable>) inputFormat.getRecordReader(
                    split,
                    jobConf,
                    Reporter.NULL);
            Properties splitProperties = new Properties();
            splitProperties.setProperty("serialization.lib", "parquet.hive.serde.ParquetHiveSerDe");
            splitProperties.setProperty("columns", COLUMN_NAMES_STRING);
            splitProperties.setProperty("columns.types", COLUMN_TYPES);
            RecordCursor cursor = new ParquetHiveRecordCursor<>(recordReader, split.getLength(), splitProperties, new ArrayList<HivePartitionKey>(), getColumns());

            checkCursor(cursor);
        }
        finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }
}
