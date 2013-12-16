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
        final RCFileOutputFormat outputFormat = new RCFileOutputFormat();
        final RCFileInputFormat inputFormat = new RCFileInputFormat();
        final SerDe serde = new ColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-text");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null);
            RecordReader<?, BytesRefArrayWritable> recordReader = (RecordReader<?, BytesRefArrayWritable>) inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
            Properties splitProperties = new Properties();
            splitProperties.setProperty("serialization.lib", "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
            splitProperties.setProperty("columns", COLUMN_NAMES_STRING);
            splitProperties.setProperty("columns.types", COLUMN_TYPES);
            RecordCursor cursor = new ColumnarTextHiveRecordCursor<>(recordReader, split.getLength(), splitProperties, new ArrayList<HivePartitionKey>(), getColumns());

            checkCursor(cursor, true);
        }
        finally {
            file.delete();
        }
    }

    @Test
    public void testRCBinary()
            throws Exception
    {
        JobConf jobConf = new JobConf();
        final RCFileOutputFormat outputFormat = new RCFileOutputFormat();
        final RCFileInputFormat inputFormat = new RCFileInputFormat();
        final SerDe serde = new LazyBinaryColumnarSerDe();
        File file = File.createTempFile("presto_test", "rc-binary");
        try {
            FileSplit split = createTestFile(file.getAbsolutePath(), outputFormat, serde, null);
            RecordReader<?, BytesRefArrayWritable> recordReader = (RecordReader<?, BytesRefArrayWritable>) inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
            Properties splitProperties = new Properties();
            splitProperties.setProperty("serialization.lib", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe");
            splitProperties.setProperty("columns", COLUMN_NAMES_STRING);
            splitProperties.setProperty("columns.types", COLUMN_TYPES);
            RecordCursor cursor = new ColumnarBinaryHiveRecordCursor<>(recordReader, split.getLength(), splitProperties, new ArrayList<HivePartitionKey>(), getColumns());

            checkCursor(cursor, true);
        }
        finally {
            file.delete();
        }
    }
}
