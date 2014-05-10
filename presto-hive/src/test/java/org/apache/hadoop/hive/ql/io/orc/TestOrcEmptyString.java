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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.ql.io.orc.OrcFile.readerOptions;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestOrcEmptyString
{
    private static final int ROWS = 100;

    @Test
    public void testOrcString()
            throws Exception
    {
        testOrcEmptyString("test");
    }

    @Test(enabled = false)
    public void testOrcEmptyString()
            throws Exception
    {
        testOrcEmptyString("");
    }

    public static void testOrcEmptyString(String expected)
            throws Exception
    {
        // create a data file
        File file = writeFile(expected);

        // create a reader
        Reader reader = OrcFile.createReader(new Path(file.toURI()), readerOptions(new Configuration()));

        boolean[] include = new boolean[2];
        Arrays.fill(include, true);

        // verify with non-vectorized code
        RecordReader recordReader = reader.rows(0, file.length(), include);
        readNonVectorized(recordReader, expected);
        recordReader.close();

        // verify with vectorized code
        recordReader = reader.rows(0, file.length(), include);
        readVectorized(recordReader, expected);
        recordReader.close();
    }

    private static void readNonVectorized(RecordReader recordReader, String expected)
            throws Exception
    {
        int rowCount = 0;
        OrcStruct struct = null;
        while (recordReader.hasNext()) {
            struct = (OrcStruct) recordReader.next(struct);
            rowCount++;

            Text text = (Text) struct.getFieldValue(0);
            assertNotNull(text);
            assertEquals(text.getLength(), expected.length());

            assertEquals(new String(text.getBytes(), 0, text.getLength(), UTF_8), expected);
        }
        assertEquals(rowCount, 100);
    }

    private static void readVectorized(RecordReader recordReader, String expected)
            throws Exception
    {
        int rowCount = 0;
        VectorizedRowBatch batch = null;
        while (recordReader.hasNext()) {
            batch = recordReader.nextBatch(batch);
            BytesColumnVector columnVector = (BytesColumnVector) batch.cols[0];

            assertTrue(columnVector.noNulls);

            byte[][] vector = columnVector.vector;
            int[] start = columnVector.start;
            int[] length = columnVector.length;
            boolean[] isNull = columnVector.isNull;

            int size = columnVector.isRepeating ? 1 : batch.size;
            for (int i = 0; i < size; i++) {
                assertFalse(isNull[i]);
                assertEquals(length[i], expected.length());
                assertNotNull(vector[i]);
                assertEquals(new String(vector[i], start[i], length[i], UTF_8), expected);
            }

            rowCount += batch.size;
        }
        assertEquals(rowCount, 100);
    }

    private static File writeFile(String expected)
            throws IOException
    {
        File file = File.createTempFile("presto_test", "rc-text");
        file.delete();

        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", "col");
        tableProperties.setProperty("columns.types", "string");

        RecordWriter recordWriter = new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(file.toURI()),
                Text.class,
                false,
                tableProperties,
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );

        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                Arrays.asList("col"),
                Arrays.<ObjectInspector>asList(javaStringObjectInspector));

        Object row = objectInspector.create();

        List<? extends StructField> fields = objectInspector.getAllStructFieldRefs();

        for (int i = 0; i < ROWS; i++) {
            objectInspector.setStructFieldData(row, fields.get(0), expected);

            Writable record = new OrcSerde().serialize(row, objectInspector);
            recordWriter.write(record);
        }
        recordWriter.close(false);
        return file;
    }
}
