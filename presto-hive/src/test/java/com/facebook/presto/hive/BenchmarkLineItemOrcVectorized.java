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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public final class BenchmarkLineItemOrcVectorized
        implements BenchmarkLineItemWithPredicatePushdown
{
    private static final String[] COLUMN_NAMES = new String[] {
            "bogus",
            "orderkey",
            "partkey",
            "suppkey",
            "linenumber",
            "quantity",
            "extendedprice",
            "discount",
            "tax",
            "returnflag",
            "linestatus",
            "shipdate",
            "commitdate",
            "receiptdate",
            "shipinstruct",
            "shipmode",
            "comment"
    };

    @Override
    public String getName()
    {
        return "vector";
    }

    @Override
    public <K, V extends Writable> long orderKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("orderkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                LongColumnVector columnVector = (LongColumnVector) batch.cols[fieldIndex];

                long[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        bigintSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    @Override
    public <K, V extends Writable> long partKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("partkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                LongColumnVector columnVector = (LongColumnVector) batch.cols[fieldIndex];

                long[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        bigintSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    @Override
    public <K, V extends Writable> long supplierKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("suppkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                LongColumnVector columnVector = (LongColumnVector) batch.cols[fieldIndex];

                long[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        bigintSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    @Override
    public <K, V extends Writable> long lineNumber(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("linenumber");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                LongColumnVector columnVector = (LongColumnVector) batch.cols[fieldIndex];

                long[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        bigintSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    @Override
    public <K, V extends Writable> long quantity(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("quantity");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                LongColumnVector columnVector = (LongColumnVector) batch.cols[fieldIndex];

                long[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        bigintSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return bigintSum;
    }

    @Override
    public <K, V extends Writable> double extendedPrice(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("extendedprice");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[fieldIndex];

                double[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        doubleSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    @Override
    public <K, V extends Writable> double discount(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("discount");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[fieldIndex];

                double[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        doubleSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    @Override
    public <K, V extends Writable> double tax(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("tax");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                DoubleColumnVector columnVector = (DoubleColumnVector) batch.cols[fieldIndex];

                double[] vector = columnVector.vector;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        doubleSum += vector[index];
                    }
                }
            }
            recordReader.close();
        }
        return doubleSum;
    }

    @Override
    public <K, V extends Writable> long returnFlag(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("returnflag");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long status(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("linestatus");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long shipDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long commitDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("commitdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long receiptDate(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("receiptdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long shipInstructions(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipinstruct");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long shipMode(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipmode");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> long comment(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);
                BytesColumnVector columnVector = (BytesColumnVector) batch.cols[fieldIndex];

                byte[][] vector = columnVector.vector;
                int[] start = columnVector.start;
                int[] length = columnVector.length;
                boolean[] isNull = columnVector.isNull;
                for (int i = 0; i < batch.size; i++) {
                    int index = columnVector.isRepeating ? 0 : i;
                    if (!isNull[index]) {
                        byte[] commentValue = Arrays.copyOfRange(vector[index], start[index], start[index] + length[index]);
                        stringLengthSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return stringLengthSum;
    }

    @Override
    public <K, V extends Writable> List<Object> tpchQuery1(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);

        StructField taxField = rowInspector.getStructFieldRef("tax");
        int taxFieldIndex = allStructFieldRefs.indexOf(taxField);

        StructField returnFlagField = rowInspector.getStructFieldRef("returnflag");
        int returnFlagFieldIndex = allStructFieldRefs.indexOf(returnFlagField);

        StructField lineStatusField = rowInspector.getStructFieldRef("linestatus");
        int lineStatusFieldIndex = allStructFieldRefs.indexOf(lineStatusField);

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[quantityFieldIndex + 1] = true;
        include[extendedPriceFieldIndex + 1] = true;
        include[discountFieldIndex + 1] = true;
        include[taxFieldIndex + 1] = true;
        include[returnFlagFieldIndex + 1] = true;
        include[lineStatusFieldIndex + 1] = true;
        include[shipDateFieldIndex + 1] = true;

        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        double taxSum = 0;
        long returnFlagSum = 0;
        long lineStatusSum = 0;
        long shipDateSum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            taxSum = 0;
            returnFlagSum = 0;
            lineStatusSum = 0;
            shipDateSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);

                LongColumnVector quantityColumnVector = (LongColumnVector) batch.cols[quantityFieldIndex];
                long[] quantityVector = quantityColumnVector.vector;
                boolean[] quantityIsNull = quantityColumnVector.isNull;

                DoubleColumnVector extendedPriceColumnVector = (DoubleColumnVector) batch.cols[extendedPriceFieldIndex];
                double[] extendedPriceVector = extendedPriceColumnVector.vector;
                boolean[] extendedPriceIsNull = extendedPriceColumnVector.isNull;

                DoubleColumnVector discountColumnVector = (DoubleColumnVector) batch.cols[discountFieldIndex];
                double[] discountVector = discountColumnVector.vector;
                boolean[] discountIsNull = discountColumnVector.isNull;

                DoubleColumnVector taxColumnVector = (DoubleColumnVector) batch.cols[taxFieldIndex];
                double[] taxVector = taxColumnVector.vector;
                boolean[] taxIsNull = taxColumnVector.isNull;

                BytesColumnVector returnFlagColumnVector = (BytesColumnVector) batch.cols[returnFlagFieldIndex];
                byte[][] returnFlagVector = returnFlagColumnVector.vector;
                int[] returnFlagStartVector = returnFlagColumnVector.start;
                int[] returnFlagLengthVector = returnFlagColumnVector.length;
                boolean[] returnFlagIsNull = returnFlagColumnVector.isNull;

                BytesColumnVector lineStatusColumnVector = (BytesColumnVector) batch.cols[lineStatusFieldIndex];
                byte[][] lineStatusVector = lineStatusColumnVector.vector;
                int[] lineStatusStartVector = lineStatusColumnVector.start;
                int[] lineStatusLengthVector = lineStatusColumnVector.length;
                boolean[] lineStatusIsNull = lineStatusColumnVector.isNull;

                BytesColumnVector shipDateColumnVector = (BytesColumnVector) batch.cols[shipDateFieldIndex];
                byte[][] shipDateVector = shipDateColumnVector.vector;
                int[] shipDateStartVector = shipDateColumnVector.start;
                int[] shipDateLengthVector = shipDateColumnVector.length;
                boolean[] shipDateIsNull = shipDateColumnVector.isNull;

                for (int i = 0; i < batch.size; i++) {
                    int quantityIndex = quantityColumnVector.isRepeating ? 0 : i;
                    if (!quantityIsNull[quantityIndex]) {
                        quantitySum += quantityVector[quantityIndex];
                    }

                    int extendedPriceIndex = extendedPriceColumnVector.isRepeating ? 0 : i;
                    if (!extendedPriceIsNull[extendedPriceIndex]) {
                        extendedPriceSum += extendedPriceVector[extendedPriceIndex];
                    }

                    int discountIndex = discountColumnVector.isRepeating ? 0 : i;
                    if (!discountIsNull[discountIndex]) {
                        discountSum += discountVector[discountIndex];
                    }

                    int taxIndex = taxColumnVector.isRepeating ? 0 : i;
                    if (!taxIsNull[taxIndex]) {
                        taxSum += taxVector[taxIndex];
                    }

                    int returnFlagIndex = returnFlagColumnVector.isRepeating ? 0 : i;
                    if (!returnFlagIsNull[returnFlagIndex]) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagVector[returnFlagIndex],
                                returnFlagStartVector[returnFlagIndex],
                                returnFlagStartVector[returnFlagIndex] + returnFlagLengthVector[returnFlagIndex]);
                        returnFlagSum += returnFlagValue.length;
                    }

                    int lineStatusIndex = lineStatusColumnVector.isRepeating ? 0 : i;
                    if (!lineStatusIsNull[lineStatusIndex]) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusVector[lineStatusIndex],
                                lineStatusStartVector[lineStatusIndex],
                                lineStatusStartVector[lineStatusIndex] + lineStatusLengthVector[lineStatusIndex]);
                        lineStatusSum += lineStatusValue.length;
                    }

                    int shipDateIndex = shipDateColumnVector.isRepeating ? 0 : i;
                    if (!shipDateIsNull[shipDateIndex]) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex] + shipDateLengthVector[shipDateIndex]);
                        shipDateSum += shipDateValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(quantitySum, extendedPriceSum, discountSum, taxSum, returnFlagSum, lineStatusSum, shipDateSum);
    }

    @Override
    public <K, V extends Writable> List<Object> tpchQuery6(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[quantityFieldIndex + 1] = true;
        include[extendedPriceFieldIndex + 1] = true;
        include[discountFieldIndex + 1] = true;
        include[shipDateFieldIndex + 1] = true;

        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        long shipDateSum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);

                LongColumnVector quantityColumnVector = (LongColumnVector) batch.cols[quantityFieldIndex];
                long[] quantityVector = quantityColumnVector.vector;
                boolean[] quantityIsNull = quantityColumnVector.isNull;

                DoubleColumnVector extendedPriceColumnVector = (DoubleColumnVector) batch.cols[extendedPriceFieldIndex];
                double[] extendedPriceVector = extendedPriceColumnVector.vector;
                boolean[] extendedPriceIsNull = extendedPriceColumnVector.isNull;

                DoubleColumnVector discountColumnVector = (DoubleColumnVector) batch.cols[discountFieldIndex];
                double[] discountVector = discountColumnVector.vector;
                boolean[] discountIsNull = discountColumnVector.isNull;

                BytesColumnVector shipDateColumnVector = (BytesColumnVector) batch.cols[shipDateFieldIndex];
                byte[][] shipDateVector = shipDateColumnVector.vector;
                int[] shipDateStartVector = shipDateColumnVector.start;
                int[] shipDateLengthVector = shipDateColumnVector.length;
                boolean[] shipDateIsNull = shipDateColumnVector.isNull;

                for (int i = 0; i < batch.size; i++) {
                    int quantityIndex = quantityColumnVector.isRepeating ? 0 : i;
                    if (!quantityIsNull[quantityIndex]) {
                        quantitySum += quantityVector[quantityIndex];
                    }

                    int extendedPriceIndex = extendedPriceColumnVector.isRepeating ? 0 : i;
                    if (!extendedPriceIsNull[extendedPriceIndex]) {
                        extendedPriceSum += extendedPriceVector[extendedPriceIndex];
                    }

                    int discountIndex = discountColumnVector.isRepeating ? 0 : i;
                    if (!discountIsNull[discountIndex]) {
                        discountSum += discountVector[discountIndex];
                    }

                    int shipDateIndex = shipDateColumnVector.isRepeating ? 0 : i;
                    if (!shipDateIsNull[shipDateIndex]) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex] + shipDateLengthVector[shipDateIndex]);
                        shipDateSum += shipDateValue.length;
                    }
                }
            }
            recordReader.close();
        }
        return ImmutableList.<Object>of(quantitySum, extendedPriceSum, discountSum, shipDateSum);
    }

    @Override
    public <K, V extends Writable> List<Object> all(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        return all(jobConf, fileSplit, inputFormat, deserializer, null);
    }

    @Override
    public <K, V extends Writable> List<Object> allReadOne(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField orderKeyField = rowInspector.getStructFieldRef("orderkey");
        int orderKeyFieldIndex = allStructFieldRefs.indexOf(orderKeyField);

        StructField partKeyField = rowInspector.getStructFieldRef("partkey");
        int partKeyFieldIndex = allStructFieldRefs.indexOf(partKeyField);

        StructField supplierKeyField = rowInspector.getStructFieldRef("suppkey");
        int supplierKeyFieldIndex = allStructFieldRefs.indexOf(supplierKeyField);

        StructField lineNumberField = rowInspector.getStructFieldRef("linenumber");
        int lineNumberFieldIndex = allStructFieldRefs.indexOf(lineNumberField);

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);

        StructField taxField = rowInspector.getStructFieldRef("tax");
        int taxFieldIndex = allStructFieldRefs.indexOf(taxField);

        StructField returnFlagField = rowInspector.getStructFieldRef("returnflag");
        int returnFlagFieldIndex = allStructFieldRefs.indexOf(returnFlagField);

        StructField lineStatusField = rowInspector.getStructFieldRef("linestatus");
        int lineStatusFieldIndex = allStructFieldRefs.indexOf(lineStatusField);

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);

        StructField commitDateField = rowInspector.getStructFieldRef("commitdate");
        int commitDateFieldIndex = allStructFieldRefs.indexOf(commitDateField);

        StructField receiptDateField = rowInspector.getStructFieldRef("receiptdate");
        int receiptDateFieldIndex = allStructFieldRefs.indexOf(receiptDateField);

        StructField shipInstructionsField = rowInspector.getStructFieldRef("shipinstruct");
        int shipInstructionsFieldIndex = allStructFieldRefs.indexOf(shipInstructionsField);

        StructField shipModeField = rowInspector.getStructFieldRef("shipmode");
        int shipModeFieldIndex = allStructFieldRefs.indexOf(shipModeField);

        StructField commentField = rowInspector.getStructFieldRef("comment");
        int commentFieldIndex = allStructFieldRefs.indexOf(commentField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[orderKeyFieldIndex + 1] = true;
        include[partKeyFieldIndex + 1] = true;
        include[supplierKeyFieldIndex + 1] = true;
        include[lineNumberFieldIndex + 1] = true;
        include[quantityFieldIndex + 1] = true;
        include[extendedPriceFieldIndex + 1] = true;
        include[discountFieldIndex + 1] = true;
        include[taxFieldIndex + 1] = true;
        include[returnFlagFieldIndex + 1] = true;
        include[lineStatusFieldIndex + 1] = true;
        include[shipDateFieldIndex + 1] = true;
        include[commitDateFieldIndex + 1] = true;
        include[receiptDateFieldIndex + 1] = true;
        include[shipInstructionsFieldIndex + 1] = true;
        include[shipModeFieldIndex + 1] = true;
        include[commentFieldIndex + 1] = true;

        long orderKeySum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            orderKeySum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include, null);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);

                LongColumnVector orderKeyColumnVector = (LongColumnVector) batch.cols[orderKeyFieldIndex];
                long[] orderKeyVector = orderKeyColumnVector.vector;
                boolean[] orderKeyIsNull = orderKeyColumnVector.isNull;

                for (int i = 0; i < batch.size; i++) {
                    int orderKeyIndex = orderKeyColumnVector.isRepeating ? 0 : i;
                    if (!orderKeyIsNull[orderKeyIndex]) {
                        orderKeySum += orderKeyVector[orderKeyIndex];
                    }
                }
            }
            recordReader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }

    @Override
    public <K, V extends Writable> List<Object> allNoMatch(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        Builder builder = SearchArgument.FACTORY.newBuilder();
        builder.startAnd();
        builder.equals("orderkey", -1L);
        builder.end();
        SearchArgument searchArgument = builder.build();

        return all(jobConf, fileSplit, inputFormat, deserializer, searchArgument);
    }

    @Override
    public <K, V extends Writable> List<Object> allSmallMatch(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        Builder builder = SearchArgument.FACTORY.newBuilder();
        builder.startAnd();
        builder.in("orderkey", 5L, 500L, 5_000L, 50_000L, 500_000L, 550_000L, 5_000_000L);
        builder.end();
        SearchArgument searchArgument = builder.build();

        return all(jobConf, fileSplit, inputFormat, deserializer, searchArgument);
    }

    public <K, V extends Writable> List<Object> all(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer, SearchArgument searchArgument)
            throws Exception
    {
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField orderKeyField = rowInspector.getStructFieldRef("orderkey");
        int orderKeyFieldIndex = allStructFieldRefs.indexOf(orderKeyField);

        StructField partKeyField = rowInspector.getStructFieldRef("partkey");
        int partKeyFieldIndex = allStructFieldRefs.indexOf(partKeyField);

        StructField supplierKeyField = rowInspector.getStructFieldRef("suppkey");
        int supplierKeyFieldIndex = allStructFieldRefs.indexOf(supplierKeyField);

        StructField lineNumberField = rowInspector.getStructFieldRef("linenumber");
        int lineNumberFieldIndex = allStructFieldRefs.indexOf(lineNumberField);

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);

        StructField taxField = rowInspector.getStructFieldRef("tax");
        int taxFieldIndex = allStructFieldRefs.indexOf(taxField);

        StructField returnFlagField = rowInspector.getStructFieldRef("returnflag");
        int returnFlagFieldIndex = allStructFieldRefs.indexOf(returnFlagField);

        StructField lineStatusField = rowInspector.getStructFieldRef("linestatus");
        int lineStatusFieldIndex = allStructFieldRefs.indexOf(lineStatusField);

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);

        StructField commitDateField = rowInspector.getStructFieldRef("commitdate");
        int commitDateFieldIndex = allStructFieldRefs.indexOf(commitDateField);

        StructField receiptDateField = rowInspector.getStructFieldRef("receiptdate");
        int receiptDateFieldIndex = allStructFieldRefs.indexOf(receiptDateField);

        StructField shipInstructionsField = rowInspector.getStructFieldRef("shipinstruct");
        int shipInstructionsFieldIndex = allStructFieldRefs.indexOf(shipInstructionsField);

        StructField shipModeField = rowInspector.getStructFieldRef("shipmode");
        int shipModeFieldIndex = allStructFieldRefs.indexOf(shipModeField);

        StructField commentField = rowInspector.getStructFieldRef("comment");
        int commentFieldIndex = allStructFieldRefs.indexOf(commentField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[orderKeyFieldIndex + 1] = true;
        include[partKeyFieldIndex + 1] = true;
        include[supplierKeyFieldIndex + 1] = true;
        include[lineNumberFieldIndex + 1] = true;
        include[quantityFieldIndex + 1] = true;
        include[extendedPriceFieldIndex + 1] = true;
        include[discountFieldIndex + 1] = true;
        include[taxFieldIndex + 1] = true;
        include[returnFlagFieldIndex + 1] = true;
        include[lineStatusFieldIndex + 1] = true;
        include[shipDateFieldIndex + 1] = true;
        include[commitDateFieldIndex + 1] = true;
        include[receiptDateFieldIndex + 1] = true;
        include[shipInstructionsFieldIndex + 1] = true;
        include[shipModeFieldIndex + 1] = true;
        include[commentFieldIndex + 1] = true;

        long rowCount = 0;
        long orderKeySum = 0;
        long partKeySum = 0;
        long supplierKeySum = 0;
        long lineNumberSum = 0;
        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        double taxSum = 0;
        long returnFlagSum = 0;
        long lineStatusSum = 0;
        long shipDateSum = 0;
        long commitDateSum = 0;
        long receiptDateSum = 0;
        long shipInstructionsSum = 0;
        long shipModeSum = 0;
        long commentSum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            rowCount = 0;
            orderKeySum = 0;
            partKeySum = 0;
            supplierKeySum = 0;
            lineNumberSum = 0;
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            taxSum = 0;
            returnFlagSum = 0;
            lineStatusSum = 0;
            shipDateSum = 0;
            commitDateSum = 0;
            receiptDateSum = 0;
            shipInstructionsSum = 0;
            shipModeSum = 0;
            commentSum = 0;

            RecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, include, searchArgument);
            VectorizedRowBatch batch = null;
            while (recordReader.hasNext()) {
                batch = recordReader.nextBatch(batch);

                LongColumnVector orderKeyColumnVector = (LongColumnVector) batch.cols[orderKeyFieldIndex];
                long[] orderKeyVector = orderKeyColumnVector.vector;
                boolean[] orderKeyIsNull = orderKeyColumnVector.isNull;

                LongColumnVector partKeyColumnVector = (LongColumnVector) batch.cols[partKeyFieldIndex];
                long[] partKeyVector = partKeyColumnVector.vector;
                boolean[] partKeyIsNull = partKeyColumnVector.isNull;

                LongColumnVector supplierKeyColumnVector = (LongColumnVector) batch.cols[supplierKeyFieldIndex];
                long[] supplierKeyVector = supplierKeyColumnVector.vector;
                boolean[] supplierKeyIsNull = supplierKeyColumnVector.isNull;

                LongColumnVector lineNumberColumnVector = (LongColumnVector) batch.cols[lineNumberFieldIndex];
                long[] lineNumberVector = lineNumberColumnVector.vector;
                boolean[] lineNumberIsNull = lineNumberColumnVector.isNull;

                LongColumnVector quantityColumnVector = (LongColumnVector) batch.cols[quantityFieldIndex];
                long[] quantityVector = quantityColumnVector.vector;
                boolean[] quantityIsNull = quantityColumnVector.isNull;

                DoubleColumnVector extendedPriceColumnVector = (DoubleColumnVector) batch.cols[extendedPriceFieldIndex];
                double[] extendedPriceVector = extendedPriceColumnVector.vector;
                boolean[] extendedPriceIsNull = extendedPriceColumnVector.isNull;

                DoubleColumnVector discountColumnVector = (DoubleColumnVector) batch.cols[discountFieldIndex];
                double[] discountVector = discountColumnVector.vector;
                boolean[] discountIsNull = discountColumnVector.isNull;

                DoubleColumnVector taxColumnVector = (DoubleColumnVector) batch.cols[taxFieldIndex];
                double[] taxVector = taxColumnVector.vector;
                boolean[] taxIsNull = taxColumnVector.isNull;

                BytesColumnVector returnFlagColumnVector = (BytesColumnVector) batch.cols[returnFlagFieldIndex];
                byte[][] returnFlagVector = returnFlagColumnVector.vector;
                int[] returnFlagStartVector = returnFlagColumnVector.start;
                int[] returnFlagLengthVector = returnFlagColumnVector.length;
                boolean[] returnFlagIsNull = returnFlagColumnVector.isNull;

                BytesColumnVector lineStatusColumnVector = (BytesColumnVector) batch.cols[lineStatusFieldIndex];
                byte[][] lineStatusVector = lineStatusColumnVector.vector;
                int[] lineStatusStartVector = lineStatusColumnVector.start;
                int[] lineStatusLengthVector = lineStatusColumnVector.length;
                boolean[] lineStatusIsNull = lineStatusColumnVector.isNull;

                BytesColumnVector shipDateColumnVector = (BytesColumnVector) batch.cols[shipDateFieldIndex];
                byte[][] shipDateVector = shipDateColumnVector.vector;
                int[] shipDateStartVector = shipDateColumnVector.start;
                int[] shipDateLengthVector = shipDateColumnVector.length;
                boolean[] shipDateIsNull = shipDateColumnVector.isNull;

                BytesColumnVector commitDateColumnVector = (BytesColumnVector) batch.cols[commitDateFieldIndex];
                byte[][] commitDateVector = commitDateColumnVector.vector;
                int[] commitDateStartVector = commitDateColumnVector.start;
                int[] commitDateLengthVector = commitDateColumnVector.length;
                boolean[] commitDateIsNull = commitDateColumnVector.isNull;

                BytesColumnVector receiptDateColumnVector = (BytesColumnVector) batch.cols[receiptDateFieldIndex];
                byte[][] receiptDateVector = receiptDateColumnVector.vector;
                int[] receiptDateStartVector = receiptDateColumnVector.start;
                int[] receiptDateLengthVector = receiptDateColumnVector.length;
                boolean[] receiptDateIsNull = receiptDateColumnVector.isNull;

                BytesColumnVector shipInstructionsColumnVector = (BytesColumnVector) batch.cols[shipInstructionsFieldIndex];
                byte[][] shipInstructionsVector = shipInstructionsColumnVector.vector;
                int[] shipInstructionsStartVector = shipInstructionsColumnVector.start;
                int[] shipInstructionsLengthVector = shipInstructionsColumnVector.length;
                boolean[] shipInstructionsIsNull = shipInstructionsColumnVector.isNull;

                BytesColumnVector shipModeColumnVector = (BytesColumnVector) batch.cols[shipModeFieldIndex];
                byte[][] shipModeVector = shipModeColumnVector.vector;
                int[] shipModeStartVector = shipModeColumnVector.start;
                int[] shipModeLengthVector = shipModeColumnVector.length;
                boolean[] shipModeIsNull = shipModeColumnVector.isNull;

                BytesColumnVector commentColumnVector = (BytesColumnVector) batch.cols[commentFieldIndex];
                byte[][] commentVector = commentColumnVector.vector;
                int[] commentStartVector = commentColumnVector.start;
                int[] commentLengthVector = commentColumnVector.length;
                boolean[] commentIsNull = commentColumnVector.isNull;

                for (int i = 0; i < batch.size; i++) {
                    rowCount++;

                    int orderKeyIndex = orderKeyColumnVector.isRepeating ? 0 : i;
                    if (!orderKeyIsNull[orderKeyIndex]) {
                        orderKeySum += orderKeyVector[orderKeyIndex];
                    }

                    int partKeyIndex = partKeyColumnVector.isRepeating ? 0 : i;
                    if (!partKeyIsNull[partKeyIndex]) {
                        partKeySum += partKeyVector[partKeyIndex];
                    }

                    int supplierKeyIndex = supplierKeyColumnVector.isRepeating ? 0 : i;
                    if (!supplierKeyIsNull[supplierKeyIndex]) {
                        supplierKeySum += supplierKeyVector[supplierKeyIndex];
                    }

                    int lineNumberIndex = lineNumberColumnVector.isRepeating ? 0 : i;
                    if (!lineNumberIsNull[lineNumberIndex]) {
                        lineNumberSum += lineNumberVector[lineNumberIndex];
                    }

                    int quantityIndex = quantityColumnVector.isRepeating ? 0 : i;
                    if (!quantityIsNull[quantityIndex]) {
                        quantitySum += quantityVector[quantityIndex];
                    }

                    int extendedPriceIndex = extendedPriceColumnVector.isRepeating ? 0 : i;
                    if (!extendedPriceIsNull[extendedPriceIndex]) {
                        extendedPriceSum += extendedPriceVector[extendedPriceIndex];
                    }

                    int discountIndex = discountColumnVector.isRepeating ? 0 : i;
                    if (!discountIsNull[discountIndex]) {
                        discountSum += discountVector[discountIndex];
                    }

                    int taxIndex = taxColumnVector.isRepeating ? 0 : i;
                    if (!taxIsNull[taxIndex]) {
                        taxSum += taxVector[taxIndex];
                    }

                    int returnFlagIndex = returnFlagColumnVector.isRepeating ? 0 : i;
                    if (!returnFlagIsNull[returnFlagIndex]) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagVector[returnFlagIndex],
                                returnFlagStartVector[returnFlagIndex],
                                returnFlagStartVector[returnFlagIndex] + returnFlagLengthVector[returnFlagIndex]);
                        returnFlagSum += returnFlagValue.length;
                    }

                    int lineStatusIndex = lineStatusColumnVector.isRepeating ? 0 : i;
                    if (!lineStatusIsNull[lineStatusIndex]) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusVector[lineStatusIndex],
                                lineStatusStartVector[lineStatusIndex],
                                lineStatusStartVector[lineStatusIndex] + lineStatusLengthVector[lineStatusIndex]);
                        lineStatusSum += lineStatusValue.length;
                    }

                    int shipDateIndex = shipDateColumnVector.isRepeating ? 0 : i;
                    if (!shipDateIsNull[shipDateIndex]) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex],
                                shipDateStartVector[shipDateIndex] + shipDateLengthVector[shipDateIndex]);
                        shipDateSum += shipDateValue.length;
                    }

                    int commitDateIndex = commitDateColumnVector.isRepeating ? 0 : i;
                    if (!commitDateIsNull[commitDateIndex]) {
                        byte[] commitDateValue = Arrays.copyOfRange(commitDateVector[commitDateIndex],
                                commitDateStartVector[commitDateIndex],
                                commitDateStartVector[commitDateIndex] + commitDateLengthVector[commitDateIndex]);
                        commitDateSum += commitDateValue.length;
                    }

                    int receiptDateIndex = receiptDateColumnVector.isRepeating ? 0 : i;
                    if (!receiptDateIsNull[receiptDateIndex]) {
                        byte[] receiptDateValue = Arrays.copyOfRange(receiptDateVector[receiptDateIndex],
                                receiptDateStartVector[receiptDateIndex],
                                receiptDateStartVector[receiptDateIndex] + receiptDateLengthVector[receiptDateIndex]);
                        receiptDateSum += receiptDateValue.length;
                    }

                    int shipInstructionsIndex = shipInstructionsColumnVector.isRepeating ? 0 : i;
                    if (!shipInstructionsIsNull[shipInstructionsIndex]) {
                        byte[] shipInstructionsValue = Arrays.copyOfRange(shipInstructionsVector[shipInstructionsIndex],
                                shipInstructionsStartVector[shipInstructionsIndex],
                                shipInstructionsStartVector[shipInstructionsIndex] + shipInstructionsLengthVector[shipInstructionsIndex]);
                        shipInstructionsSum += shipInstructionsValue.length;
                    }

                    int shipModeIndex = shipModeColumnVector.isRepeating ? 0 : i;
                    if (!shipModeIsNull[shipModeIndex]) {
                        byte[] shipModeValue = Arrays.copyOfRange(shipModeVector[shipModeIndex],
                                shipModeStartVector[shipModeIndex],
                                shipModeStartVector[shipModeIndex] + shipModeLengthVector[shipModeIndex]);
                        shipModeSum += shipModeValue.length;
                    }

                    int commentIndex = commentColumnVector.isRepeating ? 0 : i;
                    if (!commentIsNull[commentIndex]) {
                        byte[] commentValue = Arrays.copyOfRange(commentVector[commentIndex],
                                commentStartVector[commentIndex],
                                commentStartVector[commentIndex] + commentLengthVector[commentIndex]);
                        commentSum += commentValue.length;
                    }
                }
            }
            recordReader.close();
        }

        return ImmutableList.<Object>of(
                rowCount,
                orderKeySum,
                partKeySum,
                supplierKeySum,
                lineNumberSum,
                quantitySum,
                extendedPriceSum,
                discountSum,
                taxSum,
                returnFlagSum,
                lineStatusSum,
                shipDateSum,
                commitDateSum,
                receiptDateSum,
                shipInstructionsSum,
                shipModeSum,
                commentSum);
    }

    public RecordReader createVectorizedRecordReader(FileSystem fileSystem, FileSplit fileSplit, boolean[] include)
            throws IOException
    {
        return createVectorizedRecordReader(fileSystem, fileSplit, include, null);
    }

    public RecordReader createVectorizedRecordReader(FileSystem fileSystem,
            FileSplit fileSplit,
            boolean[] include,
            SearchArgument searchArgument)
            throws IOException
    {
        Reader reader = OrcFile.createReader(fileSystem, fileSplit.getPath());

        if (searchArgument == null) {
            return reader.rows(fileSplit.getStart(), fileSplit.getLength(), include);
        }
        else {
            return reader.rows(fileSplit.getStart(), fileSplit.getLength(), include, searchArgument, COLUMN_NAMES);
        }
    }
}
