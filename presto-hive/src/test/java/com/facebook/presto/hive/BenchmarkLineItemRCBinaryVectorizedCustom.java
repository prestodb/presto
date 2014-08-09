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
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public class BenchmarkLineItemRCBinaryVectorizedCustom
    implements BenchmarkLineItem
{
    @Override
    public String getName()
    {
        return "vector custom";
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long bigintValue = readVBigint(bytes, start, length);
                        bigintSum += bigintValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long bigintValue = readVBigint(bytes, start, length);
                        bigintSum += bigintValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long bigintValue = readVBigint(bytes, start, length);
                        bigintSum += bigintValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long bigintValue = readVBigint(bytes, start, length);
                        bigintSum += bigintValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long bigintValue = readVBigint(bytes, start, length);
                        bigintSum += bigintValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long longBits = unsafe.getLong(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                        double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        doubleSum += doubleValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long longBits = unsafe.getLong(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                        double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        doubleSum += doubleValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (length != 0) {
                        long longBits = unsafe.getLong(bytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + start);
                        double doubleValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        doubleSum += doubleValue;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable column = null;
            while (reader.nextColumnsBatch()) {
                column = reader.getColumn(fieldIndex, column);
                for (int i = 0; i < column.size(); i++) {
                    BytesRefWritable bytesRefWritable = column.unCheckedGet(i);
                    byte[] bytes = bytesRefWritable.getData();
                    int start = bytesRefWritable.getStart();
                    int length = bytesRefWritable.getLength();

                    if (!isNull(bytes, start, length)) {
                        byte[] stringValue = Arrays.copyOfRange(bytes, start, start + length);
                        stringSum += stringValue.length;
                    }
                }
            }
            reader.close();
        }
        return stringSum;
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                quantityFieldIndex,
                extendedPriceFieldIndex,
                discountFieldIndex,
                taxFieldIndex,
                returnFlagFieldIndex,
                lineStatusFieldIndex,
                shipDateFieldIndex));

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

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable quantityColumn = null;
            BytesRefArrayWritable extendedPriceColumn = null;
            BytesRefArrayWritable discountColumn = null;
            BytesRefArrayWritable taxColumn = null;
            BytesRefArrayWritable returnFlagColumn = null;
            BytesRefArrayWritable lineStatusColumn = null;
            BytesRefArrayWritable shipDateColumn = null;

            while (reader.nextColumnsBatch()) {
                quantityColumn = reader.getColumn(quantityFieldIndex, quantityColumn);
                extendedPriceColumn = reader.getColumn(extendedPriceFieldIndex, extendedPriceColumn);
                discountColumn = reader.getColumn(discountFieldIndex, discountColumn);
                taxColumn = reader.getColumn(taxFieldIndex, taxColumn);
                returnFlagColumn = reader.getColumn(returnFlagFieldIndex, returnFlagColumn);
                lineStatusColumn = reader.getColumn(lineStatusFieldIndex, lineStatusColumn);
                shipDateColumn = reader.getColumn(shipDateFieldIndex, shipDateColumn);

                for (int i = 0; i < quantityColumn.size(); i++) {
                    BytesRefWritable quantityBytesRefWritable = quantityColumn.unCheckedGet(i);
                    byte[] quantityBytes = quantityBytesRefWritable.getData();
                    int quantityStart = quantityBytesRefWritable.getStart();
                    int quantityLength = quantityBytesRefWritable.getLength();
                    if (quantityLength != 0) {
                        long quantityValue = readVBigint(quantityBytes, quantityStart, quantityLength);
                        quantitySum += quantityValue;
                    }

                    BytesRefWritable extendedPriceBytesRefWritable = extendedPriceColumn.unCheckedGet(i);
                    byte[] extendedPriceBytes = extendedPriceBytesRefWritable.getData();
                    int extendedPriceStart = extendedPriceBytesRefWritable.getStart();
                    int extendedPriceLength = extendedPriceBytesRefWritable.getLength();
                    if (extendedPriceLength != 0) {
                        long longBits = unsafe.getLong(extendedPriceBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + extendedPriceStart);
                        double extendedPriceValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        extendedPriceSum += extendedPriceValue;
                    }

                    BytesRefWritable discountBytesRefWritable = discountColumn.unCheckedGet(i);
                    byte[] discountBytes = discountBytesRefWritable.getData();
                    int discountStart = discountBytesRefWritable.getStart();
                    int discountLength = discountBytesRefWritable.getLength();
                    if (discountLength != 0) {
                        long longBits = unsafe.getLong(discountBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + discountStart);
                        double discountValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        discountSum += discountValue;
                    }

                    BytesRefWritable taxBytesRefWritable = taxColumn.unCheckedGet(i);
                    byte[] taxBytes = taxBytesRefWritable.getData();
                    int taxStart = taxBytesRefWritable.getStart();
                    int taxLength = taxBytesRefWritable.getLength();
                    if (taxLength != 0) {
                        long longBits = unsafe.getLong(taxBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + taxStart);
                        double taxValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        taxSum += taxValue;
                    }

                    BytesRefWritable returnFlagBytesRefWritable = returnFlagColumn.unCheckedGet(i);
                    byte[] returnFlagBytes = returnFlagBytesRefWritable.getData();
                    int returnFlagStart = returnFlagBytesRefWritable.getStart();
                    int returnFlagLength = returnFlagBytesRefWritable.getLength();
                    if (!isNull(returnFlagBytes, returnFlagStart, returnFlagLength)) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagBytes, returnFlagStart, returnFlagStart + returnFlagLength);
                        returnFlagSum += returnFlagValue.length;
                    }

                    BytesRefWritable lineStatusBytesRefWritable = lineStatusColumn.unCheckedGet(i);
                    byte[] lineStatusBytes = lineStatusBytesRefWritable.getData();
                    int lineStatusStart = lineStatusBytesRefWritable.getStart();
                    int lineStatusLength = lineStatusBytesRefWritable.getLength();
                    if (!isNull(lineStatusBytes, lineStatusStart, lineStatusLength)) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusBytes, lineStatusStart, lineStatusStart + lineStatusLength);
                        lineStatusSum += lineStatusValue.length;
                    }

                    BytesRefWritable shipDateBytesRefWritable = shipDateColumn.unCheckedGet(i);
                    byte[] shipDateBytes = shipDateBytesRefWritable.getData();
                    int shipDateStart = shipDateBytesRefWritable.getStart();
                    int shipDateLength = shipDateBytesRefWritable.getLength();
                    if (!isNull(shipDateBytes, shipDateStart, shipDateLength)) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateBytes, shipDateStart, shipDateStart + shipDateLength);
                        shipDateSum += shipDateValue.length;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                quantityFieldIndex,
                extendedPriceFieldIndex,
                discountFieldIndex,
                shipDateFieldIndex));

        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        long shipDateSum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable quantityColumn = null;
            BytesRefArrayWritable extendedPriceColumn = null;
            BytesRefArrayWritable discountColumn = null;
            BytesRefArrayWritable shipDateColumn = null;

            while (reader.nextColumnsBatch()) {
                quantityColumn = reader.getColumn(quantityFieldIndex, quantityColumn);
                extendedPriceColumn = reader.getColumn(extendedPriceFieldIndex, extendedPriceColumn);
                discountColumn = reader.getColumn(discountFieldIndex, discountColumn);
                shipDateColumn = reader.getColumn(shipDateFieldIndex, shipDateColumn);

                for (int i = 0; i < quantityColumn.size(); i++) {
                    BytesRefWritable quantityBytesRefWritable = quantityColumn.unCheckedGet(i);
                    byte[] quantityBytes = quantityBytesRefWritable.getData();
                    int quantityStart = quantityBytesRefWritable.getStart();
                    int quantityLength = quantityBytesRefWritable.getLength();
                    if (quantityLength != 0) {
                        long quantityValue = readVBigint(quantityBytes, quantityStart, quantityLength);
                        quantitySum += quantityValue;
                    }

                    BytesRefWritable extendedPriceBytesRefWritable = extendedPriceColumn.unCheckedGet(i);
                    byte[] extendedPriceBytes = extendedPriceBytesRefWritable.getData();
                    int extendedPriceStart = extendedPriceBytesRefWritable.getStart();
                    int extendedPriceLength = extendedPriceBytesRefWritable.getLength();
                    if (extendedPriceLength != 0) {
                        long longBits = unsafe.getLong(extendedPriceBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + extendedPriceStart);
                        double extendedPriceValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        extendedPriceSum += extendedPriceValue;
                    }

                    BytesRefWritable discountBytesRefWritable = discountColumn.unCheckedGet(i);
                    byte[] discountBytes = discountBytesRefWritable.getData();
                    int discountStart = discountBytesRefWritable.getStart();
                    int discountLength = discountBytesRefWritable.getLength();
                    if (discountLength != 0) {
                        long longBits = unsafe.getLong(discountBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + discountStart);
                        double discountValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        discountSum += discountValue;
                    }

                    BytesRefWritable shipDateBytesRefWritable = shipDateColumn.unCheckedGet(i);
                    byte[] shipDateBytes = shipDateBytesRefWritable.getData();
                    int shipDateStart = shipDateBytesRefWritable.getStart();
                    int shipDateLength = shipDateBytesRefWritable.getLength();
                    if (!isNull(shipDateBytes, shipDateStart, shipDateLength)) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateBytes, shipDateStart, shipDateStart + shipDateLength);
                        shipDateSum += shipDateValue.length;
                    }
                }
            }
            reader.close();
        }
        return ImmutableList.<Object>of(quantitySum, extendedPriceSum, discountSum, shipDateSum);
    }

    @Override
    public <K, V extends Writable> List<Object> all(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                orderKeyFieldIndex,
                partKeyFieldIndex,
                supplierKeyFieldIndex,
                lineNumberFieldIndex,
                quantityFieldIndex,
                extendedPriceFieldIndex,
                discountFieldIndex,
                taxFieldIndex,
                returnFlagFieldIndex,
                lineStatusFieldIndex,
                shipDateFieldIndex,
                commitDateFieldIndex,
                receiptDateFieldIndex,
                shipInstructionsFieldIndex,
                shipModeFieldIndex,
                commentFieldIndex));

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

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable orderKeyColumn = null;
            BytesRefArrayWritable partKeyColumn = null;
            BytesRefArrayWritable supplierKeyColumn = null;
            BytesRefArrayWritable lineNumberColumn = null;
            BytesRefArrayWritable quantityColumn = null;
            BytesRefArrayWritable extendedPriceColumn = null;
            BytesRefArrayWritable discountColumn = null;
            BytesRefArrayWritable taxColumn = null;
            BytesRefArrayWritable returnFlagColumn = null;
            BytesRefArrayWritable lineStatusColumn = null;
            BytesRefArrayWritable shipDateColumn = null;
            BytesRefArrayWritable commitDateColumn = null;
            BytesRefArrayWritable receiptDateColumn = null;
            BytesRefArrayWritable shipInstructionsColumn = null;
            BytesRefArrayWritable shipModeColumn = null;
            BytesRefArrayWritable commentColumn = null;

            while (reader.nextColumnsBatch()) {
                orderKeyColumn = reader.getColumn(orderKeyFieldIndex, orderKeyColumn);
                partKeyColumn = reader.getColumn(partKeyFieldIndex, partKeyColumn);
                supplierKeyColumn = reader.getColumn(supplierKeyFieldIndex, supplierKeyColumn);
                lineNumberColumn = reader.getColumn(lineNumberFieldIndex, lineNumberColumn);
                quantityColumn = reader.getColumn(quantityFieldIndex, quantityColumn);
                extendedPriceColumn = reader.getColumn(extendedPriceFieldIndex, extendedPriceColumn);
                discountColumn = reader.getColumn(discountFieldIndex, discountColumn);
                taxColumn = reader.getColumn(taxFieldIndex, taxColumn);
                returnFlagColumn = reader.getColumn(returnFlagFieldIndex, returnFlagColumn);
                lineStatusColumn = reader.getColumn(lineStatusFieldIndex, lineStatusColumn);
                shipDateColumn = reader.getColumn(shipDateFieldIndex, shipDateColumn);
                commitDateColumn = reader.getColumn(commitDateFieldIndex, commitDateColumn);
                receiptDateColumn = reader.getColumn(receiptDateFieldIndex, receiptDateColumn);
                shipInstructionsColumn = reader.getColumn(shipInstructionsFieldIndex, shipInstructionsColumn);
                shipModeColumn = reader.getColumn(shipModeFieldIndex, shipModeColumn);
                commentColumn = reader.getColumn(commentFieldIndex, commentColumn);

                for (int i = 0; i < orderKeyColumn.size(); i++) {
                    rowCount++;

                    BytesRefWritable orderKeyBytesRefWritable = orderKeyColumn.unCheckedGet(i);
                    byte[] orderKeyBytes = orderKeyBytesRefWritable.getData();
                    int orderKeyStart = orderKeyBytesRefWritable.getStart();
                    int orderKeyLength = orderKeyBytesRefWritable.getLength();
                    if (orderKeyLength != 0) {
                        long orderKeyValue = readVBigint(orderKeyBytes, orderKeyStart, orderKeyLength);
                        orderKeySum += orderKeyValue;
                    }

                    BytesRefWritable partKeyBytesRefWritable = partKeyColumn.unCheckedGet(i);
                    byte[] partKeyBytes = partKeyBytesRefWritable.getData();
                    int partKeyStart = partKeyBytesRefWritable.getStart();
                    int partKeyLength = partKeyBytesRefWritable.getLength();
                    if (partKeyLength != 0) {
                        long partKeyValue = readVBigint(partKeyBytes, partKeyStart, partKeyLength);
                        partKeySum += partKeyValue;
                    }

                    BytesRefWritable supplierKeyBytesRefWritable = supplierKeyColumn.unCheckedGet(i);
                    byte[] supplierKeyBytes = supplierKeyBytesRefWritable.getData();
                    int supplierKeyStart = supplierKeyBytesRefWritable.getStart();
                    int supplierKeyLength = supplierKeyBytesRefWritable.getLength();
                    if (supplierKeyLength != 0) {
                        long supplierKeyValue = readVBigint(supplierKeyBytes, supplierKeyStart, supplierKeyLength);
                        supplierKeySum += supplierKeyValue;
                    }

                    BytesRefWritable lineNumberBytesRefWritable = lineNumberColumn.unCheckedGet(i);
                    byte[] lineNumberBytes = lineNumberBytesRefWritable.getData();
                    int lineNumberStart = lineNumberBytesRefWritable.getStart();
                    int lineNumberLength = lineNumberBytesRefWritable.getLength();
                    if (lineNumberLength != 0) {
                        long lineNumberValue = readVBigint(lineNumberBytes, lineNumberStart, lineNumberLength);
                        lineNumberSum += lineNumberValue;
                    }

                    BytesRefWritable quantityBytesRefWritable = quantityColumn.unCheckedGet(i);
                    byte[] quantityBytes = quantityBytesRefWritable.getData();
                    int quantityStart = quantityBytesRefWritable.getStart();
                    int quantityLength = quantityBytesRefWritable.getLength();
                    if (quantityLength != 0) {
                        long quantityValue = readVBigint(quantityBytes, quantityStart, quantityLength);
                        quantitySum += quantityValue;
                    }

                    BytesRefWritable extendedPriceBytesRefWritable = extendedPriceColumn.unCheckedGet(i);
                    byte[] extendedPriceBytes = extendedPriceBytesRefWritable.getData();
                    int extendedPriceStart = extendedPriceBytesRefWritable.getStart();
                    int extendedPriceLength = extendedPriceBytesRefWritable.getLength();
                    if (extendedPriceLength != 0) {
                        long longBits = unsafe.getLong(extendedPriceBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + extendedPriceStart);
                        double extendedPriceValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        extendedPriceSum += extendedPriceValue;
                    }

                    BytesRefWritable discountBytesRefWritable = discountColumn.unCheckedGet(i);
                    byte[] discountBytes = discountBytesRefWritable.getData();
                    int discountStart = discountBytesRefWritable.getStart();
                    int discountLength = discountBytesRefWritable.getLength();
                    if (discountLength != 0) {
                        long longBits = unsafe.getLong(discountBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + discountStart);
                        double discountValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        discountSum += discountValue;
                    }

                    BytesRefWritable taxBytesRefWritable = taxColumn.unCheckedGet(i);
                    byte[] taxBytes = taxBytesRefWritable.getData();
                    int taxStart = taxBytesRefWritable.getStart();
                    int taxLength = taxBytesRefWritable.getLength();
                    if (taxLength != 0) {
                        long longBits = unsafe.getLong(taxBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + taxStart);
                        double taxValue = Double.longBitsToDouble(Long.reverseBytes(longBits));
                        taxSum += taxValue;
                    }

                    BytesRefWritable returnFlagBytesRefWritable = returnFlagColumn.unCheckedGet(i);
                    byte[] returnFlagBytes = returnFlagBytesRefWritable.getData();
                    int returnFlagStart = returnFlagBytesRefWritable.getStart();
                    int returnFlagLength = returnFlagBytesRefWritable.getLength();
                    if (!isNull(returnFlagBytes, returnFlagStart, returnFlagLength)) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagBytes, returnFlagStart, returnFlagStart + returnFlagLength);
                        returnFlagSum += returnFlagValue.length;
                    }

                    BytesRefWritable lineStatusBytesRefWritable = lineStatusColumn.unCheckedGet(i);
                    byte[] lineStatusBytes = lineStatusBytesRefWritable.getData();
                    int lineStatusStart = lineStatusBytesRefWritable.getStart();
                    int lineStatusLength = lineStatusBytesRefWritable.getLength();
                    if (!isNull(lineStatusBytes, lineStatusStart, lineStatusLength)) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusBytes, lineStatusStart, lineStatusStart + lineStatusLength);
                        lineStatusSum += lineStatusValue.length;
                    }

                    BytesRefWritable shipDateBytesRefWritable = shipDateColumn.unCheckedGet(i);
                    byte[] shipDateBytes = shipDateBytesRefWritable.getData();
                    int shipDateStart = shipDateBytesRefWritable.getStart();
                    int shipDateLength = shipDateBytesRefWritable.getLength();
                    if (!isNull(shipDateBytes, shipDateStart, shipDateLength)) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateBytes, shipDateStart, shipDateStart + shipDateLength);
                        shipDateSum += shipDateValue.length;
                    }

                    BytesRefWritable commitDateBytesRefWritable = commitDateColumn.unCheckedGet(i);
                    byte[] commitDateBytes = commitDateBytesRefWritable.getData();
                    int commitDateStart = commitDateBytesRefWritable.getStart();
                    int commitDateLength = commitDateBytesRefWritable.getLength();
                    if (!isNull(commitDateBytes, commitDateStart, commitDateLength)) {
                        byte[] commitDateValue = Arrays.copyOfRange(commitDateBytes, commitDateStart, commitDateStart + commitDateLength);
                        commitDateSum += commitDateValue.length;
                    }

                    BytesRefWritable receiptDateBytesRefWritable = receiptDateColumn.unCheckedGet(i);
                    byte[] receiptDateBytes = receiptDateBytesRefWritable.getData();
                    int receiptDateStart = receiptDateBytesRefWritable.getStart();
                    int receiptDateLength = receiptDateBytesRefWritable.getLength();
                    if (!isNull(receiptDateBytes, receiptDateStart, receiptDateLength)) {
                        byte[] receiptDateValue = Arrays.copyOfRange(receiptDateBytes, receiptDateStart, receiptDateStart + receiptDateLength);
                        receiptDateSum += receiptDateValue.length;
                    }

                    BytesRefWritable shipInstructionsBytesRefWritable = shipInstructionsColumn.unCheckedGet(i);
                    byte[] shipInstructionsBytes = shipInstructionsBytesRefWritable.getData();
                    int shipInstructionsStart = shipInstructionsBytesRefWritable.getStart();
                    int shipInstructionsLength = shipInstructionsBytesRefWritable.getLength();
                    if (!isNull(shipInstructionsBytes, shipInstructionsStart, shipInstructionsLength)) {
                        byte[] shipInstructionsValue = Arrays.copyOfRange(shipInstructionsBytes, shipInstructionsStart, shipInstructionsStart + shipInstructionsLength);
                        shipInstructionsSum += shipInstructionsValue.length;
                    }

                    BytesRefWritable shipModeBytesRefWritable = shipModeColumn.unCheckedGet(i);
                    byte[] shipModeBytes = shipModeBytesRefWritable.getData();
                    int shipModeStart = shipModeBytesRefWritable.getStart();
                    int shipModeLength = shipModeBytesRefWritable.getLength();
                    if (!isNull(shipModeBytes, shipModeStart, shipModeLength)) {
                        byte[] shipModeValue = Arrays.copyOfRange(shipModeBytes, shipModeStart, shipModeStart + shipModeLength);
                        shipModeSum += shipModeValue.length;
                    }

                    BytesRefWritable commentBytesRefWritable = commentColumn.unCheckedGet(i);
                    byte[] commentBytes = commentBytesRefWritable.getData();
                    int commentStart = commentBytesRefWritable.getStart();
                    int commentLength = commentBytesRefWritable.getLength();
                    if (!isNull(commentBytes, commentStart, commentLength)) {
                        byte[] commentValue = Arrays.copyOfRange(commentBytes, commentStart, commentStart + commentLength);
                        commentSum += commentValue.length;
                    }
                }
            }
            reader.close();
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

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                orderKeyFieldIndex,
                partKeyFieldIndex,
                supplierKeyFieldIndex,
                lineNumberFieldIndex,
                quantityFieldIndex,
                extendedPriceFieldIndex,
                discountFieldIndex,
                taxFieldIndex,
                returnFlagFieldIndex,
                lineStatusFieldIndex,
                shipDateFieldIndex,
                commitDateFieldIndex,
                receiptDateFieldIndex,
                shipInstructionsFieldIndex,
                shipModeFieldIndex,
                commentFieldIndex));

        long orderKeySum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            orderKeySum = 0;

            RCFile.Reader reader = new RCFile.Reader(fileSystem, fileSplit.getPath(), jobConf);
            BytesRefArrayWritable orderKeyColumn = null;

            while (reader.nextColumnsBatch()) {
                orderKeyColumn = reader.getColumn(orderKeyFieldIndex, orderKeyColumn);

                for (int i = 0; i < orderKeyColumn.size(); i++) {
                    BytesRefWritable orderKeyBytesRefWritable = orderKeyColumn.unCheckedGet(i);
                    byte[] orderKeyBytes = orderKeyBytesRefWritable.getData();
                    int orderKeyStart = orderKeyBytesRefWritable.getStart();
                    int orderKeyLength = orderKeyBytesRefWritable.getLength();
                    if (orderKeyLength != 0) {
                        long orderKeyValue = readVBigint(orderKeyBytes, orderKeyStart, orderKeyLength);
                        orderKeySum += orderKeyValue;
                    }
                }
            }
            reader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }

    public long readVBigint(byte[] bytes, int offset, int length)
    {
        if (length == 1) {
            return bytes[offset];
        }

        long i = 0;
        for (int idx = 0; idx < length - 1; idx++) {
            byte b = bytes[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[offset]) ? ~i : i;
    }

    private static boolean isNull(byte[] bytes, int start, int length)
    {
        return length == 2 && bytes[start] == '\\' && bytes[start + 1] == 'N';
    }

    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            if (Unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + Unsafe.ARRAY_BYTE_INDEX_SCALE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
