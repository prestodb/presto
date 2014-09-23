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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public final class BenchmarkLineItemGeneric
        implements BenchmarkLineItem
{
    private static final String[] COLUMN_NAMES = new String[] {
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
        return "generic";
    }

    @Override
    public <K, V extends Writable> long orderKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("orderkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));
        jobConf.set(IOConstants.COLUMNS, Joiner.on(',').join(COLUMN_NAMES));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("partkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("suppkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("linenumber");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("quantity");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);
        PrimitiveObjectInspector bigintFieldInspector = (PrimitiveObjectInspector) bigintField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object bigintData = rowInspector.getStructFieldData(rowData, bigintField);
                if (bigintData != null) {
                    Object bigintPrimitive = bigintFieldInspector.getPrimitiveJavaObject(bigintData);
                    long bigintValue = ((Number) bigintPrimitive).longValue();
                    bigintSum += bigintValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("extendedprice");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("discount");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("tax");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);
        PrimitiveObjectInspector doubleFieldInspector = (PrimitiveObjectInspector) doubleField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object doubleData = rowInspector.getStructFieldData(rowData, doubleField);
                if (doubleData != null) {
                    Object doublePrimitive = doubleFieldInspector.getPrimitiveJavaObject(doubleData);
                    double doubleValue = ((Number) doublePrimitive).doubleValue();
                    doubleSum += doubleValue;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("returnflag");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("linestatus");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("commitdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("receiptdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipinstruct");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipmode");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);
        PrimitiveObjectInspector stringFieldInspector = (PrimitiveObjectInspector) stringField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object stringData = rowInspector.getStructFieldData(rowData, stringField);
                if (stringData != null) {
                    Object stringPrimitive = stringFieldInspector.getPrimitiveJavaObject(stringData);
                    String stringValue = (String) stringPrimitive;
                    stringLengthSum += stringValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);
        PrimitiveObjectInspector quantityFieldInspector = (PrimitiveObjectInspector) quantityField.getFieldObjectInspector();

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);
        PrimitiveObjectInspector extendedPriceFieldInspector = (PrimitiveObjectInspector) extendedPriceField.getFieldObjectInspector();

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);
        PrimitiveObjectInspector discountFieldInspector = (PrimitiveObjectInspector) discountField.getFieldObjectInspector();

        StructField taxField = rowInspector.getStructFieldRef("tax");
        int taxFieldIndex = allStructFieldRefs.indexOf(taxField);
        PrimitiveObjectInspector taxFieldInspector = (PrimitiveObjectInspector) taxField.getFieldObjectInspector();

        StructField returnFlagField = rowInspector.getStructFieldRef("returnflag");
        int returnFlagFieldIndex = allStructFieldRefs.indexOf(returnFlagField);
        PrimitiveObjectInspector returnFlagFieldInspector = (PrimitiveObjectInspector) returnFlagField.getFieldObjectInspector();

        StructField lineStatusField = rowInspector.getStructFieldRef("linestatus");
        int lineStatusFieldIndex = allStructFieldRefs.indexOf(lineStatusField);
        PrimitiveObjectInspector lineStatusFieldInspector = (PrimitiveObjectInspector) lineStatusField.getFieldObjectInspector();

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);
        PrimitiveObjectInspector shipDateFieldInspector = (PrimitiveObjectInspector) shipDateField.getFieldObjectInspector();

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

        for (int i = 0; i < LOOPS; i++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            taxSum = 0;
            returnFlagSum = 0;
            lineStatusSum = 0;
            shipDateSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object quantity = rowInspector.getStructFieldData(rowData, quantityField);
                if (quantity != null) {
                    Object quantityPrimitive = quantityFieldInspector.getPrimitiveJavaObject(quantity);
                    double quantityValue = ((Number) quantityPrimitive).doubleValue();
                    quantitySum += quantityValue;
                }

                Object extendedPrice = rowInspector.getStructFieldData(rowData, extendedPriceField);
                if (extendedPrice != null) {
                    Object extendedPricePrimitive = extendedPriceFieldInspector.getPrimitiveJavaObject(extendedPrice);
                    double extendedPriceValue = ((Number) extendedPricePrimitive).doubleValue();
                    extendedPriceSum += extendedPriceValue;
                }

                Object discount = rowInspector.getStructFieldData(rowData, discountField);
                if (discount != null) {
                    Object discountPrimitive = discountFieldInspector.getPrimitiveJavaObject(discount);
                    double discountValue = ((Number) discountPrimitive).doubleValue();
                    discountSum += discountValue;
                }

                Object tax = rowInspector.getStructFieldData(rowData, taxField);
                if (tax != null) {
                    Object taxPrimitive = taxFieldInspector.getPrimitiveJavaObject(tax);
                    double taxValue = ((Number) taxPrimitive).doubleValue();
                    taxSum += taxValue;
                }

                Object returnFlagData = rowInspector.getStructFieldData(rowData, returnFlagField);
                if (returnFlagData != null) {
                    Object returnFlagPrimitive = returnFlagFieldInspector.getPrimitiveJavaObject(returnFlagData);
                    String returnFlagValue = (String) returnFlagPrimitive;
                    returnFlagSum += returnFlagValue.length();
                }

                Object lineStatusData = rowInspector.getStructFieldData(rowData, lineStatusField);
                if (lineStatusData != null) {
                    Object lineStatusPrimitive = lineStatusFieldInspector.getPrimitiveJavaObject(lineStatusData);
                    String lineStatusValue = (String) lineStatusPrimitive;
                    lineStatusSum += lineStatusValue.length();
                }

                Object shipDateData = rowInspector.getStructFieldData(rowData, shipDateField);
                if (shipDateData != null) {
                    Object shipDatePrimitive = shipDateFieldInspector.getPrimitiveJavaObject(shipDateData);
                    String shipDateValue = (String) shipDatePrimitive;
                    shipDateSum += shipDateValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);
        PrimitiveObjectInspector quantityFieldInspector = (PrimitiveObjectInspector) quantityField.getFieldObjectInspector();

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);
        PrimitiveObjectInspector extendedPriceFieldInspector = (PrimitiveObjectInspector) extendedPriceField.getFieldObjectInspector();

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);
        PrimitiveObjectInspector discountFieldInspector = (PrimitiveObjectInspector) discountField.getFieldObjectInspector();

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);
        PrimitiveObjectInspector shipDateFieldInspector = (PrimitiveObjectInspector) shipDateField.getFieldObjectInspector();

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(
                quantityFieldIndex,
                extendedPriceFieldIndex,
                discountFieldIndex,
                shipDateFieldIndex));

        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        long shipDateSum = 0;

        for (int i = 0; i < LOOPS; i++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object quantity = rowInspector.getStructFieldData(rowData, quantityField);
                if (quantity != null) {
                    Object quantityPrimitive = quantityFieldInspector.getPrimitiveJavaObject(quantity);
                    double quantityValue = ((Number) quantityPrimitive).doubleValue();
                    quantitySum += quantityValue;
                }

                Object extendedPrice = rowInspector.getStructFieldData(rowData, extendedPriceField);
                if (extendedPrice != null) {
                    Object extendedPricePrimitive = extendedPriceFieldInspector.getPrimitiveJavaObject(extendedPrice);
                    double extendedPriceValue = ((Number) extendedPricePrimitive).doubleValue();
                    extendedPriceSum += extendedPriceValue;
                }

                Object discount = rowInspector.getStructFieldData(rowData, discountField);
                if (discount != null) {
                    Object discountPrimitive = discountFieldInspector.getPrimitiveJavaObject(discount);
                    double discountValue = ((Number) discountPrimitive).doubleValue();
                    discountSum += discountValue;
                }

                Object shipDateData = rowInspector.getStructFieldData(rowData, shipDateField);
                if (shipDateData != null) {
                    Object shipDatePrimitive = shipDateFieldInspector.getPrimitiveJavaObject(shipDateData);
                    String shipDateValue = (String) shipDatePrimitive;
                    shipDateSum += shipDateValue.length();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField orderKeyField = rowInspector.getStructFieldRef("orderkey");
        int orderKeyFieldIndex = allStructFieldRefs.indexOf(orderKeyField);
        PrimitiveObjectInspector orderKeyFieldInspector = (PrimitiveObjectInspector) orderKeyField.getFieldObjectInspector();

        StructField partKeyField = rowInspector.getStructFieldRef("partkey");
        int partKeyFieldIndex = allStructFieldRefs.indexOf(partKeyField);
        PrimitiveObjectInspector partKeyFieldInspector = (PrimitiveObjectInspector) partKeyField.getFieldObjectInspector();

        StructField supplierKeyField = rowInspector.getStructFieldRef("suppkey");
        int supplierKeyFieldIndex = allStructFieldRefs.indexOf(supplierKeyField);
        PrimitiveObjectInspector supplierKeyFieldInspector = (PrimitiveObjectInspector) supplierKeyField.getFieldObjectInspector();

        StructField lineNumberField = rowInspector.getStructFieldRef("linenumber");
        int lineNumberFieldIndex = allStructFieldRefs.indexOf(lineNumberField);
        PrimitiveObjectInspector lineNumberFieldInspector = (PrimitiveObjectInspector) lineNumberField.getFieldObjectInspector();

        StructField quantityField = rowInspector.getStructFieldRef("quantity");
        int quantityFieldIndex = allStructFieldRefs.indexOf(quantityField);
        PrimitiveObjectInspector quantityFieldInspector = (PrimitiveObjectInspector) quantityField.getFieldObjectInspector();

        StructField extendedPriceField = rowInspector.getStructFieldRef("extendedprice");
        int extendedPriceFieldIndex = allStructFieldRefs.indexOf(extendedPriceField);
        PrimitiveObjectInspector extendedPriceFieldInspector = (PrimitiveObjectInspector) extendedPriceField.getFieldObjectInspector();

        StructField discountField = rowInspector.getStructFieldRef("discount");
        int discountFieldIndex = allStructFieldRefs.indexOf(discountField);
        PrimitiveObjectInspector discountFieldInspector = (PrimitiveObjectInspector) discountField.getFieldObjectInspector();

        StructField taxField = rowInspector.getStructFieldRef("tax");
        int taxFieldIndex = allStructFieldRefs.indexOf(taxField);
        PrimitiveObjectInspector taxFieldInspector = (PrimitiveObjectInspector) taxField.getFieldObjectInspector();

        StructField returnFlagField = rowInspector.getStructFieldRef("returnflag");
        int returnFlagFieldIndex = allStructFieldRefs.indexOf(returnFlagField);
        PrimitiveObjectInspector returnFlagFieldInspector = (PrimitiveObjectInspector) returnFlagField.getFieldObjectInspector();

        StructField lineStatusField = rowInspector.getStructFieldRef("linestatus");
        int lineStatusFieldIndex = allStructFieldRefs.indexOf(lineStatusField);
        PrimitiveObjectInspector lineStatusFieldInspector = (PrimitiveObjectInspector) lineStatusField.getFieldObjectInspector();

        StructField shipDateField = rowInspector.getStructFieldRef("shipdate");
        int shipDateFieldIndex = allStructFieldRefs.indexOf(shipDateField);
        PrimitiveObjectInspector shipDateFieldInspector = (PrimitiveObjectInspector) shipDateField.getFieldObjectInspector();

        StructField commitDateField = rowInspector.getStructFieldRef("commitdate");
        int commitDateFieldIndex = allStructFieldRefs.indexOf(commitDateField);
        PrimitiveObjectInspector commitDateFieldInspector = (PrimitiveObjectInspector) commitDateField.getFieldObjectInspector();

        StructField receiptDateField = rowInspector.getStructFieldRef("receiptdate");
        int receiptDateFieldIndex = allStructFieldRefs.indexOf(receiptDateField);
        PrimitiveObjectInspector receiptDateFieldInspector = (PrimitiveObjectInspector) receiptDateField.getFieldObjectInspector();

        StructField shipInstructionsField = rowInspector.getStructFieldRef("shipinstruct");
        int shipInstructionsFieldIndex = allStructFieldRefs.indexOf(shipInstructionsField);
        PrimitiveObjectInspector shipInstructionsFieldInspector = (PrimitiveObjectInspector) shipInstructionsField.getFieldObjectInspector();

        StructField shipModeField = rowInspector.getStructFieldRef("shipmode");
        int shipModeFieldIndex = allStructFieldRefs.indexOf(shipModeField);
        PrimitiveObjectInspector shipModeFieldInspector = (PrimitiveObjectInspector) shipModeField.getFieldObjectInspector();

        StructField commentField = rowInspector.getStructFieldRef("comment");
        int commentFieldIndex = allStructFieldRefs.indexOf(commentField);
        PrimitiveObjectInspector commentFieldInspector = (PrimitiveObjectInspector) commentField.getFieldObjectInspector();

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

        for (int i = 0; i < LOOPS; i++) {
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

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);
                rowCount++;

                Object orderKeyData = rowInspector.getStructFieldData(rowData, orderKeyField);
                if (orderKeyData != null) {
                    Object orderKeyPrimitive = orderKeyFieldInspector.getPrimitiveJavaObject(orderKeyData);
                    long orderKeyValue = ((Number) orderKeyPrimitive).longValue();
                    orderKeySum += orderKeyValue;
                }

                Object partKeyData = rowInspector.getStructFieldData(rowData, partKeyField);
                if (partKeyData != null) {
                    Object partKeyPrimitive = partKeyFieldInspector.getPrimitiveJavaObject(partKeyData);
                    long partKeyValue = ((Number) partKeyPrimitive).longValue();
                    partKeySum += partKeyValue;
                }

                Object supplierKeyData = rowInspector.getStructFieldData(rowData, supplierKeyField);
                if (supplierKeyData != null) {
                    Object supplierKeyPrimitive = supplierKeyFieldInspector.getPrimitiveJavaObject(supplierKeyData);
                    long supplierKeyValue = ((Number) supplierKeyPrimitive).longValue();
                    supplierKeySum += supplierKeyValue;
                }

                Object lineNumberData = rowInspector.getStructFieldData(rowData, lineNumberField);
                if (lineNumberData != null) {
                    Object lineNumberPrimitive = lineNumberFieldInspector.getPrimitiveJavaObject(lineNumberData);
                    long lineNumberValue = ((Number) lineNumberPrimitive).longValue();
                    lineNumberSum += lineNumberValue;
                }

                Object quantity = rowInspector.getStructFieldData(rowData, quantityField);
                if (quantity != null) {
                    Object quantityPrimitive = quantityFieldInspector.getPrimitiveJavaObject(quantity);
                    double quantityValue = ((Number) quantityPrimitive).doubleValue();
                    quantitySum += quantityValue;
                }

                Object extendedPrice = rowInspector.getStructFieldData(rowData, extendedPriceField);
                if (extendedPrice != null) {
                    Object extendedPricePrimitive = extendedPriceFieldInspector.getPrimitiveJavaObject(extendedPrice);
                    double extendedPriceValue = ((Number) extendedPricePrimitive).doubleValue();
                    extendedPriceSum += extendedPriceValue;
                }

                Object discount = rowInspector.getStructFieldData(rowData, discountField);
                if (discount != null) {
                    Object discountPrimitive = discountFieldInspector.getPrimitiveJavaObject(discount);
                    double discountValue = ((Number) discountPrimitive).doubleValue();
                    discountSum += discountValue;
                }

                Object tax = rowInspector.getStructFieldData(rowData, taxField);
                if (tax != null) {
                    Object taxPrimitive = taxFieldInspector.getPrimitiveJavaObject(tax);
                    double taxValue = ((Number) taxPrimitive).doubleValue();
                    taxSum += taxValue;
                }

                Object returnFlagData = rowInspector.getStructFieldData(rowData, returnFlagField);
                if (returnFlagData != null) {
                    Object returnFlagPrimitive = returnFlagFieldInspector.getPrimitiveJavaObject(returnFlagData);
                    String returnFlagValue = (String) returnFlagPrimitive;
                    returnFlagSum += returnFlagValue.length();
                }

                Object lineStatusData = rowInspector.getStructFieldData(rowData, lineStatusField);
                if (lineStatusData != null) {
                    Object lineStatusPrimitive = lineStatusFieldInspector.getPrimitiveJavaObject(lineStatusData);
                    String lineStatusValue = (String) lineStatusPrimitive;
                    lineStatusSum += lineStatusValue.length();
                }

                Object shipDateData = rowInspector.getStructFieldData(rowData, shipDateField);
                if (shipDateData != null) {
                    Object shipDatePrimitive = shipDateFieldInspector.getPrimitiveJavaObject(shipDateData);
                    String shipDateValue = (String) shipDatePrimitive;
                    shipDateSum += shipDateValue.length();
                }

                Object commitDateData = rowInspector.getStructFieldData(rowData, commitDateField);
                if (commitDateData != null) {
                    Object commitDatePrimitive = commitDateFieldInspector.getPrimitiveJavaObject(commitDateData);
                    String commitDateValue = (String) commitDatePrimitive;
                    commitDateSum += commitDateValue.length();
                }

                Object receiptDateData = rowInspector.getStructFieldData(rowData, receiptDateField);
                if (receiptDateData != null) {
                    Object receiptDatePrimitive = receiptDateFieldInspector.getPrimitiveJavaObject(receiptDateData);
                    String receiptDateValue = (String) receiptDatePrimitive;
                    receiptDateSum += receiptDateValue.length();
                }

                Object shipInstructionsData = rowInspector.getStructFieldData(rowData, shipInstructionsField);
                if (shipInstructionsData != null) {
                    Object shipInstructionsPrimitive = shipInstructionsFieldInspector.getPrimitiveJavaObject(shipInstructionsData);
                    String shipInstructionsValue = (String) shipInstructionsPrimitive;
                    shipInstructionsSum += shipInstructionsValue.length();
                }

                Object shipModeData = rowInspector.getStructFieldData(rowData, shipModeField);
                if (shipModeData != null) {
                    Object shipModePrimitive = shipModeFieldInspector.getPrimitiveJavaObject(shipModeData);
                    String shipModeValue = (String) shipModePrimitive;
                    shipModeSum += shipModeValue.length();
                }

                Object commentData = rowInspector.getStructFieldData(rowData, commentField);
                if (commentData != null) {
                    Object commentPrimitive = commentFieldInspector.getPrimitiveJavaObject(commentData);
                    String commentValue = (String) commentPrimitive;
                    commentSum += commentValue.length();
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

    @Override
    public <K, V extends Writable> List<Object> allReadOne(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField orderKeyField = rowInspector.getStructFieldRef("orderkey");
        int orderKeyFieldIndex = allStructFieldRefs.indexOf(orderKeyField);
        PrimitiveObjectInspector orderKeyFieldInspector = (PrimitiveObjectInspector) orderKeyField.getFieldObjectInspector();

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

        for (int i = 0; i < LOOPS; i++) {
            orderKeySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                Object rowData = deserializer.deserialize(value);

                Object orderKeyData = rowInspector.getStructFieldData(rowData, orderKeyField);
                if (orderKeyData != null) {
                    Object orderKeyPrimitive = orderKeyFieldInspector.getPrimitiveJavaObject(orderKeyData);
                    long orderKeyValue = ((Number) orderKeyPrimitive).longValue();
                    orderKeySum += orderKeyValue;
                }

            }
            recordReader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }
}
