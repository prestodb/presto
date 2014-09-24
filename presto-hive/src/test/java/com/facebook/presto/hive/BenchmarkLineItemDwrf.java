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

import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public final class BenchmarkLineItemDwrf
        implements BenchmarkLineItem
{
    @Override
    public String getName()
    {
        return "custom";
    }

    @Override
    public <K, V extends Writable> long orderKey(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("orderkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject bigintData = row.getFieldValue(fieldIndex);
                if (bigintData != null) {
                    LongWritable bigintValue = (LongWritable) bigintData.materialize();
                    if (bigintValue != null) {
                        bigintSum += bigintValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("partkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject bigintData = row.getFieldValue(fieldIndex);
                if (bigintData != null) {
                    LongWritable bigintValue = (LongWritable) bigintData.materialize();
                    if (bigintValue != null) {
                        bigintSum += bigintValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("suppkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject bigintData = row.getFieldValue(fieldIndex);
                if (bigintData != null) {
                    LongWritable bigintValue = (LongWritable) bigintData.materialize();
                    if (bigintValue != null) {
                        bigintSum += bigintValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("linenumber");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject bigintData = row.getFieldValue(fieldIndex);
                if (bigintData != null) {
                    LongWritable bigintValue = (LongWritable) bigintData.materialize();
                    if (bigintValue != null) {
                        bigintSum += bigintValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("quantity");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject bigintData = row.getFieldValue(fieldIndex);
                if (bigintData != null) {
                    LongWritable bigintValue = (LongWritable) bigintData.materialize();
                    if (bigintValue != null) {
                        bigintSum += bigintValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("extendedprice");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject doubleData = row.getFieldValue(fieldIndex);
                if (doubleData != null) {
                    DoubleWritable doubleValue = (DoubleWritable) doubleData.materialize();
                    if (doubleValue != null) {
                        doubleSum += doubleValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("discount");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject doubleData = row.getFieldValue(fieldIndex);
                if (doubleData != null) {
                    DoubleWritable doubleValue = (DoubleWritable) doubleData.materialize();
                    if (doubleValue != null) {
                        doubleSum += doubleValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField doubleField = rowInspector.getStructFieldRef("tax");
        int fieldIndex = allStructFieldRefs.indexOf(doubleField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        double doubleSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject doubleData = row.getFieldValue(fieldIndex);
                if (doubleData != null) {
                    DoubleWritable doubleValue = (DoubleWritable) doubleData.materialize();
                    if (doubleValue != null) {
                        doubleSum += doubleValue.get();
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("returnflag");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("linestatus");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("commitdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("receiptdate");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipinstruct");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("shipmode");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());

        StructField stringField = rowInspector.getStructFieldRef("comment");
        int fieldIndex = allStructFieldRefs.indexOf(stringField);

        ColumnProjectionUtils.setReadColumnIDs(jobConf, ImmutableList.of(fieldIndex));

        long stringLengthSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;
            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject stringData = row.getFieldValue(fieldIndex);
                if (stringData != null) {
                    Text stringText = (Text) stringData.materialize();
                    if (stringText != null) {
                        byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                        stringLengthSum += stringValue.length;
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
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject quantityData = row.getFieldValue(quantityFieldIndex);
                if (quantityData != null) {
                    LongWritable quantityValue = (LongWritable) quantityData.materialize();
                    if (quantityValue != null) {
                        quantitySum += quantityValue.get();
                    }
                }

                OrcLazyObject extendedPriceData = row.getFieldValue(extendedPriceFieldIndex);
                if (extendedPriceData != null) {
                    DoubleWritable extendedPriceValue = (DoubleWritable) extendedPriceData.materialize();
                    if (extendedPriceValue != null) {
                        extendedPriceSum += extendedPriceValue.get();
                    }
                }

                OrcLazyObject discountData = row.getFieldValue(discountFieldIndex);
                if (discountData != null) {
                    DoubleWritable discountValue = (DoubleWritable) discountData.materialize();
                    if (discountValue != null) {
                        discountSum += discountValue.get();
                    }
                }

                OrcLazyObject taxData = row.getFieldValue(taxFieldIndex);
                if (taxData != null) {
                    DoubleWritable taxValue = (DoubleWritable) taxData.materialize();
                    if (taxValue != null) {
                        taxSum += taxValue.get();
                    }
                }

                OrcLazyObject returnFlagData = row.getFieldValue(returnFlagFieldIndex);
                if (returnFlagData != null) {
                    Text returnFlagText = (Text) returnFlagData.materialize();
                    if (returnFlagText != null) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagText.getBytes(), 0, returnFlagText.getLength());
                        returnFlagSum += returnFlagValue.length;
                    }
                }

                OrcLazyObject lineStatusData = row.getFieldValue(lineStatusFieldIndex);
                if (lineStatusData != null) {
                    Text lineStatusText = (Text) lineStatusData.materialize();
                    if (lineStatusText != null) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusText.getBytes(), 0, lineStatusText.getLength());
                        lineStatusSum += lineStatusValue.length;
                    }
                }

                OrcLazyObject shipDateData = row.getFieldValue(shipDateFieldIndex);
                if (shipDateData != null) {
                    Text shipDateText = (Text) shipDateData.materialize();
                    if (shipDateText != null) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
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

        for (int i = 0; i < LOOPS; i++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject quantityData = row.getFieldValue(quantityFieldIndex);
                if (quantityData != null) {
                    LongWritable quantityValue = (LongWritable) quantityData.materialize();
                    if (quantityValue != null) {
                        quantitySum += quantityValue.get();
                    }
                }

                OrcLazyObject extendedPriceData = row.getFieldValue(extendedPriceFieldIndex);
                if (extendedPriceData != null) {
                    DoubleWritable extendedPriceValue = (DoubleWritable) extendedPriceData.materialize();
                    if (extendedPriceValue != null) {
                        extendedPriceSum += extendedPriceValue.get();
                    }
                }

                OrcLazyObject discountData = row.getFieldValue(discountFieldIndex);
                if (discountData != null) {
                    DoubleWritable discountValue = (DoubleWritable) discountData.materialize();
                    if (discountValue != null) {
                        discountSum += discountValue.get();
                    }
                }

                OrcLazyObject shipDateData = row.getFieldValue(shipDateFieldIndex);
                if (shipDateData != null) {
                    Text shipDateText = (Text) shipDateData.materialize();
                    if (shipDateText != null) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
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
                OrcLazyRow row = (OrcLazyRow) value;

                rowCount++;

                OrcLazyObject orderKeyData = row.getFieldValue(orderKeyFieldIndex);
                if (orderKeyData != null) {
                    LongWritable orderKeyValue = (LongWritable) orderKeyData.materialize();
                    if (orderKeyValue != null) {
                        orderKeySum += orderKeyValue.get();
                    }
                }

                OrcLazyObject partKeyData = row.getFieldValue(partKeyFieldIndex);
                if (partKeyData != null) {
                    LongWritable partKeyValue = (LongWritable) partKeyData.materialize();
                    if (partKeyValue != null) {
                        partKeySum += partKeyValue.get();
                    }
                }

                OrcLazyObject supplierKeyData = row.getFieldValue(supplierKeyFieldIndex);
                if (supplierKeyData != null) {
                    LongWritable supplierKeyValue = (LongWritable) supplierKeyData.materialize();
                    if (supplierKeyValue != null) {
                        supplierKeySum += supplierKeyValue.get();
                    }
                }

                OrcLazyObject lineNumberData = row.getFieldValue(lineNumberFieldIndex);
                if (lineNumberData != null) {
                    LongWritable lineNumberValue = (LongWritable) lineNumberData.materialize();
                    if (lineNumberValue != null) {
                        lineNumberSum += lineNumberValue.get();
                    }
                }

                OrcLazyObject quantityData = row.getFieldValue(quantityFieldIndex);
                if (quantityData != null) {
                    LongWritable quantityValue = (LongWritable) quantityData.materialize();
                    if (quantityValue != null) {
                        quantitySum += quantityValue.get();
                    }
                }

                OrcLazyObject extendedPriceData = row.getFieldValue(extendedPriceFieldIndex);
                if (extendedPriceData != null) {
                    DoubleWritable extendedPriceValue = (DoubleWritable) extendedPriceData.materialize();
                    if (extendedPriceValue != null) {
                        extendedPriceSum += extendedPriceValue.get();
                    }
                }

                OrcLazyObject discountData = row.getFieldValue(discountFieldIndex);
                if (discountData != null) {
                    DoubleWritable discountValue = (DoubleWritable) discountData.materialize();
                    if (discountValue != null) {
                        discountSum += discountValue.get();
                    }
                }

                OrcLazyObject taxData = row.getFieldValue(taxFieldIndex);
                if (taxData != null) {
                    DoubleWritable taxValue = (DoubleWritable) taxData.materialize();
                    if (taxValue != null) {
                        taxSum += taxValue.get();
                    }
                }

                OrcLazyObject returnFlagData = row.getFieldValue(returnFlagFieldIndex);
                if (returnFlagData != null) {
                    Text returnFlagText = (Text) returnFlagData.materialize();
                    if (returnFlagText != null) {
                        byte[] returnFlagValue = Arrays.copyOfRange(returnFlagText.getBytes(), 0, returnFlagText.getLength());
                        returnFlagSum += returnFlagValue.length;
                    }
                }

                OrcLazyObject lineStatusData = row.getFieldValue(lineStatusFieldIndex);
                if (lineStatusData != null) {
                    Text lineStatusText = (Text) lineStatusData.materialize();
                    if (lineStatusText != null) {
                        byte[] lineStatusValue = Arrays.copyOfRange(lineStatusText.getBytes(), 0, lineStatusText.getLength());
                        lineStatusSum += lineStatusValue.length;
                    }
                }

                OrcLazyObject shipDateData = row.getFieldValue(shipDateFieldIndex);
                if (shipDateData != null) {
                    Text shipDateText = (Text) shipDateData.materialize();
                    if (shipDateText != null) {
                        byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
                        shipDateSum += shipDateValue.length;
                    }
                }

                OrcLazyObject commitDateData = row.getFieldValue(commitDateFieldIndex);
                if (commitDateData != null) {
                    Text commitDateText = (Text) commitDateData.materialize();
                    if (commitDateText != null) {
                        byte[] commitDateValue = Arrays.copyOfRange(commitDateText.getBytes(), 0, commitDateText.getLength());
                        commitDateSum += commitDateValue.length;
                    }
                }

                OrcLazyObject receiptDateData = row.getFieldValue(receiptDateFieldIndex);
                if (receiptDateData != null) {
                    Text receiptDateText = (Text) receiptDateData.materialize();
                    if (receiptDateText != null) {
                        byte[] receiptDateValue = Arrays.copyOfRange(receiptDateText.getBytes(), 0, receiptDateText.getLength());
                        receiptDateSum += receiptDateValue.length;
                    }
                }

                OrcLazyObject shipInstructionsData = row.getFieldValue(shipInstructionsFieldIndex);
                if (shipInstructionsData != null) {
                    Text shipInstructionsText = (Text) shipInstructionsData.materialize();
                    if (shipInstructionsText != null) {
                        byte[] shipInstructionsValue = Arrays.copyOfRange(shipInstructionsText.getBytes(), 0, shipInstructionsText.getLength());
                        shipInstructionsSum += shipInstructionsValue.length;
                    }
                }

                OrcLazyObject shipModeData = row.getFieldValue(shipModeFieldIndex);
                if (shipModeData != null) {
                    Text shipModeText = (Text) shipModeData.materialize();
                    if (shipModeText != null) {
                        byte[] shipModeValue = Arrays.copyOfRange(shipModeText.getBytes(), 0, shipModeText.getLength());
                        shipModeSum += shipModeValue.length;
                    }
                }

                OrcLazyObject commentData = row.getFieldValue(commentFieldIndex);
                if (commentData != null) {
                    Text commentText = (Text) commentData.materialize();
                    if (commentText != null) {
                        byte[] commentValue = Arrays.copyOfRange(commentText.getBytes(), 0, commentText.getLength());
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

    @Override
    public <K, V extends Writable> List<Object> allReadOne(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
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

        for (int i = 0; i < LOOPS; i++) {
            orderKeySum = 0;

            RecordReader<K, V> recordReader = inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
            K key = recordReader.createKey();
            V value = recordReader.createValue();

            while (recordReader.next(key, value)) {
                OrcLazyRow row = (OrcLazyRow) value;

                OrcLazyObject orderKeyData = row.getFieldValue(orderKeyFieldIndex);
                if (orderKeyData != null) {
                    LongWritable orderKeyValue = (LongWritable) orderKeyData.materialize();
                    if (orderKeyValue != null) {
                        orderKeySum += orderKeyValue.get();
                    }
                }
            }
            recordReader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }
}
