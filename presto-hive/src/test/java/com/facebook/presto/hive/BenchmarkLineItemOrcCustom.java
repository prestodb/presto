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
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcStructUtil;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public final class BenchmarkLineItemOrcCustom
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
        FileSystem fileSystem = fileSplit.getPath().getFileSystem(jobConf);
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<StructField> allStructFieldRefs = ImmutableList.copyOf(rowInspector.getAllStructFieldRefs());
        StructField bigintField = rowInspector.getStructFieldRef("orderkey");
        int fieldIndex = allStructFieldRefs.indexOf(bigintField);

        boolean[] include = new boolean[rowInspector.getAllStructFieldRefs().size() + 1];
        include[fieldIndex + 1] = true;

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable bigintValue = (LongWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable bigintValue = (LongWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable bigintValue = (LongWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable bigintValue = (LongWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable bigintValue = (LongWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (bigintValue != null) {
                    bigintSum += bigintValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                DoubleWritable doubleValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                DoubleWritable doubleValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            doubleSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                DoubleWritable doubleValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (doubleValue != null) {
                    doubleSum += doubleValue.get();
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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
        for (int i = 0; i < LOOPS; i++) {
            stringLengthSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                Text stringText = (Text) OrcStructUtil.getFieldValue(row, fieldIndex);
                if (stringText != null) {
                    byte[] stringValue = Arrays.copyOfRange(stringText.getBytes(), 0, stringText.getLength());
                    stringLengthSum += stringValue.length;
                }
            }
            rows.close();
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

        for (int i = 0; i < LOOPS; i++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            taxSum = 0;
            returnFlagSum = 0;
            lineStatusSum = 0;
            shipDateSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable quantityValue = (LongWritable) OrcStructUtil.getFieldValue(row, quantityFieldIndex);
                if (quantityValue != null) {
                    quantitySum += quantityValue.get();
                }

                DoubleWritable extendedPriceValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, extendedPriceFieldIndex);
                if (extendedPriceValue != null) {
                    extendedPriceSum += extendedPriceValue.get();
                }

                DoubleWritable discountValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, discountFieldIndex);
                if (discountValue != null) {
                    discountSum += discountValue.get();
                }

                DoubleWritable taxValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, taxFieldIndex);
                if (taxValue != null) {
                    taxSum += taxValue.get();
                }

                Text returnFlagText = (Text) OrcStructUtil.getFieldValue(row, returnFlagFieldIndex);
                if (returnFlagText != null) {
                    byte[] returnFlagValue = Arrays.copyOfRange(returnFlagText.getBytes(), 0, returnFlagText.getLength());
                    returnFlagSum += returnFlagValue.length;
                }

                Text lineStatusText = (Text) OrcStructUtil.getFieldValue(row, lineStatusFieldIndex);
                if (lineStatusText != null) {
                    byte[] lineStatusValue = Arrays.copyOfRange(lineStatusText.getBytes(), 0, lineStatusText.getLength());
                    lineStatusSum += lineStatusValue.length;
                }

                Text shipDateText = (Text) OrcStructUtil.getFieldValue(row, shipDateFieldIndex);
                if (shipDateText != null) {
                    byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
                    shipDateSum += shipDateValue.length;
                }
            }
            rows.close();
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

        for (int i = 0; i < LOOPS; i++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable quantityValue = (LongWritable) OrcStructUtil.getFieldValue(row, quantityFieldIndex);
                if (quantityValue != null) {
                    quantitySum += quantityValue.get();
                }

                DoubleWritable extendedPriceValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, extendedPriceFieldIndex);
                if (extendedPriceValue != null) {
                    extendedPriceSum += extendedPriceValue.get();
                }

                DoubleWritable discountValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, discountFieldIndex);
                if (discountValue != null) {
                    discountSum += discountValue.get();
                }

                Text shipDateText = (Text) OrcStructUtil.getFieldValue(row, shipDateFieldIndex);
                if (shipDateText != null) {
                    byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
                    shipDateSum += shipDateValue.length;
                }
            }
            rows.close();
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

        long rowsProcessed = 0;
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
            rowsProcessed = 0;
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

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);
                rowsProcessed++;

                LongWritable orderKeyValue = (LongWritable) OrcStructUtil.getFieldValue(row, orderKeyFieldIndex);
                if (orderKeyValue != null) {
                    orderKeySum += orderKeyValue.get();
                }

                LongWritable partKeyValue = (LongWritable) OrcStructUtil.getFieldValue(row, partKeyFieldIndex);
                if (partKeyValue != null) {
                    partKeySum += partKeyValue.get();
                }

                LongWritable supplierKeyValue = (LongWritable) OrcStructUtil.getFieldValue(row, supplierKeyFieldIndex);
                if (supplierKeyValue != null) {
                    supplierKeySum += supplierKeyValue.get();
                }

                LongWritable lineNumberValue = (LongWritable) OrcStructUtil.getFieldValue(row, lineNumberFieldIndex);
                if (lineNumberValue != null) {
                    lineNumberSum += lineNumberValue.get();
                }

                LongWritable quantityValue = (LongWritable) OrcStructUtil.getFieldValue(row, quantityFieldIndex);
                if (quantityValue != null) {
                    quantitySum += quantityValue.get();
                }

                DoubleWritable extendedPriceValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, extendedPriceFieldIndex);
                if (extendedPriceValue != null) {
                    extendedPriceSum += extendedPriceValue.get();
                }

                DoubleWritable discountValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, discountFieldIndex);
                if (discountValue != null) {
                    discountSum += discountValue.get();
                }

                DoubleWritable taxValue = (DoubleWritable) OrcStructUtil.getFieldValue(row, taxFieldIndex);
                if (taxValue != null) {
                    taxSum += taxValue.get();
                }

                Text returnFlagText = (Text) OrcStructUtil.getFieldValue(row, returnFlagFieldIndex);
                if (returnFlagText != null) {
                    byte[] returnFlagValue = Arrays.copyOfRange(returnFlagText.getBytes(), 0, returnFlagText.getLength());
                    returnFlagSum += returnFlagValue.length;
                }

                Text lineStatusText = (Text) OrcStructUtil.getFieldValue(row, lineStatusFieldIndex);
                if (lineStatusText != null) {
                    byte[] lineStatusValue = Arrays.copyOfRange(lineStatusText.getBytes(), 0, lineStatusText.getLength());
                    lineStatusSum += lineStatusValue.length;
                }

                Text shipDateText = (Text) OrcStructUtil.getFieldValue(row, shipDateFieldIndex);
                if (shipDateText != null) {
                    byte[] shipDateValue = Arrays.copyOfRange(shipDateText.getBytes(), 0, shipDateText.getLength());
                    shipDateSum += shipDateValue.length;
                }

                Text commitDateText = (Text) OrcStructUtil.getFieldValue(row, commitDateFieldIndex);
                if (commitDateText != null) {
                    byte[] commitDateValue = Arrays.copyOfRange(commitDateText.getBytes(), 0, commitDateText.getLength());
                    commitDateSum += commitDateValue.length;
                }

                Text receiptDateText = (Text) OrcStructUtil.getFieldValue(row, receiptDateFieldIndex);
                if (receiptDateText != null) {
                    byte[] receiptDateValue = Arrays.copyOfRange(receiptDateText.getBytes(), 0, receiptDateText.getLength());
                    receiptDateSum += receiptDateValue.length;
                }

                Text shipInstructionsText = (Text) OrcStructUtil.getFieldValue(row, shipInstructionsFieldIndex);
                if (shipInstructionsText != null) {
                    byte[] shipInstructionsValue = Arrays.copyOfRange(shipInstructionsText.getBytes(), 0, shipInstructionsText.getLength());
                    shipInstructionsSum += shipInstructionsValue.length;
                }

                Text shipModeText = (Text) OrcStructUtil.getFieldValue(row, shipModeFieldIndex);
                if (shipModeText != null) {
                    byte[] shipModeValue = Arrays.copyOfRange(shipModeText.getBytes(), 0, shipModeText.getLength());
                    shipModeSum += shipModeValue.length;
                }

                Text commentText = (Text) OrcStructUtil.getFieldValue(row, commentFieldIndex);
                if (commentText != null) {
                    byte[] commentValue = Arrays.copyOfRange(commentText.getBytes(), 0, commentText.getLength());
                    commentSum += commentValue.length;
                }
            }
            rows.close();
        }

        return ImmutableList.<Object>of(
                rowsProcessed,
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

        for (int i = 0; i < LOOPS; i++) {
            orderKeySum = 0;

            RecordReader rows = createRecordReader(fileSystem, fileSplit, jobConf, include);

            OrcStruct row = null;
            while (rows.hasNext()) {
                row = (OrcStruct) rows.next(row);

                LongWritable orderKeyValue = (LongWritable) OrcStructUtil.getFieldValue(row, orderKeyFieldIndex);
                if (orderKeyValue != null) {
                    orderKeySum += orderKeyValue.get();
                }

            }
            rows.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }

    private static RecordReader createRecordReader(FileSystem fileSystem, FileSplit fileSplit, JobConf jobConf, boolean[] include)
            throws IOException
    {
        Reader reader = OrcFile.createReader(fileSystem, fileSplit.getPath());

        return reader.rows(fileSplit.getStart(), fileSplit.getLength(), include);
    }
}
