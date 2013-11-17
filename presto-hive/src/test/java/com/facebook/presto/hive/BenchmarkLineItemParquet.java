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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.slice.Slice;
import org.apache.hadoop.hive.ql.io.slice.Slices;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import parquet.column.Dictionary;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;

public final class BenchmarkLineItemParquet
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
        jobConf.set(IOConstants.COLUMNS, Joiner.on(',').join(COLUMN_NAMES));

        long bigintSum = 0;
        for (int i = 0; i < LOOPS; i++) {
            bigintSum = 0;

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "orderkey");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    bigintSum += record.getLong(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "partkey");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    bigintSum += record.getLong(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "suppkey");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    bigintSum += record.getLong(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "linenumber");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    bigintSum += record.getLong(0);
                }
            }
            realReader.close();
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
            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "quantity");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    bigintSum += record.getLong(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "extendedprice");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    doubleSum += record.getDouble(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "discount");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    doubleSum += record.getDouble(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "tax");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    doubleSum += record.getDouble(0);
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "returnflag");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "linestatus");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "shipdate");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "commitdate");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "receiptdate");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "shipinstruct");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "shipmode");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "comment");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                if (!record.isNull(0)) {
                    stringLengthSum += record.getSlice(0).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit,
                    "quantity",
                    "extendedprice",
                    "discount",
                    "tax",
                    "returnflag",
                    "linestatus",
                    "shipdate");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();

                if (!record.isNull(0)) {
                    quantitySum += record.getLong(0);
                }

                if (!record.isNull(1)) {
                    extendedPriceSum += record.getDouble(1);
                }

                if (!record.isNull(2)) {
                    discountSum += record.getDouble(2);
                }

                if (!record.isNull(3)) {
                    taxSum += record.getDouble(3);
                }

                if (!record.isNull(4)) {
                    returnFlagSum += record.getSlice(4).length();
                }

                if (!record.isNull(5)) {
                    lineStatusSum += record.getSlice(5).length();
                }

                if (!record.isNull(6)) {
                    shipDateSum += record.getSlice(6).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit, "quantity", "extendedprice", "discount", "shipdate");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();

                if (!record.isNull(0)) {
                    quantitySum += record.getLong(0);
                }

                if (!record.isNull(1)) {
                    extendedPriceSum += record.getDouble(1);
                }

                if (!record.isNull(2)) {
                    discountSum += record.getDouble(2);
                }

                if (!record.isNull(3)) {
                    shipDateSum += record.getSlice(3).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit,
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
                    "comment");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();
                rowCount++;

                if (!record.isNull(0)) {
                    orderKeySum += record.getLong(0);
                }

                if (!record.isNull(1)) {
                    partKeySum += record.getLong(1);
                }

                if (!record.isNull(2)) {
                    supplierKeySum += record.getLong(2);
                }

                if (!record.isNull(3)) {
                    lineNumberSum += record.getLong(3);
                }

                if (!record.isNull(4)) {
                    quantitySum += record.getLong(4);
                }

                if (!record.isNull(5)) {
                    extendedPriceSum += record.getDouble(5);
                }

                if (!record.isNull(6)) {
                    discountSum += record.getDouble(6);
                }

                if (!record.isNull(7)) {
                    taxSum += record.getDouble(7);
                }

                if (!record.isNull(8)) {
                    returnFlagSum += record.getSlice(8).length();
                }

                if (!record.isNull(9)) {
                    lineStatusSum += record.getSlice(9).length();
                }

                if (!record.isNull(10)) {
                    shipDateSum += record.getSlice(10).length();
                }

                if (!record.isNull(11)) {
                    commitDateSum += record.getSlice(11).length();
                }

                if (!record.isNull(12)) {
                    receiptDateSum += record.getSlice(12).length();
                }

                if (!record.isNull(13)) {
                    shipInstructionsSum += record.getSlice(13).length();
                }

                if (!record.isNull(14)) {
                    shipModeSum += record.getSlice(14).length();
                }

                if (!record.isNull(15)) {
                    commentSum += record.getSlice(15).length();
                }
            }
            realReader.close();
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

            ParquetRecordReader<NullRecord> realReader = createParquetRecordReader(jobConf, fileSplit,
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
                    "comment");

            while (realReader.nextKeyValue()) {
                NullRecord record = realReader.getCurrentValue();

                if (!record.isNull(0)) {
                    orderKeySum += record.getLong(0);
                }
            }
            realReader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }

    private <T> ParquetRecordReader<T> createParquetRecordReader(JobConf jobConf, FileSplit fileSplit, String columnName, String... additionalColumnNames)
            throws IOException, InterruptedException
    {
        ReadSupport readSupport = new PrestoReadSupport(columnName, additionalColumnNames);

//        conf = projectionPusher.pushProjectionsAndFilters(conf, oldSplit.getPath().getParent());

        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(jobConf, fileSplit.getPath());
        List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

        ReadContext readContext = readSupport.init(jobConf, fileMetaData.getKeyValueMetaData(), fileMetaData.getSchema());

//        int schemaSize = MessageTypeParser.parseMessageType(readContext.getReadSupportMetadata()
//                .get(DataWritableReadSupport.HIVE_SCHEMA_KEY)).getFieldCount();

        List<BlockMetaData> splitGroup = new ArrayList<>();
        long splitStart = fileSplit.getStart();
        long splitLength = fileSplit.getLength();
        for (BlockMetaData block : blocks) {
            long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
            if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                splitGroup.add(block);
            }
        }

        ParquetInputSplit split;
        if (splitGroup.isEmpty()) {
            // LOG.warn("Skipping split, could not find row group in: " + (FileSplit) oldSplit);
            split = null;
        }
        else {
            split = new ParquetInputSplit(fileSplit.getPath(),
                    splitStart,
                    splitLength,
                    fileSplit.getLocations(),
                    splitGroup,
                    readContext.getRequestedSchema().toString(),
                    fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(),
                    readContext.getReadSupportMetadata());
        }

        TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(jobConf, new TaskAttemptID());
        ParquetRecordReader<T> realReader = new ParquetRecordReader<T>(readSupport);
        realReader.initialize(split, taskContext);
        return realReader;
    }

    public static class PrestoReadSupport
            extends ReadSupport<NullRecord>
    {
        private final List<String> columnNames;

        public PrestoReadSupport(String columnName, String... additionalColumnNames)
        {
            this(ImmutableList.<String>builder().add(columnName).add(additionalColumnNames).build());
        }

        public PrestoReadSupport(List<String> columnNames)
        {
            this.columnNames = columnNames;
        }

        @Override
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema)
        {
            ImmutableList.Builder<Type> fields = ImmutableList.builder();
            for (String columnName : columnNames) {
                fields.add(fileSchema.getType(columnName));
            }
            MessageType requestedProjection = new MessageType(fileSchema.getName(), fields.build());
            return new ReadContext(requestedProjection);
        }

        @Override
        public RecordMaterializer<NullRecord> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new NullRecordConverter(readContext.getRequestedSchema());
        }
    }

    public static class NullRecordConverter
            extends RecordMaterializer<NullRecord>
    {
        private final NullRecord record;
        private final GroupConverter groupConverter;

        public NullRecordConverter(MessageType schema)
        {
            record = new NullRecord(schema.getFieldCount());
            groupConverter = new NullGroupConverter(record);
        }

        @Override
        public NullRecord getCurrentRecord()
        {
            return record;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return groupConverter;
        }
    }

    public static class NullGroupConverter
            extends GroupConverter
    {
        private final NullRecord record;
        private final Converter[] converters;

        public NullGroupConverter(NullRecord record)
        {
            this.record = record;
            this.converters = new Converter[record.getFieldCount()];

            for (int fieldIndex = 0; fieldIndex < record.getFieldCount(); fieldIndex++) {
                this.converters[fieldIndex] = new NullPrimitiveConverter(record, fieldIndex);
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters[fieldIndex];
        }

        @Override
        public void start()
        {
            record.reset();
        }

        @Override
        public void end()
        {
        }
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    private static class NullPrimitiveConverter
            extends PrimitiveConverter
    {
        private final NullRecord record;
        private final int fieldIndex;

        private NullPrimitiveConverter(NullRecord record, int fieldIndex)
        {
            this.record = record;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
            record.isNull[fieldIndex] = false;
            record.booleanValue[fieldIndex] = value;
        }

        @Override
        public void addDouble(double value)
        {
            record.isNull[fieldIndex] = false;
            record.doubleValue[fieldIndex] = value;
        }

        @Override
        public void addLong(long value)
        {
            record.isNull[fieldIndex] = false;
            record.longValue[fieldIndex] = value;
        }

        @Override
        public void addBinary(Binary value)
        {
            record.isNull[fieldIndex] = false;
            record.sliceValue[fieldIndex] = Slices.wrappedBuffer(value.getBytes());
        }

        @Override
        public void addFloat(float value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addInt(int value)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NullRecord
    {
        private final boolean[] isNull;
        private final boolean[] booleanValue;
        private final long[] longValue;
        private final double[] doubleValue;
        private final Slice[] sliceValue;

        private NullRecord(int fieldCount)
        {
            isNull = new boolean[fieldCount];
            booleanValue = new boolean[fieldCount];
            longValue = new long[fieldCount];
            doubleValue = new double[fieldCount];
            sliceValue = new Slice[fieldCount];
            reset();
        }

        public int getFieldCount()
        {
            return isNull.length;
        }

        public boolean isNull(int field)
        {
            return isNull[field];
        }

        public boolean getBoolean(int field)
        {
            return booleanValue[field];
        }

        public long getLong(int field)
        {
            return longValue[field];
        }

        public double getDouble(int field)
        {
            return doubleValue[field];
        }

        public Slice getSlice(int field)
        {
            return sliceValue[field];
        }

        private void reset()
        {
            Arrays.fill(isNull, true);
        }
    }
}
