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

import com.facebook.presto.hive.orc.DoubleVector;
import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.OrcReader;
import com.facebook.presto.hive.orc.OrcRecordReader;
import com.facebook.presto.hive.orc.SliceVector;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.LOOPS;
import static com.facebook.presto.hive.HiveType.DOUBLE;
import static com.facebook.presto.hive.HiveType.LONG;
import static com.facebook.presto.hive.HiveType.STRING;
import static org.joda.time.DateTimeZone.UTC;

public final class BenchmarkLineItemCustomOrcVectorized
        implements BenchmarkLineItemWithPredicatePushdown
{
    private static final HiveColumnHandle ORDERKEY = new HiveColumnHandle("clientId", "orderkey", 0, LONG, 0, false);
    private static final HiveColumnHandle PARTKEY = new HiveColumnHandle("clientId", "partkey", 1, LONG, 1, false);
    private static final HiveColumnHandle SUPPKEY = new HiveColumnHandle("clientId", "suppkey", 2, LONG, 2, false);
    private static final HiveColumnHandle LINENUMBER = new HiveColumnHandle("clientId", "linenumber", 3, LONG, 3, false);
    private static final HiveColumnHandle QUANTITY = new HiveColumnHandle("clientId", "quantity", 4, LONG, 4, false);
    private static final HiveColumnHandle EXTENDEDPRICE = new HiveColumnHandle("clientId", "extendedprice", 5, DOUBLE, 5, false);
    private static final HiveColumnHandle DISCOUNT = new HiveColumnHandle("clientId", "discount", 6, DOUBLE, 6, false);
    private static final HiveColumnHandle TAX = new HiveColumnHandle("clientId", "tax", 7, DOUBLE, 7, false);
    private static final HiveColumnHandle RETURNFLAG = new HiveColumnHandle("clientId", "returnflag", 8, STRING, 8, false);
    private static final HiveColumnHandle LINESTATUS = new HiveColumnHandle("clientId", "linestatus", 9, STRING, 9, false);
    private static final HiveColumnHandle SHIPDATE = new HiveColumnHandle("clientId", "shipdate", 10, STRING, 10, false);
    private static final HiveColumnHandle COMMITDATE = new HiveColumnHandle("clientId", "commitdate", 11, STRING, 11, false);
    private static final HiveColumnHandle RECEIPTDATE = new HiveColumnHandle("clientId", "receiptdate", 12, STRING, 12, false);
    private static final HiveColumnHandle SHIPINSTRUCT = new HiveColumnHandle("clientId", "shipinstruct", 13, STRING, 13, false);
    private static final HiveColumnHandle SHIPMODE = new HiveColumnHandle("clientId", "shipmode", 14, STRING, 14, false);
    private static final HiveColumnHandle COMMENT = new HiveColumnHandle("clientId", "comment", 15, STRING, 15, false);

    @Override
    public String getName()
    {
        return "custom vector";
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

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, ORDERKEY);

            LongVector vector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        bigintSum += vector.vector[i];
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

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, PARTKEY);

            LongVector vector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        bigintSum += vector.vector[i];
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

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, SUPPKEY);

            LongVector vector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        bigintSum += vector.vector[i];
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

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, LINENUMBER);

            LongVector vector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        bigintSum += vector.vector[i];
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

        long bigintSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            bigintSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, QUANTITY);

            LongVector vector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        bigintSum += vector.vector[i];
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

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, EXTENDEDPRICE);

            DoubleVector vector = new DoubleVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        doubleSum += vector.vector[i];
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

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, DISCOUNT);

            DoubleVector vector = new DoubleVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        doubleSum += vector.vector[i];
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

        double doubleSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            doubleSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, TAX);

            DoubleVector vector = new DoubleVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (!vector.isNull[i]) {
                        doubleSum += vector.vector[i];
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, RETURNFLAG);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, LINESTATUS);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, SHIPDATE);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, COMMITDATE);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, RECEIPTDATE);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, SHIPINSTRUCT);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, SHIPMODE);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

        long stringLengthSum = 0;
        for (int loop = 0; loop < LOOPS; loop++) {
            stringLengthSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, COMMENT);

            SliceVector vector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(fieldIndex, vector);

                for (int i = 0; i < batchSize; i++) {
                    if (vector.slice[i] != null) {
                        stringLengthSum += vector.slice[i].length();
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

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX, RETURNFLAG, LINESTATUS, SHIPDATE);

            LongVector quantityVector = new LongVector();
            DoubleVector extendedPriceVector = new DoubleVector();
            DoubleVector discountVector = new DoubleVector();
            DoubleVector taxVector = new DoubleVector();
            SliceVector returnFlagVector = new SliceVector();
            SliceVector lineStatusVector = new SliceVector();
            SliceVector shipDateVector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(quantityFieldIndex, quantityVector);
                recordReader.readVector(extendedPriceFieldIndex, extendedPriceVector);
                recordReader.readVector(discountFieldIndex, discountVector);
                recordReader.readVector(taxFieldIndex, taxVector);
                recordReader.readVector(returnFlagFieldIndex, returnFlagVector);
                recordReader.readVector(lineStatusFieldIndex, lineStatusVector);
                recordReader.readVector(shipDateFieldIndex, shipDateVector);

                for (int i = 0; i < batchSize; i++) {
                    if (!quantityVector.isNull[i]) {
                        quantitySum += quantityVector.vector[i];
                    }

                    if (!extendedPriceVector.isNull[i]) {
                        extendedPriceSum += extendedPriceVector.vector[i];
                    }

                    if (!discountVector.isNull[i]) {
                        discountSum += discountVector.vector[i];
                    }

                    if (!taxVector.isNull[i]) {
                        taxSum += taxVector.vector[i];
                    }

                    if (returnFlagVector.slice[i] != null) {
                        returnFlagSum += returnFlagVector.slice[i].length();
                    }

                    if (lineStatusVector.slice[i] != null) {
                        lineStatusSum += lineStatusVector.slice[i].length();
                    }

                    if (shipDateVector.slice[i] != null) {
                        shipDateSum += shipDateVector.slice[i].length();
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

        double quantitySum = 0;
        double extendedPriceSum = 0;
        double discountSum = 0;
        long shipDateSum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            quantitySum = 0;
            extendedPriceSum = 0;
            discountSum = 0;
            shipDateSum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(fileSystem, fileSplit, QUANTITY, EXTENDEDPRICE, DISCOUNT, SHIPDATE);

            LongVector quantityVector = new LongVector();
            DoubleVector extendedPriceVector = new DoubleVector();
            DoubleVector discountVector = new DoubleVector();
            SliceVector shipDateVector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(quantityFieldIndex, quantityVector);
                recordReader.readVector(extendedPriceFieldIndex, extendedPriceVector);
                recordReader.readVector(discountFieldIndex, discountVector);
                recordReader.readVector(shipDateFieldIndex, shipDateVector);

                for (int i = 0; i < batchSize; i++) {
                    if (!quantityVector.isNull[i]) {
                        quantitySum += quantityVector.vector[i];
                    }

                    if (!extendedPriceVector.isNull[i]) {
                        extendedPriceSum += extendedPriceVector.vector[i];
                    }

                    if (!discountVector.isNull[i]) {
                        discountSum += discountVector.vector[i];
                    }

                    if (shipDateVector.slice[i] != null) {
                        shipDateSum += shipDateVector.slice[i].length();
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
        return all(jobConf, fileSplit, inputFormat, deserializer, TupleDomain.<HiveColumnHandle>all());
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

        long orderKeySum = 0;

        for (int loop = 0; loop < LOOPS; loop++) {
            orderKeySum = 0;

            OrcRecordReader recordReader = createVectorizedRecordReader(
                    fileSystem,
                    fileSplit,
                    ORDERKEY,
                    PARTKEY,
                    SUPPKEY,
                    LINENUMBER,
                    QUANTITY,
                    EXTENDEDPRICE,
                    DISCOUNT,
                    TAX,
                    RETURNFLAG,
                    LINESTATUS,
                    SHIPDATE,
                    COMMITDATE,
                    RECEIPTDATE,
                    SHIPINSTRUCT,
                    SHIPMODE,
                    COMMENT);

            LongVector orderKeyVector = new LongVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                recordReader.readVector(orderKeyFieldIndex, orderKeyVector);

                for (int i = 0; i < batchSize; i++) {
                    if (!orderKeyVector.isNull[i]) {
                        orderKeySum += orderKeyVector.vector[i];
                    }
                }
            }
            recordReader.close();
        }

        return ImmutableList.<Object>of(orderKeySum);
    }

    public <K, V extends Writable> List<Object> allNoMatch(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY, Domain.singleValue(-1L)));

        return all(jobConf, fileSplit, inputFormat, deserializer, tupleDomain);
    }

    public <K, V extends Writable> List<Object> allSmallMatch(JobConf jobConf, FileSplit fileSplit, InputFormat<K, V> inputFormat, Deserializer deserializer)
            throws Exception
    {
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY, Domain.create(SortedRangeSet.of(
                Range.equal(5L),
                Range.equal(500L),
                Range.equal(5_000L),
                Range.equal(50_000L),
                Range.equal(500_000L),
                Range.equal(550_000L),
                Range.equal(5_000_000L)
        ), false)));

        return all(jobConf, fileSplit, inputFormat, deserializer, tupleDomain);
    }

    public <K, V extends Writable> List<Object> all(
            JobConf jobConf,
            FileSplit fileSplit,
            InputFormat<K, V> inputFormat,
            Deserializer deserializer,
            TupleDomain<HiveColumnHandle> tupleDomain)
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

        for (int loop = 0; loop < LOOPS; loop++) {
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

            OrcRecordReader recordReader = createVectorizedRecordReader(
                    fileSystem,
                    fileSplit,
                    tupleDomain,
                    ORDERKEY,
                    PARTKEY,
                    SUPPKEY,
                    LINENUMBER,
                    QUANTITY,
                    EXTENDEDPRICE,
                    DISCOUNT,
                    TAX,
                    RETURNFLAG,
                    LINESTATUS,
                    SHIPDATE,
                    COMMITDATE,
                    RECEIPTDATE,
                    SHIPINSTRUCT,
                    SHIPMODE,
                    COMMENT);

            LongVector orderKeyVector = new LongVector();
            LongVector partKeyVector = new LongVector();
            LongVector supplierKeyVector = new LongVector();
            LongVector lineNumberVector = new LongVector();
            LongVector quantityVector = new LongVector();
            DoubleVector extendedPriceVector = new DoubleVector();
            DoubleVector discountVector = new DoubleVector();
            DoubleVector taxVector = new DoubleVector();
            SliceVector returnFlagVector = new SliceVector();
            SliceVector lineStatusVector = new SliceVector();
            SliceVector shipDateVector = new SliceVector();
            SliceVector commitDateVector = new SliceVector();
            SliceVector receiptDateVector = new SliceVector();
            SliceVector shipInstructionsVector = new SliceVector();
            SliceVector shipModeVector = new SliceVector();
            SliceVector commentVector = new SliceVector();

            for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
                rowsProcessed += batchSize;

                recordReader.readVector(orderKeyFieldIndex, orderKeyVector);
                recordReader.readVector(partKeyFieldIndex, partKeyVector);
                recordReader.readVector(supplierKeyFieldIndex, supplierKeyVector);
                recordReader.readVector(lineNumberFieldIndex, lineNumberVector);
                recordReader.readVector(quantityFieldIndex, quantityVector);
                recordReader.readVector(extendedPriceFieldIndex, extendedPriceVector);
                recordReader.readVector(discountFieldIndex, discountVector);
                recordReader.readVector(taxFieldIndex, taxVector);
                recordReader.readVector(returnFlagFieldIndex, returnFlagVector);
                recordReader.readVector(lineStatusFieldIndex, lineStatusVector);
                recordReader.readVector(shipDateFieldIndex, shipDateVector);
                recordReader.readVector(commitDateFieldIndex, commitDateVector);
                recordReader.readVector(receiptDateFieldIndex, receiptDateVector);
                recordReader.readVector(shipInstructionsFieldIndex, shipInstructionsVector);
                recordReader.readVector(shipModeFieldIndex, shipModeVector);
                recordReader.readVector(commentFieldIndex, commentVector);

                for (int i = 0; i < batchSize; i++) {
                    if (!orderKeyVector.isNull[i]) {
                        orderKeySum += orderKeyVector.vector[i];
                    }

                    if (!partKeyVector.isNull[i]) {
                        partKeySum += partKeyVector.vector[i];
                    }

                    if (!supplierKeyVector.isNull[i]) {
                        supplierKeySum += supplierKeyVector.vector[i];
                    }

                    if (!lineNumberVector.isNull[i]) {
                        lineNumberSum += lineNumberVector.vector[i];
                    }

                    if (!quantityVector.isNull[i]) {
                        quantitySum += quantityVector.vector[i];
                    }

                    if (!extendedPriceVector.isNull[i]) {
                        extendedPriceSum += extendedPriceVector.vector[i];
                    }

                    if (!discountVector.isNull[i]) {
                        discountSum += discountVector.vector[i];
                    }

                    if (!taxVector.isNull[i]) {
                        taxSum += taxVector.vector[i];
                    }

                    if (returnFlagVector.slice[i] != null) {
                        returnFlagSum += returnFlagVector.slice[i].length();
                    }

                    if (lineStatusVector.slice[i] != null) {
                        lineStatusSum += lineStatusVector.slice[i].length();
                    }

                    if (shipDateVector.slice[i] != null) {
                        shipDateSum += shipDateVector.slice[i].length();
                    }

                    if (commitDateVector.slice[i] != null) {
                        commitDateSum += commitDateVector.slice[i].length();
                    }

                    if (receiptDateVector.slice[i] != null) {
                        receiptDateSum += receiptDateVector.slice[i].length();
                    }

                    if (shipInstructionsVector.slice[i] != null) {
                        shipInstructionsSum += shipInstructionsVector.slice[i].length();
                    }

                    if (shipModeVector.slice[i] != null) {
                        shipModeSum += shipModeVector.slice[i].length();
                    }

                    if (commentVector.slice[i] != null) {
                        commentSum += commentVector.slice[i].length();
                    }
                }
            }
            recordReader.close();
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

    public static OrcRecordReader createVectorizedRecordReader(FileSystem fileSystem, FileSplit fileSplit, HiveColumnHandle... columnHandles)
            throws IOException
    {
        return createVectorizedRecordReader(fileSystem, fileSplit, TupleDomain.<HiveColumnHandle>all(), columnHandles);
    }

    public static OrcRecordReader createVectorizedRecordReader(
            FileSystem fileSystem,
            FileSplit fileSplit,
            TupleDomain<HiveColumnHandle> tupleDomain,
            HiveColumnHandle... columnHandles)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(fileSplit.getPath(), fileSystem);
        OrcRecordReader recordReader = orcReader.createRecordReader(
                fileSplit.getStart(),
                fileSplit.getLength(),
                ImmutableList.copyOf(columnHandles),
                tupleDomain,
                UTC);

        return recordReader;
    }
}
