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

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.TpchColumn;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.google.common.collect.Lists.transform;
import static io.airlift.tpch.LineItemColumn.DISCOUNT;
import static io.airlift.tpch.LineItemColumn.EXTENDED_PRICE;
import static io.airlift.tpch.LineItemColumn.ORDER_KEY;
import static io.airlift.tpch.LineItemColumn.QUANTITY;
import static io.airlift.tpch.LineItemColumn.RETURN_FLAG;
import static io.airlift.tpch.LineItemColumn.SHIP_DATE;
import static io.airlift.tpch.LineItemColumn.SHIP_INSTRUCTIONS;
import static io.airlift.tpch.LineItemColumn.STATUS;
import static io.airlift.tpch.LineItemColumn.TAX;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class BenchmarkHiveFileFormats
{
    private static final ConnectorSession SESSION = new ConnectorSession("user", "test", "catalog", "test", UTC_KEY, Locale.ENGLISH, null, null);

    private static final File DATA_DIR = new File("target");
    private static final JobConf JOB_CONF = new JobConf();
    private static final ImmutableList<? extends TpchColumn<?>> COLUMNS = ImmutableList.copyOf(LineItemColumn.values());

    private static final List<HiveColumnHandle> BIGINT_COLUMN = getHiveColumnHandles(ORDER_KEY);
    private static final List<Integer> BIGINT_COLUMN_INDEX = ImmutableList.copyOf(transform(BIGINT_COLUMN, hiveColumnIndexGetter()));

    private static final List<HiveColumnHandle> DOUBLE_COLUMN = getHiveColumnHandles(EXTENDED_PRICE);
    private static final List<Integer> DOUBLE_COLUMN_INDEX = ImmutableList.copyOf(transform(DOUBLE_COLUMN, hiveColumnIndexGetter()));

    private static final List<HiveColumnHandle> VARCHAR_COLUMN = getHiveColumnHandles(SHIP_INSTRUCTIONS);
    private static final List<Integer> VARCHAR_COLUMN_INDEX = ImmutableList.copyOf(transform(VARCHAR_COLUMN, hiveColumnIndexGetter()));

    private static final List<HiveColumnHandle> TPCH_6_COLUMNS = getHiveColumnHandles(QUANTITY, EXTENDED_PRICE, DISCOUNT, SHIP_DATE);
    private static final List<Integer> TPCH_6_COLUMN_INDEXES = ImmutableList.copyOf(transform(TPCH_6_COLUMNS, hiveColumnIndexGetter()));

    private static final List<HiveColumnHandle> TPCH_1_COLUMNS = getHiveColumnHandles(QUANTITY, EXTENDED_PRICE, DISCOUNT, TAX, RETURN_FLAG, STATUS, SHIP_DATE);
    private static final List<Integer> TPCH_1_COLUMN_INDEXES = ImmutableList.copyOf(transform(TPCH_1_COLUMNS, hiveColumnIndexGetter()));

    private static final List<HiveColumnHandle> ALL_COLUMNS = getHiveColumnHandles(LineItemColumn.values());
    private static final List<Integer> ALL_COLUMN_INDEXES = ImmutableList.copyOf(transform(ALL_COLUMNS, hiveColumnIndexGetter()));

    private static final int LOOPS = 1;

    private BenchmarkHiveFileFormats()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        workAroundParquetBrokenLoggingSetup();
        run(true, false);
    }

    private static void run(boolean benchmarkReadSpeed, boolean benchmarkWriteSpeed)
            throws Exception
    {
        HadoopNative.requireHadoopNative();
        ReaderWriterProfiler.setProfilerOptions(JOB_CONF);
        JOB_CONF.set(IOConstants.COLUMNS, Joiner.on(',').join(transform(COLUMNS, columnNameGetter())));
        DATA_DIR.mkdirs();

        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
                new BenchmarkFile(
                        "orc",
                        "orc",
                        new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcSerde(),
                        null,
//                        new GenericHiveRecordCursorProvider()),  // orc needs special splits
                        new OrcRecordCursorProvider(),
                        new OrcVectorRecordCursorProvider()),

                new BenchmarkFile(
                        "rc binary gzip",
                        "rc-binary.gz",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        "gzip",
//                        new GenericHiveRecordCursorProvider(),
                        new ColumnarBinaryHiveRecordCursorProvider()),

                new BenchmarkFile(
                        "parquet gzip",
                        "parquet.gz",
                        new MapredParquetInputFormat(),
                        new MapredParquetOutputFormat(),
                        new ParquetHiveSerDe(),
                        "gzip",
//                        new GenericHiveRecordCursorProvider(), // something weird
                        new ParquetRecordCursorProvider()),

                new BenchmarkFile(
                        "dwrf",
                        "dwrf",
                        new com.facebook.hive.orc.OrcInputFormat(),
                        new com.facebook.hive.orc.OrcOutputFormat(),
                        new com.facebook.hive.orc.OrcSerde(),
                        null,
//                        new GenericHiveRecordCursorProvider(),
                        new DwrfRecordCursorProvider())
        );

        if (!benchmarkWriteSpeed) {
            for (BenchmarkFile benchmarkFile : benchmarkFiles) {
                if (!benchmarkFile.getFile().exists()) {
                    writeLineItems(benchmarkFile.getFile(), benchmarkFile.getOutputFormat(), benchmarkFile.getSerDe(), benchmarkFile.getCompressionCodec(), COLUMNS);
                }
            }
        }

        for (int run = 0; run < 2; run++) {
            System.out.println("==== Run " + run + " ====");
            if (benchmarkWriteSpeed) {
                benchmarkWrite(benchmarkFiles, 2);
            }
            if (benchmarkReadSpeed) {
                benchmarkRead(benchmarkFiles, 1);
            }
        }
    }

    private static void benchmarkWrite(List<BenchmarkFile> benchmarkFiles, int loopCount)
            throws Exception
    {
        long start;
        System.out.println("write");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            DataSize dataSize = null;
            start = System.nanoTime();
            for (int loop = 0; loop < loopCount; loop++) {
                dataSize = writeLineItems(benchmarkFile.getFile(),
                        benchmarkFile.getOutputFormat(),
                        benchmarkFile.getSerDe(),
                        benchmarkFile.getCompressionCodec(),
                        COLUMNS);
            }
            logDuration(benchmarkFile.getName(), start, loopCount, dataSize);
        }
        System.out.println();
    }

    private static void benchmarkRead(List<BenchmarkFile> benchmarkFiles, int loopCount)
            throws Exception
    {
        long start;

        System.out.println("bigint");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, BIGINT_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                long result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadBigint(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("double");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, DOUBLE_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                double result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadDouble(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("varchar");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, VARCHAR_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                long result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadVarchar(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("tpch6");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, TPCH_6_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                double result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadTpch6(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("tpch1");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, TPCH_1_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                double result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadTpch1(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("all");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, ALL_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                double result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkReadAll(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

        System.out.println("one (load all)");
        // noinspection deprecation
        ColumnProjectionUtils.setReadColumnIDs(JOB_CONF, BIGINT_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                double result = 0;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    result = benchmarkLoadAllReadOne(
                            benchmarkFile.getFileSplit(),
                            createPartitionProperties(benchmarkFile),
                            recordCursorProvider
                    );
                }
                logDuration(benchmarkFile.getName() + " " + getCursorType(recordCursorProvider), start, loopCount, result);
            }
        }
        System.out.println();

    }

    private static long benchmarkReadBigint(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    BIGINT_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkReadDouble(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    DOUBLE_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getDouble(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static long benchmarkReadVarchar(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    VARCHAR_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getSlice(0).length();
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkReadTpch6(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_6_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
                if (!recordCursor.isNull(1)) {
                    sum += recordCursor.getDouble(1);
                }
                if (!recordCursor.isNull(2)) {
                    sum += recordCursor.getDouble(2);
                }
                if (!recordCursor.isNull(3)) {
                    sum += recordCursor.getSlice(3).length();
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkReadTpch1(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_1_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
                if (!recordCursor.isNull(1)) {
                    sum += recordCursor.getDouble(1);
                }
                if (!recordCursor.isNull(2)) {
                    sum += recordCursor.getDouble(2);
                }
                if (!recordCursor.isNull(3)) {
                    sum += recordCursor.getDouble(3);
                }
                if (!recordCursor.isNull(4)) {
                    sum += recordCursor.getSlice(4).length();
                }
                if (!recordCursor.isNull(5)) {
                    sum += recordCursor.getSlice(5).length();
                }
                if (!recordCursor.isNull(6)) {
                    sum += recordCursor.getSlice(6).length();
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkReadAll(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
                if (!recordCursor.isNull(1)) {
                    sum += recordCursor.getLong(1);
                }
                if (!recordCursor.isNull(2)) {
                    sum += recordCursor.getLong(2);
                }
                if (!recordCursor.isNull(3)) {
                    sum += recordCursor.getLong(3);
                }
                if (!recordCursor.isNull(4)) {
                    sum += recordCursor.getLong(4);
                }
                if (!recordCursor.isNull(5)) {
                    sum += recordCursor.getDouble(5);
                }
                if (!recordCursor.isNull(6)) {
                    sum += recordCursor.getDouble(6);
                }
                if (!recordCursor.isNull(7)) {
                    sum += recordCursor.getDouble(7);
                }
                if (!recordCursor.isNull(8)) {
                    sum += recordCursor.getSlice(8).length();
                }
                if (!recordCursor.isNull(9)) {
                    sum += recordCursor.getSlice(9).length();
                }
                if (!recordCursor.isNull(10)) {
                    sum += recordCursor.getSlice(10).length();
                }
                if (!recordCursor.isNull(11)) {
                    sum += recordCursor.getSlice(11).length();
                }
                if (!recordCursor.isNull(12)) {
                    sum += recordCursor.getSlice(12).length();
                }
                if (!recordCursor.isNull(13)) {
                    sum += recordCursor.getSlice(13).length();
                }
                if (!recordCursor.isNull(14)) {
                    sum += recordCursor.getSlice(14).length();
                }
                if (!recordCursor.isNull(15)) {
                    sum += recordCursor.getSlice(15).length();
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkLoadAllReadOne(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    split.getSession(),
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    public static RecordWriter createRecordWriter(List<? extends TpchColumn<?>> columns, File outputFile, HiveOutputFormat<?, ?> outputFormat, String compressionCodec)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);
        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, CompressionType.BLOCK.toString());
            jobConf.set("parquet.compression", compressionCodec);
            jobConf.set("parquet.enable.dictionary", "true");
        }

        RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != null,
                createTableProperties(columns),
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );

        return recordWriter;
    }

    public static DataSize writeLineItems(
            File outputFile,
            HiveOutputFormat<?, ?> outputFormat,
            @SuppressWarnings("deprecation") Serializer serializer,
            String compressionCodec,
            List<? extends TpchColumn<?>> columns)
            throws Exception
    {
        RecordWriter recordWriter = createRecordWriter(columns, outputFile, outputFormat, compressionCodec);

        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(transform(columns, columnNameGetter()), transform(columns, objectInspectorGetter()));

        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        for (LineItem lineItem : new LineItemGenerator(1, 1, 1)) {
            objectInspector.setStructFieldData(row, fields.get(0), lineItem.getOrderKey());
            objectInspector.setStructFieldData(row, fields.get(1), lineItem.getPartKey());
            objectInspector.setStructFieldData(row, fields.get(2), lineItem.getSupplierKey());
            objectInspector.setStructFieldData(row, fields.get(3), lineItem.getLineNumber());
            objectInspector.setStructFieldData(row, fields.get(4), lineItem.getQuantity());
            objectInspector.setStructFieldData(row, fields.get(5), lineItem.getExtendedPrice());
            objectInspector.setStructFieldData(row, fields.get(6), lineItem.getDiscount());
            objectInspector.setStructFieldData(row, fields.get(7), lineItem.getTax());
            objectInspector.setStructFieldData(row, fields.get(8), lineItem.getReturnFlag());
            objectInspector.setStructFieldData(row, fields.get(9), lineItem.getStatus());
            objectInspector.setStructFieldData(row, fields.get(10), lineItem.getShipDate());
            objectInspector.setStructFieldData(row, fields.get(11), lineItem.getCommitDate());
            objectInspector.setStructFieldData(row, fields.get(12), lineItem.getReceiptDate());
            objectInspector.setStructFieldData(row, fields.get(13), lineItem.getShipInstructions());
            objectInspector.setStructFieldData(row, fields.get(14), lineItem.getShipMode());
            objectInspector.setStructFieldData(row, fields.get(15), lineItem.getComment());

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return getFileSize(outputFile);
    }

    public static Properties createTableProperties(List<? extends TpchColumn<?>> columns)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty(
                "columns",
                Joiner.on(',').join(transform(columns, columnNameGetter())));
        orderTableProperties.setProperty(
                "columns.types",
                Joiner.on(':').join(transform(columns, columnTypeGetter())));
        return orderTableProperties;
    }

    private static Properties createPartitionProperties(BenchmarkFile benchmarkFile)
    {
        Properties schema = createTableProperties(ImmutableList.copyOf(LineItemColumn.values()));
        schema.setProperty(FILE_INPUT_FORMAT, benchmarkFile.getInputFormat().getClass().getName());
        schema.setProperty(SERIALIZATION_LIB, benchmarkFile.getSerDe().getClass().getName());
        return schema;
    }

    private static HiveSplit createHiveSplit(FileSplit fileSplit, Properties partitionProperties)
    {
        return new HiveSplit("test",
                "test",
                "lineitem",
                "unpartitioned",
                fileSplit.getPath().toString(),
                fileSplit.getStart(),
                fileSplit.getLength(),
                partitionProperties,
                ImmutableList.<HivePartitionKey>of(),
                ImmutableList.<HostAddress>of(),
                SESSION,
                TupleDomain.<HiveColumnHandle>all());
    }

    private static List<HiveColumnHandle> getHiveColumnHandles(TpchColumn<?>... tpchColumns)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();
        for (TpchColumn<?> column : tpchColumns) {
            int ordinal = COLUMNS.indexOf(column);
            columns.add(new HiveColumnHandle("test",
                    column.getColumnName(),
                    ordinal,
                    HiveType.getHiveType(getObjectInspector(column)),
                    ordinal,
                    false));
        }

        return columns.build();
    }

    private static void logDuration(String label, long start, int loopCount, Object value)
    {
        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS).convertTo(SECONDS);
        System.out.printf("%30s %6s %s\n", label, duration, value);
    }

    private static DataSize getFileSize(File outputFile)
    {
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    private static Function<TpchColumn<?>, String> columnNameGetter()
    {
        return new Function<TpchColumn<?>, String>()
        {
            @Override
            public String apply(TpchColumn<?> input)
            {
                return input.getColumnName();
            }
        };
    }

    private static Function<TpchColumn<?>, String> columnTypeGetter()
    {
        return new Function<TpchColumn<?>, String>()
        {
            @Override
            public String apply(TpchColumn<?> input)
            {
                Class<?> type = input.getType();
                if (type == Long.class) {
                    return "bigint";
                }
                if (type == Double.class) {
                    return "double";
                }
                if (type == String.class) {
                    return "string";
                }
                throw new IllegalArgumentException("Unsupported type " + type.getName());
            }
        };
    }

    private static Function<TpchColumn<?>, ObjectInspector> objectInspectorGetter()
    {
        return new Function<TpchColumn<?>, ObjectInspector>()
        {
            @Override
            public ObjectInspector apply(TpchColumn<?> input)
            {
                return getObjectInspector(input);
            }
        };
    }

    private static ObjectInspector getObjectInspector(TpchColumn<?> input)
    {
        Class<?> type = input.getType();
        if (type == Long.class) {
            return javaLongObjectInspector;
        }
        if (type == Double.class) {
            return javaDoubleObjectInspector;
        }
        if (type == String.class) {
            return javaStringObjectInspector;
        }
        throw new IllegalArgumentException("Unsupported type " + type.getName());
    }

    private static String getCursorType(HiveRecordCursorProvider recordCursorProvider)
    {
        if (recordCursorProvider instanceof GenericHiveRecordCursorProvider) {
            return "generic";
        }
        if (recordCursorProvider instanceof OrcVectorRecordCursorProvider) {
            return "vector";
        }
        return "custom";
    }

    private static class BenchmarkFile
    {
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final HiveOutputFormat<?, ?> outputFormat;
        @SuppressWarnings("deprecation")
        private final SerDe serDe;
        private final String compressionCodec;
        private final FileSplit fileSplit;
        private final List<HiveRecordCursorProvider> recordCursorProviders;
        private final File file;

        private BenchmarkFile(
                String name,
                String fileExtension,
                InputFormat<?, ? extends Writable> inputFormat,
                HiveOutputFormat<?, ?> outputFormat,
                @SuppressWarnings("deprecation") SerDe serDe,
                String compressionCodec,
                HiveRecordCursorProvider... recordCursorProviders)
                throws Exception
        {
            this.name = name;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.serDe = serDe;
            this.compressionCodec = compressionCodec;
            this.recordCursorProviders = ImmutableList.copyOf(recordCursorProviders);

            serDe.initialize(new Configuration(), createTableProperties(COLUMNS));
            file = new File(DATA_DIR, "line_item." + fileExtension);

            Path lineitemPath = new Path(file.toURI());
            lineitemPath.getFileSystem(new Configuration()).setVerifyChecksum(true);
            fileSplit = new FileSplit(lineitemPath, 0, file.length(), new String[0]);
        }

        public String getName()
        {
            return name;
        }

        public InputFormat<?, ? extends Writable> getInputFormat()
        {
            return inputFormat;
        }

        public HiveOutputFormat<?, ?> getOutputFormat()
        {
            return outputFormat;
        }

        @SuppressWarnings("deprecation")
        public SerDe getSerDe()
        {
            return serDe;
        }

        public String getCompressionCodec()
        {
            return compressionCodec;
        }

        public FileSplit getFileSplit()
        {
            return fileSplit;
        }

        public File getFile()
        {
            return file;
        }

        public List<HiveRecordCursorProvider> getRecordCursorProviders()
        {
            return recordCursorProviders;
        }
    }

    private static void workAroundParquetBrokenLoggingSetup()
            throws IOException
    {
//        // unhook out and err while initializing logging or logger will print to them
//        PrintStream out = System.out;
//        PrintStream err = System.err;
//        try {
//            System.setOut(new PrintStream(nullOutputStream()));
//            System.setErr(new PrintStream(nullOutputStream()));
//
//            Log.getLog(Object.class);
//            Logging logging = Logging.initialize();
//            logging.configure(new LoggingConfiguration());
//            logging.disableConsole();
//        }
//        finally {
//            System.setOut(out);
//            System.setErr(err);
//        }
//
//        LoggingMBean logging = new LoggingMBean();
//        logging.setLevel("com.hadoop", "OFF");
//        logging.setLevel("org.apache.hadoop", "OFF");
//        logging.setLevel("org.apache.zookeeper", "OFF");
//        logging.setLevel("parquet", "OFF");
    }
}
