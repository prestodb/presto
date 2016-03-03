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
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTimeZone;
import parquet.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveUtil.setReadColumns;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Lists.transform;
import static com.google.common.io.ByteStreams.nullOutputStream;
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
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class BenchmarkHiveFileFormats
{
    private static final List<CompressionType> ENABLED_COMPRESSION = ImmutableList.<CompressionType>builder()
            .add(CompressionType.none)
            .add(CompressionType.snappy)
            .add(CompressionType.gzip)
            .build();

    private enum CompressionType
    {
        none(""),
        snappy(".snappy"),
        gzip(".gz");

        private final String fileExtension;

        CompressionType(String fileExtension)
        {
            this.fileExtension = fileExtension;
        }

        public String getFileExtension()
        {
            return fileExtension;
        }
    }

    private static final File DATA_DIR = new File("target");
    private static final JobConf JOB_CONF = new JobConf();
    private static final ImmutableList<? extends TpchColumn<?>> COLUMNS = ImmutableList.copyOf(LineItemColumn.values());

    private static final long FILTER_ORDER_KEY_ID = 300_000L;
    private static final List<HiveColumnHandle> BIGINT_COLUMN = getHiveColumnHandles(ORDER_KEY);
    private static final List<Integer> BIGINT_COLUMN_INDEX = ImmutableList.copyOf(transform(BIGINT_COLUMN, HiveColumnHandle::getHiveColumnIndex));

    private static final List<HiveColumnHandle> DOUBLE_COLUMN = getHiveColumnHandles(EXTENDED_PRICE);
    private static final List<Integer> DOUBLE_COLUMN_INDEX = ImmutableList.copyOf(transform(DOUBLE_COLUMN, HiveColumnHandle::getHiveColumnIndex));

    private static final List<HiveColumnHandle> VARCHAR_COLUMN = getHiveColumnHandles(SHIP_INSTRUCTIONS);
    private static final List<Integer> VARCHAR_COLUMN_INDEX = ImmutableList.copyOf(transform(VARCHAR_COLUMN, HiveColumnHandle::getHiveColumnIndex));

    private static final List<HiveColumnHandle> TPCH_6_COLUMNS = getHiveColumnHandles(QUANTITY, EXTENDED_PRICE, DISCOUNT, SHIP_DATE);
    private static final List<Integer> TPCH_6_COLUMN_INDEXES = ImmutableList.copyOf(transform(TPCH_6_COLUMNS, HiveColumnHandle::getHiveColumnIndex));

    private static final List<HiveColumnHandle> TPCH_1_COLUMNS = getHiveColumnHandles(QUANTITY, EXTENDED_PRICE, DISCOUNT, TAX, RETURN_FLAG, STATUS, SHIP_DATE);
    private static final List<Integer> TPCH_1_COLUMN_INDEXES = ImmutableList.copyOf(transform(TPCH_1_COLUMNS, HiveColumnHandle::getHiveColumnIndex));

    private static final List<HiveColumnHandle> ALL_COLUMNS = getHiveColumnHandles(LineItemColumn.values());
    private static final List<Integer> ALL_COLUMN_INDEXES = ImmutableList.copyOf(transform(ALL_COLUMNS, HiveColumnHandle::getHiveColumnIndex));

    private static final int LOOPS = 1;
    private static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

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
        DATA_DIR.mkdirs();

        List<BenchmarkFile> benchmarkFiles = ImmutableList.<BenchmarkFile>builder()
                .add(new BenchmarkFile(
                        "rc-binary",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new LazyBinaryColumnarSerDe(),
                        ImmutableList.<HiveRecordCursorProvider>builder()
                                .add(new ColumnarBinaryHiveRecordCursorProvider())
                                .build(),
                        ImmutableList.<HivePageSourceFactory>builder()
                                .add(new RcFilePageSourceFactory(TYPE_MANAGER))
                                .build()))

                .add(new BenchmarkFile(
                        "rc-text",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new ColumnarSerDe(),
                        ImmutableList.<HiveRecordCursorProvider>builder()
                                .add(new ColumnarTextHiveRecordCursorProvider())
                                .build(),
                        ImmutableList.<HivePageSourceFactory>builder()
                                .add(new RcFilePageSourceFactory(TYPE_MANAGER))
                                .build()))

                .add(new BenchmarkFile(
                        "parquet",
                        new MapredParquetInputFormat(),
                        new MapredParquetOutputFormat(),
                        new ParquetHiveSerDe(),
                        ImmutableList.<HiveRecordCursorProvider>builder()
                                .add(new ParquetRecordCursorProvider(false))
                                .build(),
                        ImmutableList.<HivePageSourceFactory>builder()
                                .build()))

                .add(new BenchmarkFile(
                        "dwrf",
                        new com.facebook.hive.orc.OrcInputFormat(),
                        new com.facebook.hive.orc.OrcOutputFormat(),
                        new com.facebook.hive.orc.OrcSerde(),
                        ImmutableList.of(),
                        ImmutableList.<HivePageSourceFactory>builder()
                                .add(new DwrfPageSourceFactory(TYPE_MANAGER))
                                .build()))

                .add(new BenchmarkFile(
                        "orc",
                        new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcSerde(),
                        ImmutableList.of(),
                        ImmutableList.<HivePageSourceFactory>builder()
                                .add(new OrcPageSourceFactory(TYPE_MANAGER, false))
                                .build()))
                .build();

        if (!benchmarkWriteSpeed) {
            for (BenchmarkFile benchmarkFile : benchmarkFiles) {
                for (CompressionType compressionType : ENABLED_COMPRESSION) {
                    if (!benchmarkFile.getFile(compressionType).exists()) {
                        writeLineItems(benchmarkFile.getFile(compressionType), benchmarkFile.getOutputFormat(), benchmarkFile.getSerDe(), compressionType, COLUMNS);
                    }
                }
            }
        }

        for (int run = 0; run < 2; run++) {
            System.out.println("==== Run " + run + " ====");
            if (benchmarkWriteSpeed) {
                benchmarkWrite(benchmarkFiles, 2, ENABLED_COMPRESSION);
            }
            if (benchmarkReadSpeed) {
                benchmarkRead(benchmarkFiles, 2 + (run > 0 ? 3 : 0), ENABLED_COMPRESSION);
            }
        }
    }

    private static void benchmarkWrite(List<BenchmarkFile> benchmarkFiles, int loopCount, List<CompressionType> compressionTypes)
            throws Exception
    {
        long start;
        System.out.println("write");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (CompressionType compressionType : compressionTypes) {
                DataSize dataSize = null;
                start = System.nanoTime();
                for (int loop = 0; loop < loopCount; loop++) {
                    dataSize = writeLineItems(benchmarkFile.getFile(compressionType),
                            benchmarkFile.getOutputFormat(),
                            benchmarkFile.getSerDe(),
                            compressionType,
                            COLUMNS);
                }
                logDuration("none", benchmarkFile.getName(), "write", compressionType, start, loopCount, dataSize);
            }
        }
        System.out.println();
    }

    private static void benchmarkRead(List<BenchmarkFile> benchmarkFiles, int loopCount, List<CompressionType> compressionTypes)
            throws Exception
    {
        long start;

        setReadColumns(JOB_CONF, ImmutableList.<Integer>of());
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadNone(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("none", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadNone(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("none", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, ImmutableList.<Integer>of());
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadBigint(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("bigint", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadBigint(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("bigint", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, DOUBLE_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadDouble(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("double", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadDouble(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("double", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, VARCHAR_COLUMN_INDEX);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadVarchar(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("varchar", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }
            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    long result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadVarchar(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("varchar", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, TPCH_6_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadTpch6(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("tpch6", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadTpch6(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("tpch6", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, TPCH_1_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadTpch1(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("tpch1", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadTpch1(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("tpch1", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, ALL_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadAll(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("all", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkReadAll(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("all", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, ALL_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkLoadAllReadOne(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("lazy", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }
            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkLoadAllReadOne(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("lazy", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }

        setReadColumns(JOB_CONF, ALL_COLUMN_INDEXES);
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            for (HiveRecordCursorProvider recordCursorProvider : benchmarkFile.getRecordCursorProviders()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        // cursor interface doesn't support predicate pushdown
                        result = benchmarkReadAll(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                recordCursorProvider
                        );
                    }
                    logDuration("pushdown", benchmarkFile.getName(), getCursorType(recordCursorProvider), compressionType, start, loopCount, result);
                }
            }

            for (HivePageSourceFactory pageSourceFactory : benchmarkFile.getPageSourceFactory()) {
                for (CompressionType compressionType : compressionTypes) {
                    double result = 0;
                    start = System.nanoTime();
                    for (int loop = 0; loop < loopCount; loop++) {
                        result = benchmarkPredicatePushDown(
                                createFileSplit(benchmarkFile.getFile(compressionType)),
                                createPartitionProperties(benchmarkFile),
                                pageSourceFactory
                        );
                    }
                    logDuration("pushdown", benchmarkFile.getName(), "page", compressionType, start, loopCount, result);
                }
            }
        }
        System.out.println();
    }

    private static long benchmarkReadNone(
            FileSplit fileSplit,
            Properties partitionProperties,
            HiveRecordCursorProvider hiveRecordCursorProvider)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long count = 0;
        for (int i = 0; i < LOOPS; i++) {
            count = 0;

            HiveRecordCursor recordCursor = hiveRecordCursorProvider.createHiveRecordCursor(
                    split.getClientId(),
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ImmutableList.<HiveColumnHandle>of(),
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                count++;
            }
            recordCursor.close();
        }
        return count;
    }

    private static long benchmarkReadNone(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long count = 0;
        for (int i = 0; i < LOOPS; i++) {
            count = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ImmutableList.<HiveColumnHandle>of(),
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                count += page.getPositionCount();
            }
            pageSource.close();
        }
        return count;
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    BIGINT_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static long benchmarkReadBigint(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    BIGINT_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (!block.isNull(position)) {
                        sum += BIGINT.getLong(block, position);
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    DOUBLE_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getDouble(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkReadDouble(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    DOUBLE_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (!block.isNull(position)) {
                        sum += DOUBLE.getDouble(block, position);
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    VARCHAR_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getSlice(0).length();
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static long benchmarkReadVarchar(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws Exception
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        long sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    VARCHAR_COLUMN,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (!block.isNull(position)) {
                        sum += VARCHAR.getSlice(block, position).length();
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_6_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

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

    private static double benchmarkReadTpch6(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_6_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                Block block0 = page.getBlock(0);
                Block block1 = page.getBlock(1);
                Block block2 = page.getBlock(2);
                Block block3 = page.getBlock(3);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!block0.isNull(position)) {
                        sum += BIGINT.getLong(block0, position);
                    }
                    if (!block1.isNull(position)) {
                        sum += DOUBLE.getDouble(block1, position);
                    }
                    if (!block2.isNull(position)) {
                        sum += DOUBLE.getDouble(block2, position);
                    }
                    if (!block3.isNull(position)) {
                        sum += VARCHAR.getSlice(block3, position).length();
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_1_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

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

    private static double benchmarkReadTpch1(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    TPCH_1_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                Block block0 = page.getBlock(0);
                Block block1 = page.getBlock(1);
                Block block2 = page.getBlock(2);
                Block block3 = page.getBlock(3);
                Block block4 = page.getBlock(4);
                Block block5 = page.getBlock(5);
                Block block6 = page.getBlock(6);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!block0.isNull(position)) {
                        sum += BIGINT.getLong(block0, position);
                    }
                    if (!block1.isNull(position)) {
                        sum += DOUBLE.getDouble(block1, position);
                    }
                    if (!block2.isNull(position)) {
                        sum += DOUBLE.getDouble(block2, position);
                    }
                    if (!block3.isNull(position)) {
                        sum += DOUBLE.getDouble(block3, position);
                    }
                    if (!block4.isNull(position)) {
                        sum += VARCHAR.getSlice(block4, position).length();
                    }
                    if (!block5.isNull(position)) {
                        sum += VARCHAR.getSlice(block5, position).length();
                    }
                    if (!block6.isNull(position)) {
                        sum += VARCHAR.getSlice(block6, position).length();
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

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

    private static double benchmarkReadAll(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                Block block0 = page.getBlock(0);
                Block block1 = page.getBlock(1);
                Block block2 = page.getBlock(2);
                Block block3 = page.getBlock(3);
                Block block4 = page.getBlock(4);
                Block block5 = page.getBlock(5);
                Block block6 = page.getBlock(6);
                Block block7 = page.getBlock(7);
                Block block8 = page.getBlock(8);
                Block block9 = page.getBlock(9);
                Block block10 = page.getBlock(10);
                Block block11 = page.getBlock(11);
                Block block12 = page.getBlock(12);
                Block block13 = page.getBlock(13);
                Block block14 = page.getBlock(14);
                Block block15 = page.getBlock(15);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!block0.isNull(position)) {
                        sum += BIGINT.getLong(block0, position);
                    }
                    if (!block1.isNull(position)) {
                        sum += BIGINT.getLong(block1, position);
                    }
                    if (!block2.isNull(position)) {
                        sum += BIGINT.getLong(block2, position);
                    }
                    if (!block3.isNull(position)) {
                        sum += BIGINT.getLong(block3, position);
                    }
                    if (!block4.isNull(position)) {
                        sum += BIGINT.getLong(block4, position);
                    }
                    if (!block5.isNull(position)) {
                        sum += DOUBLE.getDouble(block5, position);
                    }
                    if (!block6.isNull(position)) {
                        sum += DOUBLE.getDouble(block6, position);
                    }
                    if (!block7.isNull(position)) {
                        sum += DOUBLE.getDouble(block7, position);
                    }
                    if (!block8.isNull(position)) {
                        sum += VARCHAR.getSlice(block8, position).length();
                    }
                    if (!block9.isNull(position)) {
                        sum += VARCHAR.getSlice(block9, position).length();
                    }
                    if (!block10.isNull(position)) {
                        sum += VARCHAR.getSlice(block10, position).length();
                    }
                    if (!block11.isNull(position)) {
                        sum += VARCHAR.getSlice(block11, position).length();
                    }
                    if (!block12.isNull(position)) {
                        sum += VARCHAR.getSlice(block12, position).length();
                    }
                    if (!block13.isNull(position)) {
                        sum += VARCHAR.getSlice(block13, position).length();
                    }
                    if (!block14.isNull(position)) {
                        sum += VARCHAR.getSlice(block14, position).length();
                    }
                    if (!block15.isNull(position)) {
                        sum += VARCHAR.getSlice(block15, position).length();
                    }
                }
            }
            pageSource.close();
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    sum += recordCursor.getLong(0);
                }
            }
            recordCursor.close();
        }
        return sum;
    }

    private static double benchmarkLoadAllReadOne(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (!block.isNull(position)) {
                        sum += BIGINT.getLong(block, position);
                    }
                }
            }
            pageSource.close();
        }
        return sum;
    }

    private static double benchmarkPredicatePushDown(
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
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.<HiveColumnHandle>all(),
                    DateTimeZone.UTC,
                    TYPE_MANAGER).get();

            while (recordCursor.advanceNextPosition()) {
                if (!recordCursor.isNull(0)) {
                    long orderKey = recordCursor.getLong(0);
                    if (orderKey != FILTER_ORDER_KEY_ID) {
                        continue;
                    }
                    sum += orderKey;
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

    private static double benchmarkPredicatePushDown(
            FileSplit fileSplit,
            Properties partitionProperties,
            HivePageSourceFactory pageSourceFactory)
            throws IOException
    {
        HiveSplit split = createHiveSplit(fileSplit, partitionProperties);

        double sum = 0;
        for (int i = 0; i < LOOPS; i++) {
            sum = 0;

            ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                    new Configuration(),
                    SESSION,
                    new Path(split.getPath()),
                    split.getStart(),
                    split.getLength(),
                    split.getSchema(),
                    ALL_COLUMNS,
                    split.getPartitionKeys(),
                    TupleDomain.fromFixedValues(ImmutableMap.of(Iterables.getOnlyElement(getHiveColumnHandles(ORDER_KEY)), NullableValue.of(BIGINT, FILTER_ORDER_KEY_ID))),
                    DateTimeZone.UTC).get();

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                Block block0 = page.getBlock(0);
                Block block1 = page.getBlock(1);
                Block block2 = page.getBlock(2);
                Block block3 = page.getBlock(3);
                Block block4 = page.getBlock(4);
                Block block5 = page.getBlock(5);
                Block block6 = page.getBlock(6);
                Block block7 = page.getBlock(7);
                Block block8 = page.getBlock(8);
                Block block9 = page.getBlock(9);
                Block block10 = page.getBlock(10);
                Block block11 = page.getBlock(11);
                Block block12 = page.getBlock(12);
                Block block13 = page.getBlock(13);
                Block block14 = page.getBlock(14);
                Block block15 = page.getBlock(15);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!block0.isNull(position)) {
                        long orderKey = BIGINT.getLong(block0, position);
                        if (orderKey != FILTER_ORDER_KEY_ID) {
                            continue;
                        }
                        sum += orderKey;
                    }
                    if (!block1.isNull(position)) {
                        sum += BIGINT.getLong(block1, position);
                    }
                    if (!block2.isNull(position)) {
                        sum += BIGINT.getLong(block2, position);
                    }
                    if (!block3.isNull(position)) {
                        sum += BIGINT.getLong(block3, position);
                    }
                    if (!block4.isNull(position)) {
                        sum += BIGINT.getLong(block4, position);
                    }
                    if (!block5.isNull(position)) {
                        sum += DOUBLE.getDouble(block5, position);
                    }
                    if (!block6.isNull(position)) {
                        sum += DOUBLE.getDouble(block6, position);
                    }
                    if (!block7.isNull(position)) {
                        sum += DOUBLE.getDouble(block7, position);
                    }
                    if (!block8.isNull(position)) {
                        sum += VARCHAR.getSlice(block8, position).length();
                    }
                    if (!block9.isNull(position)) {
                        sum += VARCHAR.getSlice(block9, position).length();
                    }
                    if (!block10.isNull(position)) {
                        sum += VARCHAR.getSlice(block10, position).length();
                    }
                    if (!block11.isNull(position)) {
                        sum += VARCHAR.getSlice(block11, position).length();
                    }
                    if (!block12.isNull(position)) {
                        sum += VARCHAR.getSlice(block12, position).length();
                    }
                    if (!block13.isNull(position)) {
                        sum += VARCHAR.getSlice(block13, position).length();
                    }
                    if (!block14.isNull(position)) {
                        sum += VARCHAR.getSlice(block14, position).length();
                    }
                    if (!block15.isNull(position)) {
                        sum += VARCHAR.getSlice(block15, position).length();
                    }
                }
            }
            pageSource.close();
        }
        return sum;
    }

    public static RecordWriter createRecordWriter(List<? extends TpchColumn<?>> columns, File outputFile, HiveOutputFormat<?, ?> outputFormat, CompressionType compressionCodec)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);
        if (compressionCodec != CompressionType.none) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec.toString());
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK.toString());
            jobConf.set("parquet.compression", compressionCodec.toString());
            jobConf.set("parquet.enable.dictionary", "true");
            switch (compressionCodec) {
                case gzip:
                    jobConf.set("hive.exec.orc.default.compress", "ZLIB");
                    jobConf.set("hive.exec.orc.compress", "ZLIB");
                    break;
                case snappy:
                    jobConf.set("hive.exec.orc.default.compress", "SNAPPY");
                    jobConf.set("hive.exec.orc.compress", "SNAPPY");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported compression codec: " + compressionCodec);
            }
        }
        else {
            jobConf.set("parquet.enable.dictionary", "true");
            jobConf.set("hive.exec.orc.default.compress", "NONE");
            jobConf.set("hive.exec.orc.compress", "NONE");
        }

        RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != CompressionType.none,
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
            CompressionType compressionType,
            List<? extends TpchColumn<?>> columns)
            throws Exception
    {
        RecordWriter recordWriter = createRecordWriter(columns, outputFile, outputFormat, compressionType);

        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(transform(columns, input -> input.getColumnName()), transform(columns, input -> getObjectInspector(input)));

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
                Joiner.on(',').join(transform(columns, input -> input.getColumnName())));
        orderTableProperties.setProperty(
                "columns.types",
                Joiner.on(':').join(transform(columns, BenchmarkHiveFileFormats::getColumnType)));
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
                false,
                TupleDomain.<HiveColumnHandle>all());
    }

    private static List<HiveColumnHandle> getHiveColumnHandles(TpchColumn<?>... tpchColumns)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();
        for (TpchColumn<?> column : tpchColumns) {
            int ordinal = COLUMNS.indexOf(column);
            ObjectInspector inspector = getObjectInspector(column);
            HiveType hiveType = HiveType.valueOf(inspector.getTypeName());
            columns.add(new HiveColumnHandle("test",
                    column.getColumnName(),
                    hiveType,
                    hiveType.getTypeSignature(),
                    ordinal,
                    false));
        }

        return columns.build();
    }

    private static void logDuration(String test, String formatName, String readerType, CompressionType compressionType, long start, int loopCount, Object value)
    {
        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS);
        System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\n", test, formatName, readerType, compressionType, duration.toMillis(), value);
    }

    private static DataSize getFileSize(File outputFile)
    {
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    private static String getColumnType(TpchColumn<?> input)
    {
        switch (input.getType()) {
            case BIGINT:
                return "bigint";
            case DATE:
                return "date";
            case DOUBLE:
                return "double";
            case VARCHAR:
                return "string";
        }
        throw new IllegalArgumentException("Unsupported type " + input.getType());
    }

    private static ObjectInspector getObjectInspector(TpchColumn<?> input)
    {
        switch (input.getType()) {
            case BIGINT:
                return javaLongObjectInspector;
            case DATE:
                return javaDateObjectInspector;
            case DOUBLE:
                return javaDoubleObjectInspector;
            case VARCHAR:
                return javaStringObjectInspector;
        }
        throw new IllegalArgumentException("Unsupported type " + input.getType());
    }

    private static String getCursorType(HiveRecordCursorProvider recordCursorProvider)
    {
        if (recordCursorProvider instanceof GenericHiveRecordCursorProvider) {
            return "generic";
        }
        return "cursor";
    }

    private static FileSplit createFileSplit(File file)
    {
        try {
            Path lineitemPath = new Path(file.toURI());
            lineitemPath.getFileSystem(new Configuration()).setVerifyChecksum(false);
            return new FileSplit(lineitemPath, 0, file.length(), new String[0]);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class BenchmarkFile
    {
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final HiveOutputFormat<?, ?> outputFormat;
        @SuppressWarnings("deprecation")
        private final SerDe serDe;
        private final List<HiveRecordCursorProvider> recordCursorProviders;
        private final List<HivePageSourceFactory> pageSourceFactories;

        private BenchmarkFile(
                String name,
                InputFormat<?, ? extends Writable> inputFormat,
                HiveOutputFormat<?, ?> outputFormat,
                @SuppressWarnings("deprecation") SerDe serDe,
                Iterable<? extends HiveRecordCursorProvider> recordCursorProviders,
                Iterable<? extends HivePageSourceFactory> pageSourceFactories)
                throws Exception
        {
            this.name = name;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.serDe = serDe;
            this.recordCursorProviders = ImmutableList.copyOf(recordCursorProviders);
            this.pageSourceFactories = ImmutableList.copyOf(pageSourceFactories);

            serDe.initialize(new Configuration(), createTableProperties(COLUMNS));
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

        public File getFile(CompressionType compressionType)
        {
            return new File(DATA_DIR, "line_item." + getName() + compressionType.getFileExtension());
        }

        public List<HiveRecordCursorProvider> getRecordCursorProviders()
        {
            return recordCursorProviders;
        }

        public List<HivePageSourceFactory> getPageSourceFactory()
        {
            return pageSourceFactories;
        }
    }

    private static void workAroundParquetBrokenLoggingSetup()
            throws IOException
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            System.setOut(new PrintStream(nullOutputStream()));
            System.setErr(new PrintStream(nullOutputStream()));

            Log.getLog(Object.class);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
