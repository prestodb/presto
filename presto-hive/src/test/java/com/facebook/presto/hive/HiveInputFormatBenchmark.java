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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.nameGetter;
import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.objectInspectorGetter;
import static com.facebook.presto.hive.HiveInputFormatBenchmark.HiveColumn.typeNameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;

@SuppressWarnings({"deprecation", "UseOfSystemOutOrSystemErr"})
public final class HiveInputFormatBenchmark
{
    public static final int LOOPS = 1;
    private static final String NOT_SUPPORTED = "NOT_SUPPORTED";
    private static final File DATA_DIR = new File("target");
    private static final int ROWS_PER_NULL = 7;

    private static final List<HiveColumn> LINE_ITEM_COLUMNS = ImmutableList.of(
            new HiveColumn("orderkey", javaLongObjectInspector),
            new HiveColumn("partkey", javaLongObjectInspector),
            new HiveColumn("suppkey", javaLongObjectInspector),
            new HiveColumn("linenumber", javaLongObjectInspector),
            new HiveColumn("quantity", javaLongObjectInspector),
            new HiveColumn("extendedprice", javaDoubleObjectInspector),
            new HiveColumn("discount", javaDoubleObjectInspector),
            new HiveColumn("tax", javaDoubleObjectInspector),
            new HiveColumn("returnflag", javaStringObjectInspector),
            new HiveColumn("linestatus", javaStringObjectInspector),
            new HiveColumn("shipdate", javaStringObjectInspector),
            new HiveColumn("commitdate", javaStringObjectInspector),
            new HiveColumn("receiptdate", javaStringObjectInspector),
            new HiveColumn("shipinstruct", javaStringObjectInspector),
            new HiveColumn("shipmode", javaStringObjectInspector),
            new HiveColumn("comment", javaStringObjectInspector)
    );

    private HiveInputFormatBenchmark()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        workAroundParquetBrokenLoggingSetup();

        HadoopNative.requireHadoopNative();
        DATA_DIR.mkdirs();

        List<BenchmarkFile> benchmarkFiles = ImmutableList.of(
                new BenchmarkFile(
                        false,
                        "dwrf",
                        "dwrf",
                        new com.facebook.hive.orc.OrcInputFormat(),
                        new com.facebook.hive.orc.OrcOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new com.facebook.hive.orc.OrcSerde();
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(
                                new BenchmarkLineItemDwrf2(),
                                new BenchmarkLineItemDwrf())
                ),
//                                new BenchmarkLineItemGeneric())),

                new BenchmarkFile(
                        true,
                        "orc",
                        "orc",
                        new org.apache.hadoop.hive.ql.io.orc.OrcInputFormat(),
                        new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new org.apache.hadoop.hive.ql.io.orc.OrcSerde();
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(
//                                new BenchmarkLineItemGeneric(),
//                                new BenchmarkLineItemOrcCustom())
//                                new BenchmarkLineItemOrcVectorized(),
                                new BenchmarkLineItemCustomOrcVectorized(),
                                new BenchmarkLineItemOrcVectorized())
                ),

                new BenchmarkFile(
                        false,
                        "rc binary gzip",
                        "rc-binary.gz",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new LazyBinaryColumnarSerDe();
                            }
                        },
                        "gzip",
                        true,
                        ImmutableList.of(
//                                new BenchmarkLineItemGeneric(),
                                new BenchmarkLineItemRCBinary(),
//                                new BenchmarkLineItemRCBinaryVectorized(), // super slow
                                new BenchmarkLineItemRCBinaryVectorizedCustom())
                ),

                new BenchmarkFile(
                        false,
                        "parquet gzip",
                        "parquet.gz",
                        new MapredParquetInputFormat(),
                        new MapredParquetOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new ParquetHiveSerDe();
                            }
                        },
                        "gzip",
                        true,
                        ImmutableList.of(
//                                new BenchmarkLineItemGeneric(),
                                new BenchmarkLineItemParquet())
                ),

                new BenchmarkFile(
                        false,
                        "parquet snappy",
                        "parquet.snappy",
                        new MapredParquetInputFormat(),
                        new MapredParquetOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new ParquetHiveSerDe();
                            }
                        },
                        "snappy",
                        true,
                        ImmutableList.of(
                                new BenchmarkLineItemGeneric(),
                                new BenchmarkLineItemParquet())
                ),

                new BenchmarkFile(
                        false,
                        "parquet uncompressed",
                        "parquet uncompressed",
                        new MapredParquetInputFormat(),
                        new MapredParquetOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new ParquetHiveSerDe();
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(
                                new BenchmarkLineItemGeneric(),
                                new BenchmarkLineItemParquet())
                ),

                new BenchmarkFile(
                        false,
                        "rc text gzip",
                        "rc.gz",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new ColumnarSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "gzip",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "rc binary",
                        "rc-binary",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new LazyBinaryColumnarSerDe();
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "rc binary snappy",
                        "rc-binary.snappy",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                return new LazyBinaryColumnarSerDe();
                            }
                        },
                        "snappy",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "rc text",
                        "rc",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new ColumnarSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "rc text snappy",
                        "rc.snappy",
                        new RCFileInputFormat<>(),
                        new RCFileOutputFormat(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new ColumnarSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "snappy",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "text",
                        "txt",
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "text gzip",
                        "txt.gz",
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "gzip",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "text snappy",
                        "txt.snappy",
                        new TextInputFormat(),
                        new HiveIgnoreKeyTextOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "snappy",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "sequence",
                        "sequence",
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        null,
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "sequence gzip",
                        "sequence.gz",
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "gzip",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                ),

                new BenchmarkFile(
                        false,
                        "sequence snappy",
                        "sequence.snappy",
                        new SequenceFileInputFormat<Object, Writable>(),
                        new HiveSequenceFileOutputFormat<>(),
                        new Supplier<SerDe>()
                        {
                            @Override
                            public SerDe get()
                            {
                                try {
                                    return new LazySimpleSerDe();
                                }
                                catch (SerDeException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        },
                        "snappy",
                        true,
                        ImmutableList.of(new BenchmarkLineItemGeneric())
                )
        );

        benchmarkFiles = ImmutableList.copyOf(Iterables.filter(benchmarkFiles, new Predicate<BenchmarkFile>()
        {
            @Override
            public boolean apply(BenchmarkFile benchmarkFile)
            {
                return benchmarkFile.isEnabled();
            }
        }));

        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);
        runBenchmarks("WARM UP", benchmarkFiles, jobConf, 5, false, true, true);
        runBenchmarks("BENCHMARK", benchmarkFiles, jobConf, 5, false, true, true);
    }

    private static void runBenchmarks(
            String phase,
            List<BenchmarkFile> benchmarkFiles,
            JobConf jobConf,
            int loopCount,
            boolean benchmarkWrite,
            boolean benchmarkRead,
            boolean benchmarkReadWithNulls)
            throws Exception
    {
        System.out.println();
        System.out.println();
        System.out.println("============ " + phase + " ============");
        for (BenchmarkFile benchmarkFile : benchmarkFiles) {
            if (benchmarkWrite) {
                benchmarkLineItemWrite(benchmarkFile, loopCount);
            }

            for (BenchmarkLineItem benchmarkLineItem : benchmarkFile.getLineItemBenchmarks()) {
                if (benchmarkRead) {
                    benchmarkLineItem(jobConf, benchmarkFile, loopCount, benchmarkLineItem);
                }
                if (benchmarkReadWithNulls) {
                    benchmarkLineItemWithNulls(jobConf, benchmarkFile, loopCount, benchmarkLineItem);
                }
            }
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

//            Log.getLog(Object.class);
//            Logging logging = Logging.initialize();
//            logging.configure(new LoggingConfiguration());
//            logging.disableConsole();
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }

//        LoggingMBean logging = new LoggingMBean();
//        logging.setLevel("com.hadoop", "OFF");
//        logging.setLevel("org.apache.hadoop", "OFF");
//        logging.setLevel("org.apache.zookeeper", "OFF");
//        logging.setLevel("parquet", "OFF");
    }

    private static void benchmarkLineItem(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount, BenchmarkLineItem benchmarkLineItem)
            throws Exception
    {
        benchmarkLineItem(jobConf,
                loopCount,
                6001215L,
                benchmarkLineItem,
                benchmarkFile.getName(),
                benchmarkFile.getLineItemFileSplit(),
                benchmarkFile.getInputFormat(),
                benchmarkFile.getLineItemSerDe());
    }

    private static void benchmarkLineItemWithNulls(JobConf jobConf, BenchmarkFile benchmarkFile, int loopCount, BenchmarkLineItem benchmarkLineItem)
            throws Exception
    {
        benchmarkLineItem(jobConf,
                loopCount,
                6858532L,
                benchmarkLineItem,
                benchmarkFile.getName() + " w/ nulls",
                benchmarkFile.getLineItemWithNullsFileSplit(),
                benchmarkFile.getInputFormat(),
                benchmarkFile.getLineItemWithNullSerDe());
    }

    private static void benchmarkLineItem(JobConf jobConf,
            int loopCount,
            long expectedRowCount,
            BenchmarkLineItem benchmarkLineItem,
            String name, FileSplit fileSplit,
            InputFormat<?, ? extends Writable> inputFormat,
            Deserializer deserializer)
            throws Exception
    {
        System.out.println();
        System.out.println(name + " " + benchmarkLineItem.getName());

        Object value = null;

        long start;

        //
        // orderKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.orderKey(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("orderKey", start, loopCount, value);
        checkResult2(value, 18005322964949L, "orderKey");

        //
        // orderKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.partKey(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("partKey", start, loopCount, value);
        checkResult(value, 600229457837L, "partKey");

        //
        // supplierKey
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.supplierKey(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("supplierKey", start, loopCount, value);
        checkResult(value, 30009691369L, "supplierKey");

        //
        // lineNumber
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.lineNumber(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("lineNumber", start, loopCount, value);
        checkResult(value, 18007100L, "lineNumber");

        //
        // quantity
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.quantity(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("quantity", start, loopCount, value);
        checkResult(value, 153078795L, "quantity");

        //
        // extendedPrice
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.extendedPrice(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("extendedPrice", start, loopCount, value);
        checkResult(value, 2.2957731090119733E11, "extendedPrice");

        //
        // discount
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.discount(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("discount", start, loopCount, value);
        checkResult(value, 300057.33000053867, "discount");

        //
        // tax
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tax(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("tax", start, loopCount, value);
        checkResult(value, 240129.66999598275, "tax");

        //
        // returnFlag
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.returnFlag(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("returnFlag", start, loopCount, value);
        checkResult(value, 6001215L, "returnFlag");

        //
        // status
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.status(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("status", start, loopCount, value);
        checkResult(value, 6001215L, "status");

        //
        // shipDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipDate(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("shipDate", start, loopCount, value);
        checkResult(value, 60012150L, "shipDate");

        //
        // commitDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.commitDate(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("commitDate", start, loopCount, value);
        checkResult(value, 60012150L, "commitDate");

        //
        // receiptDate
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.receiptDate(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("receiptDate", start, loopCount, value);
        checkResult(value, 60012150L, "receiptDate");

        //
        // shipInstructions
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipInstructions(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("shipInstructions", start, loopCount, value);
        checkResult(value, 72006409L, "shipInstructions");

        //
        // shipMode
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.shipMode(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("shipMode", start, loopCount, value);
        checkResult(value, 25717034L, "shipMode");

        //
        // comment
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.comment(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("comment", start, loopCount, value);
        checkResult(value, 158997209L, "comment");

        //
        // tpchQuery6
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tpchQuery6(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("tpchQuery6", start, loopCount, value);
        checkResult(value, ImmutableList.of(1.53078795E8, 2.2957731090119733E11, 300057.33000053867, 60012150L), "tpchQuery6");

        //
        // tpchQuery1
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.tpchQuery1(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("tpchQuery1", start, loopCount, value);
        checkResult(value, ImmutableList.of(1.53078795E8, 2.2957731090119733E11, 300057.33000053867, 240129.66999598275, 6001215L, 6001215L, 60012150L), "tpchQuery1");

        //
        // all
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.all(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("all", start, loopCount, value);
        checkResult(value, ImmutableList.of(
                expectedRowCount,
                18005322964949L,
                600229457837L,
                30009691369L,
                18007100L,
                1.53078795E8,
                2.2957731090119733E11,
                300057.33000053867,
                240129.66999598275,
                6001215L,
                6001215L,
                60012150L,
                60012150L,
                60012150L,
                72006409L,
                25717034L,
                158997209L), "all");

        //
        // all read one
        //
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            value = benchmarkLineItem.allReadOne(jobConf, fileSplit, inputFormat, deserializer);
        }
        logDuration("all (read one)", start, loopCount, value);
        checkResult(value, ImmutableList.of(18005322964949L), "orderKey");

        if (benchmarkLineItem instanceof BenchmarkLineItemWithPredicatePushdown) {
            BenchmarkLineItemWithPredicatePushdown orcVectorized = (BenchmarkLineItemWithPredicatePushdown) benchmarkLineItem;

            //
            // allNoMatch
            //
            start = System.nanoTime();
            for (int loops = 0; loops < loopCount; loops++) {
                value = orcVectorized.allNoMatch(jobConf, fileSplit, inputFormat, deserializer);
            }
            logDuration("allNoMatch", start, loopCount, value);
            checkResult(value, ImmutableList.of(0L, 0L, 0L, 0L, 0L, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), "allNoMatch");

            //
            // allSmallMatch
            //
            start = System.nanoTime();
            for (int loops = 0; loops < loopCount; loops++) {
                value = orcVectorized.allSmallMatch(jobConf, fileSplit, inputFormat, deserializer);
            }
            logDuration("allSmallMatch", start, loopCount, value);
            value = value == null ? null : ((List<?>) value).get(0);
            checkResult(value, 50_000L, "allSmallMatch");
        }
    }

    @SuppressWarnings("PublicField")
    public static volatile Object result;

    private static void logDuration(String label, long start, int loopCount, Object value)
    {
        if (NOT_SUPPORTED.equals(value)) {
            return;
        }

        long end = System.nanoTime();
        long nanos = end - start;
        Duration duration = new Duration(1.0 * nanos / loopCount, NANOSECONDS).convertTo(SECONDS);
        System.out.printf("%16s %6s\n", label, duration);
        result = value;
    }

    private static class BenchmarkFile
    {
        private final boolean enabled;
        private final String name;
        private final InputFormat<?, ? extends Writable> inputFormat;
        private final HiveOutputFormat<?, ?> outputFormat;
        private final String compressionCodec;
        private final List<BenchmarkLineItem> lineItemBenchmarks;

        private final SerDe lineItemDeserializer;
        private final FileSplit lineItemFileSplit;

        private final SerDe lineItemWithNullDeserializer;
        private final FileSplit lineItemWithNullsFileSplit;

        public BenchmarkFile(
                boolean enabled,
                String name,
                String fileExtension,
                InputFormat<?, ? extends Writable> inputFormat,
                HiveOutputFormat<?, ?> outputFormat,
                Supplier<SerDe> serDeFactory,
                String compressionCodec,
                boolean verifyChecksum,
                List<? extends BenchmarkLineItem> lineItemBenchmarks)
                throws Exception
        {
            this.enabled = enabled;
            this.name = name;
            this.inputFormat = inputFormat;
            this.outputFormat = outputFormat;
            this.compressionCodec = compressionCodec;
            this.lineItemBenchmarks = ImmutableList.copyOf(lineItemBenchmarks);

            SerDe lineItemSerDe = serDeFactory.get();
            this.lineItemDeserializer = lineItemSerDe;
            File lineItemFile = new File(DATA_DIR, "line_item." + fileExtension);
            lineItemSerDe.initialize(new Configuration(), createTableProperties(LINE_ITEM_COLUMNS));
            if (enabled && !lineItemFile.exists()) {
                writeLineItems(lineItemFile, outputFormat, lineItemSerDe, compressionCodec, 0);
            }
            Path lineItemPath = new Path(lineItemFile.toURI());
            lineItemPath.getFileSystem(new Configuration()).setVerifyChecksum(verifyChecksum);
            this.lineItemFileSplit = new FileSplit(lineItemPath, 0, lineItemFile.length(), new String[0]);

            SerDe lineItemWithNullSerDe = serDeFactory.get();
            this.lineItemWithNullDeserializer = lineItemSerDe;
            File lineItemWithNullFile = new File(DATA_DIR, "line_item_with_nulls." + fileExtension);
            lineItemWithNullSerDe.initialize(new Configuration(), createTableProperties(LINE_ITEM_COLUMNS));
            if (enabled && !lineItemWithNullFile.exists()) {
                writeLineItems(lineItemWithNullFile, outputFormat, lineItemWithNullSerDe, compressionCodec, ROWS_PER_NULL);
            }
            Path lineItemWithNullPath = new Path(lineItemWithNullFile.toURI());
            lineItemWithNullPath.getFileSystem(new Configuration()).setVerifyChecksum(verifyChecksum);
            this.lineItemWithNullsFileSplit = new FileSplit(lineItemWithNullPath, 0, lineItemWithNullFile.length(), new String[0]);
        }

        public boolean isEnabled()
        {
            return enabled;
        }

        private String getName()
        {
            return name;
        }

        private InputFormat<?, ? extends Writable> getInputFormat()
        {
            return inputFormat;
        }

        public HiveOutputFormat<?, ?> getOutputFormat()
        {
            return outputFormat;
        }

        public SerDe getLineItemSerDe()
        {
            return lineItemDeserializer;
        }

        public FileSplit getLineItemFileSplit()
        {
            return lineItemFileSplit;
        }

        public SerDe getLineItemWithNullSerDe()
        {
            return lineItemWithNullDeserializer;
        }

        public FileSplit getLineItemWithNullsFileSplit()
        {
            return lineItemWithNullsFileSplit;
        }

        public List<BenchmarkLineItem> getLineItemBenchmarks()
        {
            return lineItemBenchmarks;
        }

        public String getCompressionCodec()
        {
            return compressionCodec;
        }
    }

    public static Properties createTableProperties(List<HiveColumn> columns)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty(
                "columns",
                Joiner.on(',').join(Iterables.transform(columns, nameGetter())));
        orderTableProperties.setProperty(
                "columns.types",
                Joiner.on(':').join(Iterables.transform(columns, typeNameGetter())));
        return orderTableProperties;
    }

    @SuppressWarnings("UnusedDeclaration")
    private static void benchmarkLineItemWrite(BenchmarkFile benchmarkFile, int loopCount)
            throws Exception
    {
        System.out.println();
        System.out.println(benchmarkFile.getName());

        long start;

        //
        // orderKey
        //
        File lineItemFile = new File(DATA_DIR, "line_item." + benchmarkFile.getName());
        start = System.nanoTime();
        for (int loops = 0; loops < loopCount; loops++) {
            lineItemFile.delete();
            writeLineItems(lineItemFile, benchmarkFile.getOutputFormat(), benchmarkFile.getLineItemSerDe(), benchmarkFile.getCompressionCodec(), 0);
        }
        logDuration("write lineItem", start, loopCount, null);
    }

    public static DataSize writeLineItems(File outputFile, HiveOutputFormat<?, ?> outputFormat, SerDe serDe, String compressionCodec, int rowsPerNull)
            throws Exception
    {
        RecordWriter recordWriter = createRecordWriter(LINE_ITEM_COLUMNS, outputFile, outputFormat, compressionCodec);

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector(LINE_ITEM_COLUMNS);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        int rowNumber = 0;
        for (LineItem lineItem : new LineItemGenerator(1, 1, 1)) {
            if (rowsPerNull > 0 && rowNumber % rowsPerNull == 0) {
                for (int columnIndex = 0; columnIndex < 16; columnIndex++) {
                    objectInspector.setStructFieldData(row, fields.get(columnIndex), null);
                }
                Writable record = serDe.serialize(row, objectInspector);
                recordWriter.write(record);
            }
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

            Writable record = serDe.serialize(row, objectInspector);
            recordWriter.write(record);
            rowNumber++;
        }

        recordWriter.close(false);
        return new DataSize(outputFile.length(), Unit.BYTE).convertToMostSuccinctDataSize();
    }

    public static RecordWriter createRecordWriter(List<HiveColumn> columns, File outputFile, HiveOutputFormat<?, ?> outputFormat, String compressionCodec)
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

    public static SettableStructObjectInspector createSettableStructObjectInspector(List<HiveColumn> columns)
    {
        return getStandardStructObjectInspector(
                Lists.transform(columns, nameGetter()),
                Lists.transform(columns, objectInspectorGetter()));
    }

    public static final class HiveColumn
    {
        private final String name;
        private final ObjectInspector objectInspector;

        private HiveColumn(String name, ObjectInspector objectInspector)
        {
            this.name = checkNotNull(name, "name is null");
            this.objectInspector = checkNotNull(objectInspector, "objectInspector is null");
        }

        public String getName()
        {
            return name;
        }

        public String getTypeName()
        {
            return objectInspector.getTypeName();
        }

        public ObjectInspector getObjectInspector()
        {
            return objectInspector;
        }

        public static Function<HiveColumn, String> nameGetter()
        {
            return new Function<HiveColumn, String>()
            {
                @Override
                public String apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getName();
                }
            };
        }

        public static Function<HiveColumn, String> typeNameGetter()
        {
            return new Function<HiveColumn, String>()
            {
                @Override
                public String apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getTypeName();
                }
            };
        }

        public static Function<HiveColumn, ObjectInspector> objectInspectorGetter()
        {
            return new Function<HiveColumn, ObjectInspector>()
            {
                @Override
                public ObjectInspector apply(HiveColumn hiveColumn)
                {
                    return hiveColumn.getObjectInspector();
                }
            };
        }
    }

    private static void checkResult(Object actual, Object expected, String name)
    {
        checkResult2(actual, expected, name);
    }

    private static void checkResult2(Object actual, Object expected, String name)
    {
        if (!Objects.equals(actual, expected)) {
            throw new RuntimeException(String.format("Expected %s to be %s but was %s", name, expected, actual));
        }
    }
}
