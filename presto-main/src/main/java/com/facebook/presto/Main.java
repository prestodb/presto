/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.cli.Console;
import com.facebook.presto.ingest.BlockWriterFactory;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.ImportingOperator;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.ingest.RuntimeIOException;
import com.facebook.presto.ingest.SerdeBlockWriterFactory;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversOperator;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.NullOutputStream;
import com.google.common.io.OutputSupplier;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.Option;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.event.client.HttpEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Callable<Void>> builder = (CliBuilder<Callable<Void>>) (Object) Cli.buildCli("presto", Callable.class)
                .withDefaultCommand(Help.class)
                .withCommand(Server.class)
                .withCommand(ExampleSumAggregation.class)
                .withCommand(Execute.class)
                .withCommand(DemoQuery2.class)
                .withCommand(DemoQuery3.class)
                .withCommands(Help.class);

        builder.withGroup("example")
                .withDescription("run example queries")
                .withDefaultCommand(Help.class)
                .withCommand(ExampleSumAggregation.class);

        builder.withGroup("convert")
                .withDescription("convert file formats")
                .withDefaultCommand(Help.class)
                .withCommand(ConvertCsv.class);

        Cli<Callable<Void>> cli = builder.build();

        cli.parse(args).call();
    }

    public static class BaseCommand
            implements Callable<Void>
    {
        @Override
        public Void call()
                throws Exception
        {
            run();
            return null;
        }

        public void run()
                throws Exception
        {
            System.out.println(getClass().getSimpleName());
        }
    }

    @Command(name = "server", description = "Run the server")
    public static class Server
            extends BaseCommand
    {

        public void run()
        {
            Logger log = Logger.get(Main.class);
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new DiscoveryModule(),
                    new HttpServerModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new MBeanModule(),
                    new JmxModule(),
                    new JmxHttpModule(),
                    new LogJmxModule(),
                    new HttpEventModule(),
                    new TraceTokenModule(),
                    new ServerMainModule());

            try {
                Injector injector = app.strictConfig().initialize();
                injector.getInstance(Announcer.class).start();
            }
            catch (Throwable e) {
                log.error(e);
                System.exit(1);
            }
        }
    }

    @Command(name = "demo2", description = "Run the demo query 2")
    public static class DemoQuery2
            extends BaseCommand
    {

        private final DatabaseStorageManager storageManager;
        private final DatabaseMetadata metadata;

        public DemoQuery2()
        {
            DBI storageManagerDbi = new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1");
            DBI metadataDbi = new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1");

            storageManager = new DatabaseStorageManager(storageManagerDbi);
            metadata = new DatabaseMetadata(metadataDbi);

        }

        public void run()
        {
            for (int i = 0; i < 30; i++) {
                try {
                    long start = System.nanoTime();

                    BlockIterable ds = getColumn(storageManager, metadata, "hivedba_query_stats", "ds");
                    BlockIterable poolName = getColumn(storageManager, metadata, "hivedba_query_stats", "pool_name");
                    BlockIterable cpuMsec = getColumn(storageManager, metadata, "hivedba_query_stats", "cpu_msec");

                    AlignmentOperator alignmentOperator = new AlignmentOperator(ds, poolName, cpuMsec);
//                    Query2FilterAndProjectOperator filterAndProject = new Query2FilterAndProjectOperator(alignmentOperator);
                    FilterAndProjectOperator filterAndProject = new FilterAndProjectOperator(alignmentOperator,
                            new Query2Filter(),
                            singleColumn(VARIABLE_BINARY, 1, 0),
                            singleColumn(FIXED_INT_64, 2, 0));
                    HashAggregationOperator aggregation = new HashAggregationOperator(filterAndProject,
                            0,
                            ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(1, 0)),
                            ImmutableList.of(concat(singleColumn(Type.VARIABLE_BINARY, 0, 0), singleColumn(Type.FIXED_INT_64, 1, 0), singleColumn(Type.FIXED_INT_64, 2, 0))));

                    printResults(start, aggregation);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        private static class Query2Filter implements FilterFunction
        {
            private static final Slice constant2 = Slices.copiedBuffer("2012-10-11", Charsets.UTF_8);

            @Override
            public boolean filter(BlockCursor[] cursors)
            {
                Slice partition = cursors[0].getSlice(0);
                return partition.equals(constant2);
            }
        }
    }

    @Command(name = "demo3", description = "Run the demo query 3")
    public static class DemoQuery3
            extends BaseCommand
    {
        private final DatabaseStorageManager storageManager;
        private final DatabaseMetadata metadata;

        public DemoQuery3()
        {
            DBI storageManagerDbi = new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1");
            DBI metadataDbi = new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1");

            storageManager = new DatabaseStorageManager(storageManagerDbi);
            metadata = new DatabaseMetadata(metadataDbi);

        }

        public void run()
        {
            for (int i = 0; i < 30; i++) {
                try {
                    long start = System.nanoTime();

                    BlockIterable ds = getColumn(storageManager, metadata, "hivedba_query_stats", "ds");
                    BlockIterable startTime = getColumn(storageManager, metadata, "hivedba_query_stats", "start_time");
                    BlockIterable endTime = getColumn(storageManager, metadata, "hivedba_query_stats", "end_time");
                    BlockIterable cpuMsec = getColumn(storageManager, metadata, "hivedba_query_stats", "cpu_msec");

                    AlignmentOperator alignmentOperator = new AlignmentOperator(ds, startTime, endTime, cpuMsec);
//                    Query3FilterAndProjectOperator filterAndProject = new Query3FilterAndProjectOperator(alignmentOperator);
                    FilterAndProjectOperator filterAndProject = new FilterAndProjectOperator(alignmentOperator, new Query3Filter(), new Query3Projection());
                    AggregationOperator aggregation = new AggregationOperator(filterAndProject,
                            ImmutableList.of(CountAggregation.PROVIDER, LongSumAggregation.provider(0, 0)),
                            ImmutableList.of(concat(singleColumn(Type.FIXED_INT_64, 0, 0), singleColumn(Type.FIXED_INT_64, 1, 0))));

                    printResults(start, aggregation);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        private static class Query3Filter implements FilterFunction
        {
            private static final Slice constant1 = Slices.copiedBuffer("2012-07-01", Charsets.UTF_8);
            private static final Slice constant2 = Slices.copiedBuffer("2012-10-11", Charsets.UTF_8);

            @Override
            public boolean filter(BlockCursor[] cursors)
            {
                long startTime = cursors[1].getLong(0);
                long endTime = cursors[2].getLong(0);
                Slice partition = cursors[0].getSlice(0);
                return startTime <= 1343350800 && endTime > 1343350800 && partition.compareTo(constant1) >= 0 && partition.compareTo(constant2) <= 0;
            }
        }

        private static class Query3Projection implements ProjectionFunction
        {
            @Override
            public TupleInfo getTupleInfo()
            {
                return TupleInfo.SINGLE_LONG;
            }

            @Override
            public void project(BlockCursor[] cursors, BlockBuilder blockBuilder)
            {
                long startTime = cursors[1].getLong(0);
                long endTime = cursors[2].getLong(0);
                long cpuMsec = cursors[3].getLong(0);
                project(blockBuilder, startTime, endTime, cpuMsec);
            }

            @Override
            public void project(Tuple[] tuples, BlockBuilder blockBuilder)
            {
                long startTime = tuples[1].getLong(0);
                long endTime = tuples[2].getLong(0);
                long cpuMsec = tuples[3].getLong(0);
                project(blockBuilder, startTime, endTime, cpuMsec);
            }

            private BlockBuilder project(BlockBuilder blockBuilder, long startTime, long endTime, long cpuMsec)
            {
                return blockBuilder.append((cpuMsec * (1343350800 - startTime) + endTime + startTime) + (1000 * 86400));
            }
        }
    }

    private static BlockIterable getColumn(StorageManager storageManager, Metadata metadata, final String tableName, String columnName)
    {
        TableMetadata tableMetadata = metadata.getTable("default", "default", tableName);
        int index = 0;
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnName.equals(columnMetadata.getName())) {
                break;
            }
            ++index;
        }
        final int columnIndex = index;
        return storageManager.getBlocks("default", tableName, columnIndex);
    }

    private static void printResults(long start, Operator aggregation)
    {
        long rows = ConsolePrinter.print(aggregation);
        Duration duration = Duration.nanosSince(start);
        System.out.printf("%d rows in %4.2f ms\n", rows, duration.toMillis());
    }


    @Command(name = "sum", description = "Run an example sum aggregation")
    public static class ExampleSumAggregation
            extends BaseCommand
    {
        @Arguments(required = true)
        public URI server;

        public void run()
        {
            initializeLogging(false);

            ExecutorService executor = Executors.newCachedThreadPool();
            try {
                long start = System.nanoTime();

                ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setReadTimeout(new Duration(1, TimeUnit.MINUTES)));
                AsyncHttpClient asyncHttpClient = new AsyncHttpClient(httpClient, executor);
                QueryDriversOperator operator = new QueryDriversOperator(10,
                        new HttpQueryProvider("sum", asyncHttpClient, server)
                );
                // TODO: this currently leaks query resources (need to delete)
                printResults(start, operator);
            }
            finally {
                executor.shutdownNow();
            }
        }
    }

    @Command(name = "execute", description = "Execute a query")
    public static class Execute
            extends BaseCommand
    {
        private static final Logger log = Logger.get(Execute.class);

        @Option(name = "-s", title = "server", required = true)
        public URI server;

        @Option(name = "-q", title = "query", required = true)
        public String query;

        public void run()
        {
            initializeLogging(false);

            ExecutorService executor = Executors.newCachedThreadPool();
            try {
                long start = System.nanoTime();

                ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setReadTimeout(new Duration(30, TimeUnit.MINUTES)));
                AsyncHttpClient asyncHttpClient = new AsyncHttpClient(httpClient, executor);
                QueryDriversOperator operator = new QueryDriversOperator(10,
                        new HttpQueryProvider(query, asyncHttpClient, server)
                );
                // TODO: this currently leaks query resources (need to delete)
                printResults(start, operator);
            }
            finally {
                executor.shutdownNow();
            }
        }
    }

    @Command(name = "csv", description = "Convert CSV to columns")
    public static class ConvertCsv
            extends BaseCommand
    {
        private static final Logger log = Logger.get(ConvertCsv.class);

        @Option(name = {"-d", "--column-delimiter"}, description = "Column delimiter character")
        public String columnSeparator = ",";

        @Option(name = {"-o", "--output-dir"}, description = "Output directory")
        public String outputDir = "data";

        @Option(name = {"-t", "--column-type"}, description = "Column type specifications for extraction (e.g. 3:string:rle)")
        public List<String> extractionSpecs;

        @Arguments(description = "CSV file to convert")
        public String csvFile;

        @Override
        public void run()
                throws Exception
        {
            checkArgument(extractionSpecs != null && !extractionSpecs.isEmpty(), "Extraction Spec is required");
            InputSupplier<InputStreamReader> readerSupplier;
            if (csvFile != null) {
                readerSupplier = Files.newReaderSupplier(new File(csvFile), Charsets.UTF_8);
            }
            else {
                readerSupplier = new InputSupplier<InputStreamReader>()
                {
                    public InputStreamReader getInput()
                    {
                        return new InputStreamReader(System.in, Charsets.UTF_8);
                    }
                };
            }

            ImmutableSortedMap.Builder<Integer, Type> schemaBuilder = ImmutableSortedMap.naturalOrder();
            ImmutableList.Builder<OutputSupplier<? extends OutputStream>> outputSupplierBuilder = ImmutableList.builder();
            for (String extractionSpec : extractionSpecs) {
                // Extract column index, base type, and encodingName
                // Examples: '0:long:raw', '3:string:rle', '4:double:dicrle'
                List<String> parts = ImmutableList.copyOf(Splitter.on(':').split(extractionSpec));
                checkState(parts.size() == 3, "type format: <column_index>:<data_type>:<encoding> (e.g. 0:long:raw, 3:string:rle)");
                Integer columnIndex;
                try {
                    columnIndex = Integer.parseInt(parts.get(0));
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Malformed column index: " + parts.get(0));
                }
                String dataTypeName = parts.get(1);
                String encodingName = parts.get(2);

                schemaBuilder.put(columnIndex, TupleInfo.Type.fromName(dataTypeName));
                outputSupplierBuilder.add(newOutputStreamSupplier(new File(outputDir, String.format("column%d.%s_%s.data", columnIndex, dataTypeName, encodingName))));
            }
            
            ImmutableSortedMap<Integer, Type> schema = schemaBuilder.build();
            ImmutableList<OutputSupplier<? extends OutputStream>> outputSuppliers = outputSupplierBuilder.build();

            ImmutableList.Builder<RecordProjection> recordProjectionBuilder = ImmutableList.builder();
            ImmutableList.Builder<BlockWriterFactory> writersBuilder = ImmutableList.builder();
            for (int index = 0; index <= schema.lastKey(); index++) {
                // Default to VARIABLE_BINARY if we don't know some of the intermediate types
                Type type = VARIABLE_BINARY;
                if (schema.containsKey(index)) {
                    type = schema.get(index);
                }
                recordProjectionBuilder.add(RecordProjections.createProjection(index, type));
                writersBuilder.add(new SerdeBlockWriterFactory(FileEncoding.RAW, outputSuppliers.get(index)));
            }
            List<RecordProjection> recordProjections = recordProjectionBuilder.build();
            List<BlockWriterFactory> writers = writersBuilder.build();

            DelimitedRecordIterable records = new DelimitedRecordIterable(readerSupplier, Splitter.on(toChar(columnSeparator)));
            Operator source = new RecordProjectOperator(records, recordProjections);

            long rowCount = ImportingOperator.importData(source, writers);
            log.info("Importoed %d rows", rowCount);
        }

        private char toChar(String string)
        {
            checkArgument(!string.isEmpty(), "String is empty");
            if (string.length() == 1) {
                return string.charAt(0);
            }
            if (string.length() == 6 && string.startsWith("\\u")) {
                int value = Integer.parseInt(string.substring(2), 16);
                return (char) value;
            }
            throw new IllegalArgumentException(String.format("Can not convert '%s' to a char", string));
        }

        private OutputSupplier<FileOutputStream> newOutputStreamSupplier(final File file)
        {
            return new OutputSupplier<FileOutputStream>()
            {
                public FileOutputStream getOutput()
                        throws IOException
                {
                    Files.createParentDirs(file);
                    return new FileOutputStream(file);
                }
            };
        }
    }

    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = new Logging();
                logging.initialize(new LoggingConfiguration());
            }
            else {
                System.setOut(new PrintStream(new NullOutputStream()));
                System.setErr(new PrintStream(new NullOutputStream()));

                Logging logging = new Logging();
                logging.initialize(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
