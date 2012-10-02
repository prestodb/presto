/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.ingest.BlockDataImporter;
import com.facebook.presto.ingest.BlockExtractor;
import com.facebook.presto.ingest.DelimitedBlockExtractor;
import com.facebook.presto.ingest.RuntimeIOException;
import com.facebook.presto.operator.ConsolePrinter.DelimitedTuplePrinter;
import com.facebook.presto.operator.ConsolePrinter.TuplePrinter;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversTupleStream;
import com.facebook.presto.server.ServerMainModule;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
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
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.facebook.presto.ingest.BlockDataImporter.ColumnImportSpec;
import static com.facebook.presto.ingest.DelimitedBlockExtractor.ColumnDefinition;
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

                AsyncHttpClient asyncHttpClient = new AsyncHttpClient(new ApacheHttpClient(), executor);
                QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(new TupleInfo(Type.VARIABLE_BINARY, Type.FIXED_INT_64), 10,
                        new HttpQueryProvider("sum", asyncHttpClient, server)
                );

//                TuplePrinter tuplePrinter = new RecordTuplePrinter();
                TuplePrinter tuplePrinter = new DelimitedTuplePrinter();

                int count = 0;
                Cursor cursor = tupleStream.cursor();
                while (cursor.advanceNextPosition()) {
                    count++;
                    tuplePrinter.print(cursor.getTuple());
                }
                Duration duration = Duration.nanosSince(start);

                System.out.printf("%d rows in %4.2f ms\n", count, duration.toMillis());
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
            InputSupplier<InputStreamReader> inputSupplier;
            if (csvFile != null) {
                inputSupplier = Files.newReaderSupplier(new File(csvFile), Charsets.UTF_8);
            }
            else {
                inputSupplier = new InputSupplier<InputStreamReader>()
                {
                    public InputStreamReader getInput()
                    {
                        return new InputStreamReader(System.in, Charsets.UTF_8);
                    }
                };
            }

            ImmutableList.Builder<ColumnDefinition> columnDefinitionBuilder = ImmutableList.builder();
            ImmutableList.Builder<ColumnImportSpec> columnImportSpecBuilder = ImmutableList.builder();
            for (String extractionSpec : extractionSpecs) {
                // Extract column index, base type, and encodingName
                // Examples: '0:long:raw', '3:string:rle', '4:double:dic/rle'
                List<String> parts = ImmutableList.copyOf(Splitter.on(':').split(extractionSpec));
                checkState(parts.size() == 3, "type format: <column_index>:<data_type>:<encoding> (e.g. 0:long:raw, 3:string:rle)");
                Integer columnIndex;
                try {
                    columnIndex = Integer.parseInt(parts.get(0));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Malformed column index: " + parts.get(0));
                }
                String dataTypeName = parts.get(1);
                String encodingName = parts.get(2);

                columnDefinitionBuilder.add(new ColumnDefinition(columnIndex, TupleInfo.Type.fromName(dataTypeName)));
                columnImportSpecBuilder.add(
                        new ColumnImportSpec(
                                TupleStreamSerdes.createTupleStreamSerde(TupleStreamSerdes.Encoding.fromName(encodingName)),
                                // HACK: replace('/', '-') to deal with illegal file name characters
                                newOutputStreamSupplier(new File(outputDir, String.format("column%d.%s_%s.data", columnIndex, dataTypeName, encodingName.replace('/', '-'))))
                        )
                );
            }

            BlockExtractor blockExtractor = new DelimitedBlockExtractor(Splitter.on(toChar(columnSeparator)), columnDefinitionBuilder.build());
            BlockDataImporter importer = new BlockDataImporter(blockExtractor, columnImportSpecBuilder.build());
            importer.importFrom(inputSupplier);
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
