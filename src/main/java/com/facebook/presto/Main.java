/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.Option;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.CsvReader.CsvColumnProcessor;
import static com.facebook.presto.CsvReader.csvNumericColumn;
import static com.facebook.presto.CsvReader.csvStringColumn;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Callable<Void>> builder = (CliBuilder<Callable<Void>>) (Object) Cli.buildCli("presto", Callable.class)
                .withDefaultCommand(Help.class)
                .withCommands(Help.class);

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

    @Command(name = "csv", description = "Convert CSV to columns")
    public static class ConvertCsv
            extends BaseCommand
    {
        @Option(name = {"-d", "--column-delimiter"}, description = "Column delimiter character")
        public String columnSeparator = ",";

        @Option(name = {"-o", "--output-dir"}, description = "Output directory")
        public String outputDir = "data";

        @Option(name = {"-t", "--type"}, description = "Column type")
        public List<String> types;

        @Arguments(description = "CSV file to convert")
        public String csvFile;

        @Override
        public void run()
                throws Exception
        {
            Preconditions.checkArgument(types != null && !types.isEmpty(), "Type is required");

            File dir = new File(outputDir);

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

            ImmutableList.Builder<TupleInfo.Type> typeBuilder = ImmutableList.builder();
            ImmutableList.Builder<CsvColumnProcessor> csvColumns = ImmutableList.builder();
            for (String type : types) {
                switch (type) {
                    case "long":
                        typeBuilder.add(FIXED_INT_64);
                        csvColumns.add(csvNumericColumn());
                        break;
                    case "string":
                        typeBuilder.add(VARIABLE_BINARY);
                        csvColumns.add(csvStringColumn());
                        break;
                    case "fmillis":
                        typeBuilder.add(FIXED_INT_64);
                        csvColumns.add(csvFloatMillisColumn());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }

            ImmutableList<TupleInfo.Type> columnTypes = typeBuilder.build();
            TupleInfo tupleInfo = new TupleInfo(columnTypes);
            CsvReader csvReader = new CsvReader(tupleInfo, inputSupplier, toChar(columnSeparator), csvColumns.build());

            ImmutableList.Builder<ColumnProcessor> processorsBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowSource> rowSources = ImmutableList.builder();
            ImmutableList.Builder<OutputStream> outputs = ImmutableList.builder();
            for (int index = 0; index < columnTypes.size(); index++) {
                TupleInfo.Type type = columnTypes.get(index);
                RowSource rowSource = csvReader.getInput();
                File file = new File(dir, "column" + index + ".data");
                OutputStream out = new FileOutputStream(file);
                processorsBuilder.add(new UncompressedColumnWriter(type, index, rowSource.cursor(), out));
                rowSources.add(rowSource);
                outputs.add(out);
            }
            List<ColumnProcessor> processors = processorsBuilder.build();

            ColumnProcessors.process(processors);

            for (ColumnProcessor processor : processors) {
                processor.finish();
            }

            for (RowSource rowSource : rowSources.build()) {
                rowSource.close();
            }
            for (OutputStream out : outputs.build()) {
                out.close();
            }
        }

        private char toChar(String string)
        {
            Preconditions.checkArgument(!string.isEmpty(), "String is empty");
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
                    file.getParentFile().mkdirs();
                    return new FileOutputStream(file);
                }
            };
        }

        public static CsvColumnProcessor csvFloatMillisColumn()
        {
            return new CsvColumnProcessor()
            {
                @Override
                public void process(String value, RowSourceBuilder.RowBuilder rowBuilder)
                {
                    rowBuilder.append((long) Double.parseDouble(value));
                }
            };
        }
    }
}
