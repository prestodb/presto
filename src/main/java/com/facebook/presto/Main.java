/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.UncompressedBlockSerde.UncompressedColumnWriter;
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
import java.util.List;
import java.util.concurrent.Callable;

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

    public static class BaseCommand implements Callable<Void>
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

    @Command(name = "csv", description = "Convert CSB to columns")
    public static class ConvertCsv extends BaseCommand
    {
        @Option(name = {"-d", "--column-delimiter"}, description = "Column delimiter character")
        public String columnSeparator = ",";

        @Option(name = {"-o", "--output-dir"}, description = "Output dir")
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

            ImmutableList.Builder<ColumnProcessor> processors = ImmutableList.builder();
            int index = 0;
            for (String type : types) {
                File file = new File(dir, "column" + index++ + ".data");
                switch (type) {
                    case "long":
                        processors.add(new UncompressedColumnWriter(newCreateDirectoryOutputStreamSupplier(file), FIXED_INT_64));
                        break;
                    case "string":
                        processors.add(new UncompressedColumnWriter(newCreateDirectoryOutputStreamSupplier(file), VARIABLE_BINARY));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }

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
            Csv.processCsv(inputSupplier, toChar(columnSeparator), processors.build());
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

        private OutputSupplier<FileOutputStream> newCreateDirectoryOutputStreamSupplier(final File file)
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
    }
}
