/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.command.Option;

import javax.validation.constraints.Min;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;

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
                .withCommand(FromCsv.class);

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

    @Command(name = "fromCsv", description = "Convert file from csv to new format")
    public static class FromCsv extends BaseCommand
    {
        @Option(name = {"-c", "--column"}, description = "Column to read")
        @Min(0)
        public int column;

        @Option(name = {"--column-separator"}, description = "Column separator")
        public char columnSeparator = ',';

        @Option(name = {"-o", "--output-file"}, description = "Output file")
        public String outputFile;

        @Arguments(description = "Files to convert")
        public List<String> files;

        @Override
        public void run()
                throws Exception
        {
            OutputStream out;
            if (outputFile != null) {
                out = new FileOutputStream(outputFile);
            } else {
                out = System.out;
            }

            try {
                for (String fileName : files) {
                    CsvFileScanner scanner = new CsvFileScanner(Files.newReaderSupplier(new File(fileName), Charsets.UTF_8), column, columnSeparator, Type.VARIABLE_BINARY);
                    UncompressedBlockSerde.write(scanner.iterator(), out);
                }
            }
            finally {
                if (outputFile != null) {
                    out.close();
                }
            }
        }
    }
}
