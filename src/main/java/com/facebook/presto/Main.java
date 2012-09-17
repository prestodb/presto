/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.block.ColumnMappingTupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.ingest.CsvReader;
import com.facebook.presto.ingest.CsvReader.CsvColumnProcessor;
import com.facebook.presto.ingest.RowSource;
import com.facebook.presto.ingest.RowSourceBuilder;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.airlift.command.*;
import io.airlift.command.Cli.CliBuilder;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.TupleInfo.Type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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
            checkArgument(types != null && !types.isEmpty(), "Type is required");

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
            ImmutableList.Builder<TupleStreamSerde> columnSerdeBuilder = ImmutableList.builder();
            for (String type : types) {
                // Extract base type and encoding
                // Examples: 'long_raw', 'string_rle', 'double_dic-rle'
                List<String> parts = ImmutableList.copyOf(Splitter.on('_').split(type));
                checkState(parts.size() == 2, "type format: <data_type>_<encoding> (e.g. long_raw, string_rle)");
                String dataType = parts.get(0);
                String encoding = parts.get(1);

                switch (dataType) {
                    case "long":
                        typeBuilder.add(Type.FIXED_INT_64);
                        csvColumns.add(CsvReader.csvNumericColumn());
                        break;
                    case "double":
                        typeBuilder.add(Type.DOUBLE);
                        csvColumns.add(CsvReader.csvDoubleColumn());
                        break;
                    case "string":
                        typeBuilder.add(Type.VARIABLE_BINARY);
                        csvColumns.add(CsvReader.csvStringColumn());
                        break;
                    case "fmillis":
                        typeBuilder.add(Type.FIXED_INT_64);
                        csvColumns.add(csvFloatMillisColumn());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
                columnSerdeBuilder.add(getTupleStreamSerde(encoding));
            }

            ImmutableList<TupleInfo.Type> columnTypes = typeBuilder.build();
            TupleInfo tupleInfo = new TupleInfo(columnTypes);
            CsvReader csvReader = new CsvReader(tupleInfo, inputSupplier, toChar(columnSeparator), csvColumns.build());
            ImmutableList<TupleStreamSerde> columnSerdes = columnSerdeBuilder.build();

            ImmutableList.Builder<RowSource> rowSources = ImmutableList.builder();
            ImmutableList.Builder<OutputStream> outputs = ImmutableList.builder();
            for (int index = 0; index < columnTypes.size(); index++) {
                RowSource rowSource = csvReader.getInput();
                OutputStream out = new FileOutputStream(new File(dir, "column" + index + "." + types.get(index) + ".data"));
                // TODO: chunk these tuple streams for better efficiency
                columnSerdes.get(index)
                        .createTupleStreamWriter(new OutputStreamSliceOutput(out))
                        .append(ColumnMappingTupleStream.map(rowSource, index))
                        .close();
                rowSources.add(rowSource);
                outputs.add(out);
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

    public static TupleStreamSerde getTupleStreamSerde(String encoding)
    {
        // Example: 'rle', 'uncompressed', 'dic-rle', 'dic-uncompressed'
        Iterator<String> partsIterator = Splitter.on('-').limit(2).split(encoding).iterator();
        checkArgument(partsIterator.hasNext(), "encoding malformed: " + encoding);
        switch (partsIterator.next()) {
            case "raw":
                return new UncompressedSerde();
            case "rle":
                return new RunLengthEncodedSerde();
            case "dic":
                checkArgument(partsIterator.hasNext(), "dictionary encoding requires an embedded serde");
                return new DictionarySerde(getTupleStreamSerde(partsIterator.next()));
            default:
                throw new IllegalArgumentException("Unsupported encoding " + encoding);
        }
    }

}
