package com.facebook.presto.cli;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.ProjectionTupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.ingest.DelimitedTupleStream;
import com.facebook.presto.ingest.StreamWriterTupleValueSink;
import com.facebook.presto.ingest.TupleStreamImporter;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@Command(name = "csv", description = "Convert CSV to columns")
public class ConvertCsv
        implements Runnable
{
    private static final Logger log = Logger.get(ConvertCsv.class);

    @Option(name = {"-d", "--column-delimiter"}, description = "Column delimiter character")
    public String columnSeparator = ",";

    @Option(name = {"-o", "--output-dir"}, description = "Output directory")
    public String outputDir = "data";

    @Option(name = {"-t",
                    "--column-type"}, description = "Column type specifications for extraction (e.g. 3:string:rle)")
    public List<String> extractionSpecs;

    @Arguments(description = "CSV file to convert")
    public String csvFile;

    @Override
    public void run()
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

        ImmutableSortedMap.Builder<Integer, TupleInfo.Type> schemaBuilder = ImmutableSortedMap.naturalOrder();
        ImmutableList.Builder<Integer> extractionColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<TupleStreamSerializer> serializerBuilder = ImmutableList.builder();
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
            extractionColumnsBuilder.add(columnIndex);
            serializerBuilder.add(TupleStreamSerdes.Encoding.fromName(encodingName).createSerde().createSerializer());
            outputSupplierBuilder.add(newOutputStreamSupplier(new File(outputDir, String.format("column%d.%s_%s.data", columnIndex, dataTypeName, encodingName))));
        }
        ImmutableSortedMap<Integer, TupleInfo.Type> schema = schemaBuilder.build();
        ImmutableList<Integer> extractionColumns = extractionColumnsBuilder.build();
        ImmutableList<TupleStreamSerializer> serializers = serializerBuilder.build();
        ImmutableList<OutputSupplier<? extends OutputStream>> outputSuppliers = outputSupplierBuilder.build();

        ImmutableList.Builder<TupleInfo.Type> inferredTypes = ImmutableList.builder();
        for (int index = 0; index <= schema.lastKey(); index++) {
            if (schema.containsKey(index)) {
                inferredTypes.add(schema.get(index));
            }
            else {
                // Default to VARIABLE_BINARY if we don't know some of the intermediate types
                inferredTypes.add(TupleInfo.Type.VARIABLE_BINARY);
            }
        }

        TupleInfo tupleInfo = new TupleInfo(inferredTypes.build());

        try (InputStreamReader input = inputSupplier.getInput()) {
            DelimitedTupleStream delimitedTupleStream = new DelimitedTupleStream(input, Splitter.on(toChar(columnSeparator)), tupleInfo);
            ProjectionTupleStream projectedStream = new ProjectionTupleStream(delimitedTupleStream, extractionColumns);

            ImmutableList.Builder<StreamWriterTupleValueSink> tupleValueSinkBuilder = ImmutableList.builder();
            for (int index = 0; index < extractionColumns.size(); index++) {
                tupleValueSinkBuilder.add(new StreamWriterTupleValueSink(outputSuppliers.get(index), serializers.get(index)));
            }

            long rowCount = TupleStreamImporter.importFrom(projectedStream, tupleValueSinkBuilder.build());
            log.info("Imported %d rows", rowCount);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
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
                Files.createParentDirs(file);
                return new FileOutputStream(file);
            }
        };
    }
}
