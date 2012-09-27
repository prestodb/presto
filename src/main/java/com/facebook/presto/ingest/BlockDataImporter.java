package com.facebook.presto.ingest;

import com.facebook.presto.block.ColumnMappingTupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class BlockDataImporter
{
    private final BlockExtractor blockExtractor;
    private final List<ColumnImportSpec> columnImportSpecs;

    public BlockDataImporter(BlockExtractor blockExtractor, List<ColumnImportSpec> columnImportSpecs)
    {
        checkNotNull(blockExtractor, "blockExtractor is null");
        checkNotNull(columnImportSpecs, "columnImportSpecs is null");
        checkArgument(!columnImportSpecs.isEmpty(), "need to specify at least one column to import");

        this.blockExtractor = blockExtractor;
        this.columnImportSpecs = ImmutableList.copyOf(columnImportSpecs);
    }

    public void importFrom(InputSupplier<InputStreamReader> inputSupplier)
            throws IOException
    {
        checkNotNull(inputSupplier, "inputSupplier is null");
        ImmutableList.Builder<OutputStream> outputStreamBuilder = ImmutableList.builder();
        ImmutableList.Builder<TupleStreamWriter> tupleStreamWriterBuilder = ImmutableList.builder();
        for (ColumnImportSpec columnImportSpec : columnImportSpecs) {
            OutputStream outputStream =  columnImportSpec.getOutputSupplier().getOutput();
            outputStreamBuilder.add(outputStream);
            tupleStreamWriterBuilder.add(columnImportSpec.getTupleStreamSerde().createTupleStreamWriter(new OutputStreamSliceOutput(outputStream)));
        }
        List<OutputStream> outputStreams = outputStreamBuilder.build();
        List<TupleStreamWriter> tupleStreamWriters = tupleStreamWriterBuilder.build();

        try (Reader reader = inputSupplier.getInput()) {
            Iterator<UncompressedBlock> iterator = blockExtractor.extract(reader);
            while (iterator.hasNext()) {
                UncompressedBlock block = iterator.next();
                checkState(columnImportSpecs.size() == block.getTupleInfo().getFieldCount(), "spec mismatch with tuple info");
                for (int index = 0; index < columnImportSpecs.size(); index++) {
                    tupleStreamWriters.get(index).append(ColumnMappingTupleStream.map(block, index));
                }
            }
        }

        for (TupleStreamWriter tupleStreamWriter : tupleStreamWriters) {
            tupleStreamWriter.close();
        }
        for (OutputStream outputStream : outputStreams) {
            outputStream.close();
        }
    }

    /**
     * Defines the serde and target output stream for a column
     */
    public static class ColumnImportSpec
    {
        private final TupleStreamSerde tupleStreamSerde;
        private final OutputSupplier<? extends OutputStream> outputSupplier;

        public ColumnImportSpec(TupleStreamSerde tupleStreamSerde, OutputSupplier<? extends OutputStream> outputSupplier)
        {
            this.tupleStreamSerde = checkNotNull(tupleStreamSerde, "tupleStreamSerde is null");
            this.outputSupplier = checkNotNull(outputSupplier, "outputSupplier is null");
        }

        public TupleStreamSerde getTupleStreamSerde()
        {
            return tupleStreamSerde;
        }

        public OutputSupplier<? extends OutputStream> getOutputSupplier()
        {
            return outputSupplier;
        }


    }
}
