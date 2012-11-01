/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.ingest;

import com.facebook.presto.serde.BlockSerde;
import com.facebook.presto.serde.BlocksSerde;
import com.facebook.presto.serde.BlocksWriter;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.OutputSupplier;

import java.io.IOException;
import java.io.OutputStream;

public class SerdeBlockWriterFactory implements BlockWriterFactory
{
    private final BlockSerde blockSerde;
    private final OutputSupplier<? extends OutputStream> outputSupplier;

    public SerdeBlockWriterFactory(BlockSerde blockSerde, OutputSupplier<? extends OutputStream> outputSupplier)
    {
        Preconditions.checkNotNull(blockSerde, "blockSerde is null");
        Preconditions.checkNotNull(outputSupplier, "outputSupplier is null");

        this.blockSerde = blockSerde;
        this.outputSupplier = outputSupplier;
    }

    public BlocksWriter create()
    {
        try {
            OutputStream outputStream = outputSupplier.getOutput();
            SliceOutput sliceOutput;
            if (outputStream instanceof SliceOutput) {
                sliceOutput = (SliceOutput) outputStream;
            } else {
                sliceOutput = new OutputStreamSliceOutput(outputStream);
            }
            return BlocksSerde.createBlocksWriter(sliceOutput, blockSerde);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
