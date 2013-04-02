/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.OutputSupplier;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.facebook.presto.block.BlockUtils.toTupleIterable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BlocksFileWriter implements Closeable
{
    public static void writeBlocks(BlocksFileEncoding encoding, OutputSupplier<? extends OutputStream> sliceOutput, Block... blocks)
    {
        writeBlocks(encoding, sliceOutput, ImmutableList.copyOf(blocks));
    }

    public static void writeBlocks(BlocksFileEncoding encoding, OutputSupplier<? extends OutputStream> sliceOutput, Iterable<? extends Block> blocks)
    {
        writeBlocks(encoding, sliceOutput, blocks.iterator());
    }

    public static void writeBlocks(BlocksFileEncoding encoding, OutputSupplier<? extends OutputStream> sliceOutput, Iterator<? extends Block> blocks)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        BlocksFileWriter fileWriter = new BlocksFileWriter(encoding, sliceOutput);
        while (blocks.hasNext()) {
            fileWriter.append(toTupleIterable(blocks.next()));
        }
        fileWriter.close();
    }

    private final BlocksFileEncoding encoding;
    private final OutputSupplier<? extends OutputStream> outputSupplier;
    private final StatsBuilder statsBuilder = new StatsBuilder();
    private Encoder encoder;
    private SliceOutput sliceOutput;
    private boolean closed;

    public BlocksFileWriter(BlocksFileEncoding encoding, OutputSupplier<? extends OutputStream> outputSupplier)
    {
        checkNotNull(encoding, "encoding is null");
        checkNotNull(outputSupplier, "outputSupplier is null");

        this.encoding = encoding;
        this.outputSupplier = outputSupplier;
    }

    public BlocksFileWriter append(Iterable<Tuple> tuples)
    {
        Preconditions.checkNotNull(tuples, "tuples is null");
        if (!Iterables.isEmpty(tuples)) {
            if (encoder == null) {
                open();
            }
            statsBuilder.process(tuples);
            encoder.append(tuples);
        }
        return this;
    }

    private void open()
    {
        try {
            OutputStream outputStream = outputSupplier.getOutput();
            if (outputStream instanceof SliceOutput) {
                sliceOutput = (SliceOutput) outputStream;
            } else {
                sliceOutput = new OutputStreamSliceOutput(outputStream);
            }
            encoder = encoding.createBlocksWriter(sliceOutput);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void close()
    {
        if (!closed && encoder != null) {
            BlockEncoding blockEncoding = encoder.finish();

            int startingIndex = sliceOutput.size();

            // write file encoding
            BlockEncodings.writeBlockEncoding(sliceOutput, blockEncoding);

            // write stats
            BlocksFileStats.serialize(statsBuilder.build(), sliceOutput);

            // write footer size
            int footerSize = sliceOutput.size() - startingIndex;
            checkState(footerSize > 0);
            sliceOutput.writeInt(footerSize);

            try {
                sliceOutput.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            closed = true;
        }
    }

    private static class StatsBuilder
    {
        private static final int MAX_UNIQUE_COUNT = 1000;

        private long rowCount;
        private long runsCount;
        private Tuple lastTuple;
        private final Set<Tuple> set = new HashSet<>(MAX_UNIQUE_COUNT);

        public void process(Iterable<Tuple> tuples)
        {
            Preconditions.checkNotNull(tuples, "tuples is null");

            for (Tuple tuple : tuples) {
                if (lastTuple == null) {
                    lastTuple = tuple;
                    if (set.size() < MAX_UNIQUE_COUNT) {
                        set.add(lastTuple);
                    }
                }
                else if (!tuple.equals(lastTuple)) {
                    runsCount++;
                    lastTuple = tuple;
                    if (set.size() < MAX_UNIQUE_COUNT) {
                        set.add(lastTuple);
                    }
                }
                rowCount++;
            }
        }

        public BlocksFileStats build()
        {
            // TODO: expose a way to indicate whether the unique count is EXACT or APPROXIMATE
            return new BlocksFileStats(rowCount, runsCount + 1, rowCount / (runsCount + 1), (set.size() == MAX_UNIQUE_COUNT) ? Integer.MAX_VALUE : set.size());
        }
    }
}
