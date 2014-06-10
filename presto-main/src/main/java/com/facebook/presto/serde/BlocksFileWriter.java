/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.serde;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.base.Throwables;
import com.google.common.io.OutputSupplier;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BlocksFileWriter
        implements Closeable
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final BlocksFileEncoding encoding;
    private final OutputSupplier<? extends OutputStream> outputSupplier;
    private final StatsBuilder statsBuilder = new StatsBuilder();
    private Encoder encoder;
    private SliceOutput sliceOutput;
    private boolean closed;

    public BlocksFileWriter(BlockEncodingSerde blockEncodingSerde, BlocksFileEncoding encoding, OutputSupplier<? extends OutputStream> outputSupplier)
    {
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingManager is null");
        this.encoding = checkNotNull(encoding, "encoding is null");
        this.outputSupplier = checkNotNull(outputSupplier, "outputSupplier is null");
    }

    public BlocksFileWriter append(Block block)
    {
        checkNotNull(block, "block is null");
        if (encoder == null) {
            open();
        }
        statsBuilder.process(block);
        encoder.append(block);
        return this;
    }

    private void open()
    {
        try {
            OutputStream outputStream = outputSupplier.getOutput();
            if (outputStream instanceof SliceOutput) {
                sliceOutput = (SliceOutput) outputStream;
            }
            else {
                sliceOutput = new OutputStreamSliceOutput(outputStream);
            }
            encoder = encoding.createBlocksWriter(sliceOutput);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        if (encoder == null) {
            // No rows were written, so create an empty file. We need to keep
            // the empty files in order to tell the difference between a
            // missing file and a file that legitimately has no rows.
            createEmptyFile();
            return;
        }

        BlockEncoding blockEncoding = encoder.finish();

        int startingIndex = sliceOutput.size();

        // write file encoding
        blockEncodingSerde.writeBlockEncoding(sliceOutput, blockEncoding);

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
    }

    private void createEmptyFile()
    {
        try {
            outputSupplier.getOutput().close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class StatsBuilder
    {
        private static final int MAX_UNIQUE_COUNT = 1000;

        private long rowCount;
        private long runsCount;
        private Block lastValue;
        private DictionaryBuilder dictionaryBuilder;

        public void process(Block block)
        {
            checkNotNull(block, "block is null");

            if (dictionaryBuilder == null) {
                dictionaryBuilder = new DictionaryBuilder(block.getType());
            }

            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                // update run length stats
                Block value = cursor.getSingleValueBlock();
                if (lastValue == null) {
                    lastValue = value;
                }
                else if (!value.equalTo(0, lastValue, 0)) {
                    runsCount++;
                    lastValue = value;
                }

                // update dictionary stats
                if (dictionaryBuilder.size() < MAX_UNIQUE_COUNT) {
                    dictionaryBuilder.putIfAbsent(cursor);
                }

                rowCount++;
            }
        }

        public BlocksFileStats build()
        {
            // TODO: expose a way to indicate whether the unique count is EXACT or APPROXIMATE
            return new BlocksFileStats(
                    rowCount,
                    runsCount + 1,
                    rowCount / (runsCount + 1),
                    (dictionaryBuilder.size() >= MAX_UNIQUE_COUNT) ? Integer.MAX_VALUE : dictionaryBuilder.size());
        }
    }
}
