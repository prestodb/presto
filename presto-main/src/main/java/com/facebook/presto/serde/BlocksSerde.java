package com.facebook.presto.serde;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;

import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;
import static com.google.common.base.Preconditions.checkNotNull;

public final class BlocksSerde
{
    private BlocksSerde()
    {
    }

    public static BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return createBlocksWriter(sliceOutput, null);
    }

    public static BlocksWriter createBlocksWriter(final SliceOutput sliceOutput, @Nullable final BlockSerde blockSerde)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new BlocksWriter() {
            private BlocksWriter blocksWriter;

            @Override
            public BlocksWriter append(Tuple tuple)
            {
                Preconditions.checkNotNull(tuple, "tuple is null");

                if (blocksWriter == null) {
                    BlockSerde serde = blockSerde;
                    if (blockSerde == null) {
                        serde = UNCOMPRESSED_BLOCK_SERDE;
                    }
                    blocksWriter = serde.createBlockWriter(sliceOutput);

                    BlockSerdeSerde.writeBlockSerde(sliceOutput, serde);
                    TupleInfoSerde.writeTupleInfo(sliceOutput, tuple.getTupleInfo());
                }

                blocksWriter.append(tuple);
                return this;
            }

            @Override
            public BlocksWriter append(Block block)
            {
                Preconditions.checkNotNull(block, "block is null");

                if (blocksWriter == null) {
                    BlockSerde serde = blockSerde;
                    if (blockSerde == null) {
                        serde = BlockSerdes.getSerdeForBlock(block);
                    }
                    blocksWriter = serde.createBlockWriter(sliceOutput);

                    BlockSerdeSerde.writeBlockSerde(sliceOutput, serde);
                    TupleInfoSerde.writeTupleInfo(sliceOutput, block.getTupleInfo());
                }

                blocksWriter.append(block);
                return this;
            }

            @Override
            public void finish()
            {
                if (blocksWriter != null) {
                    blocksWriter.finish();
                }
            }
        };
    }

    public static void writeBlocks(SliceOutput sliceOutput, Block... blocks) {
        writeBlocks(sliceOutput, null, ImmutableList.copyOf(blocks).iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, Iterable<? extends Block> blocks) {
        writeBlocks(sliceOutput, null, blocks.iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, Iterator<? extends Block> blocks) {
        writeBlocks(sliceOutput, null, blocks);
    }

    public static void writeBlocks(SliceOutput sliceOutput, BlockSerde blocksSerde, Block... blocks) {
        writeBlocks(sliceOutput, blocksSerde, ImmutableList.copyOf(blocks).iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, BlockSerde blocksSerde, Iterable<? extends Block> blocks) {
        writeBlocks(sliceOutput, blocksSerde, blocks.iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, @Nullable BlockSerde blocksSerde, Iterator<? extends Block> blocks) {
        BlocksWriter blocksWriter = createBlocksWriter(sliceOutput, blocksSerde);
        while (blocks.hasNext()) {
            blocksWriter.append(blocks.next());
        }
        blocksWriter.finish();
    }

    public static BlockIterable readBlocks(InputSupplier<SliceInput> sliceInputSupplier)
    {
        return readBlocks(sliceInputSupplier, 0);
    }

    public static BlockIterable readBlocks(final InputSupplier<SliceInput> sliceInputSupplier, final long startPosition)
    {
        Preconditions.checkNotNull(sliceInputSupplier, "sliceInputSupplier is null");
        Preconditions.checkArgument(startPosition >= 0, "startPosition is negative");

        return new BlockIterable()
        {
            @Override
            public Iterator<Block> iterator()
            {
                try {
                    return readBlocks(sliceInputSupplier.getInput(), startPosition);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Iterator<Block> readBlocks(SliceInput input, long startPosition)
    {
        return new BlocksReader(input, startPosition);
    }

    private static class BlocksReader
            extends AbstractIterator<Block>
    {
        private final SliceInput sliceInput;
        private final BlockSerde blockSerde;
        private final TupleInfo tupleInfo;
        private final long positionOffset;

        private BlocksReader(SliceInput sliceInput, long startPosition)
        {
            this.positionOffset = startPosition;
            this.sliceInput = sliceInput;

            blockSerde = BlockSerdeSerde.readBlockSerde(sliceInput);
            tupleInfo = TupleInfoSerde.readTupleInfo(sliceInput);
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block block = blockSerde.readBlock(sliceInput, tupleInfo, positionOffset);
            return block;
        }
    }
}
