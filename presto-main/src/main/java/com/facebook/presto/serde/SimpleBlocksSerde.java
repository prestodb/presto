package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;

import static com.facebook.presto.serde.TupleInfoSerde.readTupleInfo;
import static com.facebook.presto.serde.TupleInfoSerde.writeTupleInfo;
import static com.google.common.base.Preconditions.checkNotNull;

public class SimpleBlocksSerde implements BlocksSerde
{
    private final BlockSerde blockSerde;

    public SimpleBlocksSerde(BlockSerde blockSerde)
    {
        checkNotNull(blockSerde, "blockSerde is null");
        this.blockSerde = blockSerde;
    }

    public void writeBlocks(SliceOutput sliceOutput, Block... blocks)
    {
        writeBlocks(sliceOutput, ImmutableList.copyOf(blocks));
    }

    public void writeBlocks(SliceOutput sliceOutput, Iterable<Block> blocks)
    {
        writeBlocks(sliceOutput, blocks.iterator());
    }

    public void writeBlocks(SliceOutput sliceOutput, Iterator<Block> blocks)
    {
        BlocksWriter blocksWriter = createBlocksWriter(sliceOutput);
        while (blocks.hasNext()) {
            Block block = blocks.next();
            blocksWriter.append(BlockUtils.toTupleIterable(block));
        }
        blocksWriter.finish();
    }

    @Override
    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        Preconditions.checkNotNull(sliceOutput, "sliceOutput is null");
        return new SimpleBlocksWriter(sliceOutput, blockSerde);
    }

    private static class SimpleBlocksWriter implements BlocksWriter
    {
        private final BlockSerde blockSerde;
        private final SliceOutput sliceOutput;
        private BlocksWriter blocksWriter;

        public SimpleBlocksWriter(SliceOutput sliceOutput, BlockSerde blockSerde)
        {
            checkNotNull(sliceOutput, "sliceOutput is null");
            checkNotNull(blockSerde, "blockSerde is null");
            this.sliceOutput = sliceOutput;
            this.blockSerde = blockSerde;
        }

        @Override
        public BlocksWriter append(Iterable<Tuple> tuples)
        {
            checkNotNull(tuples, "tuples is null");

            if (Iterables.isEmpty(tuples)) {
                return this;
            }

            if (blocksWriter == null) {
                blocksWriter = blockSerde.createBlocksWriter(sliceOutput);
                Tuple firstTuple = Iterables.get(tuples, 0);
                writeTupleInfo(sliceOutput, firstTuple.getTupleInfo());
            }

            blocksWriter.append(tuples);
            return this;
        }

        @Override
        public void finish()
        {
            if (blocksWriter != null) {
                blocksWriter.finish();
            }
        }
    }

    @Override
    public BlockIterable createBlocksReader(Slice slice, final long positionOffset)
    {
        Preconditions.checkNotNull(slice, "slice is null");
        Preconditions.checkArgument(positionOffset >= 0, "positionOffset is negative");

        SliceInput sliceInput = slice.getInput();
        TupleInfo tupleInfo = readTupleInfo(sliceInput);
        Slice blocksSlice = slice.slice(sliceInput.position(), slice.length() - sliceInput.position());

        return new SimpleBlockIterable(blockSerde, blocksSlice, tupleInfo, positionOffset);
    }

    private static class SimpleBlockIterable implements BlockIterable
    {
        private final BlockSerde blockSerde;
        private final Slice blocksSlice;
        private final TupleInfo tupleInfo;
        private final long positionOffset;

        public SimpleBlockIterable(BlockSerde blockSerde, Slice blocksSlice, TupleInfo tupleInfo, long positionOffset)
        {
            this.blockSerde = blockSerde;
            this.blocksSlice = blocksSlice;
            this.tupleInfo = tupleInfo;
            this.positionOffset = positionOffset;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return new SimpleBlockIterator(blockSerde, blocksSlice.getInput(), tupleInfo, positionOffset);
        }

    }
    private static class SimpleBlockIterator
            extends AbstractIterator<Block>
    {
        private final BlockSerde blockSerde;
        private final SliceInput sliceInput;
        private final TupleInfo tupleInfo;
        private final long positionOffset;

        private SimpleBlockIterator(BlockSerde blockSerde, SliceInput sliceInput, TupleInfo tupleInfo, long positionOffset)
        {
            this.blockSerde = blockSerde;
            this.sliceInput = sliceInput;
            this.tupleInfo = tupleInfo;
            this.positionOffset = positionOffset;
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
