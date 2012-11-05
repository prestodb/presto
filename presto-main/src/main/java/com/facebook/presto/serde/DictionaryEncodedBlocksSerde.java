package com.facebook.presto.serde;

import com.facebook.presto.SizeOf;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.dictionary.Dictionary;
import com.facebook.presto.block.dictionary.Dictionary.DictionaryBuilder;
import com.facebook.presto.block.dictionary.DictionaryEncodedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncodedBlocksSerde
    implements BlocksSerde
{
    private final BlocksSerde idSerde;

    public DictionaryEncodedBlocksSerde(BlocksSerde idSerde)
    {
        this.idSerde = checkNotNull(idSerde, "idSerde is null");
    }

    public BlocksSerde getIdSerde()
    {
        return idSerde;
    }

    @Override
    public DictionaryEncodedBlockIterable createBlocksReader(Slice slice, long positionOffset)
    {
        // Get dictionary byte length from tail and reset to beginning
        int dictionaryLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);

        // Slice out dictionary data and extract it
        Slice dictionarySlice = slice.slice(slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT, dictionaryLength);
        final Dictionary dictionary = DictionarySerde.readDictionary(dictionarySlice);

        Slice idSlice = slice.slice(0, slice.length() - dictionaryLength - SizeOf.SIZE_OF_INT);
        final BlockIterable idBlocks = idSerde.createBlocksReader(idSlice, positionOffset);
        return new DictionaryEncodedBlockIterable(dictionary, idBlocks);
    }

    public static class DictionaryEncodedBlockIterable implements BlockIterable
    {
        private final Dictionary dictionary;
        private final BlockIterable idBlocks;

        public DictionaryEncodedBlockIterable(Dictionary dictionary, BlockIterable idBlocks)
        {
            this.dictionary = dictionary;
            this.idBlocks = idBlocks;
        }

        public Dictionary getDictionary()
        {
            return dictionary;
        }

        @Override
        public Iterator<Block> iterator()
        {
            return new DictionaryBlockIterator(dictionary, idBlocks.iterator());
        }
    }

    private static class DictionaryBlockIterator extends AbstractIterator<Block>
    {
        private final Iterator<Block> idBlocks;
        private final Dictionary dictionary;

        public DictionaryBlockIterator(Dictionary dictionary, Iterator<Block> idBlocks)
        {
            Preconditions.checkNotNull(idBlocks, "idBlocks is null");
            Preconditions.checkNotNull(dictionary, "dictionary is null");

            this.dictionary = dictionary;
            this.idBlocks = idBlocks;
        }

        @Override
        protected Block computeNext()
        {
            if (!idBlocks.hasNext()) {
                return endOfData();
            }
            Block idBlock = idBlocks.next();
            return new DictionaryEncodedBlock(dictionary, idBlock);
        }
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
        DictionaryEncodedBlocksWriter blocksWriter = createBlocksWriter(sliceOutput);
        while (blocks.hasNext()) {
            Block block = blocks.next();
            blocksWriter.append(block);
        }
        blocksWriter.finish();
    }

    public DictionaryEncodedBlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return new DictionaryEncodedBlocksWriter(idSerde.createBlocksWriter(sliceOutput), sliceOutput);
    }

    public static class DictionaryEncodedBlocksWriter implements BlocksWriter
    {
        private final BlocksWriter idWriter;
        private final SliceOutput sliceOutput;
        private TupleInfo tupleInfo;
        private DictionaryBuilder dictionaryBuilder;
        private boolean finished;

        public DictionaryEncodedBlocksWriter(BlocksWriter idWriter, SliceOutput sliceOutput)
        {
            this.idWriter = checkNotNull(idWriter, "idWriter is null");
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public BlocksWriter append(Tuple tuple)
        {
            checkNotNull(tuple, "tuple is null");
            checkState(!finished, "already finished");

            if (tupleInfo == null) {
                tupleInfo = tuple.getTupleInfo();
                dictionaryBuilder = new DictionaryBuilder(tupleInfo);
            }

            long id = dictionaryBuilder.getId(tuple);
            idWriter.append(createTuple(id));

            return this;
        }

        @Override
        public DictionaryEncodedBlocksWriter append(final Block block)
        {
            checkNotNull(block, "block is null");
            checkState(!finished, "already finished");

            if (tupleInfo == null) {
                tupleInfo = block.getTupleInfo();
                dictionaryBuilder = new DictionaryBuilder(tupleInfo);
            }

            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                long id = dictionaryBuilder.getId(cursor.getTuple());
                idWriter.append(createTuple(id));
            }

            return this;
        }

        @Override
        public void finish()
        {
            checkState(tupleInfo != null, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            idWriter.finish();

            // Serialize dictionary
            int dictionaryBytes = DictionarySerde.writeDictionary(sliceOutput, dictionaryBuilder.build());

            // Write length of dictionary
            sliceOutput.writeInt(dictionaryBytes);
        }
    }
}
